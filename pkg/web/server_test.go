// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package web

import (
	"context"
	"errors"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/run"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
	"github.com/gke-labs/auto-optimizer/pkg/web/auth"
)

type recordingSandbox struct {
	mu    sync.Mutex
	calls []tools.ExecCommandOptions
}

func (s *recordingSandbox) ExecCommand(_ context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, opts)

	return &tools.ExecCommandResult{
		Stdout:   "Linux fake-test-node 6.6.0 #1 SMP\n",
		Stderr:   "",
		ExitCode: 0,
	}, nil
}

type blockingSandbox struct {
	startedCh chan struct{}
	blockCh   chan struct{}
}

func (s *blockingSandbox) ExecCommand(_ context.Context, _ tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	select {
	case s.startedCh <- struct{}{}:
	default:
	}
	<-s.blockCh
	return &tools.ExecCommandResult{
		Stdout:   "Linux unblocked\n",
		ExitCode: 0,
	}, nil
}

func setupTestWebServer(t *testing.T, sb tools.Sandbox) (*httptest.Server, *sessionapi.Server) {
	t.Helper()

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("llm.NewFromEnv: %v", err)
	}

	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get: %v", err)
	}

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = session.DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts session.RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()

	harness := session.NewHarness(llmClient, nil, toolsRegistry, opts)
	if sb != nil {
		harness.SetSandboxOverride(sb)
	}

	store := session.NewFileStore(t.TempDir())

	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          store,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("sessionapi.NewServer: %v", err)
	}

	webServer, err := NewServer(ServerOptions{Service: srv})
	if err != nil {
		t.Fatalf("web.NewServer: %v", err)
	}

	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	return ts, srv
}

func csrfFromJar(t *testing.T, jar http.CookieJar, serverURL string) string {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	for _, c := range jar.Cookies(u) {
		if c.Name == csrfCookieName {
			return c.Value
		}
	}
	t.Fatalf("csrf cookie %q not found in jar for %s", csrfCookieName, serverURL)
	return ""
}

func TestAcceptance_SendMessageAndPoll(t *testing.T) {
	sb := &recordingSandbox{}
	ts, _ := setupTestWebServer(t, sb)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{Jar: jar}

	// 1. Visit list page to establish SameSite CSRF cookie
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp.StatusCode)
	}

	// 2. Post a message to POST /sessions/test/messages
	prompt := "What is the capital of France?"
	postResp, err := client.PostForm(ts.URL+"/sessions/test/messages", url.Values{
		"text": {prompt},
		"csrf": {csrfFromJar(t, jar, ts.URL)},
	})
	if err != nil {
		t.Fatalf("POST /sessions/test/messages: %v", err)
	}
	defer postResp.Body.Close()

	// http.Client followed the 303 redirect to /sessions/test
	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("post redirect status = %d, want 200", postResp.StatusCode)
	}

	// 3. Poll the transcript page until the refresh tag disappears
	const refreshTag = `<meta http-equiv="refresh"`
	deadline := time.Now().Add(5 * time.Second)
	var finalBody string

	for time.Now().Before(deadline) {
		getResp, err := client.Get(ts.URL + "/sessions/test")
		if err != nil {
			t.Fatalf("GET /sessions/test: %v", err)
		}
		bodyBytes, err := io.ReadAll(getResp.Body)
		_ = getResp.Body.Close()
		if err != nil {
			t.Fatalf("reading body: %v", err)
		}
		finalBody = string(bodyBytes)

		if !strings.Contains(finalBody, refreshTag) {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if strings.Contains(finalBody, refreshTag) {
		t.Fatal("turn timed out: refresh tag still present on transcript page")
	}

	// 4. Assert the Eliza reply is rendered in the page
	wantReply := "What do you think it means when you say 'What is the capital of France?'?"
	if !strings.Contains(html.UnescapeString(finalBody), wantReply) {
		t.Errorf("expected reply %q in transcript page, got:\n%s", wantReply, finalBody)
	}
}

func TestSendMessage_ToolCallExecution(t *testing.T) {
	sb := &recordingSandbox{}
	ts, _ := setupTestWebServer(t, sb)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{Jar: jar}

	// Initialize cookie
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()

	// Post message triggering run_command tool call
	postResp, err := client.PostForm(ts.URL+"/sessions/toolcall/messages", url.Values{
		"text": {"run: uname -a"},
		"csrf": {csrfFromJar(t, jar, ts.URL)},
	})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = postResp.Body.Close()

	// Poll until turn finishes
	const refreshTag = `<meta http-equiv="refresh"`
	deadline := time.Now().Add(5 * time.Second)
	var finalBody string

	for time.Now().Before(deadline) {
		getResp, err := client.Get(ts.URL + "/sessions/toolcall")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		bodyBytes, err := io.ReadAll(getResp.Body)
		_ = getResp.Body.Close()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		finalBody = string(bodyBytes)
		if !strings.Contains(finalBody, refreshTag) {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if strings.Contains(finalBody, refreshTag) {
		t.Fatal("turn timed out: refresh tag still present")
	}

	if !strings.Contains(finalBody, "run_command") {
		t.Errorf("expected tool call run_command in transcript, got:\n%s", finalBody)
	}
	if !strings.Contains(finalBody, "Linux fake-test-node") {
		t.Errorf("expected tool result output in transcript, got:\n%s", finalBody)
	}
}

func TestSendMessage_BusySessionReturns409(t *testing.T) {
	sb := &blockingSandbox{
		startedCh: make(chan struct{}, 1),
		blockCh:   make(chan struct{}),
	}
	ts, _ := setupTestWebServer(t, sb)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // don't follow redirects automatically
		},
	}

	// Initialize cookie
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()

	// First message starts a blocking turn
	csrf := csrfFromJar(t, jar, ts.URL)
	postResp1, err := client.PostForm(ts.URL+"/sessions/busysess/messages", url.Values{
		"text": {"run: sleep 10"},
		"csrf": {csrf},
	})
	if err != nil {
		t.Fatalf("first POST: %v", err)
	}
	_ = postResp1.Body.Close()
	if postResp1.StatusCode != http.StatusSeeOther {
		t.Fatalf("first POST status = %d, want 303 See Other", postResp1.StatusCode)
	}

	// Wait until sandbox starts execution
	select {
	case <-sb.startedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn to start")
	}

	// Second POST to the busy session must return 409 Conflict
	postResp2, err := client.PostForm(ts.URL+"/sessions/busysess/messages", url.Values{
		"text": {"second message"},
		"csrf": {csrf},
	})
	if err != nil {
		t.Fatalf("second POST: %v", err)
	}
	bodyBytes, _ := io.ReadAll(postResp2.Body)
	_ = postResp2.Body.Close()

	if postResp2.StatusCode != http.StatusConflict {
		t.Fatalf("second POST status = %d, want 409 Conflict. Body: %s", postResp2.StatusCode, string(bodyBytes))
	}
	if !strings.Contains(string(bodyBytes), "409 Conflict") {
		t.Errorf("expected 409 Conflict page, got: %s", string(bodyBytes))
	}

	// Unblock and wait for in-flight turn to finish before returning
	close(sb.blockCh)

	const refreshTag = `<meta http-equiv="refresh"`
	deadline := time.Now().Add(5 * time.Second)
	var finalBody string
	for time.Now().Before(deadline) {
		getResp, err := client.Get(ts.URL + "/sessions/busysess")
		if err != nil {
			t.Fatalf("GET /sessions/busysess: %v", err)
		}
		bodyBytes, err := io.ReadAll(getResp.Body)
		_ = getResp.Body.Close()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		finalBody = string(bodyBytes)
		if !strings.Contains(finalBody, refreshTag) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if strings.Contains(finalBody, refreshTag) {
		t.Fatal("turn timed out: refresh tag still present on transcript page")
	}
}

func TestCSRFProtection_RejectsCrossSitePost(t *testing.T) {
	ts, _ := setupTestWebServer(t, nil)

	// Client without cookiejar (no cookies sent)
	client := &http.Client{}

	resp, err := client.PostForm(ts.URL+"/sessions/nocookie/messages", url.Values{
		"text": {"hello"},
	})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST without cookie status = %d, want 403 Forbidden", resp.StatusCode)
	}

	// Create session without cookie must also fail
	resp, err = client.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"newsess"},
		"toolset": {"basic"},
	})
	if err != nil {
		t.Fatalf("POST /sessions: %v", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /sessions without cookie status = %d, want 403 Forbidden", resp.StatusCode)
	}

	// Client with cookiejar (cookie present)
	jar, _ := cookiejar.New(nil)
	jarClient := &http.Client{Jar: jar}
	getResp, err := jarClient.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = getResp.Body.Close()
	csrf := csrfFromJar(t, jar, ts.URL)

	// POST with cookie but without csrf field -> 403
	resp, err = jarClient.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"newsess"},
		"toolset": {"basic"},
	})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST without csrf field status = %d, want 403 Forbidden", resp.StatusCode)
	}

	// POST with cookie and wrong csrf field -> 403
	resp, err = jarClient.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"newsess"},
		"toolset": {"basic"},
		"csrf":    {"wrong-token"},
	})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST with wrong csrf field status = %d, want 403 Forbidden", resp.StatusCode)
	}

	// POST with cookie and correct csrf field -> 303
	jarClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err = jarClient.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"newsess"},
		"toolset": {"basic"},
		"csrf":    {csrf},
	})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST with right csrf field status = %d, want 303 See Other", resp.StatusCode)
	}
}

func TestCreateSessionForm(t *testing.T) {
	ts, _ := setupTestWebServer(t, nil)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Get cookie
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	_ = resp.Body.Close()

	// Post new session form
	postResp, err := client.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"createdsess"},
		"toolset": {"basic"},
		"csrf":    {csrfFromJar(t, jar, ts.URL)},
	})
	if err != nil {
		t.Fatalf("POST /sessions: %v", err)
	}
	_ = postResp.Body.Close()

	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303 See Other", postResp.StatusCode)
	}
	if loc := postResp.Header.Get("Location"); loc != "/sessions/createdsess" {
		t.Fatalf("Location = %q, want /sessions/createdsess", loc)
	}

	// Verify transcript page is accessible
	getResp, err := client.Get(ts.URL + "/sessions/createdsess")
	if err != nil {
		t.Fatalf("GET /sessions/createdsess: %v", err)
	}
	body, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResp.StatusCode)
	}
	if !strings.Contains(string(body), "Session: createdsess") {
		t.Errorf("expected session name in body, got: %s", string(body))
	}
}

func TestHealthzAndStatic(t *testing.T) {
	ts, _ := setupTestWebServer(t, nil)
	client := &http.Client{}

	// GET /healthz
	resp, err := client.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok\n" {
		t.Errorf("healthz body = %q, want ok\\n", string(body))
	}

	// GET /static/style.css
	resp, err = client.Get(ts.URL + "/static/style.css")
	if err != nil {
		t.Fatalf("GET /static/style.css: %v", err)
	}
	cssBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("static status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(cssBody), "--indigo") {
		t.Errorf("expected CSS content, got: %s", string(cssBody))
	}
}

func TestTruncateRunes(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		maxRunes      int
		wantPrefix    string
		wantTruncated bool
	}{
		{
			name:          "empty",
			input:         "",
			maxRunes:      10,
			wantPrefix:    "",
			wantTruncated: false,
		},
		{
			name:          "short ascii",
			input:         "hello",
			maxRunes:      10,
			wantPrefix:    "hello",
			wantTruncated: false,
		},
		{
			name:          "exact ascii",
			input:         "hello",
			maxRunes:      5,
			wantPrefix:    "hello",
			wantTruncated: false,
		},
		{
			name:          "truncated ascii",
			input:         "hello world",
			maxRunes:      5,
			wantPrefix:    "hello",
			wantTruncated: true,
		},
		{
			name:          "multibyte utf8 within limit",
			input:         "你好世界",
			maxRunes:      4,
			wantPrefix:    "你好世界",
			wantTruncated: false,
		},
		{
			name:          "multibyte utf8 truncated",
			input:         "你好世界",
			maxRunes:      2,
			wantPrefix:    "你好",
			wantTruncated: true,
		},
		{
			name:          "emoji boundary not split",
			input:         "abc🚀def",
			maxRunes:      4,
			wantPrefix:    "abc🚀",
			wantTruncated: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPrefix, gotTruncated := truncateRunes(tc.input, tc.maxRunes)
			if gotTruncated != tc.wantTruncated {
				t.Errorf("truncateRunes(%q, %d) truncated = %v, want %v", tc.input, tc.maxRunes, gotTruncated, tc.wantTruncated)
			}
			if gotPrefix != tc.wantPrefix {
				t.Errorf("truncateRunes(%q, %d) prefix = %q, want %q", tc.input, tc.maxRunes, gotPrefix, tc.wantPrefix)
			}
			if !utf8.ValidString(gotPrefix) {
				t.Errorf("truncateRunes(%q, %d) returned invalid UTF-8: %q", tc.input, tc.maxRunes, gotPrefix)
			}
		})
	}
}

func TestTranscriptTruncation_UTF8(t *testing.T) {
	// 600 multibyte characters (each 3 bytes in UTF-8: 1800 bytes total).
	// Byte slicing at 500 would split the 167th rune; rune-aware truncation must keep 500 complete runes.
	const totalRunes = 600
	var sb strings.Builder
	for i := 0; i < totalRunes; i++ {
		sb.WriteRune('世')
	}
	longContent := sb.String()

	fakeSvc := &fakeSessionService{
		details: map[string]*sessionapi.SessionDetail{
			"utf8-session": {
				Summary: sessionapi.SessionSummary{
					Name:         "utf8-session",
					Toolset:      "basic",
					MessageCount: 1,
					CreatedAt:    time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC),
				},
				Messages: []sessionapi.Message{
					{
						Role:    "assistant",
						Content: longContent,
					},
				},
			},
		},
	}

	webServer, err := NewServer(ServerOptions{Service: fakeSvc})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	client := &http.Client{}

	// 1. Default request should be truncated at maxTruncateLen (500 runes)
	resp, err := client.Get(ts.URL + "/sessions/utf8-session")
	if err != nil {
		t.Fatalf("GET /sessions/utf8-session: %v", err)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	body := string(bodyBytes)

	if !utf8.Valid(bodyBytes) {
		t.Fatal("response body is not valid UTF-8")
	}

	wantPrefix := strings.Repeat("世", maxTruncateLen) + "..."
	if !strings.Contains(body, wantPrefix) {
		t.Errorf("expected truncated prefix %q in body", wantPrefix)
	}
	if !strings.Contains(body, `href="/sessions/utf8-session?full=1"`) {
		t.Errorf("expected 'show all' link with full=1 in body")
	}

	// 2. Request with ?full=1 should show all 600 runes
	fullResp, err := client.Get(ts.URL + "/sessions/utf8-session?full=1")
	if err != nil {
		t.Fatalf("GET /sessions/utf8-session?full=1: %v", err)
	}
	fullBodyBytes, err := io.ReadAll(fullResp.Body)
	_ = fullResp.Body.Close()
	if err != nil {
		t.Fatalf("reading full body: %v", err)
	}
	fullBody := string(fullBodyBytes)

	if !utf8.Valid(fullBodyBytes) {
		t.Fatal("full response body is not valid UTF-8")
	}
	if !strings.Contains(fullBody, longContent) {
		t.Errorf("expected full content (%d runes) in body", totalRunes)
	}
	if !strings.Contains(fullBody, `href="/sessions/utf8-session"`) {
		t.Errorf("expected 'show less' link in full body")
	}
}

func TestServer_ReposAndRuns(t *testing.T) {
	ctx := t.Context()
	stateDir := t.TempDir()

	sessStore := session.NewFileStore(stateDir)
	stateStore := state.NewFileStore(stateDir)

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("llm.NewFromEnv: %v", err)
	}
	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = session.DefaultToolTimeout
	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get: %v", err)
	}
	toolset.RegisterTools(toolsRegistry)

	var opts session.RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()
	harness := session.NewHarness(llmClient, nil, toolsRegistry, opts)

	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          sessStore,
		StateStore:     stateStore,
		DefaultToolset: "basic",
		Harness:        harness,
	})
	if err != nil {
		t.Fatalf("sessionapi.NewServer: %v", err)
	}

	webServer, err := NewServer(ServerOptions{
		Service:    srv,
		StateStore: stateStore,
	})
	if err != nil {
		t.Fatalf("web.NewServer: %v", err)
	}

	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	client := ts.Client()

	// 1. Populate state with a repository, a goal, a run, events, and a session
	repoKey := state.RepoKey{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "auto-optimizer",
	}
	syncTime := time.Date(2026, 10, 29, 14, 0, 0, 0, time.UTC)
	repo := &state.Repo{
		URL:            "https://github.com/gke-labs/auto-optimizer",
		Host:           repoKey.Host,
		Owner:          repoKey.Owner,
		Name:           repoKey.Name,
		DefaultBranch:  "main",
		HeadCommit:     "commit-head-123456",
		ForgeKind:      "github",
		RegisteredTime: time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC),
		LastChangeTime: syncTime,
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261029T140000Z-0001"
	sessionID := "sessrun1"

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "latency-reduction",
			Schedule: "on-change",
			Body:     "# Latency Reduction\nReduce p99 latency by 20%.\n",
		},
		Hash:       "goal-hash-9999",
		BaseCommit: "commit-head-123456",
		LastRunID:  runID,
	}
	if err := stateStore.PutGoal(ctx, repoKey, goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	run := &state.Run{
		ID:         runID,
		Repo:       repoKey,
		GoalName:   "latency-reduction",
		Phase:      state.PhaseWorking,
		BaseCommit: "commit-head-123456",
		Branch:     "ao/latency-reduction/run-1",
		PRURL:      "https://github.com/gke-labs/auto-optimizer/pull/99",
		SessionID:  sessionID,
		Goal:       goalRec.Goal,
	}
	if err := stateStore.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	if err := stateStore.AppendEvent(ctx, repoKey, runID,
		state.Event{
			Time:    syncTime,
			Phase:   state.PhasePreparing,
			Kind:    "clone",
			Message: "Cloning repository",
		},
		state.Event{
			Time:    syncTime.Add(30 * time.Second),
			Phase:   state.PhaseWorking,
			Kind:    "session_start",
			Message: "Session initiated",
		},
	); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	// Create session linked to the run
	if err := srv.CreateNewSession(ctx, sessionID, "basic"); err != nil {
		t.Fatalf("CreateNewSession: %v", err)
	}

	// 2. Test GET /repos
	resp, err := client.Get(ts.URL + "/repos")
	if err != nil {
		t.Fatalf("GET /repos: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repos status = %d, want 200", resp.StatusCode)
	}
	bodyStr := string(body)
	for _, want := range []string{
		"github.com/gke-labs/auto-optimizer",
		"main",
		"github",
		"2026-10-29 14:00:00 UTC",
	} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("GET /repos missing expected substring %q", want)
		}
	}

	// 3. Test GET /repos/github.com/gke-labs/auto-optimizer
	resp, err = client.Get(ts.URL + "/repos/github.com/gke-labs/auto-optimizer")
	if err != nil {
		t.Fatalf("GET /repos/...: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /repos/... status = %d, want 200", resp.StatusCode)
	}
	bodyStr = string(body)
	for _, want := range []string{
		"latency-reduction",
		"Working",
		runID,
		"commit-head-123456",
		"ao/latency-reduction/run-1",
		"https://github.com/gke-labs/auto-optimizer/pull/99",
	} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("GET /repos/... missing expected substring %q", want)
		}
	}

	// 4. Test GET /repos/missing/repo/here returns 404
	resp, err = client.Get(ts.URL + "/repos/missing/repo/here")
	if err != nil {
		t.Fatalf("GET /repos/missing/repo/here: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for missing repo, got %d", resp.StatusCode)
	}

	// 5. Test GET /runs/<runID>
	resp, err = client.Get(ts.URL + "/runs/" + runID)
	if err != nil {
		t.Fatalf("GET /runs/<runID>: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /runs/... status = %d, want 200", resp.StatusCode)
	}
	bodyStr = string(body)
	for _, want := range []string{
		"Run: " + runID,
		"Working",
		"Cloning repository",
		"Session initiated",
		"ao/latency-reduction/run-1",
		"https://github.com/gke-labs/auto-optimizer/pull/99",
		"/sessions/" + sessionID,
		"Reduce p99 latency by 20%.",
		"<meta http-equiv=\"refresh\" content=\"2\">",
	} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("GET /runs/... missing expected substring %q", want)
		}
	}

	// 6. Test GET /runs/missing-id returns 404
	resp, err = client.Get(ts.URL + "/runs/20261029T000000Z-9999")
	if err != nil {
		t.Fatalf("GET /runs/missing: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for missing run, got %d", resp.StatusCode)
	}

	// 7. Test GET / (session list page shows run next to session)
	resp, err = client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", resp.StatusCode)
	}
	bodyStr = string(body)
	if !strings.Contains(bodyStr, sessionID) {
		t.Errorf("expected session list to contain session ID %q", sessionID)
	}
	if !strings.Contains(bodyStr, `/runs/`+runID) {
		t.Errorf("expected session list to link to run /runs/%s, got:\n%s", runID, bodyStr)
	}
	if !strings.Contains(bodyStr, `href="/repos"`) {
		t.Errorf("expected session list to link to /repos, got:\n%s", bodyStr)
	}
}

func TestRunAgain(t *testing.T) {
	ctx := t.Context()
	stateStore := state.NewFileStore(t.TempDir())

	scheduler, err := run.NewScheduler(run.SchedulerOptions{
		Store: stateStore,
	})
	if err != nil {
		t.Fatalf("run.NewScheduler: %v", err)
	}

	webServer, err := NewServer(ServerOptions{
		Service:    &fakeSessionService{},
		StateStore: stateStore,
		Starter:    scheduler,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ts := httptest.NewServer(webServer)
	t.Cleanup(ts.Close)

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/auto-optimizer",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "auto-optimizer",
		DefaultBranch: "main",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name: "test-goal",
			Body: "Test goal description",
		},
		BaseCommit: "commit-base-123",
	}
	if err := stateStore.PutGoal(ctx, repoKey, goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	failedRunID, err := state.NewRunIDAt(now)
	if err != nil {
		t.Fatalf("NewRunIDAt: %v", err)
	}

	failedRun := &state.Run{
		ID:         failedRunID,
		Repo:       repoKey,
		GoalName:   "test-goal",
		Phase:      state.PhaseFailed,
		BaseCommit: "commit-base-123",
		Error:      "verification failed",
		Goal:       goalRec.Goal,
	}
	if err := stateStore.PutRun(ctx, failedRun); err != nil {
		t.Fatalf("PutRun failed: %v", err)
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	post := func(c *http.Client, path string, extraValues ...url.Values) (*http.Response, string) {
		t.Helper()
		form := url.Values{}
		if len(extraValues) > 0 {
			form = extraValues[0]
		}
		resp, err := c.PostForm(ts.URL+path, form)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return resp, string(b)
	}

	// 1. Refuses without a valid CSRF token
	if resp, _ := post(&http.Client{}, "/runs/"+failedRunID+"/rerun"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 Forbidden", resp.StatusCode)
	}

	// 2. Establish CSRF cookie via GET /runs/<runID> and check "Run again" button
	getResp, err := client.Get(ts.URL + "/runs/" + failedRunID)
	if err != nil {
		t.Fatalf("GET /runs: %v", err)
	}
	getBody, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /runs status = %d, want 200", getResp.StatusCode)
	}
	if !strings.Contains(string(getBody), `action="/runs/`+failedRunID+`/rerun"`) {
		t.Errorf("expected finished run page to point form to /rerun, got:\n%s", string(getBody))
	}
	if !strings.Contains(string(getBody), "Run again") {
		t.Errorf("expected finished run page to contain 'Run again' button, got:\n%s", string(getBody))
	}

	csrf := csrfFromJar(t, jar, ts.URL)
	postWithCSRF := func(c *http.Client, path string) (*http.Response, string) {
		return post(c, path, url.Values{"csrf": {csrf}})
	}

	// 2b. Refuses when cookie is present but csrf field is missing or wrong
	if resp, _ := post(client, "/runs/"+failedRunID+"/rerun"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST without csrf field status = %d, want 403 Forbidden", resp.StatusCode)
	}
	if resp, _ := post(client, "/runs/"+failedRunID+"/rerun", url.Values{"csrf": {"invalid-token"}}); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST with wrong csrf field status = %d, want 403 Forbidden", resp.StatusCode)
	}

	// 3. POST to the run's own URL /runs/<id> is not accepted (returns 404)
	if resp, _ := postWithCSRF(client, "/runs/"+failedRunID); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST /runs/<id> status = %d, want 404 Not Found", resp.StatusCode)
	}

	// 4. Successfully starts a run for the failed goal
	postResp, _ := postWithCSRF(client, "/runs/"+failedRunID+"/rerun")
	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /runs/<id>/rerun status = %d, want 303 See Other", postResp.StatusCode)
	}
	newRunLocation := postResp.Header.Get("Location")
	if !strings.HasPrefix(newRunLocation, "/runs/") {
		t.Fatalf("Location = %q, want prefix /runs/", newRunLocation)
	}
	newRunID := strings.TrimPrefix(newRunLocation, "/runs/")

	// Verify new run in store
	newRun, err := stateStore.GetRun(ctx, repoKey, newRunID)
	if err != nil {
		t.Fatalf("GetRun newRunID: %v", err)
	}
	if newRun.Phase != state.PhasePending {
		t.Errorf("new run phase = %s, want Pending", newRun.Phase)
	}
	events, err := stateStore.ReadEvents(ctx, repoKey, newRunID)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected 1 event, got %d (err: %v)", len(events), err)
	}
	if events[0].Data.(map[string]any)["reason"] != "manual" {
		t.Errorf("event reason = %v, want manual", events[0].Data)
	}
	if events[0].Data.(map[string]any)["by"] != "anonymous" {
		t.Errorf("event by = %v, want anonymous", events[0].Data)
	}

	// 5. Refuses while run is in progress
	activeResp, activeBody := postWithCSRF(client, "/runs/"+newRunID+"/rerun")
	if activeResp.StatusCode != http.StatusConflict {
		t.Fatalf("POST active run status = %d, want 409 Conflict", activeResp.StatusCode)
	}
	if !strings.Contains(activeBody, "Cannot Start Run") || !strings.Contains(activeBody, "already has an active run") {
		t.Errorf("expected refusal error on run page, got:\n%s", activeBody)
	}

	// Active run page does not show "Run again" button
	activeGetResp, err := client.Get(ts.URL + "/runs/" + newRunID)
	if err != nil {
		t.Fatalf("GET active run: %v", err)
	}
	activeGetBody, _ := io.ReadAll(activeGetResp.Body)
	_ = activeGetResp.Body.Close()
	if strings.Contains(string(activeGetBody), "Run again") {
		t.Errorf("active run page should NOT contain 'Run again' button")
	}

	// 6. Refuses with 409 Conflict when goal is no longer in repository
	missingGoalRunID, err := state.NewRunIDAt(now.Add(2 * time.Minute))
	if err != nil {
		t.Fatalf("NewRunIDAt: %v", err)
	}
	missingRun := &state.Run{
		ID:       missingGoalRunID,
		Repo:     repoKey,
		GoalName: "removed-goal",
		Phase:    state.PhaseFailed,
	}
	if err := stateStore.PutRun(ctx, missingRun); err != nil {
		t.Fatalf("PutRun: %v", err)
	}
	missingResp, missingBody := postWithCSRF(client, "/runs/"+missingGoalRunID+"/rerun")
	if missingResp.StatusCode != http.StatusConflict {
		t.Fatalf("POST missing goal status = %d, want 409 Conflict", missingResp.StatusCode)
	}
	if !strings.Contains(missingBody, "is no longer in the repository") {
		t.Errorf("expected missing goal error on run page, got:\n%s", missingBody)
	}

	// 7. Store / server error produces 500 Internal Server Error
	errServer, err := NewServer(ServerOptions{
		Service:    &fakeSessionService{},
		StateStore: stateStore,
		Starter:    &failRunStarter{err: errors.New("simulated internal store failure")},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	tsErr := httptest.NewServer(errServer)
	t.Cleanup(tsErr.Close)
	errJar, _ := cookiejar.New(nil)
	errClient := tsErr.Client()
	errClient.Jar = errJar
	getErrResp, err := errClient.Get(tsErr.URL + "/runs/" + failedRunID)
	if err != nil {
		t.Fatalf("GET tsErr: %v", err)
	}
	_ = getErrResp.Body.Close()
	errCSRF := csrfFromJar(t, errJar, tsErr.URL)
	errResp, err := errClient.PostForm(tsErr.URL+"/runs/"+failedRunID+"/rerun", url.Values{"csrf": {errCSRF}})
	if err != nil {
		t.Fatalf("POST with error starter: %v", err)
	}
	_ = errResp.Body.Close()
	if errResp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("POST error starter status = %d, want 500 Internal Server Error", errResp.StatusCode)
	}
}

func TestCSRF_AuthSessionHMAC(t *testing.T) {
	key := []byte("secret-key-32-bytes-length-long!")
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	fakeSvc := &fakeSessionService{}
	webServer, err := NewServer(ServerOptions{
		Service:    fakeSvc,
		SessionKey: key,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	handler, err := auth.NewMiddleware(key, func() time.Time { return now }, webServer)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	sessionVal, err := (auth.Session{Login: "octocat", Expires: now.Add(time.Hour)}).Encode(key)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	jar, _ := cookiejar.New(nil)
	client := ts.Client()
	client.Jar = jar
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	u, _ := url.Parse(ts.URL)
	jar.SetCookies(u, []*http.Cookie{
		{
			Name:  auth.CookieName,
			Value: sessionVal,
			Path:  "/",
		},
	})

	// GET / to render the page with CSRF token
	getResp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	bodyBytes, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResp.StatusCode)
	}

	expectedHMAC := auth.CSRFToken(sessionVal, key)
	wantField := `<input type="hidden" name="csrf" value="` + expectedHMAC + `">`
	if !strings.Contains(string(bodyBytes), wantField) {
		t.Fatalf("expected rendered form to contain %s, got:\n%s", wantField, string(bodyBytes))
	}

	// 1. POST without csrf field -> 403
	postResp, err := client.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"sess1"},
		"toolset": {"basic"},
	})
	if err != nil {
		t.Fatalf("POST without csrf: %v", err)
	}
	_ = postResp.Body.Close()
	if postResp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST without csrf status = %d, want 403 Forbidden", postResp.StatusCode)
	}

	// 2. POST with wrong csrf field -> 403
	postResp, err = client.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"sess1"},
		"toolset": {"basic"},
		"csrf":    {"wrong-csrf-token"},
	})
	if err != nil {
		t.Fatalf("POST with wrong csrf: %v", err)
	}
	_ = postResp.Body.Close()
	if postResp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST with wrong csrf status = %d, want 403 Forbidden", postResp.StatusCode)
	}

	// 3. POST with right HMAC csrf field -> passes (303)
	postResp, err = client.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"sess1"},
		"toolset": {"basic"},
		"csrf":    {expectedHMAC},
	})
	if err != nil {
		t.Fatalf("POST with right csrf: %v", err)
	}
	_ = postResp.Body.Close()
	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST with right csrf status = %d, want 303 See Other", postResp.StatusCode)
	}
}

func TestRunAgain_CarriesLogin(t *testing.T) {
	ctx := t.Context()
	key := []byte("secret-key-32-bytes-length-long!")
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	stateStore := state.NewFileStore(t.TempDir())

	scheduler, err := run.NewScheduler(run.SchedulerOptions{
		Store: stateStore,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	webServer, err := NewServer(ServerOptions{
		Service:    &fakeSessionService{},
		StateStore: stateStore,
		Starter:    scheduler,
		SessionKey: key,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	handler, err := auth.NewMiddleware(key, func() time.Time { return now }, webServer)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/auto-optimizer",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "auto-optimizer",
		DefaultBranch: "main",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name: "test-goal",
			Body: "Test goal description",
		},
		BaseCommit: "commit-base-123",
	}
	if err := stateStore.PutGoal(ctx, repoKey, goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	failedRunID, err := state.NewRunIDAt(now)
	if err != nil {
		t.Fatalf("NewRunIDAt: %v", err)
	}
	failedRun := &state.Run{
		ID:         failedRunID,
		Repo:       repoKey,
		GoalName:   "test-goal",
		Phase:      state.PhaseFailed,
		BaseCommit: "commit-base-123",
		Goal:       goalRec.Goal,
	}
	if err := stateStore.PutRun(ctx, failedRun); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	sessionVal, err := (auth.Session{Login: "monalisa", Expires: now.Add(time.Hour)}).Encode(key)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	jar, _ := cookiejar.New(nil)
	client := ts.Client()
	client.Jar = jar
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	u, _ := url.Parse(ts.URL)
	jar.SetCookies(u, []*http.Cookie{
		{
			Name:  auth.CookieName,
			Value: sessionVal,
			Path:  "/",
		},
	})

	csrfToken := auth.CSRFToken(sessionVal, key)

	// Post rerun with csrf
	postResp, err := client.PostForm(ts.URL+"/runs/"+failedRunID+"/rerun", url.Values{
		"csrf": {csrfToken},
	})
	if err != nil {
		t.Fatalf("POST rerun: %v", err)
	}
	_ = postResp.Body.Close()
	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303 See Other", postResp.StatusCode)
	}

	newRunID := strings.TrimPrefix(postResp.Header.Get("Location"), "/runs/")
	events, err := stateStore.ReadEvents(ctx, repoKey, newRunID)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected 1 event, got %d (err: %v)", len(events), err)
	}
	data, ok := events[0].Data.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any in event data, got %T", events[0].Data)
	}
	if data["reason"] != "manual" {
		t.Errorf("reason = %v, want manual", data["reason"])
	}
	if data["by"] != "monalisa" {
		t.Errorf("by = %v, want monalisa", data["by"])
	}
}

func TestPostMessage_RecordsActor(t *testing.T) {
	ctx := t.Context()
	key := []byte("secret-key-32-bytes-length-long!")
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	stateStore := state.NewFileStore(t.TempDir())

	fakeSvc := &fakeSessionService{}
	webServer, err := NewServer(ServerOptions{
		Service:    fakeSvc,
		StateStore: stateStore,
		SessionKey: key,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	handler, err := auth.NewMiddleware(key, func() time.Time { return now }, webServer)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/auto-optimizer",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "auto-optimizer",
		DefaultBranch: "main",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID, err := state.NewRunIDAt(now)
	if err != nil {
		t.Fatalf("NewRunIDAt: %v", err)
	}
	run := &state.Run{
		ID:        runID,
		Repo:      repoKey,
		GoalName:  "test-goal",
		Phase:     state.PhaseWorking,
		SessionID: "sess-user-1",
	}
	if err := stateStore.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	sessionVal, err := (auth.Session{Login: "octocat", Expires: now.Add(time.Hour)}).Encode(key)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	jar, _ := cookiejar.New(nil)
	client := ts.Client()
	client.Jar = jar
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	u, _ := url.Parse(ts.URL)
	jar.SetCookies(u, []*http.Cookie{
		{
			Name:  auth.CookieName,
			Value: sessionVal,
			Path:  "/",
		},
	})

	csrfToken := auth.CSRFToken(sessionVal, key)

	// Post message as octocat
	postResp, err := client.PostForm(ts.URL+"/sessions/sess-user-1/messages", url.Values{
		"text": {"please check logs"},
		"csrf": {csrfToken},
	})
	if err != nil {
		t.Fatalf("POST message: %v", err)
	}
	_ = postResp.Body.Close()
	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303 See Other", postResp.StatusCode)
	}

	events, err := stateStore.ReadEvents(ctx, repoKey, runID)
	if err != nil || len(events) != 1 {
		t.Fatalf("expected 1 event, got %d (err: %v)", len(events), err)
	}
	if events[0].Kind != "message_posted" {
		t.Errorf("event kind = %v, want message_posted", events[0].Kind)
	}
	if events[0].Message != "" {
		t.Errorf("event message = %q, want empty", events[0].Message)
	}
	data, ok := events[0].Data.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any in event data, got %T", events[0].Data)
	}
	if data["by"] != "octocat" {
		t.Errorf("event by = %v, want octocat", data["by"])
	}
}

func TestCSRF_KeySetAuthenticationOff(t *testing.T) {
	// In --insecure-no-auth mode, SessionKey is nil even if AO_SESSION_KEY is present in the environment.
	// Verify that the server uses double-submit cookie mode and successfully processes POST requests.
	t.Setenv("AO_SESSION_KEY", "some-key-set-in-env-32-bytes-long!")
	ts, _ := setupTestWebServer(t, nil)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// GET establishes the per-browser double-submit csrf_token cookie and renders it into the form
	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}

	csrf := csrfFromJar(t, jar, ts.URL)
	if csrf == "" {
		t.Fatal("expected non-empty csrf cookie")
	}
	if !strings.Contains(string(bodyBytes), `<input type="hidden" name="csrf" value="`+csrf+`">`) {
		t.Fatalf("expected rendered form to contain csrf token from cookie, got:\n%s", string(bodyBytes))
	}

	// POST with the form token succeeds (303 See Other)
	postResp, err := client.PostForm(ts.URL+"/sessions", url.Values{
		"name":    {"insecuresess"},
		"toolset": {"basic"},
		"csrf":    {csrf},
	})
	if err != nil {
		t.Fatalf("POST /sessions: %v", err)
	}
	_ = postResp.Body.Close()
	if postResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST status = %d, want 303 See Other", postResp.StatusCode)
	}
}

type failRunStarter struct {
	err error
}

func (f *failRunStarter) StartRun(_ context.Context, _ *state.Repo, _ string, _ run.StartOptions) (*state.Run, error) {
	return nil, f.err
}
