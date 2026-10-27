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

package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

type fakeTokenSource struct {
	tokens []string
	index  atomic.Int32
	err    error
}

func (f *fakeTokenSource) Token() (*oauth2.Token, error) {
	if f.err != nil {
		return nil, f.err
	}
	idx := int(f.index.Add(1)) - 1
	if idx >= len(f.tokens) {
		idx = len(f.tokens) - 1
	}
	return &oauth2.Token{
		AccessToken: f.tokens[idx],
		Expiry:      time.Now().Add(time.Hour),
	}, nil
}

func TestVertexBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		project  string
		location string
		want     string
	}{
		{
			name:     "global location",
			project:  "my-project",
			location: "global",
			want:     "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/endpoints/openapi",
		},
		{
			name:     "empty location defaults to global",
			project:  "my-project",
			location: "",
			want:     "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/endpoints/openapi",
		},
		{
			name:     "regional location",
			project:  "my-project",
			location: "us-central1",
			want:     "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/endpoints/openapi",
		},
		{
			name:     "another regional location",
			project:  "gke-labs-corp-dev",
			location: "europe-west1",
			want:     "https://europe-west1-aiplatform.googleapis.com/v1/projects/gke-labs-corp-dev/locations/europe-west1/endpoints/openapi",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := VertexBaseURL(tc.project, tc.location)
			if got != tc.want {
				t.Errorf("VertexBaseURL(%q, %q) = %q, want %q", tc.project, tc.location, got, tc.want)
			}
		})
	}
}

func TestClientWithTokenSource(t *testing.T) {
	var gotBearer string
	var gotPath string
	var gotModel string

	ts := &fakeTokenSource{
		tokens: []string{"test-token-1", "test-token-2"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBearer = r.Header.Get("Authorization")

		var req ChatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		gotModel = req.Model

		resp := ChatCompletionResponse{
			ID: "chatcmpl-test",
			Choices: []Choice{
				{
					Index: 0,
					Message: Message{
						Role:    "assistant",
						Content: strPtr("Paris"),
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(server.Close)

	baseURL := server.URL + "/v1/projects/test-proj/locations/global/endpoints/openapi"
	client, err := NewClient(baseURL, "", WithTokenSource(ts))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// First request: should use test-token-1
	resp, err := client.CreateChatCompletion(t.Context(), ChatCompletionRequest{
		Model: "google/gemini-3.5-flash",
		Messages: []Message{
			{Role: "user", Content: strPtr("What is the capital of France?")},
		},
	})
	if err != nil {
		t.Fatalf("CreateChatCompletion: %v", err)
	}

	if gotPath != "/v1/projects/test-proj/locations/global/endpoints/openapi/chat/completions" {
		t.Errorf("path = %q, want %q", gotPath, "/v1/projects/test-proj/locations/global/endpoints/openapi/chat/completions")
	}
	if gotBearer != "Bearer test-token-1" {
		t.Errorf("Authorization = %q, want %q", gotBearer, "Bearer test-token-1")
	}
	if gotModel != "google/gemini-3.5-flash" {
		t.Errorf("model = %q, want %q", gotModel, "google/gemini-3.5-flash")
	}
	if resp.Choices[0].Message.Content == nil || *resp.Choices[0].Message.Content != "Paris" {
		t.Errorf("content = %v, want Paris", resp.Choices[0].Message.Content)
	}

	// Second request: token source should refresh/return next token
	_, err = client.CreateChatCompletion(t.Context(), ChatCompletionRequest{
		Model: "google/gemini-3.5-flash",
		Messages: []Message{
			{Role: "user", Content: strPtr("Next question")},
		},
	})
	if err != nil {
		t.Fatalf("CreateChatCompletion second call: %v", err)
	}
	if gotBearer != "Bearer test-token-2" {
		t.Errorf("second call Authorization = %q, want %q", gotBearer, "Bearer test-token-2")
	}
	if gotModel != "google/gemini-3.5-flash" {
		t.Errorf("model = %q, want %q", gotModel, "google/gemini-3.5-flash")
	}
}

func TestNewFromEnv(t *testing.T) {
	savedDefaultTokenSource := defaultTokenSource
	t.Cleanup(func() {
		defaultTokenSource = savedDefaultTokenSource
	})

	var tokenSourceCalled bool
	defaultTokenSource = func(_ context.Context, _ ...string) (oauth2.TokenSource, error) {
		tokenSourceCalled = true
		return &fakeTokenSource{tokens: []string{"mock-token"}}, nil
	}

	t.Run("FakeModelEliza returns ElizaClient", func(t *testing.T) {
		c, err := NewFromEnv(FakeModelEliza)
		if err != nil {
			t.Fatalf("NewFromEnv: %v", err)
		}
		if _, ok := c.(*ElizaClient); !ok {
			t.Errorf("client type = %T, want *ElizaClient", c)
		}
	})

	t.Run("GEMINI_API_KEY wins over VERTEX_PROJECT", func(t *testing.T) {
		t.Setenv("GEMINI_API_KEY", "gemini-key")
		t.Setenv("VERTEX_PROJECT", "test-project")
		tokenSourceCalled = false

		c, err := NewFromEnv("gemini-3.5-flash")
		if err != nil {
			t.Fatalf("NewFromEnv: %v", err)
		}
		realClient, ok := c.(*Client)
		if !ok {
			t.Fatalf("client type = %T, want *Client", c)
		}
		if realClient.apiKey != "gemini-key" {
			t.Errorf("apiKey = %q, want gemini-key", realClient.apiKey)
		}
		if tokenSourceCalled {
			t.Errorf("token source was unexpectedly called")
		}
	})

	t.Run("VERTEX_PROJECT selects Vertex AI mode with global default", func(t *testing.T) {
		t.Setenv("GEMINI_API_KEY", "")
		t.Setenv("OPENAI_API_KEY", "")
		t.Setenv("OPENAI_BASE_URL", "")
		t.Setenv("VERTEX_PROJECT", "my-vertex-proj")
		t.Setenv("VERTEX_LOCATION", "")
		tokenSourceCalled = false

		c, err := NewFromEnv("gemini-3.5-flash")
		if err != nil {
			t.Fatalf("NewFromEnv: %v", err)
		}
		realClient, ok := c.(*Client)
		if !ok {
			t.Fatalf("client type = %T, want *Client", c)
		}
		if !tokenSourceCalled {
			t.Errorf("token source was not called")
		}
		wantBase := "https://aiplatform.googleapis.com/v1/projects/my-vertex-proj/locations/global/endpoints/openapi"
		if realClient.baseURL != wantBase {
			t.Errorf("baseURL = %q, want %q", realClient.baseURL, wantBase)
		}
	})

	t.Run("VERTEX_PROJECT with regional VERTEX_LOCATION", func(t *testing.T) {
		t.Setenv("GEMINI_API_KEY", "")
		t.Setenv("OPENAI_API_KEY", "")
		t.Setenv("OPENAI_BASE_URL", "")
		t.Setenv("VERTEX_PROJECT", "my-vertex-proj")
		t.Setenv("VERTEX_LOCATION", "us-central1")
		tokenSourceCalled = false

		c, err := NewFromEnv("gemini-3.5-flash")
		if err != nil {
			t.Fatalf("NewFromEnv: %v", err)
		}
		realClient, ok := c.(*Client)
		if !ok {
			t.Fatalf("client type = %T, want *Client", c)
		}
		wantBase := "https://us-central1-aiplatform.googleapis.com/v1/projects/my-vertex-proj/locations/us-central1/endpoints/openapi"
		if realClient.baseURL != wantBase {
			t.Errorf("baseURL = %q, want %q", realClient.baseURL, wantBase)
		}
	})

	t.Run("OPENAI_API_KEY used when no GEMINI_API_KEY or VERTEX_PROJECT", func(t *testing.T) {
		t.Setenv("GEMINI_API_KEY", "")
		t.Setenv("VERTEX_PROJECT", "")
		t.Setenv("OPENAI_API_KEY", "openai-key")

		c, err := NewFromEnv("gemini-3.5-flash")
		if err != nil {
			t.Fatalf("NewFromEnv: %v", err)
		}
		realClient, ok := c.(*Client)
		if !ok {
			t.Fatalf("client type = %T, want *Client", c)
		}
		if realClient.apiKey != "openai-key" {
			t.Errorf("apiKey = %q, want openai-key", realClient.apiKey)
		}
	})

	t.Run("Missing all env vars returns error", func(t *testing.T) {
		t.Setenv("GEMINI_API_KEY", "")
		t.Setenv("OPENAI_API_KEY", "")
		t.Setenv("VERTEX_PROJECT", "")
		t.Setenv("OPENAI_BASE_URL", "")

		_, err := NewFromEnv("gemini-3.5-flash")
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

func strPtr(s string) *string {
	return &s
}
