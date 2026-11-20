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

package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestSessionRoundTrip(t *testing.T) {
	key := []byte("secret-session-key-for-test-32b!")
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	sess := Session{Login: "testuser", Expires: now.Add(2 * time.Hour)}

	encoded, err := sess.Encode(key)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if encoded == "" {
		t.Fatal("expected non-empty encoded string")
	}

	decoded, err := Decode(key, encoded, now)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded.Login != sess.Login {
		t.Errorf("Login = %q, want %q", decoded.Login, sess.Login)
	}
	if !decoded.Expires.Equal(sess.Expires) {
		t.Errorf("Expires = %v, want %v", decoded.Expires, sess.Expires)
	}
}

func TestCSRFToken(t *testing.T) {
	key := []byte("secret-session-key-for-test-32b!")
	otherKey := []byte("other-secret-session-key-32bytes")
	sessVal := "payload.signature"
	otherSessVal := "different-payload.signature"

	tok1 := CSRFToken(sessVal, key)
	if tok1 == "" {
		t.Fatal("expected non-empty CSRF token")
	}

	tok2 := CSRFToken(sessVal, key)
	if tok1 != tok2 {
		t.Errorf("CSRFToken not deterministic: got %q and %q", tok1, tok2)
	}

	tokDiffSess := CSRFToken(otherSessVal, key)
	if tok1 == tokDiffSess {
		t.Error("expected different tokens for different session values")
	}

	tokDiffKey := CSRFToken(sessVal, otherKey)
	if tok1 == tokDiffKey {
		t.Error("expected different tokens for different keys")
	}

	// Verify domain separation: token must differ from a bare HMAC of the cookie value.
	bareMac := hmac.New(sha256.New, key)
	bareMac.Write([]byte(sessVal))
	bareToken := hex.EncodeToString(bareMac.Sum(nil))
	if tok1 == bareToken {
		t.Error("expected CSRF token to differ from bare HMAC of session value due to domain separation prefix")
	}

	if got := CSRFToken("", key); got != "" {
		t.Errorf("CSRFToken with empty session value = %q, want empty", got)
	}
	if got := CSRFToken(sessVal, nil); got != "" {
		t.Errorf("CSRFToken with nil key = %q, want empty", got)
	}
	if got := CSRFToken(sessVal, []byte{}); got != "" {
		t.Errorf("CSRFToken with empty key = %q, want empty", got)
	}
}

func TestShortKeyRejections(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	sess := Session{Login: "alice", Expires: now.Add(time.Hour)}
	shortKeys := [][]byte{nil, {}, []byte("too-short-key")}

	for _, k := range shortKeys {
		if _, err := sess.Encode(k); !errors.Is(err, ErrKeyTooShort) {
			t.Errorf("Encode(len=%d) expected ErrKeyTooShort, got %v", len(k), err)
		}
		if _, err := Decode(k, "any.value", now); !errors.Is(err, ErrKeyTooShort) {
			t.Errorf("Decode(len=%d) expected ErrKeyTooShort, got %v", len(k), err)
		}
		if _, err := NewMiddleware(k, nil, nil); !errors.Is(err, ErrKeyTooShort) {
			t.Errorf("NewMiddleware(len=%d) expected ErrKeyTooShort, got %v", len(k), err)
		}
	}
}

func TestDecodeRejections(t *testing.T) {
	key := []byte("correct-session-key-32-bytes-ok!")
	wrongKey := []byte("wrong-session-key-32-bytes-wrong")
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

	validToken, err := (Session{Login: "alice", Expires: now.Add(time.Hour)}).Encode(key)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	parts := strings.Split(validToken, ".")

	t.Run("wrong key", func(t *testing.T) {
		if _, err := Decode(wrongKey, validToken, now); !errors.Is(err, ErrInvalidSignature) {
			t.Fatalf("expected ErrInvalidSignature, got %v", err)
		}
	})

	t.Run("tampered payload", func(t *testing.T) {
		tampered := base64.RawURLEncoding.EncodeToString([]byte(`{"login":"hacker","expires":"2026-11-16T13:00:00Z"}`)) + "." + parts[1]
		if _, err := Decode(key, tampered, now); !errors.Is(err, ErrInvalidSignature) {
			t.Fatalf("expected ErrInvalidSignature, got %v", err)
		}
	})

	t.Run("tampered signature", func(t *testing.T) {
		tampered := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte("invalid-signature-bytes-32-len!!"))
		if _, err := Decode(key, tampered, now); !errors.Is(err, ErrInvalidSignature) {
			t.Fatalf("expected ErrInvalidSignature, got %v", err)
		}
	})

	t.Run("signature over base64 string rejected", func(t *testing.T) {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(parts[0]))
		sigOverB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		token := parts[0] + "." + sigOverB64
		if _, err := Decode(key, token, now); !errors.Is(err, ErrInvalidSignature) {
			t.Fatalf("expected ErrInvalidSignature for signature over base64 string, got %v", err)
		}
	})

	t.Run("expired session", func(t *testing.T) {
		expired, err := (Session{Login: "bob", Expires: now.Add(-time.Minute)}).Encode(key)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if _, err := Decode(key, expired, now); !errors.Is(err, ErrExpiredSession) {
			t.Fatalf("expected ErrExpiredSession, got %v", err)
		}
	})

	t.Run("malformed tokens", func(t *testing.T) {
		for _, tc := range []string{
			"", "nodots", "a.b.c", "bad!b64.sig", parts[0] + ".bad!b64",
			base64.RawURLEncoding.EncodeToString([]byte("not json")) + "." + parts[1],
			base64.RawURLEncoding.EncodeToString([]byte(`{"login":"","expires":"2026-11-16T13:00:00Z"}`)) + "." + parts[1],
		} {
			if _, err := Decode(key, tc, now); err == nil {
				t.Errorf("Decode(%q) expected error, got nil", tc)
			}
		}
	})
}

func TestMiddleware(t *testing.T) {
	key := []byte("test-signing-key-32-bytes-length")
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	handler, err := NewMiddleware(key, func() time.Time { return now }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user, ok := UserFromContext(r.Context()); ok {
			w.Header().Set("X-User", user)
		}
		w.WriteHeader(http.StatusOK)
	}))
	if err != nil {
		t.Fatalf("NewMiddleware failed: %v", err)
	}

	t.Run("valid session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
		cookie, err := (Session{Login: "octocat", Expires: now.Add(time.Hour)}).Encode(key)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		req.AddCookie(&http.Cookie{Name: CookieName, Value: cookie})
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Header().Get("X-User") != "octocat" {
			t.Fatalf("status = %d, user = %q", rec.Code, rec.Header().Get("X-User"))
		}
	})

	t.Run("browser GET redirect plain path", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/repos/test", nil)
		req.Header.Set("Accept", "text/html,application/xhtml+xml")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("code = %d, want 303", rec.Code)
		}
		wantLocation := "/login?" + url.Values{"next": {"/repos/test"}}.Encode()
		if loc := rec.Header().Get("Location"); loc != wantLocation {
			t.Fatalf("Location = %q, want %q", loc, wantLocation)
		}
	})

	t.Run("browser GET redirect path with query string", func(t *testing.T) {
		targetPath := "/runs/x?tab=events&y=1"
		req := httptest.NewRequest(http.MethodGet, targetPath, nil)
		req.Header.Set("Accept", "text/html,application/xhtml+xml")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("code = %d, want 303", rec.Code)
		}
		wantLocation := "/login?" + url.Values{"next": {targetPath}}.Encode()
		if loc := rec.Header().Get("Location"); loc != wantLocation {
			t.Fatalf("Location = %q, want %q", loc, wantLocation)
		}
	})

	t.Run("POST answers 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/repos/test", nil)
		req.Header.Set("Accept", "text/html")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code = %d, want 401", rec.Code)
		}
	})

	t.Run("API GET answers 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/repos/test", nil)
		req.Header.Set("Accept", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code = %d, want 401", rec.Code)
		}
	})

	t.Run("open paths pass through", func(t *testing.T) {
		for _, path := range []string{"/healthz", "/static/style.css", "/login", "/login/", "/auth/github/callback"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("Accept", "text/html")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("%s status = %d, want 200", path, rec.Code)
			}
		}
	})
}

func TestCookieHelpers(t *testing.T) {
	t.Run("SetSessionCookie sets Secure unconditionally", func(t *testing.T) {
		rec := httptest.NewRecorder()
		SetSessionCookie(rec, "token123")
		c := rec.Result().Cookies()[0]
		if c.Name != CookieName || c.Value != "token123" || !c.HttpOnly || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || !c.Secure {
			t.Fatalf("unexpected cookie: %+v", c)
		}
	})

	t.Run("ClearSessionCookie sets Secure unconditionally", func(t *testing.T) {
		rec := httptest.NewRecorder()
		ClearSessionCookie(rec)
		c := rec.Result().Cookies()[0]
		if c.Name != CookieName || c.Value != "" || c.MaxAge >= 0 || !c.HttpOnly || c.SameSite != http.SameSiteLaxMode || !c.Secure {
			t.Fatalf("unexpected clear cookie: %+v", c)
		}
	})
}

func TestGitHubOAuth(t *testing.T) {
	mockGitHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login/oauth/access_token":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			code := r.FormValue("code")
			w.Header().Set("Content-Type", "application/json")
			switch code {
			case "valid-code":
				_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "token-allowed"})
			case "unallowed-code":
				_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "token-denied"})
			default:
				http.Error(w, "invalid code", http.StatusBadRequest)
			}
		case "/user":
			authHeader := r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			switch authHeader {
			case "Bearer token-allowed":
				_ = json.NewEncoder(w).Encode(map[string]string{"login": "OctoCat"})
			case "Bearer token-denied":
				_ = json.NewEncoder(w).Encode(map[string]string{"login": "EvilHacker"})
			default:
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockGitHub.Close()

	key := []byte("session-signing-key-32-bytes-long!")
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

	opts := GitHubOptions{
		ClientID:     "client-123",
		ClientSecret: "secret-abc",
		PublicURL:    "https://ao.example.com",
		AllowedUsers: []string{"octocat"},
		BaseURL:      mockGitHub.URL,
		APIURL:       mockGitHub.URL,
	}

	handler, err := NewGitHubHandler(opts, key, func() time.Time { return now }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	if err != nil {
		t.Fatalf("NewGitHubHandler failed: %v", err)
	}

	t.Run("full sign-in sets session for allowed user", func(t *testing.T) {
		loginReq := httptest.NewRequest(http.MethodGet, "/login?next=/repos", nil)
		loginRec := httptest.NewRecorder()
		handler.ServeHTTP(loginRec, loginReq)

		if loginRec.Code != http.StatusFound {
			t.Fatalf("login code = %d, want %d", loginRec.Code, http.StatusFound)
		}
		loc := loginRec.Header().Get("Location")
		u, err := url.Parse(loc)
		if err != nil {
			t.Fatalf("failed to parse redirect: %v", err)
		}
		state := u.Query().Get("state")
		if state == "" {
			t.Fatal("expected state parameter in redirect URL")
		}

		cookies := loginRec.Result().Cookies()
		var stateCookie, nextCookie *http.Cookie
		for _, c := range cookies {
			if c.Name == OAuthStateCookieName {
				stateCookie = c
			}
			if c.Name == OAuthNextCookieName {
				nextCookie = c
			}
		}
		if stateCookie == nil || stateCookie.Value != state {
			t.Fatalf("state cookie = %v, want %s", stateCookie, state)
		}
		if nextCookie == nil || nextCookie.Value != "/repos" {
			t.Fatalf("next cookie = %v, want /repos", nextCookie)
		}

		cbReq := httptest.NewRequest(http.MethodGet, "/auth/github/callback?code=valid-code&state="+state, nil)
		cbReq.AddCookie(stateCookie)
		cbReq.AddCookie(nextCookie)
		cbRec := httptest.NewRecorder()
		handler.ServeHTTP(cbRec, cbReq)

		if cbRec.Code != http.StatusSeeOther {
			t.Fatalf("callback code = %d, want %d", cbRec.Code, http.StatusSeeOther)
		}
		if cbLoc := cbRec.Header().Get("Location"); cbLoc != "/repos" {
			t.Errorf("callback Location = %q, want /repos", cbLoc)
		}

		var sessionCookie *http.Cookie
		for _, c := range cbRec.Result().Cookies() {
			if c.Name == CookieName && c.Value != "" {
				sessionCookie = c
			}
		}
		if sessionCookie == nil {
			t.Fatal("expected session cookie to be set")
		}
		sess, err := Decode(key, sessionCookie.Value, now)
		if err != nil {
			t.Fatalf("failed to decode session: %v", err)
		}
		if sess.Login != "OctoCat" {
			t.Errorf("session login = %q, want OctoCat", sess.Login)
		}
	})

	t.Run("user who is not allowed gets 403 and no cookie", func(t *testing.T) {
		loginReq := httptest.NewRequest(http.MethodGet, "/login", nil)
		loginRec := httptest.NewRecorder()
		handler.ServeHTTP(loginRec, loginReq)

		u, _ := url.Parse(loginRec.Header().Get("Location"))
		state := u.Query().Get("state")
		var stateCookie *http.Cookie
		for _, c := range loginRec.Result().Cookies() {
			if c.Name == OAuthStateCookieName {
				stateCookie = c
			}
		}

		cbReq := httptest.NewRequest(http.MethodGet, "/auth/github/callback?code=unallowed-code&state="+state, nil)
		cbReq.AddCookie(stateCookie)
		cbRec := httptest.NewRecorder()
		handler.ServeHTTP(cbRec, cbReq)

		if cbRec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", cbRec.Code)
		}
		if !strings.Contains(cbRec.Body.String(), "EvilHacker") {
			t.Errorf("body %q does not contain EvilHacker", cbRec.Body.String())
		}
		for _, c := range cbRec.Result().Cookies() {
			if c.Name == CookieName && c.Value != "" && c.MaxAge >= 0 {
				t.Fatalf("unexpected session cookie: %+v", c)
			}
		}
	})

	t.Run("wrong state is rejected", func(t *testing.T) {
		loginReq := httptest.NewRequest(http.MethodGet, "/login", nil)
		loginRec := httptest.NewRecorder()
		handler.ServeHTTP(loginRec, loginReq)

		var stateCookie *http.Cookie
		for _, c := range loginRec.Result().Cookies() {
			if c.Name == OAuthStateCookieName {
				stateCookie = c
			}
		}

		cbReq := httptest.NewRequest(http.MethodGet, "/auth/github/callback?code=valid-code&state=wrong-state-value", nil)
		cbReq.AddCookie(stateCookie)
		cbRec := httptest.NewRecorder()
		handler.ServeHTTP(cbRec, cbReq)

		if cbRec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", cbRec.Code)
		}
	})

	t.Run("absolute next URL is ignored", func(t *testing.T) {
		loginReq := httptest.NewRequest(http.MethodGet, "/login?next=https://attacker.com/phish", nil)
		loginRec := httptest.NewRecorder()
		handler.ServeHTTP(loginRec, loginReq)

		u, _ := url.Parse(loginRec.Header().Get("Location"))
		state := u.Query().Get("state")
		var stateCookie, nextCookie *http.Cookie
		for _, c := range loginRec.Result().Cookies() {
			if c.Name == OAuthStateCookieName {
				stateCookie = c
			}
			if c.Name == OAuthNextCookieName {
				nextCookie = c
			}
		}
		if nextCookie != nil && nextCookie.Value != "/" {
			t.Errorf("next cookie = %q, want /", nextCookie.Value)
		}

		// Verify callback ignores any attempt to inject next via query parameter and relies solely on the cookie.
		cbReq := httptest.NewRequest(http.MethodGet, "/auth/github/callback?code=valid-code&state="+state+"&next=/evil-query", nil)
		cbReq.AddCookie(stateCookie)
		if nextCookie != nil {
			cbReq.AddCookie(nextCookie)
		}
		cbRec := httptest.NewRecorder()
		handler.ServeHTTP(cbRec, cbReq)

		if cbRec.Code != http.StatusSeeOther {
			t.Fatalf("code = %d, want 303", cbRec.Code)
		}
		if loc := cbRec.Header().Get("Location"); loc != "/" {
			t.Errorf("Location = %q, want /", loc)
		}
	})

	t.Run("POST logout clears session cookie", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/logout", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusSeeOther {
			t.Fatalf("logout code = %d, want 303", rec.Code)
		}
		var clearCookie *http.Cookie
		for _, c := range rec.Result().Cookies() {
			if c.Name == CookieName {
				clearCookie = c
			}
		}
		if clearCookie == nil || clearCookie.Value != "" || clearCookie.MaxAge >= 0 {
			t.Fatalf("expected cleared session cookie, got %+v", clearCookie)
		}
	})
}

func TestGitHubOptionsValidation(t *testing.T) {
	valid := GitHubOptions{
		ClientID:     "id",
		ClientSecret: "secret",
		PublicURL:    "https://example.com",
		AllowedUsers: []string{"octocat"},
		BaseURL:      "https://github.com",
		APIURL:       "https://api.github.com",
	}

	t.Run("valid options", func(t *testing.T) {
		if err := valid.Validate(); err != nil {
			t.Errorf("expected valid, got: %v", err)
		}
	})

	t.Run("missing client ID", func(t *testing.T) {
		o := valid
		o.ClientID = ""
		if err := o.Validate(); err == nil {
			t.Error("expected error for missing client ID")
		}
	})

	t.Run("missing client secret", func(t *testing.T) {
		o := valid
		o.ClientSecret = ""
		if err := o.Validate(); err == nil {
			t.Error("expected error for missing client secret")
		}
	})

	t.Run("missing allowed users", func(t *testing.T) {
		o := valid
		o.AllowedUsers = nil
		err := o.Validate()
		if err == nil {
			t.Fatal("expected error for missing allowed users")
		}
		if !strings.Contains(err.Error(), "--github-allowed-user") {
			t.Errorf("expected error to mention --github-allowed-user, got %v", err)
		}
	})

	t.Run("public URL validation", func(t *testing.T) {
		validURLs := []string{
			"https://ao.example.com",
			"https://ao.example.com:8443",
			"http://localhost",
			"http://localhost:8080",
			"http://127.0.0.1",
			"http://127.0.0.1:8080",
			"http://[::1]",
			"http://[::1]:8080",
		}
		for _, u := range validURLs {
			o := valid
			o.PublicURL = u
			if err := o.Validate(); err != nil {
				t.Errorf("expected %q to be valid, got: %v", u, err)
			}
		}

		invalidURLs := []string{
			"",
			"ftp://foo",
			"invalid",
			"http://",
			"http://ao.example.com",
			"http://example.com:8080",
		}
		for _, u := range invalidURLs {
			o := valid
			o.PublicURL = u
			if err := o.Validate(); err == nil {
				t.Errorf("expected error for public URL %q", u)
			}
		}
	})
}
