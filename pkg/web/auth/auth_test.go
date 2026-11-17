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
		req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/test", nil)
		SetSessionCookie(rec, req, "token123")
		c := rec.Result().Cookies()[0]
		if c.Name != CookieName || c.Value != "token123" || !c.HttpOnly || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || !c.Secure {
			t.Fatalf("unexpected cookie: %+v", c)
		}
	})

	t.Run("ClearSessionCookie sets Secure unconditionally", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "http://localhost:8080/logout", nil)
		ClearSessionCookie(rec, req)
		c := rec.Result().Cookies()[0]
		if c.Name != CookieName || c.Value != "" || c.MaxAge >= 0 || !c.HttpOnly || c.SameSite != http.SameSiteLaxMode || !c.Secure {
			t.Fatalf("unexpected clear cookie: %+v", c)
		}
	})
}
