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
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/web/auth"
)

func TestParseForm(t *testing.T) {
	key := []byte("12345678901234567890123456789012")
	srv := &Server{sessionKey: key}

	newPostReq := func(body string, cookie *http.Cookie) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != nil {
			req.AddCookie(cookie)
		}
		return req
	}

	t.Run("good form", func(t *testing.T) {
		body := url.Values{
			"csrf":    {"tok123"},
			"name":    {"  alpha  "},
			"toolset": {"expert"},
		}.Encode()
		req := newPostReq(body, &http.Cookie{Name: csrfCookieName, Value: "tok123"})

		var form struct {
			Name    string `form:"name"`
			Toolset string `form:"toolset"`
		}
		if err := srv.parseForm(req, &form); err != nil {
			t.Fatalf("parseForm: %v", err)
		}
		if form.Name != "alpha" || form.Toolset != "expert" {
			t.Errorf("form = %+v, want alpha / expert", form)
		}
	})

	t.Run("empty struct", func(t *testing.T) {
		body := url.Values{"csrf": {"tok123"}}.Encode()
		req := newPostReq(body, &http.Cookie{Name: csrfCookieName, Value: "tok123"})

		var form struct{}
		if err := srv.parseForm(req, &form); err != nil {
			t.Fatalf("parseForm: %v", err)
		}
	})

	errorCases := []struct {
		name    string
		body    string
		cookie  *http.Cookie
		wantErr error
	}{
		{"missing token", "name=alpha", &http.Cookie{Name: csrfCookieName, Value: "tok123"}, ErrBadToken},
		{"wrong token", "csrf=wrong&name=alpha", &http.Cookie{Name: csrfCookieName, Value: "tok123"}, ErrBadToken},
		{"unknown field", "csrf=tok123&name=alpha&extra=bad", &http.Cookie{Name: csrfCookieName, Value: "tok123"}, ErrMalformedForm},
		{"malformed body", "%zz", &http.Cookie{Name: csrfCookieName, Value: "tok123"}, ErrMalformedForm},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			var form struct {
				Name string `form:"name"`
			}
			req := newPostReq(tc.body, tc.cookie)
			if err := srv.parseForm(req, &form); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got err = %v, want %v", err, tc.wantErr)
			}
		})
	}

	t.Run("require sign-in", func(t *testing.T) {
		authSrv := &Server{requireSignIn: true, sessionKey: key}
		sessVal := "sess-cookie-val"
		validCSRF := auth.CSRFToken(sessVal, key)

		goodReq := newPostReq(url.Values{"csrf": {validCSRF}, "name": {"ok"}}.Encode(), &http.Cookie{
			Name:  auth.CookieName,
			Value: sessVal,
		})
		var goodForm struct {
			Name string `form:"name"`
		}
		if err := authSrv.parseForm(goodReq, &goodForm); err != nil || goodForm.Name != "ok" {
			t.Fatalf("parseForm good token: err=%v, name=%q", err, goodForm.Name)
		}

		wrongReq := newPostReq(url.Values{"csrf": {"bad-token"}, "name": {"ok"}}.Encode(), &http.Cookie{
			Name:  auth.CookieName,
			Value: sessVal,
		})
		var wrongForm struct {
			Name string `form:"name"`
		}
		if err := authSrv.parseForm(wrongReq, &wrongForm); !errors.Is(err, ErrBadToken) {
			t.Fatalf("got err = %v, want ErrBadToken", err)
		}
	})

	t.Run("invalid destination", func(t *testing.T) {
		body := url.Values{"csrf": {"tok123"}}.Encode()
		req := newPostReq(body, &http.Cookie{Name: csrfCookieName, Value: "tok123"})

		if err := srv.parseForm(req, nil); !errors.Is(err, ErrMalformedForm) {
			t.Errorf("got %v, want ErrMalformedForm for nil dst", err)
		}
		if err := srv.parseForm(req, struct{}{}); !errors.Is(err, ErrMalformedForm) {
			t.Errorf("got %v, want ErrMalformedForm for non-pointer dst", err)
		}
		var badField struct {
			Count int `form:"count"`
		}
		if err := srv.parseForm(req, &badField); !errors.Is(err, ErrMalformedForm) {
			t.Errorf("got %v, want ErrMalformedForm for non-string field", err)
		}
	})
}

func TestHandlers_RejectUnknownField(t *testing.T) {
	fakeSvc := &fakeSessionService{}
	srv, err := NewServer(fakeSvc, nil, nil, ServerOptions{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	tok := "valid-csrf"
	req := httptest.NewRequest(http.MethodPost, "/sessions", strings.NewReader(url.Values{
		"csrf":    {tok},
		"name":    {"newsess"},
		"toolset": {"basic"},
		"extra":   {"not-allowed"},
	}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: tok})
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("POST /sessions with unknown field status = %d, want 400", rec.Code)
	}
}
