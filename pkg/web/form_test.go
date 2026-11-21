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
)

func TestParseForm(t *testing.T) {
	tok := "tok123"
	srv := &Server{}

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
			csrfFormField: {tok},
			"name":        {"  alpha  "},
			"toolset":     {"expert"},
		}.Encode()
		req := newPostReq(body, &http.Cookie{Name: csrfCookieName, Value: tok})

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
		body := url.Values{csrfFormField: {tok}}.Encode()
		req := newPostReq(body, &http.Cookie{Name: csrfCookieName, Value: tok})

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
		{"missing token", "name=alpha", &http.Cookie{Name: csrfCookieName, Value: tok}, errBadToken},
		{"wrong token", "csrf=wrong&name=alpha", &http.Cookie{Name: csrfCookieName, Value: tok}, errBadToken},
		{"unknown field", "csrf=" + tok + "&name=alpha&extra=bad", &http.Cookie{Name: csrfCookieName, Value: tok}, errMalformedForm},
		{"malformed body", "%zz", &http.Cookie{Name: csrfCookieName, Value: tok}, errMalformedForm},
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
}

func TestServeHTTP_MalformedBody(t *testing.T) {
	srv, err := NewServer(&fakeSessionService{}, nil, nil, ServerOptions{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	tok := "valid-csrf"
	for _, body := range []string{"%zz", "csrf=" + tok + "&name=%zz"} {
		req := httptest.NewRequest(http.MethodPost, "/sessions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: tok})
		rec := httptest.NewRecorder()

		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("POST with body %q status = %d, want 400", body, rec.Code)
		}
	}
}
