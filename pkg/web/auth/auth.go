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

// Package auth provides session management and authentication middleware for the web UI.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CookieName is the name of the session cookie used by ao.
const CookieName = "session_id"

// MinKeyLength is the minimum byte length required for session signing keys.
const MinKeyLength = 32

var (
	// ErrKeyTooShort is returned when a session signing key is shorter than MinKeyLength.
	ErrKeyTooShort = errors.New("session key must be at least 32 bytes")
	// ErrInvalidSignature is returned when the session HMAC does not match.
	ErrInvalidSignature = errors.New("invalid signature")
	// ErrMalformedSession is returned when the cookie value or payload cannot be parsed.
	ErrMalformedSession = errors.New("malformed session")
	// ErrExpiredSession is returned when the session expiration time has passed.
	ErrExpiredSession = errors.New("session expired")
)

type userContextKey struct{}

// Session represents an authenticated user's session.
type Session struct {
	Login   string    `json:"login"`
	Expires time.Time `json:"expires"`
}

// Encode serializes the session as a JSON payload and an HMAC-SHA256 signature,
// both encoded with base64url and separated by a period.
func (s Session) Encode(key []byte) (string, error) {
	if len(key) < MinKeyLength {
		return "", ErrKeyTooShort
	}
	payload, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("marshaling session: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	sig := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// Decode parses and verifies a session token against key and now.
// It rejects keys shorter than MinKeyLength, bad signatures, malformed values, and expired sessions.
func Decode(key []byte, value string, now time.Time) (Session, error) {
	if len(key) < MinKeyLength {
		return Session{}, ErrKeyTooShort
	}
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return Session{}, ErrMalformedSession
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Session{}, fmt.Errorf("%w: invalid payload: %v", ErrMalformedSession, err)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Session{}, fmt.Errorf("%w: invalid signature: %v", ErrMalformedSession, err)
	}

	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	expected := mac.Sum(nil)

	if !hmac.Equal(sig, expected) {
		return Session{}, ErrInvalidSignature
	}

	var sess Session
	if err := json.Unmarshal(payload, &sess); err != nil {
		return Session{}, fmt.Errorf("%w: invalid JSON: %v", ErrMalformedSession, err)
	}

	if sess.Login == "" {
		return Session{}, fmt.Errorf("%w: missing login", ErrMalformedSession)
	}

	if !now.Before(sess.Expires) {
		return Session{}, ErrExpiredSession
	}

	return sess, nil
}

// UserFromContext returns the authenticated user's login from ctx, if present.
func UserFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	u, ok := ctx.Value(userContextKey{}).(string)
	return u, ok && u != ""
}

// WithUser returns a copy of parent context with the user login stored.
func WithUser(parent context.Context, login string) context.Context {
	return context.WithValue(parent, userContextKey{}, login)
}

// NewMiddleware returns an HTTP handler that enforces valid session cookies.
// It returns an error if key is shorter than MinKeyLength.
// With a valid session, the user login is injected into the request context.
// Without one, GET requests accepting HTML are redirected to /login?next=<path>,
// while other requests are answered with 401 Unauthorized.
// Open paths (/healthz, /static/, /login, /auth/) pass through without a session.
func NewMiddleware(key []byte, now func() time.Time, next http.Handler) (http.Handler, error) {
	if len(key) < MinKeyLength {
		return nil, ErrKeyTooShort
	}
	if now == nil {
		now = time.Now
	}
	if next == nil {
		next = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cookie, err := r.Cookie(CookieName); err == nil && cookie.Value != "" {
			if sess, err := Decode(key, cookie.Value, now()); err == nil && sess.Login != "" {
				r = r.WithContext(WithUser(r.Context(), sess.Login))
				next.ServeHTTP(w, r)
				return
			}
		}

		if isOpenPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		if r.Method == http.MethodGet && acceptsHTML(r) {
			path := r.URL.RequestURI()
			if path == "" || !strings.HasPrefix(path, "/") {
				path = "/" + strings.TrimPrefix(path, "/")
			}
			redirectURL := "/login?" + url.Values{"next": {path}}.Encode()
			http.Redirect(w, r, redirectURL, http.StatusSeeOther)
			return
		}

		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	}), nil
}

func isOpenPath(path string) bool {
	if path == "/healthz" || path == "/login" || path == "/login/" {
		return true
	}
	if path == "/static" || strings.HasPrefix(path, "/static/") {
		return true
	}
	if path == "/auth" || strings.HasPrefix(path, "/auth/") {
		return true
	}
	return false
}

func acceptsHTML(r *http.Request) bool {
	for _, v := range r.Header.Values("Accept") {
		if strings.Contains(v, "text/html") {
			return true
		}
	}
	return false
}

// SetSessionCookie writes the session cookie with HttpOnly, SameSite=Lax, Path=/,
// and Secure set unconditionally.
func SetSessionCookie(w http.ResponseWriter, _ *http.Request, value string) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   true,
	})
}

// ClearSessionCookie clears the session cookie with HttpOnly, SameSite=Lax, Path=/,
// and Secure set unconditionally.
func ClearSessionCookie(w http.ResponseWriter, _ *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   true,
	})
}
