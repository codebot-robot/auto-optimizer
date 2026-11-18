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
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
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

// CSRFToken derives an anti-forgery token from a session cookie value and session signing key.
func CSRFToken(sessionValue string, key []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(sessionValue))
	return hex.EncodeToString(mac.Sum(nil))
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
func SetSessionCookie(w http.ResponseWriter, value string) {
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
func ClearSessionCookie(w http.ResponseWriter) {
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

// SessionDuration is the lifetime of an authenticated web session.
const SessionDuration = 24 * time.Hour

// OAuth cookie names.
const (
	// OAuthStateCookieName is the cookie name for the OAuth CSRF state parameter.
	OAuthStateCookieName = "ao_oauth_state"
	// OAuthNextCookieName is the cookie name for preserving the post-login destination path.
	OAuthNextCookieName = "ao_oauth_next"
)

// GitHubOptions configures GitHub OAuth authentication for the web UI.
type GitHubOptions struct {
	ClientID     string
	ClientSecret string
	PublicURL    string
	AllowedUsers []string
	BaseURL      string
	APIURL       string
}

// InitDefaults fills zero-valued fields with defaults and environment variables.
func (o *GitHubOptions) InitDefaults() {
	if o == nil {
		return
	}
	if o.BaseURL == "" {
		if env := os.Getenv("AO_GITHUB_BASE_URL"); env != "" {
			o.BaseURL = env
		} else {
			o.BaseURL = "https://github.com"
		}
	}
	if o.APIURL == "" {
		if env := os.Getenv("AO_GITHUB_API_URL"); env != "" {
			o.APIURL = env
		} else {
			o.APIURL = "https://api.github.com"
		}
	}
	if o.ClientID == "" {
		o.ClientID = os.Getenv("AO_GITHUB_CLIENT_ID")
	}
	if o.ClientSecret == "" {
		o.ClientSecret = os.Getenv("AO_GITHUB_CLIENT_SECRET")
	}
	if len(o.AllowedUsers) == 0 {
		if env := os.Getenv("AO_GITHUB_ALLOWED_USERS"); env != "" {
			for _, u := range strings.Split(env, ",") {
				if u = strings.TrimSpace(u); u != "" {
					o.AllowedUsers = append(o.AllowedUsers, u)
				}
			}
		}
	}
}

// AddFlags binds GitHub OAuth configuration flags to fs.
func (o *GitHubOptions) AddFlags(fs *pflag.FlagSet) {
	o.InitDefaults()
	fs.StringVar(&o.ClientID, "github-client-id", o.ClientID, "GitHub OAuth client ID")
	fs.StringSliceVar(&o.AllowedUsers, "github-allowed-user", o.AllowedUsers, "GitHub user login allowed to access the web UI (repeatable, comma-separated)")
	fs.StringVar(&o.BaseURL, "github-base-url", o.BaseURL, "GitHub base URL for OAuth")
	fs.StringVar(&o.APIURL, "github-api-url", o.APIURL, "GitHub API base URL")
}

// Validate reports any invalid or missing configuration in o.
func (o *GitHubOptions) Validate() error {
	if o == nil {
		return errors.New("github options are required")
	}
	if o.ClientID == "" {
		return errors.New("github client ID is required")
	}
	if o.ClientSecret == "" {
		return errors.New("github client secret is required (AO_GITHUB_CLIENT_SECRET)")
	}
	if len(o.AllowedUsers) == 0 {
		return errors.New("at least one allowed user is required (--github-allowed-user)")
	}
	if o.PublicURL == "" {
		return errors.New("public URL is required")
	}
	u, err := url.Parse(o.PublicURL)
	if err != nil || u.Host == "" {
		return fmt.Errorf("invalid public URL %q", o.PublicURL)
	}
	host := u.Hostname()
	isLocal := host == "localhost" || host == "127.0.0.1" || host == "::1"
	if u.Scheme == "http" {
		if !isLocal {
			return fmt.Errorf("invalid public URL %q: scheme must be https (http is only permitted for localhost/127.0.0.1/[::1])", o.PublicURL)
		}
	} else if u.Scheme != "https" {
		return fmt.Errorf("invalid public URL %q: scheme must be https", o.PublicURL)
	}
	if o.BaseURL == "" {
		return errors.New("github base URL is required")
	}
	if _, err := url.Parse(o.BaseURL); err != nil {
		return fmt.Errorf("invalid base URL %q: %w", o.BaseURL, err)
	}
	if o.APIURL == "" {
		return errors.New("github API URL is required")
	}
	if _, err := url.Parse(o.APIURL); err != nil {
		return fmt.Errorf("invalid API URL %q: %w", o.APIURL, err)
	}
	return nil
}

// NewGitHubHandler returns an HTTP handler that serves GitHub OAuth routes (/login, /auth/github/callback, and /logout)
// and delegates all other requests to next wrapped in NewMiddleware.
func NewGitHubHandler(opts GitHubOptions, key []byte, now func() time.Time, next http.Handler) (http.Handler, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	authed, err := NewMiddleware(key, now, next)
	if err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}

	return &gitHubHandler{
		opts:   opts,
		key:    key,
		now:    now,
		authed: authed,
		client: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

type gitHubHandler struct {
	opts   GitHubOptions
	key    []byte
	now    func() time.Time
	authed http.Handler
	client *http.Client
}

func (h *gitHubHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case (r.URL.Path == "/login" || r.URL.Path == "/login/") && r.Method == http.MethodGet:
		h.handleLogin(w, r)
	case r.URL.Path == "/auth/github/callback" && r.Method == http.MethodGet:
		h.handleCallback(w, r)
	case (r.URL.Path == "/logout" || r.URL.Path == "/logout/") && r.Method == http.MethodPost:
		ClearSessionCookie(w)
		http.Redirect(w, r, "/login", http.StatusSeeOther)
	default:
		h.authed.ServeHTTP(w, r)
	}
}

func (h *gitHubHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	next := r.URL.Query().Get("next")
	if !isLocalPath(next) {
		next = "/"
	}

	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		http.Error(w, "Failed to generate state", http.StatusInternalServerError)
		return
	}
	state := hex.EncodeToString(b)

	setOAuthCookie(w, OAuthStateCookieName, state, h.now)
	setOAuthCookie(w, OAuthNextCookieName, next, h.now)

	redirectURI := strings.TrimRight(h.opts.PublicURL, "/") + "/auth/github/callback"
	q := url.Values{
		"client_id":    {h.opts.ClientID},
		"redirect_uri": {redirectURI},
		"state":        {state},
	}
	authURL := strings.TrimRight(h.opts.BaseURL, "/") + "/login/oauth/authorize?" + q.Encode()
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (h *gitHubHandler) handleCallback(w http.ResponseWriter, r *http.Request) {
	stateCookie, err := r.Cookie(OAuthStateCookieName)
	if err != nil || stateCookie.Value == "" {
		http.Error(w, "Missing state cookie", http.StatusBadRequest)
		return
	}

	queryState := r.URL.Query().Get("state")
	if queryState == "" || subtle.ConstantTimeCompare([]byte(stateCookie.Value), []byte(queryState)) != 1 {
		http.Error(w, "Invalid state parameter", http.StatusBadRequest)
		return
	}

	clearOAuthCookie(w, OAuthStateCookieName)

	next := "/"
	if c, err := r.Cookie(OAuthNextCookieName); err == nil && isLocalPath(c.Value) {
		next = c.Value
	}
	clearOAuthCookie(w, OAuthNextCookieName)

	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Missing code parameter", http.StatusBadRequest)
		return
	}

	tokenEndpoint := strings.TrimRight(h.opts.BaseURL, "/") + "/login/oauth/access_token"
	redirectURI := strings.TrimRight(h.opts.PublicURL, "/") + "/auth/github/callback"
	form := url.Values{
		"client_id":     {h.opts.ClientID},
		"client_secret": {h.opts.ClientSecret},
		"code":          {code},
		"redirect_uri":  {redirectURI},
	}
	tokenReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		http.Error(w, "Failed to create token request", http.StatusInternalServerError)
		return
	}
	tokenReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokenReq.Header.Set("Accept", "application/json")

	tokenResp, err := h.client.Do(tokenReq)
	if err != nil {
		http.Error(w, "Token exchange failed", http.StatusBadGateway)
		return
	}
	defer func() {
		if closeErr := tokenResp.Body.Close(); closeErr != nil {
			scoped.Log(r.Context()).V(2).Info("failed to close response body", "error", closeErr)
		}
	}()

	if tokenResp.StatusCode != http.StatusOK {
		http.Error(w, "Token exchange failed", http.StatusBadGateway)
		return
	}

	var tokenData struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	if err := json.NewDecoder(tokenResp.Body).Decode(&tokenData); err != nil || tokenData.AccessToken == "" {
		http.Error(w, "Invalid token response", http.StatusBadGateway)
		return
	}

	userEndpoint := strings.TrimRight(h.opts.APIURL, "/") + "/user"
	userReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, userEndpoint, nil)
	if err != nil {
		http.Error(w, "Failed to create user request", http.StatusInternalServerError)
		return
	}
	userReq.Header.Set("Authorization", "Bearer "+tokenData.AccessToken)
	userReq.Header.Set("Accept", "application/json")

	userResp, err := h.client.Do(userReq)
	if err != nil {
		http.Error(w, "Failed to fetch user", http.StatusBadGateway)
		return
	}
	defer func() {
		if closeErr := userResp.Body.Close(); closeErr != nil {
			scoped.Log(r.Context()).V(2).Info("failed to close response body", "error", closeErr)
		}
	}()

	if userResp.StatusCode != http.StatusOK {
		http.Error(w, "Failed to fetch user", http.StatusBadGateway)
		return
	}

	var userData struct {
		Login string `json:"login"`
	}
	if err := json.NewDecoder(userResp.Body).Decode(&userData); err != nil || userData.Login == "" {
		http.Error(w, "Invalid user response", http.StatusBadGateway)
		return
	}

	allowed := false
	for _, u := range h.opts.AllowedUsers {
		if strings.EqualFold(u, userData.Login) {
			allowed = true
			break
		}
	}
	if !allowed {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusForbidden)
		body := fmt.Sprintf("<!doctype html><html><head><title>403 Forbidden</title></head><body><h1>403 Forbidden</h1><p>User %q is not authorized.</p></body></html>\n", html.EscapeString(userData.Login))
		if _, err := w.Write([]byte(body)); err != nil {
			return
		}
		return
	}

	sess := Session{
		Login:   userData.Login,
		Expires: h.now().Add(SessionDuration),
	}
	encoded, err := sess.Encode(h.key)
	if err != nil {
		http.Error(w, "Failed to encode session", http.StatusInternalServerError)
		return
	}
	SetSessionCookie(w, encoded)
	http.Redirect(w, r, next, http.StatusSeeOther)
}

func setOAuthCookie(w http.ResponseWriter, name, value string, now func() time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		MaxAge:   300,
		Expires:  now().Add(5 * time.Minute),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   true,
	})
}

func clearOAuthCookie(w http.ResponseWriter, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   true,
	})
}

func isLocalPath(p string) bool {
	if p == "" || !strings.HasPrefix(p, "/") || strings.HasPrefix(p, "//") || strings.Contains(p, "\\") {
		return false
	}
	u, err := url.Parse(p)
	if err != nil {
		return false
	}
	return u.Scheme == "" && u.Host == "" && strings.HasPrefix(u.Path, "/") && !strings.HasPrefix(u.Path, "//")
}
