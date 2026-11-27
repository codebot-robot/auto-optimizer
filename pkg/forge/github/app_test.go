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

package github

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func generateTestRSAKey(t *testing.T) (*rsa.PrivateKey, []byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating RSA key: %v", err)
	}
	pkcs1PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	pkcs8DER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling PKCS#8 key: %v", err)
	}
	pkcs8PEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8DER,
	})
	return key, pkcs1PEM, pkcs8PEM
}

func verifyAppJWT(t *testing.T, authHeader string, pub *rsa.PublicKey, wantAppID string, now time.Time) {
	t.Helper()
	if !strings.HasPrefix(authHeader, "Bearer ") {
		t.Fatalf("Authorization = %q, want Bearer prefix", authHeader)
	}
	parts := strings.Split(strings.TrimPrefix(authHeader, "Bearer "), ".")
	if len(parts) != 3 {
		t.Fatalf("JWT has %d parts, want 3", len(parts))
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decoding JWT header: %v", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		t.Fatalf("unmarshaling JWT header: %v", err)
	}
	if header.Alg != "RS256" || header.Typ != "JWT" {
		t.Errorf("JWT header = %+v, want alg=RS256 typ=JWT", header)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decoding JWT payload: %v", err)
	}
	var claims struct {
		IAT int64  `json:"iat"`
		EXP int64  `json:"exp"`
		ISS string `json:"iss"`
	}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		t.Fatalf("unmarshaling JWT payload: %v", err)
	}
	if claims.ISS != wantAppID {
		t.Errorf("JWT iss = %q, want %q", claims.ISS, wantAppID)
	}
	if wantIAT := now.Add(-60 * time.Second).Unix(); claims.IAT != wantIAT {
		t.Errorf("JWT iat = %d, want %d", claims.IAT, wantIAT)
	}
	if wantEXP := now.Add(9 * time.Minute).Unix(); claims.EXP != wantEXP {
		t.Errorf("JWT exp = %d, want %d", claims.EXP, wantEXP)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decoding JWT signature: %v", err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
		t.Errorf("RS256 signature verification failed: %v", err)
	}
}

func TestNewApp_KeyFormatsAndErrors(t *testing.T) {
	_, pkcs1PEM, pkcs8PEM := generateTestRSAKey(t)

	if _, err := NewApp("12345", pkcs1PEM); err != nil {
		t.Errorf("NewApp with PKCS#1 key failed: %v", err)
	}
	if _, err := NewApp("12345", pkcs8PEM); err != nil {
		t.Errorf("NewApp with PKCS#8 key failed: %v", err)
	}

	if _, err := NewApp("", pkcs1PEM); err == nil {
		t.Error("expected error for empty appID, got nil")
	}
	if _, err := NewApp("12345", []byte("not-a-pem")); err == nil {
		t.Error("expected error for invalid PEM, got nil")
	}
	if _, err := NewApp("12345", pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("bad")})); err == nil {
		t.Error("expected error for unsupported PEM block type, got nil")
	}

	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating EC key: %v", err)
	}
	ecDER, err := x509.MarshalPKCS8PrivateKey(ecKey)
	if err != nil {
		t.Fatalf("marshaling EC PKCS#8 key: %v", err)
	}
	ecPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: ecDER})
	if _, err := NewApp("12345", ecPEM); err == nil {
		t.Error("expected error for non-RSA PKCS#8 key, got nil")
	}
}

func TestAppAuth_JWTCachingAndRefresh(t *testing.T) {
	key, pkcs1PEM, _ := generateTestRSAKey(t)

	var mu sync.Mutex
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	getNow := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	setNow := func(ts time.Time) {
		mu.Lock()
		defer mu.Unlock()
		now = ts
	}

	var installLookups atomic.Int32
	var tokenMints atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && (r.URL.Path == "/repos/org1/repoA/installation" || r.URL.Path == "/repos/org1/repoB/installation"):
			verifyAppJWT(t, r.Header.Get("Authorization"), &key.PublicKey, "98765", getNow())
			installLookups.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"id": 4242}`)

		case r.Method == http.MethodPost && r.URL.Path == "/app/installations/4242/access_tokens":
			verifyAppJWT(t, r.Header.Get("Authorization"), &key.PublicKey, "98765", getNow())
			n := tokenMints.Add(1)
			exp := getNow().Add(time.Hour).Format(time.RFC3339)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"token": "ghs_token_%d", "expires_at": %q}`, n, exp)

		default:
			http.Error(w, "unexpected request: "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewApp("98765", pkcs1PEM,
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
		WithClock(getNow),
	)
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}

	// First call for repoA resolves installation 4242 and mints ghs_token_1.
	u, p := client.PushCredentials("https://github.com/org1/repoA")
	if u != "x-access-token" || p != "ghs_token_1" {
		t.Fatalf("first PushCredentials = (%q, %q), want (x-access-token, ghs_token_1)", u, p)
	}
	if installLookups.Load() != 1 || tokenMints.Load() != 1 {
		t.Fatalf("counts after first call: lookups=%d mints=%d, want 1, 1", installLookups.Load(), tokenMints.Load())
	}

	// Second call for repoA (54m59s later, still > 5m before expiry) uses both caches.
	setNow(time.Date(2026, 11, 27, 12, 54, 59, 0, time.UTC))
	u, p = client.PushCredentials("https://github.com/org1/repoA")
	if u != "x-access-token" || p != "ghs_token_1" {
		t.Fatalf("cached PushCredentials = (%q, %q), want (x-access-token, ghs_token_1)", u, p)
	}
	if installLookups.Load() != 1 || tokenMints.Load() != 1 {
		t.Fatalf("counts while cached: lookups=%d mints=%d, want 1, 1", installLookups.Load(), tokenMints.Load())
	}

	// Another repo under the same installation looks up installation once and shares cached token.
	u, p = client.PushCredentials("https://github.com/org1/repoB")
	if u != "x-access-token" || p != "ghs_token_1" {
		t.Fatalf("repoB PushCredentials = (%q, %q), want (x-access-token, ghs_token_1)", u, p)
	}
	if installLookups.Load() != 2 || tokenMints.Load() != 1 {
		t.Fatalf("counts for shared installation: lookups=%d mints=%d, want 2, 1", installLookups.Load(), tokenMints.Load())
	}

	// Advance clock to exactly 5 minutes before expiry: must refresh token without re-looking up installation.
	setNow(time.Date(2026, 11, 27, 12, 55, 0, 0, time.UTC))
	u, p = client.PushCredentials("https://github.com/org1/repoA")
	if u != "x-access-token" || p != "ghs_token_2" {
		t.Fatalf("refreshed PushCredentials = (%q, %q), want (x-access-token, ghs_token_2)", u, p)
	}
	if installLookups.Load() != 2 || tokenMints.Load() != 2 {
		t.Fatalf("counts after refresh: lookups=%d mints=%d, want 2, 2", installLookups.Load(), tokenMints.Load())
	}
}

func TestAppAuth_PullRequestsAndGitRemote(t *testing.T) {
	key, _, pkcs8PEM := generateTestRSAKey(t)
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)

	var gotOpenAuth, gotListAuth, gotGitUser, gotGitPass string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widgets/installation":
			verifyAppJWT(t, r.Header.Get("Authorization"), &key.PublicKey, "555", now)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"id": 77}`)

		case r.Method == http.MethodPost && r.URL.Path == "/app/installations/77/access_tokens":
			verifyAppJWT(t, r.Header.Get("Authorization"), &key.PublicKey, "555", now)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"token": "ghs_install_77", "expires_at": %q}`, now.Add(time.Hour).Format(time.RFC3339))

		case r.Method == http.MethodPost && r.URL.Path == "/repos/acme/widgets/pulls":
			gotOpenAuth = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintln(w, `{"number": 9, "title": "Optimize", "html_url": "https://github.com/acme/widgets/pull/9", "state": "open", "head": {"ref": "ao/g/1"}, "base": {"ref": "main"}}`)

		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widgets/pulls":
			gotListAuth = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `[{"number": 9, "title": "Optimize", "html_url": "https://github.com/acme/widgets/pull/9", "state": "open", "head": {"ref": "ao/g/1"}, "base": {"ref": "main"}}]`)

		case r.Method == http.MethodGet && r.URL.Path == "/acme/widgets/info/refs":
			gotGitUser, gotGitPass, _ = r.BasicAuth()
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			serviceLine := "# service=git-upload-pack\n"
			headLine := "1111111111111111111111111111111111111111 HEAD\x00symref=HEAD:refs/heads/main\n"
			refLine := "1111111111111111111111111111111111111111 refs/heads/main\n"
			fmt.Fprintf(w, "%04x%s0000%04x%s%04x%s0000", len(serviceLine)+4, serviceLine, len(headLine)+4, headLine, len(refLine)+4, refLine)

		default:
			http.Error(w, "not found: "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewApp("555", pkcs8PEM,
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
		WithClock(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}

	ctx := t.Context()
	repoURL := "https://github.com/acme/widgets"

	pr, err := client.OpenPullRequest(ctx, repoURL, "ao/g/1", "main", "Optimize", "Body")
	if err != nil {
		t.Fatalf("OpenPullRequest: %v", err)
	}
	if pr.Number != 9 {
		t.Errorf("pr.Number = %d, want 9", pr.Number)
	}
	if gotOpenAuth != "Bearer ghs_install_77" {
		t.Errorf("OpenPullRequest Authorization = %q, want Bearer ghs_install_77", gotOpenAuth)
	}

	prs, err := client.ListPullRequests(ctx, repoURL, "ao/")
	if err != nil {
		t.Fatalf("ListPullRequests: %v", err)
	}
	if len(prs) != 1 || prs[0].Number != 9 {
		t.Errorf("ListPullRequests = %+v, want PR #9", prs)
	}
	if gotListAuth != "Bearer ghs_install_77" {
		t.Errorf("ListPullRequests Authorization = %q, want Bearer ghs_install_77", gotListAuth)
	}

	gitURL := server.URL + "/acme/widgets"
	branch, err := client.DefaultBranch(ctx, gitURL)
	if err != nil {
		t.Fatalf("DefaultBranch: %v", err)
	}
	if branch != "main" {
		t.Errorf("DefaultBranch = %q, want main", branch)
	}
	if gotGitUser != "x-access-token" || gotGitPass != "ghs_install_77" {
		t.Errorf("git BasicAuth = (%q, %q), want (x-access-token, ghs_install_77)", gotGitUser, gotGitPass)
	}
}

func TestNewFromEnv(t *testing.T) {
	_, pkcs1PEM, pkcs8PEM := generateTestRSAKey(t)
	keyPath := filepath.Join(t.TempDir(), "app.pem")
	if err := os.WriteFile(keyPath, pkcs8PEM, 0600); err != nil {
		t.Fatalf("writing key file: %v", err)
	}

	t.Run("unset returns nil", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "")
		t.Setenv("GITHUB_APP_PRIVATE_KEY", "")
		t.Setenv("GITHUB_APP_PRIVATE_KEY_FILE", "")
		t.Setenv("GITHUB_TOKEN", "")
		f, err := NewFromEnv()
		if err != nil || f != nil {
			t.Errorf("NewFromEnv() = (%v, %v), want (nil, nil)", f, err)
		}
	})

	t.Run("falls back to GITHUB_TOKEN", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "")
		t.Setenv("GITHUB_TOKEN", "ghp_pat")
		f, err := NewFromEnv()
		if err != nil || f == nil {
			t.Fatalf("NewFromEnv() = (%v, %v), want non-nil Forge", f, err)
		}
		if u, p := f.PushCredentials("https://github.com/o/r"); u != "x-access-token" || p != "ghp_pat" {
			t.Errorf("PushCredentials = (%q, %q), want (x-access-token, ghp_pat)", u, p)
		}
	})

	t.Run("uses GITHUB_APP_PRIVATE_KEY over GITHUB_TOKEN", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "101")
		t.Setenv("GITHUB_APP_PRIVATE_KEY", string(pkcs1PEM))
		t.Setenv("GITHUB_APP_PRIVATE_KEY_FILE", "")
		t.Setenv("GITHUB_TOKEN", "ghp_ignored")
		f, err := NewFromEnv()
		if err != nil || f == nil || f.app == nil {
			t.Fatalf("NewFromEnv() = (%v, %v), want App Forge", f, err)
		}
	})

	t.Run("uses GITHUB_APP_PRIVATE_KEY_FILE", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "102")
		t.Setenv("GITHUB_APP_PRIVATE_KEY", "")
		t.Setenv("GITHUB_APP_PRIVATE_KEY_FILE", keyPath)
		f, err := NewFromEnv()
		if err != nil || f == nil || f.app == nil {
			t.Fatalf("NewFromEnv() = (%v, %v), want App Forge", f, err)
		}
	})

	t.Run("fails when GITHUB_APP_ID has no key", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "103")
		t.Setenv("GITHUB_APP_PRIVATE_KEY", "")
		t.Setenv("GITHUB_APP_PRIVATE_KEY_FILE", "")
		if _, err := NewFromEnv(); err == nil {
			t.Error("expected error when private key is missing, got nil")
		}
	})

	t.Run("fails when GITHUB_APP_PRIVATE_KEY is invalid", func(t *testing.T) {
		t.Setenv("GITHUB_APP_ID", "104")
		t.Setenv("GITHUB_APP_PRIVATE_KEY", "not-valid-pem")
		t.Setenv("GITHUB_APP_PRIVATE_KEY_FILE", "")
		if _, err := NewFromEnv(); err == nil {
			t.Error("expected error when private key is invalid, got nil")
		}
	})
}
