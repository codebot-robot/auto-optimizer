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
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
)

const (
	jwtIssuedAtSkew    = 60 * time.Second
	jwtLifetime        = 9 * time.Minute
	tokenRefreshMargin = 5 * time.Minute
	pushTokenTimeout   = 30 * time.Second
)

var jwtHeader = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))

type installationToken struct {
	token     string
	expiresAt time.Time
	remote    forge.Forge
}

type appAuth struct {
	appID         string
	key           *rsa.PrivateKey
	installations map[string]int64
	tokens        map[int64]installationToken
}

// WithClock overrides the clock used for JWT signing and token expiry checks.
func WithClock(now func() time.Time) Option {
	return func(f *Forge) { f.now = now }
}

// NewApp creates a GitHub Forge authenticated as a GitHub App using a PKCS#1 or PKCS#8 RSA PEM key.
func NewApp(appID string, privateKeyPEM []byte, opts ...Option) (*Forge, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, errors.New("GitHub App ID is required")
	}
	key, err := parseRSAPrivateKey(privateKeyPEM)
	if err != nil {
		return nil, err
	}
	f := newForge(opts...)
	f.app = &appAuth{
		appID:         appID,
		key:           key,
		installations: make(map[string]int64),
		tokens:        make(map[int64]installationToken),
	}
	return f, nil
}

func parseRSAPrivateKey(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("decoding GitHub App private key: no PEM block found")
	}
	switch block.Type {
	case "RSA PRIVATE KEY":
		return x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing PKCS#8 private key: %w", err)
		}
		key, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("PKCS#8 private key is %T, want *rsa.PrivateKey", parsed)
		}
		return key, nil
	default:
		return nil, fmt.Errorf("unsupported PEM block type %q", block.Type)
	}
}

func (a *appAuth) signJWT(now time.Time) (string, error) {
	claims, err := json.Marshal(struct {
		IAT int64  `json:"iat"`
		EXP int64  `json:"exp"`
		ISS string `json:"iss"`
	}{now.Add(-jwtIssuedAtSkew).Unix(), now.Add(jwtLifetime).Unix(), a.appID})
	if err != nil {
		return "", err
	}
	input := jwtHeader + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, a.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("signing GitHub App JWT: %w", err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (f *Forge) clock() time.Time {
	if f.now != nil {
		return f.now()
	}
	return time.Now()
}

func (f *Forge) appRequest(ctx context.Context, method, path string, out any) error {
	jwt, err := f.app.signJWT(f.clock())
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, f.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	req.Header.Set("Authorization", "Bearer "+jwt)
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GitHub App %s %s: %w", method, path, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			scoped.Log(ctx).V(2).Info("failed to close response body", "error", closeErr)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading GitHub App %s %s response: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GitHub App %s %s returned %d", method, path, resp.StatusCode)
	}
	return json.Unmarshal(body, out)
}

func (f *Forge) installationTokenForRepo(ctx context.Context, repoURL string) (installationToken, error) {
	owner, name, err := parseRepo(repoURL)
	if err != nil {
		return installationToken{}, err
	}
	repoKey := strings.ToLower(owner + "/" + name)

	f.mu.Lock()
	installID, ok := f.app.installations[repoKey]
	f.mu.Unlock()
	if !ok {
		var inst struct {
			ID int64 `json:"id"`
		}
		if err := f.appRequest(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/%s/installation", owner, name), &inst); err != nil {
			return installationToken{}, err
		}
		if inst.ID == 0 {
			return installationToken{}, fmt.Errorf("invalid installation ID for %s/%s", owner, name)
		}
		installID = inst.ID
		f.mu.Lock()
		f.app.installations[repoKey] = installID
		f.mu.Unlock()
	}

	now := f.clock()
	f.mu.Lock()
	cached, ok := f.app.tokens[installID]
	f.mu.Unlock()
	if ok && now.Add(tokenRefreshMargin).Before(cached.expiresAt) {
		return cached, nil
	}

	var tokResp struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := f.appRequest(ctx, http.MethodPost, fmt.Sprintf("/app/installations/%d/access_tokens", installID), &tokResp); err != nil {
		return installationToken{}, err
	}
	if tokResp.Token == "" {
		return installationToken{}, fmt.Errorf("empty installation token for installation %d", installID)
	}
	var remoteOpts []forge.GitRemoteOption
	if f.httpClient != nil && f.httpClient != http.DefaultClient {
		remoteOpts = append(remoteOpts, forge.WithHTTPClient(f.httpClient))
	}
	tok := installationToken{
		token:     tokResp.Token,
		expiresAt: tokResp.ExpiresAt,
		remote:    forge.NewGitRemoteWithCredentials("x-access-token", tokResp.Token, remoteOpts...),
	}
	f.mu.Lock()
	f.app.tokens[installID] = tok
	f.mu.Unlock()
	return tok, nil
}
