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

package session

import (
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DefaultTokenTTL is the default lifetime for signed sandboxd bearer tokens.
const DefaultTokenTTL = 5 * time.Minute

// SignedTokenPayload is the JSON payload contained in an asymmetric bearer token.
type SignedTokenPayload struct {
	Aud   string `json:"aud"`
	Iat   int64  `json:"iat"`
	Exp   int64  `json:"exp"`
	Nonce string `json:"nonce"`
}

// MintSandboxToken signs a fresh bearer token for sandboxd in the v1 format:
//
//	v1.<base64url(payload)>.<base64url(signature)>
//
// with payload {"aud":aud,"iat":now,"exp":now+ttl,"nonce":<random hex>}.
func MintSandboxToken(priv ed25519.PrivateKey, aud string, ttl time.Duration) (string, error) {
	return MintSandboxTokenAt(priv, aud, ttl, time.Now())
}

// MintSandboxTokenAt mints a signed bearer token with an explicit reference time.
func MintSandboxTokenAt(priv ed25519.PrivateKey, aud string, ttl time.Duration, now time.Time) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	if ttl <= 0 {
		ttl = DefaultTokenTTL
	}

	nonceBytes := make([]byte, 16)
	if _, err := cryptorand.Read(nonceBytes); err != nil {
		return "", fmt.Errorf("generating nonce: %w", err)
	}

	payload := SignedTokenPayload{
		Aud:   aud,
		Iat:   now.Unix(),
		Exp:   now.Add(ttl).Unix(),
		Nonce: hex.EncodeToString(nonceBytes),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshaling token payload: %w", err)
	}

	sig := ed25519.Sign(priv, payloadBytes)
	if len(sig) != ed25519.SignatureSize {
		return "", errors.New("failed to generate ed25519 signature")
	}

	encodedPayload := base64.RawURLEncoding.EncodeToString(payloadBytes)
	encodedSig := base64.RawURLEncoding.EncodeToString(sig)

	return fmt.Sprintf("v1.%s.%s", encodedPayload, encodedSig), nil
}

// LoadOrGenerateSigningKey loads an ed25519 private key from path. The file
// must contain exactly a raw 64-byte ed25519 private key. If the file does
// not exist, it generates a new ed25519 keypair and writes the raw private key
// to path with mode 0600.
func LoadOrGenerateSigningKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		if len(data) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("invalid ed25519 private key in %s: length %d, want %d", path, len(data), ed25519.PrivateKeySize)
		}
		return ed25519.PrivateKey(data), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading signing key from %s: %w", path, err)
	}

	_, priv, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating ed25519 key: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("creating directory for signing key: %w", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil, fmt.Errorf("reading concurrent signing key: %w", readErr)
			}
			if len(data) != ed25519.PrivateKeySize {
				return nil, fmt.Errorf("invalid ed25519 private key in %s: length %d, want %d", path, len(data), ed25519.PrivateKeySize)
			}
			return ed25519.PrivateKey(data), nil
		}
		return nil, fmt.Errorf("writing signing key to %s: %w", path, err)
	}

	if _, err := f.Write(priv); err != nil {
		closeErr := f.Close()
		return nil, errors.Join(fmt.Errorf("writing signing key data: %w", err), closeErr)
	}
	if err := f.Sync(); err != nil {
		closeErr := f.Close()
		return nil, errors.Join(fmt.Errorf("syncing signing key file: %w", err), closeErr)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("closing signing key file: %w", err)
	}
	return priv, nil
}
