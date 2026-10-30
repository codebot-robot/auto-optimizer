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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMintSandboxToken(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	aud := "ao-sandboxes/test-sandbox-123"
	now := time.Now().Truncate(time.Second)
	ttl := 5 * time.Minute

	token, err := MintSandboxTokenAt(priv, aud, ttl, now)
	if err != nil {
		t.Fatalf("MintSandboxTokenAt: %v", err)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token format mismatch: want 3 parts separated by '.', got %d: %q", len(parts), token)
	}

	if parts[0] != "v1" {
		t.Errorf("token version = %q, want v1", parts[0])
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}

	var payload SignedTokenPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		t.Fatalf("failed to unmarshal payload JSON: %v", err)
	}

	if payload.Aud != aud {
		t.Errorf("payload.Aud = %q, want %q", payload.Aud, aud)
	}
	if payload.Iat != now.Unix() {
		t.Errorf("payload.Iat = %d, want %d", payload.Iat, now.Unix())
	}
	if payload.Exp != now.Add(ttl).Unix() {
		t.Errorf("payload.Exp = %d, want %d", payload.Exp, now.Add(ttl).Unix())
	}
	if payload.Nonce == "" {
		t.Errorf("payload.Nonce is empty")
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("failed to decode signature: %v", err)
	}

	if !ed25519.Verify(pub, payloadBytes, sigBytes) {
		t.Errorf("ed25519.Verify failed on payload and signature")
	}

	// Verify tampering with payload fails verification
	tampered := append([]byte(nil), payloadBytes...)
	tampered[0] ^= 0xff
	if ed25519.Verify(pub, tampered, sigBytes) {
		t.Errorf("ed25519.Verify succeeded on tampered payload")
	}

	// Verify verification fails with a different key
	pub2, _, _ := ed25519.GenerateKey(cryptorand.Reader)
	if ed25519.Verify(pub2, payloadBytes, sigBytes) {
		t.Errorf("ed25519.Verify succeeded with different public key")
	}
}

func TestMintSandboxToken_InvalidKey(t *testing.T) {
	_, err := MintSandboxToken([]byte("short-key"), "aud", 5*time.Minute)
	if err == nil {
		t.Error("MintSandboxToken with invalid key size should fail, got nil err")
	}
}

func TestLoadOrGenerateSigningKey(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "sub", "sandbox-signing-key")

	// 1. Missing file generates a new key with mode 0600
	key1, err := LoadOrGenerateSigningKey(keyPath)
	if err != nil {
		t.Fatalf("LoadOrGenerateSigningKey (generate): %v", err)
	}
	if len(key1) != ed25519.PrivateKeySize {
		t.Fatalf("key1 len = %d, want %d", len(key1), ed25519.PrivateKeySize)
	}

	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("key file mode = %o, want 0600", perm)
	}

	// 2. Existing file loads the same key
	key2, err := LoadOrGenerateSigningKey(keyPath)
	if err != nil {
		t.Fatalf("LoadOrGenerateSigningKey (load): %v", err)
	}
	if !key1.Equal(key2) {
		t.Errorf("loaded key2 does not match generated key1")
	}

	// 3. Reject non-64-byte key files (e.g. 32-byte seed or base64 text)
	badPath := filepath.Join(dir, "short-key")
	if err := os.WriteFile(badPath, make([]byte, 32), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := LoadOrGenerateSigningKey(badPath); err == nil {
		t.Errorf("LoadOrGenerateSigningKey should reject 32-byte file")
	}

	b64Path := filepath.Join(dir, "b64-key")
	if err := os.WriteFile(b64Path, []byte(base64.StdEncoding.EncodeToString(key1)), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := LoadOrGenerateSigningKey(b64Path); err == nil {
		t.Errorf("LoadOrGenerateSigningKey should reject base64 encoded key file")
	}
}
