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

package portal

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"k8s.io/apimachinery/pkg/types"

	portalsv1 "github.com/gke-labs/service-portals/pkg/portals/v1"
)

func generateTestCertificate(t *testing.T, ip net.IP) (certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: ip.String()},
		IPAddresses:  []net.IP{ip},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	privDer, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDer})
	return certPEM, keyPEM
}

type fakeBindServer struct {
	portalsv1.UnimplementedPodBindServiceServer
	mu         sync.Mutex
	lastPodUID string
	lastToken  string
	lastWait   bool
	resp       *portalsv1.BindResponse
	err        error
}

func (s *fakeBindServer) Bind(ctx context.Context, req *portalsv1.PodPolicy) (*portalsv1.BindResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if auth := md.Get("authorization"); len(auth) > 0 {
			s.lastToken = auth[0]
		}
	}
	s.lastPodUID = req.GetPodUid()
	s.lastWait = req.GetWait()
	if s.err != nil {
		return nil, s.err
	}
	return s.resp, nil
}

func TestOptions_Defaults(t *testing.T) {
	var opts Options
	opts.InitDefaults()

	if diff := cmp.Diff(DefaultEgressHosts, opts.EgressHosts); diff != "" {
		t.Errorf("EgressHosts mismatch (-want +got):\n%s", diff)
	}

	custom := Options{EgressHosts: []string{"custom.org"}}
	custom.InitDefaults()
	if len(custom.EgressHosts) != 1 || custom.EgressHosts[0] != "custom.org" {
		t.Errorf("expected custom hosts preserved, got: %v", custom.EgressHosts)
	}
}

func TestOptions_AddFlags(t *testing.T) {
	var opts Options
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts.AddFlags(fs)

	args := []string{
		"--portal-controller-addr", "10.0.0.1:8443",
		"--portal-controller-ca-file", "/etc/ca.crt",
		"--portal-token-file", "/var/token",
		"--sandbox-egress-hosts", "h1.com,h2.com",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("fs.Parse: %v", err)
	}

	if opts.ControllerAddr != "10.0.0.1:8443" {
		t.Errorf("ControllerAddr = %q, want 10.0.0.1:8443", opts.ControllerAddr)
	}
	if opts.CAFile != "/etc/ca.crt" {
		t.Errorf("CAFile = %q, want /etc/ca.crt", opts.CAFile)
	}
	if opts.TokenFile != "/var/token" {
		t.Errorf("TokenFile = %q, want /var/token", opts.TokenFile)
	}
	expectedHosts := []string{"h1.com", "h2.com"}
	if diff := cmp.Diff(expectedHosts, opts.EgressHosts); diff != "" {
		t.Errorf("EgressHosts mismatch (-want +got):\n%s", diff)
	}
}

func TestOptions_Validate(t *testing.T) {
	var nilOpts *Options
	if err := nilOpts.Validate(); err == nil {
		t.Error("expected error for nil Options, got nil")
	}

	// Portal is off when ControllerAddr is empty
	off := Options{}
	if err := off.Validate(); err != nil {
		t.Errorf("expected empty ControllerAddr to be valid, got: %v", err)
	}

	// Invalid egress host IP
	invalidHost := Options{EgressHosts: []string{"192.168.1.1"}}
	if err := invalidHost.Validate(); err == nil {
		t.Error("expected error for IP address egress host, got nil")
	} else if !strings.Contains(err.Error(), "192.168.1.1") {
		t.Errorf("expected error to name %q, got: %v", "192.168.1.1", err)
	}

	// CAFile missing
	missingCA := Options{ControllerAddr: "127.0.0.1:8443", TokenFile: "/var/token"}
	if err := missingCA.Validate(); err == nil {
		t.Error("expected error for missing CAFile, got nil")
	}

	// TokenFile missing
	missingToken := Options{ControllerAddr: "127.0.0.1:8443", CAFile: "/var/ca.crt"}
	if err := missingToken.Validate(); err == nil {
		t.Error("expected error for missing TokenFile, got nil")
	}

	// Valid
	valid := Options{
		ControllerAddr: "127.0.0.1:8443",
		CAFile:         "/var/ca.crt",
		TokenFile:      "/var/token",
	}
	if err := valid.Validate(); err != nil {
		t.Errorf("unexpected error for valid Options: %v", err)
	}
}

func TestOptions_Enabled(t *testing.T) {
	if (Options{}).Enabled() {
		t.Error("expected Enabled() == false for empty ControllerAddr")
	}
	if !(Options{ControllerAddr: "127.0.0.1:8443"}).Enabled() {
		t.Error("expected Enabled() == true when ControllerAddr is set")
	}
}

func TestPolicyForHosts(t *testing.T) {
	t.Run("bare names only", func(t *testing.T) {
		hosts := []string{"github.com", "proxy.golang.org"}
		policy, err := PolicyForHosts(hosts)
		if err != nil {
			t.Fatalf("PolicyForHosts: %v", err)
		}

		if len(policy.Rules) != 1 {
			t.Fatalf("len(Rules) = %d, want 1", len(policy.Rules))
		}
		r := policy.Rules[0]
		if r.Https == nil {
			t.Fatal("expected Https rule, got nil")
		}
		if r.Http != nil || r.TlsPassthrough != nil {
			t.Errorf("expected only Https rule, got Http=%v, TlsPassthrough=%v", r.Http, r.TlsPassthrough)
		}
		if diff := cmp.Diff(hosts, r.Https.Hostnames); diff != "" {
			t.Errorf("hostnames mismatch (-want +got):\n%s", diff)
		}
		if r.Https.Effects != nil {
			t.Errorf("expected nil effects, got: %v", r.Https.Effects)
		}
		if err := policy.Validate(); err != nil {
			t.Errorf("expected valid policy, got: %v", err)
		}
	})

	t.Run("http with and without a port", func(t *testing.T) {
		hosts := []string{"http://git-server", "http://git-server:8080"}
		policy, err := PolicyForHosts(hosts)
		if err != nil {
			t.Fatalf("PolicyForHosts: %v", err)
		}
		if len(policy.Rules) != 2 {
			t.Fatalf("len(Rules) = %d, want 2", len(policy.Rules))
		}

		r0 := policy.Rules[0]
		if r0.Http == nil {
			t.Fatal("expected Http rule for first entry, got nil")
		}
		if diff := cmp.Diff([]string{"git-server"}, r0.Http.Hostnames); diff != "" {
			t.Errorf("r0 hostnames mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]int32{80}, r0.Http.Ports.Numbers); diff != "" {
			t.Errorf("r0 ports mismatch (-want +got):\n%s", diff)
		}

		r1 := policy.Rules[1]
		if r1.Http == nil {
			t.Fatal("expected Http rule for second entry, got nil")
		}
		if diff := cmp.Diff([]string{"git-server"}, r1.Http.Hostnames); diff != "" {
			t.Errorf("r1 hostnames mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]int32{8080}, r1.Http.Ports.Numbers); diff != "" {
			t.Errorf("r1 ports mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("a mix", func(t *testing.T) {
		hosts := []string{"github.com", "http://git-server:8080", "proxy.golang.org", "http://git-internal"}
		policy, err := PolicyForHosts(hosts)
		if err != nil {
			t.Fatalf("PolicyForHosts: %v", err)
		}
		if len(policy.Rules) != 3 {
			t.Fatalf("len(Rules) = %d, want 3", len(policy.Rules))
		}

		// Https rule for bare names
		r0 := policy.Rules[0]
		if r0.Https == nil {
			t.Fatal("expected Https rule for bare names, got nil")
		}
		if diff := cmp.Diff([]string{"github.com", "proxy.golang.org"}, r0.Https.Hostnames); diff != "" {
			t.Errorf("bare names mismatch (-want +got):\n%s", diff)
		}

		// Http rules
		r1 := policy.Rules[1]
		if r1.Http == nil {
			t.Fatal("expected Http rule, got nil")
		}
		if diff := cmp.Diff([]string{"git-server"}, r1.Http.Hostnames); diff != "" {
			t.Errorf("r1 hostnames mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]int32{8080}, r1.Http.Ports.Numbers); diff != "" {
			t.Errorf("r1 ports mismatch (-want +got):\n%s", diff)
		}

		r2 := policy.Rules[2]
		if r2.Http == nil {
			t.Fatal("expected Http rule, got nil")
		}
		if diff := cmp.Diff([]string{"git-internal"}, r2.Http.Hostnames); diff != "" {
			t.Errorf("r2 hostnames mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]int32{80}, r2.Http.Ports.Numbers); diff != "" {
			t.Errorf("r2 ports mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("an IP address rejected", func(t *testing.T) {
		invalidHosts := []struct {
			entry   string
			wantSub string
		}{
			{"192.168.1.1", "192.168.1.1"},
			{"10.0.0.1", "10.0.0.1"},
			{"http://192.168.1.1", "192.168.1.1"},
			{"http://192.168.1.1:8080", "192.168.1.1"},
			{"::1", "::1"},
			{"http://[::1]:8080", "::1"},
		}
		for _, tc := range invalidHosts {
			t.Run(tc.entry, func(t *testing.T) {
				_, err := PolicyForHosts([]string{tc.entry})
				if err == nil {
					t.Fatalf("expected error for IP address %q, got nil", tc.entry)
				}
				if !strings.Contains(err.Error(), tc.wantSub) {
					t.Errorf("expected error to name %q, got: %v", tc.wantSub, err)
				}
			})
		}
	})
}

func TestHostListPolicyFactory(t *testing.T) {
	tests := []struct {
		name      string
		hosts     []string
		repoURL   string
		wantHosts []string
	}{
		{
			name:      "repo host already in hosts",
			hosts:     []string{"github.com", "proxy.golang.org"},
			repoURL:   "https://github.com/gke-labs/test-repo",
			wantHosts: []string{"github.com", "proxy.golang.org"},
		},
		{
			name:      "repo host added to configured hosts",
			hosts:     []string{"proxy.golang.org", "pypi.org"},
			repoURL:   "https://github.com/gke-labs/test-repo",
			wantHosts: []string{"proxy.golang.org", "pypi.org", "github.com"},
		},
		{
			name:      "duplicates in configured hosts are removed",
			hosts:     []string{"github.com", "proxy.golang.org", "github.com", "pypi.org", "pypi.org"},
			repoURL:   "https://github.com/gke-labs/test-repo",
			wantHosts: []string{"github.com", "proxy.golang.org", "pypi.org"},
		},
		{
			name:      "scp-like repo URL extracts host",
			hosts:     []string{"proxy.golang.org"},
			repoURL:   "git@gitlab.com:gke-labs/test-repo.git",
			wantHosts: []string{"proxy.golang.org", "gitlab.com"},
		},
		{
			name:      "https url with port extracts hostname",
			hosts:     []string{"proxy.golang.org"},
			repoURL:   "https://custom-forge.internal:8443/gke-labs/test-repo",
			wantHosts: []string{"proxy.golang.org", "custom-forge.internal"},
		},
		{
			name:      "empty repo URL leaves hosts unchanged",
			hosts:     []string{"github.com"},
			repoURL:   "",
			wantHosts: []string{"github.com"},
		},
		{
			name:      "local repo path leaves hosts unchanged",
			hosts:     []string{"github.com"},
			repoURL:   "/workspace/test-repo",
			wantHosts: []string{"github.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := HostListPolicyFactory(tt.hosts)
			req := PolicyRequest{
				RepoURL:     tt.repoURL,
				GoalName:    "test-goal",
				RunID:       "run-123",
				SandboxName: "sandbox-123",
			}
			policy, err := factory(t.Context(), req)
			if err != nil {
				t.Fatalf("unexpected factory error: %v", err)
			}
			if len(policy.Rules) != 1 || policy.Rules[0].Https == nil {
				t.Fatalf("expected 1 Https rule, got: %+v", policy.Rules)
			}
			gotHosts := policy.Rules[0].Https.Hostnames
			if diff := cmp.Diff(tt.wantHosts, gotHosts); diff != "" {
				t.Errorf("hostnames mismatch (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("http repo URL with port and no configured hosts", func(t *testing.T) {
		factory := HostListPolicyFactory(nil)
		req := PolicyRequest{
			RepoURL:     "http://custom-forge.internal:8080/gke-labs/test-repo",
			GoalName:    "test-goal",
			RunID:       "run-123",
			SandboxName: "sandbox-123",
		}
		policy, err := factory(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected factory error: %v", err)
		}
		if len(policy.Rules) != 1 {
			t.Fatalf("len(Rules) = %d, want 1", len(policy.Rules))
		}
		r := policy.Rules[0]
		if r.Http == nil {
			t.Fatalf("expected Http rule, got: %+v", r)
		}
		if diff := cmp.Diff([]string{"custom-forge.internal"}, r.Http.Hostnames); diff != "" {
			t.Errorf("hostnames mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]int32{8080}, r.Http.Ports.Numbers); diff != "" {
			t.Errorf("ports mismatch (-want +got):\n%s", diff)
		}
		if r.Https != nil {
			t.Errorf("expected nil Https rule, got: %+v", r.Https)
		}
	})

	t.Run("http repo URL without port with configured hosts", func(t *testing.T) {
		factory := HostListPolicyFactory([]string{"proxy.golang.org"})
		req := PolicyRequest{
			RepoURL:     "http://custom-forge.internal/gke-labs/test-repo",
			GoalName:    "test-goal",
			RunID:       "run-123",
			SandboxName: "sandbox-123",
		}
		policy, err := factory(t.Context(), req)
		if err != nil {
			t.Fatalf("unexpected factory error: %v", err)
		}
		if len(policy.Rules) != 2 {
			t.Fatalf("len(Rules) = %d, want 2", len(policy.Rules))
		}
		if policy.Rules[0].Https == nil {
			t.Fatalf("expected Https rule for rule 0, got nil")
		}
		if diff := cmp.Diff([]string{"proxy.golang.org"}, policy.Rules[0].Https.Hostnames); diff != "" {
			t.Errorf("Https hostnames mismatch (-want +got):\n%s", diff)
		}
		if policy.Rules[1].Http == nil {
			t.Fatalf("expected Http rule for rule 1, got nil")
		}
		if diff := cmp.Diff([]string{"custom-forge.internal"}, policy.Rules[1].Http.Hostnames); diff != "" {
			t.Errorf("Http hostnames mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]int32{80}, policy.Rules[1].Http.Ports.Numbers); diff != "" {
			t.Errorf("Http ports mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestPolicyHostnames(t *testing.T) {
	if got := PolicyHostnames(nil); got != nil {
		t.Errorf("PolicyHostnames(nil) = %v, want nil", got)
	}

	policy := &portalsv1.EgressPolicy{
		Rules: []*portalsv1.EgressRule{
			{
				Https: &portalsv1.HttpsRule{
					Hostnames: []string{"github.com", "proxy.golang.org"},
				},
			},
			{
				Http: &portalsv1.HttpRule{
					Hostnames: []string{"proxy.golang.org", "insecure.org"},
				},
			},
			{
				TlsPassthrough: &portalsv1.TlsPassthroughRule{
					Hostnames: []string{"custom.org", "github.com"},
				},
			},
		},
	}

	want := []string{"github.com", "proxy.golang.org", "insecure.org", "custom.org"}
	got := PolicyHostnames(policy)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("PolicyHostnames mismatch (-want +got):\n%s", diff)
	}
}

func TestBinder_GRPC(t *testing.T) {
	ip := net.ParseIP("127.0.0.1")
	certPEM, keyPEM := generateTestCertificate(t, ip)
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("tls.X509KeyPair: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer lis.Close()

	fakeServer := &fakeBindServer{}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&tlsCert)))
	portalsv1.RegisterPodBindServiceServer(grpcServer, fakeServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	tmpDir := t.TempDir()
	caFile := filepath.Join(tmpDir, "ca.crt")
	if err := os.WriteFile(caFile, certPEM, 0600); err != nil {
		t.Fatalf("writing CA file: %v", err)
	}

	tokenFile := filepath.Join(tmpDir, "token")
	const initialToken = "initial-sa-token"
	if err := os.WriteFile(tokenFile, []byte(initialToken), 0600); err != nil {
		t.Fatalf("writing token file: %v", err)
	}

	opts := Options{
		ControllerAddr: lis.Addr().String(),
		CAFile:         caFile,
		TokenFile:      tokenFile,
	}
	client, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	policy, err := PolicyForHosts(DefaultEgressHosts)
	if err != nil {
		t.Fatalf("PolicyForHosts: %v", err)
	}
	const testPodUID = types.UID("pod-uid-test-1234")

	t.Run("accepted bind returns nil and verifies server state", func(t *testing.T) {
		fakeServer.resp = &portalsv1.BindResponse{
			PodUid: string(testPodUID),
			Bound:  true,
			Status: portalsv1.AcceptanceStatus_ACCEPTANCE_STATUS_ACCEPTED,
		}

		if err := client.Bind(context.Background(), testPodUID, policy); err != nil {
			t.Fatalf("expected nil error on accepted bind, got: %v", err)
		}
		if fakeServer.lastToken != "Bearer "+initialToken {
			t.Errorf("server saw token %q, want %q", fakeServer.lastToken, "Bearer "+initialToken)
		}
		if fakeServer.lastPodUID != string(testPodUID) {
			t.Errorf("server saw podUID %q, want %q", fakeServer.lastPodUID, string(testPodUID))
		}
		if !fakeServer.lastWait {
			t.Errorf("server saw wait=false, want wait=true")
		}
	})

	t.Run("token rotation on every call", func(t *testing.T) {
		const rotatedToken = "rotated-sa-token-999"
		if err := os.WriteFile(tokenFile, []byte(rotatedToken), 0600); err != nil {
			t.Fatalf("updating token file: %v", err)
		}
		fakeServer.resp = &portalsv1.BindResponse{
			PodUid: string(testPodUID),
			Bound:  true,
			Status: portalsv1.AcceptanceStatus_ACCEPTANCE_STATUS_ACCEPTED,
		}

		if err := client.Bind(context.Background(), testPodUID, policy); err != nil {
			t.Fatalf("expected nil error on accepted bind, got: %v", err)
		}
		if fakeServer.lastToken != "Bearer "+rotatedToken {
			t.Errorf("server saw token %q, want %q", fakeServer.lastToken, "Bearer "+rotatedToken)
		}
	})

	t.Run("refused bind returns error with reason", func(t *testing.T) {
		fakeServer.resp = &portalsv1.BindResponse{
			PodUid:        string(testPodUID),
			Status:        portalsv1.AcceptanceStatus_ACCEPTANCE_STATUS_REFUSED,
			RefusalReason: "pod not found on node",
		}

		err := client.Bind(context.Background(), testPodUID, policy)
		if err == nil {
			t.Fatal("expected error on refused bind, got nil")
		}
		if !strings.Contains(err.Error(), "pod not found on node") {
			t.Errorf("expected refusal reason in error, got: %v", err)
		}
	})

	t.Run("pending bind is an error", func(t *testing.T) {
		fakeServer.resp = &portalsv1.BindResponse{
			PodUid:        string(testPodUID),
			Status:        portalsv1.AcceptanceStatus_ACCEPTANCE_STATUS_PENDING,
			RefusalReason: "waiting for node agent",
		}

		err := client.Bind(context.Background(), testPodUID, policy)
		if err == nil {
			t.Fatal("expected error on pending bind, got nil")
		}
		if !strings.Contains(err.Error(), "waiting for node agent") {
			t.Errorf("expected error to mention refusal reason, got: %v", err)
		}
	})

	t.Run("wrong CA fails the call", func(t *testing.T) {
		wrongCertPEM, _ := generateTestCertificate(t, ip)
		wrongCAFile := filepath.Join(tmpDir, "wrong-ca.crt")
		if err := os.WriteFile(wrongCAFile, wrongCertPEM, 0600); err != nil {
			t.Fatalf("writing wrong CA file: %v", err)
		}

		wrongClient, err := New(Options{
			ControllerAddr: lis.Addr().String(),
			CAFile:         wrongCAFile,
			TokenFile:      tokenFile,
		})
		if err != nil {
			t.Fatalf("New with wrong CA: %v", err)
		}
		defer wrongClient.Close()

		err = wrongClient.Bind(context.Background(), testPodUID, policy)
		if err == nil {
			t.Fatal("expected error with wrong CA, got nil")
		}
	})

	t.Run("reading missing token file returns error", func(t *testing.T) {
		badTokenClient, err := New(Options{
			ControllerAddr: lis.Addr().String(),
			CAFile:         caFile,
			TokenFile:      filepath.Join(tmpDir, "nonexistent-token"),
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer badTokenClient.Close()

		err = badTokenClient.Bind(context.Background(), testPodUID, policy)
		if err == nil {
			t.Fatal("expected error for missing token file, got nil")
		}
	})
}
