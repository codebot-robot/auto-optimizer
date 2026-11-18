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

// Package portal provides a client for binding egress policies to sandbox pods
// via the Service Portal Controller.
package portal

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"k8s.io/apimachinery/pkg/types"

	portalsv1 "github.com/gke-labs/service-portals/pkg/portals/v1"
)

// DefaultEgressHosts is the default list of allowed egress hosts for sandbox pods.
var DefaultEgressHosts = []string{
	"github.com",
	"proxy.golang.org",
	"sum.golang.org",
	"registry.npmjs.org",
	"pypi.org",
	"files.pythonhosted.org",
}

// Options configures the Service Portal client.
type Options struct {
	ControllerAddr string
	CAFile         string
	TokenFile      string
	EgressHosts    []string
}

// InitDefaults fills every zero-valued field that has a default with its initial value.
func (o *Options) InitDefaults() {
	if o == nil {
		return
	}
	if len(o.EgressHosts) == 0 {
		o.EgressHosts = append([]string(nil), DefaultEgressHosts...)
	}
}

// AddFlags binds the portal configuration flags to fs.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.InitDefaults()
	fs.StringVar(&o.ControllerAddr, "portal-controller-addr", o.ControllerAddr, "Address of the Service Portal Controller")
	fs.StringVar(&o.CAFile, "portal-controller-ca-file", o.CAFile, "Path to CA certificate for verifying the Service Portal Controller TLS certificate")
	fs.StringVar(&o.TokenFile, "portal-token-file", o.TokenFile, "Path to projected service account token file for authenticating with the Service Portal Controller")
	fs.StringSliceVar(&o.EgressHosts, "sandbox-egress-hosts", o.EgressHosts, "Allowed egress hosts for sandboxes")
}

// Validate reports any invalid option values. The portal is off when ControllerAddr is empty.
func (o *Options) Validate() error {
	if o == nil {
		return errors.New("portal options are required")
	}
	if o.ControllerAddr == "" {
		return nil
	}
	if o.CAFile == "" {
		return errors.New("portal CA file is required when controller address is set")
	}
	if o.TokenFile == "" {
		return errors.New("portal token file is required when controller address is set")
	}
	return nil
}

// Enabled reports whether the Service Portal client is configured.
func (o Options) Enabled() bool {
	return o.ControllerAddr != ""
}

// Binder binds an egress policy to a sandbox pod.
type Binder interface {
	Bind(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error
}

// PolicyRequest identifies the sandbox and its session for which an egress policy is requested.
type PolicyRequest struct {
	RepoURL     string
	GoalName    string
	RunID       string
	SandboxName string
}

// PolicyFactory generates an EgressPolicy for a given sandbox and session.
type PolicyFactory func(ctx context.Context, req PolicyRequest) (*portalsv1.EgressPolicy, error)

// Client is a gRPC client that binds egress policies via the Service Portal Controller.
type Client struct {
	conn      *grpc.ClientConn
	client    portalsv1.PodBindServiceClient
	tokenFile string
}

var _ Binder = (*Client)(nil)

// New creates a new portal Client configured with opts.
func New(opts Options, dialOpts ...grpc.DialOption) (*Client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.ControllerAddr == "" {
		return nil, errors.New("portal controller address is required")
	}
	creds, err := credentials.NewClientTLSFromFile(opts.CAFile, "")
	if err != nil {
		return nil, fmt.Errorf("loading CA cert from %s: %w", opts.CAFile, err)
	}
	allDialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(creds)}, dialOpts...)
	conn, err := grpc.NewClient(opts.ControllerAddr, allDialOpts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to controller at %s: %w", opts.ControllerAddr, err)
	}
	return &Client{
		conn:      conn,
		client:    portalsv1.NewPodBindServiceClient(conn),
		tokenFile: opts.TokenFile,
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Bind binds an egress policy to the pod identified by podUID.
func (c *Client) Bind(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error {
	tokenBytes, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return fmt.Errorf("reading token file: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	callCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	resp, err := c.client.Bind(callCtx, &portalsv1.PodPolicy{
		PodUid: string(podUID),
		Policy: policy,
		Wait:   true,
	})
	if err != nil {
		return fmt.Errorf("bind RPC failed: %w", err)
	}
	if resp == nil {
		return errors.New("bind response is nil")
	}

	switch resp.GetStatus() {
	case portalsv1.AcceptanceStatus_ACCEPTANCE_STATUS_ACCEPTED:
		return nil
	case portalsv1.AcceptanceStatus_ACCEPTANCE_STATUS_REFUSED:
		if reason := resp.GetRefusalReason(); reason != "" {
			return fmt.Errorf("bind refused: %s", reason)
		}
		return errors.New("bind refused")
	case portalsv1.AcceptanceStatus_ACCEPTANCE_STATUS_PENDING:
		if reason := resp.GetRefusalReason(); reason != "" {
			return fmt.Errorf("bind pending: %s", reason)
		}
		if len(resp.GetMissingSecrets()) > 0 {
			return fmt.Errorf("bind pending: missing secrets: %s", strings.Join(resp.GetMissingSecrets(), ", "))
		}
		return errors.New("bind pending")
	default:
		return fmt.Errorf("unexpected bind status: %v", resp.GetStatus())
	}
}

// PolicyForHosts returns an EgressPolicy with a single HTTPS rule for hosts,
// with no effects and no credentials.
func PolicyForHosts(hosts []string) *portalsv1.EgressPolicy {
	return &portalsv1.EgressPolicy{
		Rules: []*portalsv1.EgressRule{
			{
				Https: &portalsv1.HttpsRule{
					Hostnames: append([]string(nil), hosts...),
				},
			},
		},
	}
}

func repoHost(repoURL string) string {
	raw := strings.TrimSpace(repoURL)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "@") && strings.Contains(raw, ":") && !strings.Contains(raw, "://") {
		at := strings.Index(raw, "@")
		colon := strings.Index(raw, ":")
		if colon > at {
			return raw[at+1 : colon]
		}
	}
	if !strings.Contains(raw, "://") {
		if strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, ".") {
			return ""
		}
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if u.Scheme == "file" {
		return ""
	}
	h := u.Hostname()
	if h != "" {
		return h
	}
	return u.Host
}

// HostListPolicyFactory returns a PolicyFactory that creates an EgressPolicy with a
// single HTTPS rule containing the configured hosts plus the host from the request's
// repository URL, without duplicates.
func HostListPolicyFactory(hosts []string) PolicyFactory {
	return func(ctx context.Context, req PolicyRequest) (*portalsv1.EgressPolicy, error) {
		var combined []string
		seen := make(map[string]bool)
		for _, h := range hosts {
			if h != "" && !seen[h] {
				seen[h] = true
				combined = append(combined, h)
			}
		}
		if h := repoHost(req.RepoURL); h != "" && !seen[h] {
			seen[h] = true
			combined = append(combined, h)
		}
		return PolicyForHosts(combined), nil
	}
}

// PolicyHostnames returns all distinct hostnames referenced across rules in policy.
func PolicyHostnames(policy *portalsv1.EgressPolicy) []string {
	if policy == nil {
		return nil
	}
	var hosts []string
	seen := make(map[string]bool)
	for _, r := range policy.GetRules() {
		var list []string
		if r.GetHttps() != nil {
			list = r.GetHttps().GetHostnames()
		} else if r.GetHttp() != nil {
			list = r.GetHttp().GetHostnames()
		} else if r.GetTlsPassthrough() != nil {
			list = r.GetTlsPassthrough().GetHostnames()
		}
		for _, h := range list {
			if !seen[h] {
				seen[h] = true
				hosts = append(hosts, h)
			}
		}
	}
	return hosts
}
