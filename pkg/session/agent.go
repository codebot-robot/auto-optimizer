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

// Package session implements the agentic loop of the sandboxed-tools example:
// it sends the conversation to an LLM, and executes the tool calls the LLM
// requests inside an Agent Sandbox, creating (and re-creating) the sandbox
// on demand. It is shared by the interactive CLI (cmd/ao) and other front ends.
// Forked from kubernetes-sigs/agent-sandbox/examples/sandboxed-tools (commit f83f2d87c1aeeb90407d8cfe9a0c1fad74044304).
package session

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/client-go/util/retry"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	agentsclientset "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
)

// SandboxInactivityTimeout is how long we keep sandboxes around after we last used them.
const SandboxInactivityTimeout = 5 * time.Minute

// DefaultToolTimeout bounds how long a single tool invocation may run before
// it is cancelled, so a hung command (e.g. "sleep 99999") fails fast instead
// of blocking the agent loop indefinitely.
const DefaultToolTimeout = 2 * time.Minute

// DefaultSandboxdImage is the default container image containing the sandboxd binary, pinned by digest.
const DefaultSandboxdImage = "us-central1-docker.pkg.dev/k8s-staging-images/agent-sandbox/sandboxd:latest-main@sha256:576d312c8e4019a52e58e6adbd1bffb8964ad41ca7fa941cfe8a662c64dca85c"

// SandboxClient is a simple low-level client for managing Sandbox resources directly.
type SandboxClient struct {
	agentsClient agentsclientset.Interface
	coreClient   corev1client.CoreV1Interface
	restConfig   *rest.Config
	connectivity string

	// mutex guards the mutable values below
	mutex sync.Mutex

	// sandboxes is a map of sandboxes we have created and not yet deleted.
	sandboxes map[types.NamespacedName]*Sandbox
}

// GetRESTConfig loads the kubernetes client configuration, preferring
// local kubeconfig (or KUBECONFIG) if present, and falling back to
// in-cluster configuration.
func GetRESTConfig() (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	restConfig, err := kubeConfig.ClientConfig()
	if err == nil {
		return restConfig, nil
	}
	restConfig, inClusterErr := rest.InClusterConfig()
	if inClusterErr == nil {
		return restConfig, nil
	}
	return nil, fmt.Errorf("failed to load kubeconfig (%v) or in-cluster config (%w)", err, inClusterErr)
}

// NewSandboxClient initializes a new SandboxClient using the provided rest.Config.
func NewSandboxClient(restConfig *rest.Config) (*SandboxClient, error) {
	httpClient, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes HTTP client: %w", err)
	}

	agentsCS, err := agentsclientset.NewForConfigAndClient(restConfig, httpClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for kubernetes agent-sandbox types: %w", err)
	}

	coreClient, err := corev1client.NewForConfigAndClient(restConfig, httpClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create core v1 client: %w", err)
	}

	return &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreClient,
		restConfig:   restConfig,
		connectivity: os.Getenv("AO_SANDBOX_CONNECTIVITY"),
		sandboxes:    make(map[types.NamespacedName]*Sandbox),
	}, nil
}

// Session represents an agentic "chat" session (a stream of messages / tools calls etc).
type Session struct {
	// Name is the identifier for the session
	Name string

	// client is the sandbox client to use to interact with the cluster
	client *SandboxClient

	// HomeDir is the home directory; we mount a EmptyDir volume here.
	// We currently only snapshot and restore this directory.
	HomeDir string

	// stateDir is the root directory for session state and snapshots.
	stateDir string

	// sessionStore is the store for session state.
	sessionStore Store

	// Metadata records where the session came from (its parent, if any) and
	// which toolset it runs with. It is persisted when the session is
	// created; sessions recorded before metadata existed have zero values.
	Metadata Metadata

	// messagesMu protects messages against concurrent reads (e.g. Messages())
	// while a turn is in flight (AddMessages).
	messagesMu sync.RWMutex

	// messages is a list of all the messages in the current session chat.
	messages []llm.Message

	// sandboxID is the ID of the sandbox we use.
	// Note that the sandbox does not always exist (for example, when idle)
	sandboxID types.NamespacedName

	// activeSandbox is the sandbox currently provisioned for this session,
	// or nil when no sandbox is running (e.g. before the first tool call).
	activeSandbox *Sandbox
}

// Messages returns a copy of the messages in the session so far.
func (s *Session) Messages() []llm.Message {
	s.messagesMu.RLock()
	defer s.messagesMu.RUnlock()
	return slices.Clone(s.messages)
}

// AddMessages appends messages to the session, persisting them to the session store.
func (s *Session) AddMessages(ctx context.Context, messages ...llm.Message) error {
	if err := s.sessionStore.AppendMessages(ctx, s.Name, messages...); err != nil {
		return fmt.Errorf("failed to persist messages: %w", err)
	}
	s.messagesMu.Lock()
	s.messages = append(s.messages, messages...)
	s.messagesMu.Unlock()

	return nil
}

// Sandbox represents an active sandbox instance.
type Sandbox struct {
	session *Session

	id types.NamespacedName

	podName string
	podIP   string

	authToken             string
	controlPlaneNamespace string

	// grpcMu guards gRPC connection and process client initialization
	grpcMu          sync.Mutex
	grpcConn        *grpc.ClientConn
	processClient   processv1.ProcessServiceClient
	portForwardStop func()

	// created is true if we have created the sandbox (and not deleted it)
	created bool
}

// NamespacedName returns the namespace and name of the Sandbox resource.
func (s *Sandbox) NamespacedName() types.NamespacedName {
	return s.id
}

// SandboxName returns the name of the sandbox.
func (s *Sandbox) SandboxName() string {
	return s.id.Name
}

// PodNamespacedName returns the namespace and name of the underlying Pod.
func (s *Sandbox) PodNamespacedName() types.NamespacedName {
	return types.NamespacedName{
		Namespace: s.id.Namespace,
		Name:      s.podName,
	}
}

// Close closes any active gRPC connection and stops any port-forward associated with the sandbox.
func (s *Sandbox) Close() error {
	s.grpcMu.Lock()
	defer s.grpcMu.Unlock()

	var errs []error
	if s.grpcConn != nil {
		if err := s.grpcConn.Close(); err != nil {
			errs = append(errs, err)
		}
		s.grpcConn = nil
		s.processClient = nil
	}
	if s.portForwardStop != nil {
		s.portForwardStop()
		s.portForwardStop = nil
	}
	return errors.Join(errs...)
}

// ExtendLifecycle updates the ShutdownTime of the Sandbox in Kubernetes to now + inactivityTimeout.
func (s *Sandbox) ExtendLifecycle(ctx context.Context, inactivityTimeout time.Duration) error {
	agentsClient := s.session.client.agentsClient

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Fetch latest spec
		latest, err := agentsClient.AgentsV1beta1().Sandboxes(s.id.Namespace).Get(ctx, s.id.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		// Update ShutdownTime
		latest.Spec.ShutdownTime = &metav1.Time{Time: time.Now().Add(inactivityTimeout)}

		_, err = agentsClient.AgentsV1beta1().Sandboxes(s.id.Namespace).Update(ctx, latest, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		return nil
	})
}

// WaitForReady polls the Sandbox resource until it becomes ready and resolves the underlying Pod name.
func (s *Sandbox) WaitForReady(ctx context.Context) error {
	timeout := time.After(3 * time.Minute)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	agentsClient := s.session.client.agentsClient

readyLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timed out waiting for Sandbox %s to become ready", s.SandboxName())
		case <-ticker.C:
			latest, err := agentsClient.AgentsV1beta1().Sandboxes(s.NamespacedName().Namespace).Get(ctx, s.NamespacedName().Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("error polling state of sandbox: %w", err)
			}
			ready := false
			for _, cond := range latest.Status.Conditions {
				if cond.Type == string(sandboxv1beta1.SandboxConditionReady) && cond.Status == metav1.ConditionTrue {
					ready = true
					break
				}
			}
			if ready {
				s.podName = latest.Name
				if len(latest.Status.PodIPs) > 0 {
					s.podIP = latest.Status.PodIPs[0]
				}
				break readyLoop
			}

			if err := s.checkPodImagePullStatus(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkPodImagePullStatus inspects the backing Pod's container statuses to fail fast
// if any container is stuck in ImagePullBackOff or ErrImagePull.
func (s *Sandbox) checkPodImagePullStatus(ctx context.Context) error {
	if s.session == nil || s.session.client == nil || s.session.client.coreClient == nil {
		return nil
	}

	pod, err := s.session.client.coreClient.Pods(s.NamespacedName().Namespace).Get(ctx, s.NamespacedName().Name, metav1.GetOptions{})
	if err != nil {
		return nil
	}

	allStatuses := make([]corev1.ContainerStatus, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses)+len(pod.Status.EphemeralContainerStatuses))
	allStatuses = append(allStatuses, pod.Status.InitContainerStatuses...)
	allStatuses = append(allStatuses, pod.Status.ContainerStatuses...)
	allStatuses = append(allStatuses, pod.Status.EphemeralContainerStatuses...)

	for _, cs := range allStatuses {
		if cs.State.Waiting != nil {
			reason := cs.State.Waiting.Reason
			if reason == "ImagePullBackOff" || reason == "ErrImagePull" {
				msg := cs.State.Waiting.Message
				if evMsg := s.getPodPullEventMessage(ctx, pod); evMsg != "" {
					msg = evMsg
				}
				if msg == "" {
					msg = reason
				}
				return fmt.Errorf("failed to pull image for container %q in pod %s (%s): %s", cs.Name, pod.Name, reason, msg)
			}
		}
	}

	return nil
}

// getPodPullEventMessage searches for an event associated with the pod that describes an image pull failure.
func (s *Sandbox) getPodPullEventMessage(ctx context.Context, pod *corev1.Pod) string {
	events, err := s.session.client.coreClient.Events(pod.Namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.name=%s", pod.Name),
	})
	if err != nil || events == nil {
		return ""
	}

	// Look backwards from the newest events for a detailed pull failure message.
	for i := len(events.Items) - 1; i >= 0; i-- {
		ev := events.Items[i]
		if ev.InvolvedObject.Name != "" && ev.InvolvedObject.Name != pod.Name {
			continue
		}
		if strings.Contains(ev.Message, "Failed to pull image") || strings.Contains(ev.Message, "rpc error") {
			return ev.Message
		}
	}

	// Fall back to any Warning event for image pull.
	for i := len(events.Items) - 1; i >= 0; i-- {
		ev := events.Items[i]
		if ev.InvolvedObject.Name != "" && ev.InvolvedObject.Name != pod.Name {
			continue
		}
		if ev.Type == corev1.EventTypeWarning && (ev.Reason == "Failed" || ev.Reason == "FailedToPullImage" || ev.Reason == "ErrImagePull" || ev.Reason == "ImagePullBackOff") {
			if ev.Message != "" {
				return ev.Message
			}
		}
	}

	return ""
}

// startPortForward establishes a SPDY port-forward to targetPort on the sandbox pod,
// returning the local forwarded port number and a function to stop the forwarding tunnel.
//
// Why this is needed:
// While in-cluster services (such as the `ao serve` deployment running in ao-system) can dial
// sandbox pod IPs directly on port 9090, the `ao` binary is also run as a CLI tool (`ao session run`,
// `ao session send`). When executed outside the cluster (e.g. from developer workstations,
// laptops, dev containers, or CI test harnesses), cluster pod CIDRs (such as 10.244.0.0/16 in
// kind) are unrouted and unreachable directly from the host. In those out-of-cluster scenarios,
// startPortForward tunnels traffic to sandboxd through the Kubernetes API server and kubelet.
func (s *Sandbox) startPortForward(ctx context.Context, targetPort int) (int, func(), error) {
	if s.session == nil || s.session.client == nil || s.session.client.coreClient == nil {
		return 0, nil, errors.New("kubernetes client is not available for port-forwarding")
	}

	coreClient := s.session.client.coreClient
	restConfig := s.session.client.restConfig
	podName := s.PodNamespacedName().Name
	if podName == "" {
		podName = s.id.Name
	}
	namespace := s.id.Namespace

	req := coreClient.RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward")

	transport, upgrader, err := spdy.RoundTripperFor(restConfig)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create SPDY round tripper: %w", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", req.URL())
	stopChan := make(chan struct{})
	readyChan := make(chan struct{})

	ports := []string{fmt.Sprintf("0:%d", targetPort)}
	var out, errOut bytes.Buffer

	fw, err := portforward.New(dialer, ports, stopChan, readyChan, &out, &errOut)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create port-forwarder: %w", err)
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- fw.ForwardPorts()
	}()

	select {
	case <-readyChan:
	case pfErr := <-errChan:
		close(stopChan)
		return 0, nil, fmt.Errorf("port-forward failed to start: %w (stderr: %s)", pfErr, errOut.String())
	case <-time.After(30 * time.Second):
		close(stopChan)
		return 0, nil, fmt.Errorf("timed out waiting for port-forward to be ready (stderr: %s)", errOut.String())
	case <-ctx.Done():
		close(stopChan)
		return 0, nil, ctx.Err()
	}

	forwardedPorts, err := fw.GetPorts()
	if err != nil || len(forwardedPorts) == 0 {
		close(stopChan)
		return 0, nil, fmt.Errorf("failed to get forwarded ports: %w", err)
	}

	localPort := int(forwardedPorts[0].Local)
	var stopOnce sync.Once
	stopFn := func() {
		stopOnce.Do(func() {
			close(stopChan)
		})
	}

	return localPort, stopFn, nil
}

// resolveTarget determines the gRPC address for reaching sandboxd.
//
// Connectivity behavior:
//  1. In-cluster: When ao runs inside Kubernetes, podIP is directly routable. resolveTarget
//     dials podIP:9090 directly without port-forward overhead.
//  2. Out-of-cluster: When running outside the cluster (developer laptops, CLI session runs,
//     or e2e tests), dialing podIP directly times out, so resolveTarget transparently falls back
//     to establishing a SPDY port-forwarding tunnel via startPortForward.
//
// Can also be explicitly forced via AO_SANDBOX_CONNECTIVITY ("pod-ip" or "port-forward").
func (s *Sandbox) resolveTarget(ctx context.Context) (string, func(), error) {
	connMode := ""
	if s.session != nil && s.session.client != nil {
		connMode = s.session.client.connectivity
	}

	podIP := s.podIP
	if podIP == "" && s.session != nil && s.session.client != nil && s.session.client.coreClient != nil {
		podName := s.PodNamespacedName().Name
		if podName == "" {
			podName = s.id.Name
		}
		pod, err := s.session.client.coreClient.Pods(s.id.Namespace).Get(ctx, podName, metav1.GetOptions{})
		if err == nil && pod != nil {
			podIP = pod.Status.PodIP
			s.podIP = podIP
		}
	}

	if connMode == "pod-ip" {
		if podIP == "" {
			return "", nil, fmt.Errorf("sandbox pod IP not found for %s", s.id)
		}
		return net.JoinHostPort(podIP, "9090"), nil, nil
	}

	if connMode == "port-forward" {
		localPort, stopFn, err := s.startPortForward(ctx, 9090)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("127.0.0.1:%d", localPort), stopFn, nil
	}

	// Auto-detection: test direct Pod IP connection first with a brief timeout.
	if podIP != "" {
		dialer := net.Dialer{Timeout: 300 * time.Millisecond}
		conn, dialErr := dialer.DialContext(ctx, "tcp", net.JoinHostPort(podIP, "9090"))
		if dialErr != nil {
			// Pod IP was unreachable directly (e.g. outside cluster); fall through to port-forwarding.
		} else {
			if closeErr := conn.Close(); closeErr != nil {
				scoped.Log(ctx).V(2).Info("failed to close probe connection", "error", closeErr)
			}
			return net.JoinHostPort(podIP, "9090"), nil, nil
		}
	}

	// Direct Pod IP connection was unreachable (e.g. outside the cluster). Fall back to port-forwarding.
	localPort, stopFn, err := s.startPortForward(ctx, 9090)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("127.0.0.1:%d", localPort), stopFn, nil
}

// ensureClient initializes the gRPC client to sandboxd if it is not already connected.
func (s *Sandbox) ensureClient(ctx context.Context) error {
	s.grpcMu.Lock()
	defer s.grpcMu.Unlock()

	if s.processClient != nil {
		return nil
	}

	// Retrieve auth token from Secret if not held in memory
	if s.authToken == "" && s.session != nil && s.session.client != nil && s.session.client.coreClient != nil {
		cpNamespace := s.controlPlaneNamespace
		if cpNamespace == "" {
			cpNamespace = "ao-system"
		}
		secretName := fmt.Sprintf("sandbox-token-%s", s.id.Name)
		secret, err := s.session.client.coreClient.Secrets(cpNamespace).Get(ctx, secretName, metav1.GetOptions{})
		if err == nil && len(secret.Data["token"]) > 0 {
			s.authToken = string(secret.Data["token"])
		}
	}

	target, stopFn, err := s.resolveTarget(ctx)
	if err != nil {
		return fmt.Errorf("resolving sandbox endpoint: %w", err)
	}

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(32*1024*1024)),
	)

	token := s.authToken
	if token != "" {
		dialOpts = append(dialOpts,
			grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
				return invoker(ctx, method, req, reply, cc, callOpts...)
			}),
			grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
				return streamer(ctx, desc, cc, method, callOpts...)
			}),
		)
	}

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		if stopFn != nil {
			stopFn()
		}
		return fmt.Errorf("creating gRPC client for %s: %w", target, err)
	}

	s.grpcConn = conn
	s.processClient = processv1.NewProcessServiceClient(conn)
	s.portForwardStop = stopFn
	return nil
}

// execExecute executes a short-lived command synchronously via ProcessService.Execute.
// On context cancellation (such as tool timeout or turn cancellation), sandboxd's
// ProcessServer.Execute invokes killProcessGroupOnContextDone to terminate the entire
// process group with SIGKILL, ensuring clean cancellation on the unary path without
// requiring explicit client signal delivery.
func (s *Sandbox) execExecute(ctx context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	req := &processv1.ExecuteRequest{
		Config: &processv1.ProcessConfig{
			Command: opts.Command,
		},
	}

	resp, err := s.processClient.Execute(ctx, req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("sandboxd execute failed: %w", err)
	}

	return &tools.ExecCommandResult{
		ExitCode: int(resp.GetExitCode()),
		Stdout:   string(resp.GetStdout()),
		Stderr:   string(resp.GetStderr()),
	}, nil
}

// execStart executes a command via ProcessService.Start, streaming stdout/stderr events
// and delivering a cancellation signal if ctx expires or is cancelled.
func (s *Sandbox) execStart(ctx context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	stream, err := s.processClient.Start(ctx, &processv1.StartRequest{
		Config: &processv1.ProcessConfig{
			Command: opts.Command,
		},
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("sandboxd start failed: %w", err)
	}

	// The first event from sandboxd must be InitEvent containing the process ID.
	firstResp, err := stream.Recv()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to receive init event from sandboxd: %w", err)
	}

	initEvent := firstResp.GetInit()
	if initEvent == nil {
		return nil, fmt.Errorf("expected InitEvent as first message, got: %v", firstResp)
	}
	pid := initEvent.GetProcessId()

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Watch for context cancellation / tool timeout and signal the process group.
	go func() {
		select {
		case <-ctx.Done():
		case <-doneCh:
		}
		if ctx.Err() != nil {
			sigCtx, sigCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer sigCancel()
			if _, sigErr := s.processClient.SendSignal(sigCtx, &processv1.SendSignalRequest{
				ProcessId: pid,
				Signal:    processv1.Signal_SIGNAL_SIGKILL,
			}); sigErr != nil {
				scoped.Log(ctx).V(2).Info("failed to deliver cancellation signal to sandboxd", "error", sigErr)
			}
		}
	}()

	// Feed standard input concurrently if provided.
	if opts.Stdin != nil {
		go func() {
			buf := make([]byte, 4096)
			for {
				if ctx.Err() != nil {
					return
				}

				select {
				case <-doneCh:
					return
				default:
				}

				n, readErr := opts.Stdin.Read(buf)
				if n > 0 {
					_, wErr := s.processClient.WriteStdin(ctx, &processv1.WriteStdinRequest{
						ProcessId: pid,
						Payload: &processv1.WriteStdinRequest_Input{
							Input: buf[:n],
						},
					})
					if wErr != nil {
						return
					}
				}
				if readErr != nil {
					if errors.Is(readErr, io.EOF) {
						if _, eofErr := s.processClient.WriteStdin(ctx, &processv1.WriteStdinRequest{
							ProcessId: pid,
							Payload: &processv1.WriteStdinRequest_Eof{
								Eof: &emptypb.Empty{},
							},
						}); eofErr != nil {
							scoped.Log(ctx).V(2).Info("failed to send eof to sandboxd stdin", "error", eofErr)
						}
					}
					return
				}
			}
		}()
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	stdoutWriter := opts.Stdout
	if stdoutWriter == nil {
		stdoutWriter = &stdoutBuf
	}
	stderrWriter := opts.Stderr
	if stderrWriter == nil {
		stderrWriter = &stderrBuf
	}

	exitCode := 0
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, fmt.Errorf("error reading sandboxd process stream: %w", recvErr)
		}

		switch ev := resp.GetEvent().(type) {
		case *processv1.StartResponse_Stdout:
			if len(ev.Stdout) > 0 {
				if _, err := stdoutWriter.Write(ev.Stdout); err != nil {
					return nil, fmt.Errorf("writing stdout: %w", err)
				}
			}
		case *processv1.StartResponse_Stderr:
			if len(ev.Stderr) > 0 {
				if _, err := stderrWriter.Write(ev.Stderr); err != nil {
					return nil, fmt.Errorf("writing stderr: %w", err)
				}
			}
		case *processv1.StartResponse_Exit:
			exitCode = int(ev.Exit.GetExitCode())
		default:
			scoped.Log(ctx).V(2).Info("received unknown event from sandboxd process stream", "event", resp.GetEvent())
		}
	}

	res := &tools.ExecCommandResult{
		ExitCode: exitCode,
	}
	if opts.Stdout == nil {
		res.Stdout = stdoutBuf.String()
	}
	if opts.Stderr == nil {
		res.Stderr = stderrBuf.String()
	}

	return res, nil
}

// ExecCommand executes a command inside the sandbox container with specified options.
// If Stdout or Stderr are nil in tools.ExecCommandOptions, they are captured internally and returned in the tools.ExecCommandResult.
// If the command returns a non-zero exit code, that is _not_ treated as an error; the exit code is returned in the result.
func (s *Sandbox) ExecCommand(ctx context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	if err := s.ensureClient(ctx); err != nil {
		return nil, err
	}

	if opts.Stdin != nil || opts.Stdout != nil || opts.Stderr != nil || opts.Stream {
		return s.execStart(ctx, opts)
	}

	return s.execExecute(ctx, opts)
}

// SetStateDir configures the state directory for this session.
func (s *Session) SetStateDir(dir string) {
	s.stateDir = dir
}

// getBackupDir gets the backup directory for the session.
// It creates the directory if it doesn't exist.
func (s *Session) getBackupDir() (string, error) {
	base := s.stateDir
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory: %w", err)
		}
		base = filepath.Join(home, ".local", "auto-optimizer")
	}
	dir := filepath.Join(base, s.Name, "fs")

	// Ensure the session's backup directory exists;
	// use mode 700 because it might contain sensitive data.
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", fmt.Errorf("failed to create backup directory: %w", err)
	}
	return dir, nil
}

// FindLatestBackup searches for the latest backup tarball in the session's backup directory.
func (s *Session) FindLatestBackup() (string, error) {
	dir, err := s.getBackupDir()
	if err != nil {
		return "", err
	}

	matches, err := filepath.Glob(filepath.Join(dir, "backup-*.tar.gz"))
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", nil
	}
	// Since Glob returns matches sorted alphabetically, and YYYYMMDDTHHMMSS
	// naturally sorts alphabetically in chronological order, the last match is the latest one!
	return matches[len(matches)-1], nil
}

// PruneBackups deletes older backups, keeping only the most recent keepCount backups.
func (s *Session) PruneBackups(ctx context.Context, keepCount int) error {
	log := scoped.Log(ctx)
	dir, err := s.getBackupDir()
	if err != nil {
		return err
	}

	matches, err := filepath.Glob(filepath.Join(dir, "backup-*.tar.gz"))
	if err != nil {
		return err
	}

	if len(matches) <= keepCount {
		return nil
	}

	pruneCount := len(matches) - keepCount
	for i := range pruneCount {
		if err := os.Remove(matches[i]); err != nil {
			log.Error(err, "unable to prune old backup", "backup", matches[i])
		} else {
			log.Info("pruned old backup", "backup", matches[i])
		}
	}

	return nil
}

// RestoreFS restores the filesystem in the sandbox from the latest local backup tarball, if one exists.
func (s *Sandbox) RestoreFS(ctx context.Context) error {
	log := scoped.Log(ctx)

	latestBackup, err := s.session.FindLatestBackup()
	if err != nil {
		return fmt.Errorf("failed to search for latest backup: %w", err)
	}
	if latestBackup == "" {
		// No previous backup found, start fresh
		return nil
	}

	log.Info("restoring filesystem from latest backup", "backup", latestBackup)
	f, err := os.Open(latestBackup)
	if err != nil {
		return fmt.Errorf("failed to open backup file %s: %w", latestBackup, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.V(2).Info("failed to close backup file", "error", closeErr)
		}
	}()

	opts := tools.ExecCommandOptions{
		Command: []string{"tar", "-zxf", "-", "-C", s.session.HomeDir},
		Stdin:   f,
	}
	res, err := s.ExecCommand(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to execute restore: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("restore failed with exit code %d: %s", res.ExitCode, res.Stderr)
	}

	return nil
}

// SnapshotFS archives the filesystem in the sandbox and saves it to a new timestamped backup inside the session's backup directory.
func (s *Sandbox) SnapshotFS(ctx context.Context) error {
	log := scoped.Log(ctx)

	dir, err := s.session.getBackupDir()
	if err != nil {
		return err
	}

	timestamp := time.Now().Format("20060102T150405")
	backupFilename := filepath.Join(dir, fmt.Sprintf("backup-%s.tar.gz", timestamp))
	tmpFilename := backupFilename + ".tmp"

	backupFile, err := os.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create backup file %s: %w", tmpFilename, err)
	}
	defer func() {
		if closeErr := backupFile.Close(); closeErr != nil {
			log.V(2).Info("failed to close temporary backup file", "error", closeErr)
		}
	}()

	// Clean up the temp file if something goes wrong
	shouldDeleteTempFile := true
	defer func() {
		if shouldDeleteTempFile {
			if err := os.Remove(tmpFilename); err != nil {
				log.Error(err, "unable to remove temporary backup file")
			}
		}
	}()

	opts := tools.ExecCommandOptions{
		Command: []string{"tar", "-zcf", "-", "-C", s.session.HomeDir, "."},
		Stdout:  backupFile,
	}
	res, err := s.ExecCommand(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to execute snapshot: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("snapshot failed with exit code %d: %s", res.ExitCode, res.Stderr)
	}

	// Close the file explicitly before renaming
	if err := backupFile.Close(); err != nil {
		return fmt.Errorf("failed to close backup file %s: %w", tmpFilename, err)
	}

	if err := os.Rename(tmpFilename, backupFilename); err != nil {
		return fmt.Errorf("failed to rename temp backup file to final path: %w", err)
	}

	// Don't delete the temp file; we successfully created the backup
	shouldDeleteTempFile = false

	// Prune backups, keeping only the last 5
	if err := s.session.PruneBackups(ctx, 5); err != nil {
		log.Error(err, "failed to prune old backups")
	}

	log.Info("saved filesystem state to new backup", "backup", backupFilename)
	return nil
}

// CreateSandbox creates a Sandbox resource for the session using topology B
// (an init container copies sandboxd into a shared volume, and the worker container runs it with tini as PID 1).
func (h *Harness) CreateSandbox(ctx context.Context, session *Session) (*Sandbox, error) {
	agentsClient := h.sandboxClient.agentsClient

	id := session.sandboxID
	image := h.opts.Image
	if image == "" {
		image = DefaultImage
	}
	sandboxdImage := h.opts.SandboxdImage
	if sandboxdImage == "" {
		sandboxdImage = DefaultSandboxdImage
	}
	homeDir := h.opts.HomeDir
	cpNamespace := h.opts.ControlPlaneNamespace
	if cpNamespace == "" {
		return nil, errors.New("ControlPlaneNamespace must be specified in RunOptions")
	}

	// Why we include an authentication token:
	// Without authentication, any process that can reach sandboxd on port 8080/9090
	// (other pods in the cluster or unauthorized processes if port-forwarded) could
	// invoke ProcessService or FilesystemService to execute arbitrary commands or
	// access files with the agent container's privileges. The bearer token restricts
	// execution to the control plane that minted it.
	//
	// Symmetric SHA-256 vs Asymmetric ed25519:
	// sandboxd currently verifies bearer tokens against a SHA-256 digest (--auth-token-sha256).
	// We pass only the SHA-256 digest so the raw token never appears in the pod spec or
	// container environment, and store the token in a Secret in the control plane's namespace.
	// (An ideal future upstream enhancement for sandboxd is ed25519 public key verification,
	// allowing the public key to be passed in args while keeping the private key only in
	// control plane memory, eliminating Secrets entirely).

	// 1. Mint a random bearer token per sandbox.
	tokenBytes := make([]byte, 32)
	if _, err := cryptorand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("generating auth token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)

	// 2. Store the bearer token in a Secret in the control plane's namespace.
	secretName := fmt.Sprintf("sandbox-token-%s", id.Name)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: cpNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by":        "auto-optimizer",
				"auto-optimizer.gke-labs.dev/sandbox": id.Name,
			},
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"token": token,
		},
	}
	if h.sandboxClient.coreClient != nil {
		_, err := h.sandboxClient.coreClient.Secrets(cpNamespace).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			if k8serrors.IsAlreadyExists(err) {
				existing, getErr := h.sandboxClient.coreClient.Secrets(cpNamespace).Get(ctx, secretName, metav1.GetOptions{})
				if getErr == nil && len(existing.Data["token"]) > 0 {
					token = string(existing.Data["token"])
				}
			} else {
				return nil, fmt.Errorf("failed to create sandbox token secret in %s: %w", cpNamespace, err)
			}
		}
	}

	// 3. Pass only the SHA-256 digest of the token to sandboxd so the token never appears in the pod spec.
	tokenDigest := sha256.Sum256([]byte(token))
	tokenSHA256 := hex.EncodeToString(tokenDigest[:])

	policy := sandboxv1beta1.ShutdownPolicyDelete
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      id.Name,
			Namespace: id.Namespace,
		},
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				Service: new(true),
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{
						AutomountServiceAccountToken: new(false),
						SecurityContext: &corev1.PodSecurityContext{
							RunAsNonRoot: new(true),
							FSGroup:      new(int64(1000)),
						},
						InitContainers: []corev1.Container{
							{
								Name:  "install-sandboxd",
								Image: sandboxdImage,
								Command: []string{
									"/bin/sh", "-c", "cp /usr/local/bin/sandboxd /sandboxd-bin/sandboxd",
								},
								SecurityContext: &corev1.SecurityContext{
									RunAsUser:                new(int64(1000)),
									RunAsGroup:               new(int64(1000)),
									RunAsNonRoot:             new(true),
									AllowPrivilegeEscalation: new(false),
									Capabilities: &corev1.Capabilities{
										Drop: []corev1.Capability{"ALL"},
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "sandboxd-bin",
										MountPath: "/sandboxd-bin",
									},
								},
							},
						},
						Containers: []corev1.Container{
							{
								Name:  "sandbox",
								Image: image,
								// tini runs as PID 1 to act as a proper subreaper for child processes
								// spawned by commands executed via sandboxd, ensuring orphan zombie
								// process reaping, signal forwarding to process groups, and graceful cleanup.
								Command: []string{
									"tini", "-g", "--",
									"/sandboxd-bin/sandboxd",
									"--root-dir=/workspace",
									"--listen-host=0.0.0.0",
									"--grpc-port=9090",
									"--rest-port=8080",
									fmt.Sprintf("--auth-token-sha256=%s", tokenSHA256),
								},
								SecurityContext: &corev1.SecurityContext{
									RunAsUser:                new(int64(1000)),
									RunAsGroup:               new(int64(1000)),
									RunAsNonRoot:             new(true),
									AllowPrivilegeEscalation: new(false),
									Capabilities: &corev1.Capabilities{
										Drop: []corev1.Capability{"ALL"},
									},
								},
								Ports: []corev1.ContainerPort{
									{
										Name:          "rest",
										ContainerPort: 8080,
									},
									{
										Name:          "grpc",
										ContainerPort: 9090,
									},
								},
								ReadinessProbe: &corev1.Probe{
									ProbeHandler: corev1.ProbeHandler{
										HTTPGet: &corev1.HTTPGetAction{
											Path: "/v1/health",
											Port: intstr.FromInt32(8080),
										},
									},
									InitialDelaySeconds: 1,
									PeriodSeconds:       1,
								},
								VolumeMounts: []corev1.VolumeMount{
									{
										Name:      "sandboxd-bin",
										MountPath: "/sandboxd-bin",
										ReadOnly:  true,
									},
									{
										Name:      "workspace",
										MountPath: "/workspace",
									},
									{
										Name:      "home",
										MountPath: homeDir,
									},
								},
								Env: []corev1.EnvVar{
									{
										Name:  "HOME",
										Value: homeDir,
									},
								},
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "sandboxd-bin",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							{
								Name: "workspace",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							{
								Name: "home",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
						},
						RestartPolicy: corev1.RestartPolicyNever,
					},
				},
			},
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(SandboxInactivityTimeout)},
				ShutdownPolicy: &policy,
			},
		},
	}

	_, err := agentsClient.AgentsV1beta1().Sandboxes(id.Namespace).Create(ctx, sb, metav1.CreateOptions{})
	if err != nil {
		// Note: we need to handle the case when the sandbox already exists,
		// we want to confirm the sandbox configuration matches before proceeding.
		return nil, fmt.Errorf("failed to create Sandbox: %w", err)
	}

	sandbox := &Sandbox{
		session:               session,
		id:                    id,
		created:               true,
		authToken:             token,
		controlPlaneNamespace: cpNamespace,
	}

	h.sandboxClient.mutex.Lock()
	h.sandboxClient.sandboxes[sandbox.NamespacedName()] = sandbox
	h.sandboxClient.mutex.Unlock()

	return sandbox, nil
}

// DeleteSandbox deletes the Sandbox resource and its bearer token Secret.
func (c *SandboxClient) DeleteSandbox(ctx context.Context, sb *Sandbox) error {
	if closeErr := sb.Close(); closeErr != nil {
		scoped.Log(ctx).V(2).Info("error closing sandbox connection", "error", closeErr)
	}

	if !sb.created {
		return nil
	}
	id := sb.NamespacedName()

	// Delete bearer token secret in control plane namespace
	cpNamespace := sb.controlPlaneNamespace
	if cpNamespace == "" {
		cpNamespace = "ao-system"
	}
	secretName := fmt.Sprintf("sandbox-token-%s", id.Name)
	if c.coreClient != nil {
		if err := c.coreClient.Secrets(cpNamespace).Delete(ctx, secretName, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			scoped.Log(ctx).V(1).Info("failed to delete auth token secret", "secret", secretName, "error", err)
		}
	}

	// NotFound means the sandbox already expired (ShutdownPolicy Delete),
	// which is success for our purposes.
	if err := c.agentsClient.AgentsV1beta1().Sandboxes(id.Namespace).Delete(ctx, id.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete Sandbox: %w", err)
	}
	sb.created = false

	c.mutex.Lock()
	delete(c.sandboxes, sb.NamespacedName())
	c.mutex.Unlock()

	return nil
}

// DeleteAllSandboxes deletes all active Sandboxes tracked by this client.
func (c *SandboxClient) DeleteAllSandboxes(ctx context.Context) error {
	var errs []error

	c.mutex.Lock()
	sandboxes := make([]*Sandbox, 0, len(c.sandboxes))
	for _, ts := range c.sandboxes {
		sandboxes = append(sandboxes, ts)
	}
	c.mutex.Unlock()

	for _, sb := range sandboxes {
		if err := c.DeleteSandbox(ctx, sb); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// RunOptions are the options for running the agent.
type RunOptions struct {
	StateDir              string
	Namespace             string
	ControlPlaneNamespace string
	Image                 string
	SandboxdImage         string

	// HomeDir is the home directory inside the sandbox.
	// This is currently the only path that we persist between execs in the sandbox.
	HomeDir string

	// ModelName is the name of the model to use with the LLM.
	ModelName string

	// SystemPrompt seeds every new session. It is provided by the selected
	// toolset, so the prompt always matches the registered tools.
	SystemPrompt string

	// ToolTimeout bounds how long a single tool invocation may run before it
	// is cancelled. <= 0 disables the timeout. Default: DefaultToolTimeout.
	ToolTimeout time.Duration
}

// DefaultImage is the sandbox image used when neither the flags, the
// environment, nor the selected toolset specify one.
const DefaultImage = "auto-optimizer.local/geminicli-toolbox:dev"

// InitDefaults populates the options from environment variables and defaults.
// Image is left empty when SANDBOX_IMAGE is unset, so callers can
// distinguish "unset" from an explicit choice and apply the toolset's
// default image (falling back to DefaultImage).
func (o *RunOptions) InitDefaults() {
	o.Image = os.Getenv("SANDBOX_IMAGE")

	o.Namespace = os.Getenv("SANDBOX_NAMESPACE")
	if o.Namespace == "" {
		o.Namespace = "ao-sandboxes"
	}

	o.ControlPlaneNamespace = os.Getenv("AO_NAMESPACE")
	if o.ControlPlaneNamespace == "" {
		o.ControlPlaneNamespace = "ao-system"
	}

	o.SandboxdImage = os.Getenv("SANDBOXD_IMAGE")
	if o.SandboxdImage == "" {
		o.SandboxdImage = DefaultSandboxdImage
	}

	o.HomeDir = os.Getenv("SANDBOX_HOME_DIR")
	if o.HomeDir == "" {
		o.HomeDir = "/home/clawtainer"
	}

	modelName := os.Getenv("OPENAI_MODEL")
	if modelName == "" {
		modelName = os.Getenv("MODEL")
	}
	if modelName == "" {
		modelName = "gemini-3.5-flash"
	}
	o.ModelName = modelName

	o.ToolTimeout = DefaultToolTimeout
}

// TurnEvents receives progress callbacks while RunTurn processes a user
// message. Implementations render them to a local console, or stream them
// to a remote client (e.g. as ACP session/update notifications).
type TurnEvents interface {
	// AssistantMessage is called with the assistant's final text reply for
	// the turn.
	AssistantMessage(ctx context.Context, text string)

	// ApproveToolCall reports whether a requested tool call may execute.
	// Returning false sends a denial to the LLM instead of running the
	// tool; returning an error aborts the turn (for example, if the
	// approver has disconnected).
	ApproveToolCall(ctx context.Context, call llm.ToolCall) (bool, error)

	// ToolCallStarted is called immediately before an approved tool executes.
	ToolCallStarted(ctx context.Context, call llm.ToolCall)

	// ToolCallFinished is called after a tool executes; err is non-nil if
	// the tool failed.
	ToolCallFinished(ctx context.Context, call llm.ToolCall, err error)

	// MessageAppended is called whenever a message is added to the session history.
	MessageAppended(ctx context.Context, msg llm.Message)
}

// Harness wires the LLM, the tool registry, and the sandbox client together
// into an agentic loop.
type Harness struct {
	// llmClient is the client we use to talk to the llm.
	llmClient llm.ChatClient

	// toolsRegistry holds all the tools we have available to the llm.
	toolsRegistry *tools.Registry

	// sandboxClient is the client we use to interact with sandboxes.
	sandboxClient *SandboxClient

	// opts contains the options for the run command.
	opts RunOptions

	// sandboxOverride allows substituting a custom tools.Sandbox implementation
	// (e.g. for testing without a Kubernetes cluster).
	sandboxOverride tools.Sandbox
}

// SetSandboxOverride allows setting a custom tools.Sandbox implementation
// (e.g. for testing without a Kubernetes cluster or network).
func (h *Harness) SetSandboxOverride(sb tools.Sandbox) {
	h.sandboxOverride = sb
}

// NewHarness constructs a Harness.
func NewHarness(llmClient llm.ChatClient, sandboxClient *SandboxClient, toolsRegistry *tools.Registry, opts RunOptions) *Harness {
	return &Harness{
		llmClient:     llmClient,
		sandboxClient: sandboxClient,
		toolsRegistry: toolsRegistry,
		opts:          opts,
	}
}

// NewSession creates a new Session named sessionName and persists its
// metadata to the session store. If metadata.CreatedAt is zero, it is set to
// the current time.
func (h *Harness) NewSession(ctx context.Context, sessionStore Store, sessionName string, metadata Metadata) (*Session, error) {
	// Session names become Kubernetes object names and filesystem path
	// components, so validate here even though front ends generally do too.
	if err := ValidateSessionName(sessionName); err != nil {
		return nil, fmt.Errorf("invalid session name %q: %w", sessionName, err)
	}

	if metadata.CreatedAt.IsZero() {
		metadata.CreatedAt = time.Now()
	}
	if err := sessionStore.NewSession(ctx, sessionName, metadata); err != nil {
		return nil, fmt.Errorf("creating session: %w", err)
	}

	return &Session{
		Name:         sessionName,
		client:       h.sandboxClient,
		HomeDir:      h.opts.HomeDir,
		stateDir:     h.opts.StateDir,
		sessionStore: sessionStore,
		Metadata:     metadata,
		sandboxID: types.NamespacedName{
			Name:      sessionName,
			Namespace: h.opts.Namespace,
		},
	}, nil
}

// LoadSession loads an existing Session named sessionName (its message
// history and metadata) from the session store. If the session does not
// exist, it returns an error wrapping ErrNotFound.
func (h *Harness) LoadSession(ctx context.Context, sessionStore Store, sessionName string) (*Session, error) {
	if err := ValidateSessionName(sessionName); err != nil {
		return nil, fmt.Errorf("invalid session name %q: %w", sessionName, err)
	}

	messages, metadata, err := sessionStore.LoadSession(ctx, sessionName)
	if err != nil {
		return nil, fmt.Errorf("loading session: %w", err)
	}

	var meta Metadata
	if metadata != nil {
		meta = *metadata
	}

	return &Session{
		Name:         sessionName,
		client:       h.sandboxClient,
		HomeDir:      h.opts.HomeDir,
		stateDir:     h.opts.StateDir,
		sessionStore: sessionStore,
		Metadata:     meta,
		messages:     messages,
		sandboxID: types.NamespacedName{
			Name:      sessionName,
			Namespace: h.opts.Namespace,
		},
	}, nil
}

// EnsureSystemPrompt seeds a new session with the configured system prompt.
// It is a no-op for resumed sessions that already have history.
func (h *Harness) EnsureSystemPrompt(ctx context.Context, session *Session) error {
	if len(session.Messages()) > 0 {
		return nil
	}
	if h.opts.SystemPrompt == "" {
		return fmt.Errorf("SystemPrompt is not configured")
	}
	prompt := h.opts.SystemPrompt
	return session.AddMessages(ctx, llm.Message{Role: "system", Content: &prompt})
}

// RunTurn processes one user message: it calls the LLM, executes (approved)
// tool calls in the session's sandbox, and repeats until the LLM produces a
// final text reply, which is delivered via events.AssistantMessage.
func (h *Harness) RunTurn(ctx context.Context, session *Session, input string, events TurnEvents) error {
	log := scoped.Log(ctx)

	userMsg := llm.Message{Role: "user", Content: &input}
	if err := session.AddMessages(ctx, userMsg); err != nil {
		return fmt.Errorf("adding user message: %w", err)
	}
	events.MessageAppended(ctx, userMsg)

	shouldSnapshot := false
	for {
		req := llm.ChatCompletionRequest{
			Model:    h.opts.ModelName,
			Messages: session.Messages(),
			Tools:    h.toolsRegistry.All(),
		}

		assistantResponse, err := h.llmClient.CreateChatCompletion(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to call LLM: %w", err)
		}

		if len(assistantResponse.Choices) == 0 {
			return fmt.Errorf("LLM returned no choices: %v", assistantResponse)
		}

		assistantMessage := assistantResponse.Choices[0].Message
		if err := session.AddMessages(ctx, assistantMessage); err != nil {
			return fmt.Errorf("adding assistant message: %w", err)
		}
		events.MessageAppended(ctx, assistantMessage)

		log.V(1).Info("got message from LLM", "msg", assistantMessage)

		// We keep iterating with the LLM as long as there are tool calls to respond to
		if len(assistantMessage.ToolCalls) == 0 {
			events.AssistantMessage(ctx, valueOf(assistantMessage.Content))

			// We take a snapshot at these "boundaries", rather than after every tool call
			if shouldSnapshot && session.activeSandbox != nil {
				log.Info("snapshotting filesystem from sandbox...", "sandbox.name", session.activeSandbox.SandboxName())
				if err := session.activeSandbox.SnapshotFS(ctx); err != nil {
					log.Error(err, "failed to snapshot filesystem")
				}
			}

			return nil
		}

		for _, tc := range assistantMessage.ToolCalls {
			approved, err := events.ApproveToolCall(ctx, tc)
			if err != nil {
				return fmt.Errorf("requesting tool call approval: %w", err)
			}

			var toolMsg llm.Message
			if !approved {
				log.Info("tool call denied by user", "tool", tc.Function.Name)
				content := fmt.Sprintf("The user denied permission to run tool %q.", tc.Function.Name)
				toolMsg = llm.Message{Role: "tool", ToolCallID: tc.ID, Content: &content}
			} else {
				sandbox, err := h.ensureSandbox(ctx, session)
				if err != nil {
					return err
				}

				events.ToolCallStarted(ctx, tc)
				result, err := h.toolsRegistry.Call(ctx, sandbox, tc)
				events.ToolCallFinished(ctx, tc, err)

				// The filesystem may have changed (even on error), so
				// snapshot at the next boundary to keep the persisted
				// filesystem state consistent with the session state.
				shouldSnapshot = true

				if err != nil {
					log.Error(err, "error calling tool", "tool", tc.Function.Name)
					content := fmt.Sprintf("Error calling tool %q: %v", tc.Function.Name, err)
					toolMsg = llm.Message{Role: "tool", ToolCallID: tc.ID, Content: &content}
				} else {
					toolMsg = result
				}

				// The tool call could take a while, so extend the lifecycle.
				// This might need more careful handling later; what if the command takes 20 minutes to run?
				if sb, ok := sandbox.(*Sandbox); ok {
					if err := sb.ExtendLifecycle(ctx, SandboxInactivityTimeout); err != nil {
						log.Error(err, "extending sandbox lifecycle")
					}
				}
			}

			if err := session.AddMessages(ctx, toolMsg); err != nil {
				return fmt.Errorf("adding tool response: %w", err)
			}
			events.MessageAppended(ctx, toolMsg)
		}

		// Here we continue the LLM loop, with the tool responses at the tail of the chat thread.
	}
}

// ensureSandbox returns the session's running sandbox, creating it (and
// restoring its filesystem from the latest snapshot) if it does not exist
// or was cleaned up due to inactivity.
func (h *Harness) ensureSandbox(ctx context.Context, session *Session) (tools.Sandbox, error) {
	if h.sandboxOverride != nil {
		return h.sandboxOverride, nil
	}

	log := scoped.Log(ctx)

	if sb := session.activeSandbox; sb != nil {
		if err := sb.ExtendLifecycle(ctx, SandboxInactivityTimeout); err != nil {
			if k8serrors.IsNotFound(err) {
				log.Info("Active sandbox was deleted or expired, will recreate it", "sandbox.name", session.sandboxID.Name)
				if closeErr := sb.Close(); closeErr != nil {
					log.V(2).Info("error closing expired sandbox connection", "error", closeErr)
				}
				session.activeSandbox = nil
			} else {
				return nil, fmt.Errorf("extending sandbox TTL: %w", err)
			}
		}
	}

	if session.activeSandbox == nil {
		log.Info("launching sandbox for tool execution...")

		sb, err := h.CreateSandbox(ctx, session)
		if err != nil {
			return nil, fmt.Errorf("failed to create sandbox: %w", err)
		}

		if err := sb.WaitForReady(ctx); err != nil {
			log.Error(err, "sandbox not ready")
			if delErr := h.sandboxClient.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
				log.Error(delErr, "failed to delete unready sandbox")
			}
			return nil, err
		}

		log.V(1).Info("sandbox ready", "sandbox.name", sb.SandboxName())

		log.Info("restoring filesystem to sandbox...", "sandbox.name", sb.SandboxName())
		if err := sb.RestoreFS(ctx); err != nil {
			log.Error(err, "failed to restore filesystem; starting with a fresh sandbox instead", "sandbox.name", sb.SandboxName())
		}

		session.activeSandbox = sb
	}

	return session.activeSandbox, nil
}

// valueOf is a helper that safely gets a value from a pointer,
// if the pointer is nil it returns the default (zero) value.
func valueOf[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}
