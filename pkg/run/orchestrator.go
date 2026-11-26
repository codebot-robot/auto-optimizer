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

package run

import (
	"cmp"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	crcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/portal"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

const (
	// LabelRun is the Sandbox label key containing the run ID.
	LabelRun = "auto-optimizer.labs.gke.io/run"
	// AnnotationRepo is the Sandbox annotation key containing the repository key.
	AnnotationRepo = "auto-optimizer.labs.gke.io/repo"

	// DefaultMaxTurns is the default maximum number of turns for a run.
	DefaultMaxTurns = 30

	// DefaultMaxConcurrentRuns is the default maximum number of concurrent runs.
	DefaultMaxConcurrentRuns = 4

	// DefaultMaxSessionDepth is the default maximum depth of child sessions in a session tree.
	DefaultMaxSessionDepth = 3

	// DefaultReadyTimeout is the default duration to wait for a Sandbox to become ready.
	DefaultReadyTimeout = 3 * time.Minute

	// DefaultVerifyTimeout is the default duration to wait for a goal's verify command to complete.
	DefaultVerifyTimeout = 10 * time.Minute

	// DefaultMaxDuration is the default maximum duration for a run when the goal does not specify budget.maxDuration.
	DefaultMaxDuration = 1 * time.Hour

	// DefaultWorkspaceSize is the default storage request size for run workspace claims.
	DefaultWorkspaceSize = "10Gi"

	// SandboxShutdownGrace is the grace period added to a run's deadline when computing its Sandbox shutdown time.
	SandboxShutdownGrace = 15 * time.Minute

	// readySafetyRequeueInterval is the slow safety requeue interval when waiting for a Sandbox to become ready.
	readySafetyRequeueInterval = 1 * time.Minute

	// portalBindRetryInterval is the retry interval used when binding an egress policy fails.
	portalBindRetryInterval = 5 * time.Second
)

type runIndexEntry struct {
	repo  state.RepoKey
	runID string
}

// OrchestratorOptions configures the run Orchestrator.
type OrchestratorOptions struct {
	Store             state.Store
	SessionStore      session.Store
	Harness           *session.Harness
	Clock             func() time.Time
	MaxTurns          int
	MaxConcurrentRuns int
	MaxSessionDepth   int

	// GitAuthor is the identity configured as git user.name and user.email in
	// each run's sandbox, as `Name <email>`.
	GitAuthor string

	// PublicURL is the public base URL of this server, used for links in pull request bodies.
	PublicURL string

	ReadyTimeout time.Duration

	// KubeClient is the controller-runtime client used to store and reconcile HeroRun custom resources.
	KubeClient client.Client

	// VerifyTimeout is the maximum duration for running a goal's verify command.
	VerifyTimeout time.Duration

	// DefaultMaxDuration is the default maximum duration for a run when the goal does not specify budget.maxDuration.
	// When unset or <= 0, defaults to DefaultMaxDuration (1 hour).
	DefaultMaxDuration time.Duration

	// ForgeFor returns the Forge for a repository URL.
	// If nil or returns an error, push operations in Selecting fail.
	ForgeFor func(repoURL string) (forge.Forge, error)

	// PortalBinder binds an egress policy to a sandbox pod.
	// Optional: when nil, portal egress policy binding is disabled.
	PortalBinder portal.Binder

	// PolicyFactory generates the egress policy for a sandbox.
	// Required when PortalBinder is configured.
	PolicyFactory portal.PolicyFactory

	// WorkspaceStorageClass is the StorageClass used for run workspace claims.
	// When empty, sandboxes use an emptyDir volume for the workspace.
	WorkspaceStorageClass string

	// WorkspaceSize is the storage request size for run workspace claims.
	// When unset, defaults to DefaultWorkspaceSize ("10Gi").
	WorkspaceSize string
}

// InitDefaults fills every zero-valued field that has a default with its Default* constant.
func (o *OrchestratorOptions) InitDefaults() {
	if o == nil {
		return
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.MaxTurns == 0 {
		o.MaxTurns = DefaultMaxTurns
	}
	if o.MaxConcurrentRuns == 0 {
		o.MaxConcurrentRuns = DefaultMaxConcurrentRuns
	}
	if o.MaxSessionDepth == 0 {
		o.MaxSessionDepth = DefaultMaxSessionDepth
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = DefaultReadyTimeout
	}
	if o.VerifyTimeout == 0 {
		o.VerifyTimeout = DefaultVerifyTimeout
	}
	if o.DefaultMaxDuration == 0 {
		o.DefaultMaxDuration = DefaultMaxDuration
	}
	if o.WorkspaceSize == "" {
		o.WorkspaceSize = DefaultWorkspaceSize
	}
}

// AddFlags binds orchestrator configuration flags to fs.
func (o *OrchestratorOptions) AddFlags(fs *pflag.FlagSet) {
	o.InitDefaults()
	fs.IntVar(&o.MaxTurns, "max-turns", o.MaxTurns, "Maximum number of session turns before ending a run")
	fs.IntVar(&o.MaxConcurrentRuns, "max-concurrent-runs", o.MaxConcurrentRuns, "Maximum number of concurrent runs reconciled")
	fs.IntVar(&o.MaxSessionDepth, "max-session-depth", o.MaxSessionDepth, "Maximum depth of child sessions in the session tree")
	fs.StringVar(&o.PublicURL, "public-url", o.PublicURL, "Public base URL of this server, used for links in pull request bodies, for example https://ao.example.com")
	fs.DurationVar(&o.ReadyTimeout, "ready-timeout", o.ReadyTimeout, "Maximum duration to wait for a sandbox to become ready")
	fs.DurationVar(&o.VerifyTimeout, "verify-timeout", o.VerifyTimeout, "Maximum duration for running a goal's verify command")
	fs.DurationVar(&o.DefaultMaxDuration, "max-run-duration", o.DefaultMaxDuration, "Default maximum duration for a run when the goal does not specify budget.maxDuration")
	fs.StringVar(&o.WorkspaceStorageClass, "workspace-storage-class", o.WorkspaceStorageClass, "StorageClass for run workspace claims (empty to use emptyDir)")
	fs.StringVar(&o.WorkspaceSize, "workspace-size", o.WorkspaceSize, "Storage request size for run workspace claims")
}

// Validate reports any missing required dependencies or invalid option values.
func (o *OrchestratorOptions) Validate() error {
	if o == nil {
		return errors.New("options are required")
	}
	if o.Store == nil {
		return errors.New("state store is required")
	}
	if o.Harness == nil {
		return errors.New("harness is required")
	}
	if o.KubeClient == nil {
		return errors.New("kube client is required")
	}
	if o.MaxTurns < 1 {
		return errors.New("MaxTurns must be at least 1")
	}
	if o.MaxConcurrentRuns < 1 {
		return errors.New("MaxConcurrentRuns must be at least 1")
	}
	if o.MaxSessionDepth < 1 {
		return errors.New("MaxSessionDepth must be at least 1")
	}
	if o.ReadyTimeout <= 0 {
		return errors.New("ReadyTimeout must be positive")
	}
	if o.VerifyTimeout <= 0 {
		return errors.New("VerifyTimeout must be positive")
	}
	if o.DefaultMaxDuration <= 0 {
		return errors.New("DefaultMaxDuration must be positive")
	}
	if o.PortalBinder != nil && o.PolicyFactory == nil {
		return errors.New("policy factory is required when portal binder is set")
	}
	if o.WorkspaceSize != "" {
		if _, err := resource.ParseQuantity(o.WorkspaceSize); err != nil {
			return fmt.Errorf("invalid WorkspaceSize: %w", err)
		}
	}
	return nil
}

// Orchestrator reconciles runs in phase Pending and Working using a controller-runtime controller.
type Orchestrator struct {
	store              state.Store
	harness            *session.Harness
	clock              func() time.Time
	maxTurns           int
	maxConcurrentRuns  int
	maxSessionDepth    int
	gitAuthor          string
	publicURL          string
	readyTimeout       time.Duration
	kubeClient         client.Client
	verifyTimeout      time.Duration
	defaultMaxDuration time.Duration
	forgeFor           func(repoURL string) (forge.Forge, error)
	portalBinder       portal.Binder
	policyFactory      portal.PolicyFactory

	workspaceStorageClass string
	workspaceSize         string

	runIndexMu sync.RWMutex
	runIndex   map[string]runIndexEntry

	notifyCh chan event.TypedGenericEvent[reconcile.Request]

	runStatesMu sync.Mutex
	runStates   map[string]*runState

	msgSeq atomic.Uint64

	managerCtxMu sync.Mutex
	managerCtx   context.Context
}

// ErrRunNotWorking indicates that a message cannot be posted because the run has left the working state.
var ErrRunNotWorking = errors.New("run is no longer working")

// runState holds in-memory runtime state shared across sessions of an active run.
type runState struct {
	sandboxOwner *session.SandboxOwner
}

var _ reconcile.Reconciler = (*Orchestrator)(nil)
var _ manager.Runnable = (*Orchestrator)(nil)

// NewScheme builds a runtime.Scheme containing client-go types, the HeroRun types,
// and the agent-sandbox types.
func NewScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("adding client-go to scheme: %w", err)
	}
	if err := herorunv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("adding herorun v1alpha1 to scheme: %w", err)
	}
	if err := sandboxv1beta1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("adding sandbox v1beta1 to scheme: %w", err)
	}
	return scheme, nil
}

// NewManager builds a controller-runtime manager from rest.Config with metrics and health
// endpoints disabled, leader election disabled, and the cache scoped to namespace.
func NewManager(restConfig *rest.Config, namespace string) (manager.Manager, error) {
	scheme, err := NewScheme()
	if err != nil {
		return nil, err
	}
	cacheOpts := crcache.Options{}
	if namespace != "" {
		cacheOpts.DefaultNamespaces = map[string]crcache.Config{
			namespace: {},
		}
	}
	return ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Logger: klog.NewKlogr(),
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		Cache:                  cacheOpts,
	})
}

// NewOrchestrator creates a new run Orchestrator.
func NewOrchestrator(opts OrchestratorOptions) (*Orchestrator, error) {
	opts.InitDefaults()
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	orch := &Orchestrator{
		store:                 opts.Store,
		harness:               opts.Harness,
		clock:                 opts.Clock,
		maxTurns:              opts.MaxTurns,
		maxConcurrentRuns:     opts.MaxConcurrentRuns,
		maxSessionDepth:       opts.MaxSessionDepth,
		gitAuthor:             opts.GitAuthor,
		publicURL:             strings.TrimRight(opts.PublicURL, "/"),
		readyTimeout:          opts.ReadyTimeout,
		kubeClient:            opts.KubeClient,
		verifyTimeout:         opts.VerifyTimeout,
		defaultMaxDuration:    opts.DefaultMaxDuration,
		forgeFor:              opts.ForgeFor,
		portalBinder:          opts.PortalBinder,
		policyFactory:         opts.PolicyFactory,
		workspaceStorageClass: opts.WorkspaceStorageClass,
		workspaceSize:         opts.WorkspaceSize,
		runIndex:              make(map[string]runIndexEntry),
		notifyCh:              make(chan event.TypedGenericEvent[reconcile.Request], 1024),
		runStates:             make(map[string]*runState),
	}
	return orch, nil
}

func (o *Orchestrator) recordRunIndex(name string, repo state.RepoKey, runID string) {
	o.runIndexMu.Lock()
	defer o.runIndexMu.Unlock()
	o.runIndex[name] = runIndexEntry{repo: repo, runID: runID}
}

func (o *Orchestrator) deleteRunIndex(name string) {
	o.runIndexMu.Lock()
	defer o.runIndexMu.Unlock()
	delete(o.runIndex, name)
}

// RunPageURL returns the public URL for a run's detail page, or empty when unset.
func (o *Orchestrator) RunPageURL(repo *state.Repo, run *state.Run) string {
	if o == nil || o.publicURL == "" || run == nil || run.ID == "" {
		return ""
	}
	return fmt.Sprintf("%s/runs/%s", o.publicURL, run.ID)
}

// Notify enqueues a run for reconciliation.
// If o is nil, Notify is a no-op.
func (o *Orchestrator) Notify(repo state.RepoKey, runID string) {
	if o == nil {
		return
	}
	name := sandboxNameForRun(runID)
	o.recordRunIndex(name, repo, runID)

	var ns string
	if o.harness != nil {
		ns = o.harness.Namespace()
	}
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: ns,
			Name:      name,
		},
	}
	select {
	case o.notifyCh <- event.TypedGenericEvent[reconcile.Request]{Object: req}:
	default:
		go func() {
			o.notifyCh <- event.TypedGenericEvent[reconcile.Request]{Object: req}
		}()
	}
}

// SetupWithManager registers the orchestrator with the controller-runtime manager.
func (o *Orchestrator) SetupWithManager(mgr manager.Manager) error {
	src := source.TypedChannel[reconcile.Request, reconcile.Request](
		o.notifyCh,
		handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, req reconcile.Request) []reconcile.Request {
			return []reconcile.Request{req}
		}),
	)

	if err := ctrl.NewControllerManagedBy(mgr).
		For(&herorunv1alpha1.HeroRun{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&sandboxv1beta1.Sandbox{}).
		Owns(&herorunv1alpha1.Session{}).
		WatchesRawSource(src).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: o.maxConcurrentRuns,
		}).
		Complete(o); err != nil {
		return fmt.Errorf("creating run controller: %w", err)
	}

	return mgr.Add(o)
}

// Start implements manager.Runnable. On manager startup it calls Resume,
// which enqueues all non-terminal runs. It then blocks until ctx is cancelled.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.managerCtxMu.Lock()
	o.managerCtx = ctx
	o.managerCtxMu.Unlock()
	if err := o.Resume(ctx); err != nil {
		scoped.Log(ctx).Error(err, "failed to resume interrupted runs on startup")
	}
	<-ctx.Done()
	return nil
}

// Resume inspects existing HeroRuns across the cluster on startup.
// Non-terminal runs in the kube-apiserver are enqueued for reconciliation.
func (o *Orchestrator) Resume(ctx context.Context) error {
	var hrList herorunv1alpha1.HeroRunList
	if err := o.kubeClient.List(ctx, &hrList, client.InNamespace(o.namespace())); err != nil {
		return fmt.Errorf("listing HeroRuns during resume: %w", err)
	}

	for i := range hrList.Items {
		hr := &hrList.Items[i]
		run, err := HeroRunToRun(hr)
		if err != nil {
			scoped.Log(ctx).Error(err, "failed to convert HeroRun during resume", "heroRun", hr.Name)
			continue
		}
		if !run.Phase.IsTerminal() {
			o.recordRunIndex(sandboxNameForRun(run.ID), run.Repo, run.ID)
			o.Notify(run.Repo, run.ID)
		}
	}
	return nil
}

func (o *Orchestrator) getRunForReconcile(ctx context.Context, req reconcile.Request) (*state.Run, *state.Repo, *herorunv1alpha1.HeroRun, error) {
	ns := req.Namespace
	if ns == "" {
		ns = o.namespace()
	}

	var hr herorunv1alpha1.HeroRun
	if err := o.kubeClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: req.Name}, &hr); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("getting HeroRun %s: %w", req.Name, err)
	}

	run, err := HeroRunToRun(&hr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("converting HeroRun %s: %w", req.Name, err)
	}

	repo, err := o.store.GetRepo(ctx, run.Repo)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("getting repo %s: %w", run.Repo, err)
	}
	o.recordRunIndex(req.Name, run.Repo, run.ID)
	return run, repo, &hr, nil
}

// Reconcile loads the run and the repository, dispatches on the phase to
// runWorkflow.Run, and returns.
func (o *Orchestrator) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	run, repo, hr, err := o.getRunForReconcile(ctx, req)
	if err != nil {
		return reconcile.Result{}, err
	}
	if run == nil {
		return reconcile.Result{}, nil
	}

	if run.Phase.IsTerminal() {
		o.deleteRunIndex(req.Name)
		o.deleteRunState(run.ID)
		return reconcile.Result{}, nil
	}

	switch run.Phase {
	case state.PhasePending, state.PhasePreparing, state.PhaseWorking, state.PhaseSelecting, state.PhaseOpeningPR:
		return o.reconcileWorkflow(ctx, newRunWorkflow(o, repo, run, hr))
	}

	return reconcile.Result{}, nil
}

// reconcileWorkflow executes one step of wf, saves the resulting state, records any
// phase-change event, and cleans up when the run leaves Selecting or reaches a terminal phase.
func (o *Orchestrator) reconcileWorkflow(ctx context.Context, wf *runWorkflow) (reconcile.Result, error) {
	log := scoped.Log(ctx)
	prev := wf.run.Phase

	res, err := wf.Run(ctx)
	if err != nil {
		log.Error(err, "workflow run failed", "repo", wf.repo.URL, "run", wf.run.ID)
		return reconcile.Result{}, err
	}
	if ctx.Err() != nil || o.isManagerCanceled() {
		return reconcile.Result{}, nil
	}

	if res.done {
		eventTime := o.clock().UTC()
		if t, ok := wf.run.PhaseTimestamps[wf.run.Phase]; ok {
			eventTime = t
		}
		end := runEnd{
			Phase: wf.run.Phase,
		}
		if wf.end != nil {
			end = *wf.end
			end.Phase = wf.run.Phase
		} else if wf.run.Phase == state.PhaseFailed {
			end.Kind = wf.run.ErrorKind
			end.Message = wf.run.Error
		}
		if wf.run.SandboxName != "" {
			end.Sandbox = o.harness.GetOrAttachSandbox(wf.run.SandboxName)
		}
		if err := o.saveTerminalRun(ctx, wf.repo, wf.run, wf.hr, prev, eventTime, end); err != nil {
			return reconcile.Result{}, err
		}
		if wf.run.Phase == state.PhaseFailed {
			log.Error(errors.New(wf.run.Error), "run failed", "repo", wf.repo.URL, "run", wf.run.ID)
		}
		return reconcile.Result{}, nil
	}

	if err := o.putRun(ctx, wf.repo, wf.run, wf.hr, prev); err != nil {
		return reconcile.Result{}, fmt.Errorf("updating run: %w", err)
	}
	if prev == state.PhaseSelecting && wf.run.Phase != prev && wf.run.SandboxName != "" {
		if sb := o.harness.GetOrAttachSandbox(wf.run.SandboxName); sb != nil {
			if delErr := o.harness.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
				log.V(2).Info("error deleting sandbox after run left selecting", "sandbox", wf.run.SandboxName, "error", delErr)
			}
		}
	}
	if wf.run.Phase != prev {
		eventTime := o.clock().UTC()
		if t, ok := wf.run.PhaseTimestamps[wf.run.Phase]; ok {
			eventTime = t
		}
		if err := o.store.AppendEvent(ctx, wf.repo.Key(), wf.run.ID, state.Event{
			Time:    eventTime,
			Phase:   wf.run.Phase,
			Kind:    "phase",
			Message: fmt.Sprintf("Run %s entered %s phase", wf.run.ID, wf.run.Phase),
		}); err != nil {
			log.Error(err, "failed to append phase event", "phase", wf.run.Phase, "run", wf.run.ID)
		}
	}
	if res.wait || res.requeueAfter > 0 {
		return reconcile.Result{RequeueAfter: res.requeueAfter}, nil
	}
	o.Notify(wf.repo.Key(), wf.run.ID)
	return reconcile.Result{}, nil
}

// sandboxNameForRun returns the canonical name for a run's Sandbox and HeroRun objects.
func sandboxNameForRun(runID string) string {
	return fmt.Sprintf("run-%s", strings.ToLower(runID))
}

// heroRunOwnerReferences constructs an metav1.OwnerReference slice pointing to the HeroRun.
func heroRunOwnerReferences(heroRun *herorunv1alpha1.HeroRun) []metav1.OwnerReference {
	if heroRun == nil || heroRun.UID == "" {
		return nil
	}
	isController := true
	return []metav1.OwnerReference{
		{
			APIVersion: herorunv1alpha1.GroupVersion.String(),
			Kind:       "HeroRun",
			Name:       heroRun.Name,
			UID:        heroRun.UID,
			Controller: &isController,
		},
	}
}

// portalBindError marks a failure the caller may retry, because the portal can lag
// behind the pod. Every other error from prepareSandbox is final for that Sandbox.
type portalBindError struct {
	err error
}

func (e *portalBindError) Error() string {
	return e.err.Error()
}

func (e *portalBindError) Unwrap() error {
	return e.err
}

// prepareSandbox executes the preparation steps on a ready Sandbox: binds portal egress
// policy if configured, ensures the repository clone is present at the base commit,
// and configures git identity. It is safe to call again on the same Sandbox.
// It writes nothing to the run record and knows nothing about deadlines.
func (o *Orchestrator) prepareSandbox(ctx context.Context, repo *state.Repo, run *state.Run, sb *session.Sandbox) error {
	if sb == nil {
		return errors.New("sandbox handle is nil")
	}

	failCommand := func(desc string, args []string, errMsg string, cmdTime time.Time, res *tools.ExecCommandResult) error {
		var exitCode int
		var stdout, stderr string
		if res != nil {
			exitCode = int(res.ExitCode)
			stdout = res.Stdout
			stderr = res.Stderr
		}

		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    cmdTime,
			Phase:   run.Phase,
			Kind:    "command",
			Message: errMsg,
			Error:   errMsg,
			Data: map[string]any{
				"command":   desc,
				"args":      args,
				"exit_code": exitCode,
				"stdout":    stdout,
				"stderr":    stderr,
			},
		}); err != nil {
			scoped.Log(ctx).Error(err, "failed to append command failure event", "run", run.ID)
		}

		return errors.New(errMsg)
	}

	if o.portalBinder != nil {
		policy, err := o.policyFactory(ctx, portal.PolicyRequest{
			RepoURL:     repo.URL,
			GoalName:    run.GoalName,
			RunID:       run.ID,
			SandboxName: run.SandboxName,
		})
		if err != nil {
			return &portalBindError{err: err}
		}
		podUID, err := sb.PodUID(ctx)
		if err != nil {
			return &portalBindError{err: err}
		}
		if err := o.portalBinder.Bind(ctx, podUID, policy); err != nil {
			return &portalBindError{err: err}
		}

		hosts := portal.PolicyHostnames(policy)
		if hosts == nil {
			hosts = []string{}
		}
		nowBound := o.clock().UTC()
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    nowBound,
			Phase:   run.Phase,
			Kind:    "portal_bound",
			Message: fmt.Sprintf("portal egress policy bound for hosts: %s", strings.Join(hosts, ", ")),
			Data: map[string]any{
				"hosts": append([]string(nil), hosts...),
			},
		}); err != nil {
			scoped.Log(ctx).Error(err, "failed to append portal_bound event", "run", run.ID)
		}
	}

	checkDesc := "git -C /workspace/repo rev-parse --git-dir"
	checkArgs := []string{"git", "-C", "/workspace/repo", "rev-parse", "--git-dir"}
	checkRes, checkErr := sb.ExecCommand(ctx, tools.ExecCommandOptions{
		Command: checkArgs,
	})
	checkTime := o.clock().UTC()

	var workspaceReused bool
	if checkErr != nil {
		return failCommand(checkDesc, checkArgs, fmt.Sprintf("command error: %v", checkErr), checkTime, checkRes)
	} else if checkRes == nil {
		return failCommand(checkDesc, checkArgs, "command returned nil result", checkTime, checkRes)
	} else if checkRes.ExitCode == 0 {
		workspaceReused = true
	} else if checkRes.ExitCode != 128 {
		stderr := strings.TrimSpace(checkRes.Stderr)
		var errMsg string
		if stderr != "" {
			errMsg = fmt.Sprintf("command failed with exit code %d: %s", checkRes.ExitCode, stderr)
		} else {
			errMsg = fmt.Sprintf("command failed with exit code %d", checkRes.ExitCode)
		}
		return failCommand(checkDesc, checkArgs, errMsg, checkTime, checkRes)
	}

	if workspaceReused {
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    checkTime,
			Phase:   run.Phase,
			Kind:    "workspace_reused",
			Message: fmt.Sprintf("Workspace reused in sandbox %s", run.SandboxName),
			Data:    map[string]any{"sandbox": run.SandboxName},
		}); err != nil {
			scoped.Log(ctx).Error(err, "failed to append workspace_reused event", "run", run.ID)
		}
	} else {
		commands := []struct {
			desc string
			args []string
		}{
			{
				desc: fmt.Sprintf("git clone -- %s /workspace/repo", repo.URL),
				args: []string{"git", "clone", "--", repo.URL, "/workspace/repo"},
			},
			{
				desc: fmt.Sprintf("git -C /workspace/repo checkout %s", run.BaseCommit),
				args: []string{"git", "-C", "/workspace/repo", "checkout", run.BaseCommit},
			},
		}

		for _, cmd := range commands {
			scoped.Log(ctx).Info("executing command inside sandbox", "run", run.ID, "cmd", cmd.desc)

			res, execErr := sb.ExecCommand(ctx, tools.ExecCommandOptions{
				Command: cmd.args,
			})

			cmdTime := o.clock().UTC()
			var cmdErrMsg string

			if execErr != nil {
				cmdErrMsg = fmt.Sprintf("command error: %v", execErr)
			} else if res == nil {
				cmdErrMsg = "command returned nil result"
			} else if res.ExitCode != 0 {
				stderr := strings.TrimSpace(res.Stderr)
				if stderr != "" {
					cmdErrMsg = fmt.Sprintf("command failed with exit code %d: %s", res.ExitCode, stderr)
				} else {
					cmdErrMsg = fmt.Sprintf("command failed with exit code %d", res.ExitCode)
				}
			}

			if cmdErrMsg != "" {
				return failCommand(cmd.desc, cmd.args, cmdErrMsg, cmdTime, res)
			}

			// Command succeeded! Record command result event
			if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
				Time:    cmdTime,
				Phase:   run.Phase,
				Kind:    "command",
				Message: fmt.Sprintf("Executed %s", cmd.desc),
				Data: map[string]any{
					"command":   cmd.desc,
					"args":      cmd.args,
					"exit_code": 0,
					"stdout":    res.Stdout,
					"stderr":    res.Stderr,
				},
			}); err != nil {
				scoped.Log(ctx).Error(err, "failed to append command success event", "run", run.ID)
			}
		}
	}

	if o.gitAuthor != "" {
		if err := sb.ConfigureGitIdentity(ctx, "/workspace/repo", o.gitAuthor); err != nil {
			return err
		}

		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    o.clock().UTC(),
			Phase:   run.Phase,
			Kind:    "git_identity_configured",
			Message: fmt.Sprintf("git identity configured: %s", o.gitAuthor),
			Data:    map[string]any{"author": o.gitAuthor},
		}); err != nil {
			scoped.Log(ctx).Error(err, "failed to append git_identity_configured event", "run", run.ID)
		}
	}

	return nil
}

func (o *Orchestrator) releaseSandbox(sandboxName string) {
	if o.harness == nil || sandboxName == "" {
		return
	}
	sb := o.harness.GetOrAttachSandbox(sandboxName)
	if sb != nil {
		o.harness.ReleaseSandbox(sb)
	}
}

func (o *Orchestrator) deleteRunState(runID string) {
	o.runStatesMu.Lock()
	defer o.runStatesMu.Unlock()
	delete(o.runStates, runID)
}

// topLevelSessionID returns the top-level session ID for a run.
// Because the top-level session ID is derived deterministically from the run ID
// via generateSessionID, it is known before the session is created or recorded on the run.
func topLevelSessionID(runID string, sessionID string) string {
	if sessionID != "" {
		return sessionID
	}
	return generateSessionID(runID)
}

// PostMessage enqueues a user message to be delivered to the session of a run before its next model call.
func (o *Orchestrator) PostMessage(ctx context.Context, _ state.RepoKey, runID string, text string) error {
	if strings.TrimSpace(text) == "" {
		return errors.New("message text is required")
	}

	name := sandboxNameForRun(runID)
	var hr herorunv1alpha1.HeroRun
	if err := o.kubeClient.Get(ctx, client.ObjectKey{Namespace: o.namespace(), Name: name}, &hr); err != nil {
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf("run %s not found", runID)
		}
		return fmt.Errorf("getting HeroRun %s: %w", name, err)
	}

	phase := HeroRunPhase(&hr)
	if phase != state.PhaseWorking && phase != state.PhasePending && phase != state.PhasePreparing {
		return fmt.Errorf("%w: run %s is in phase %s", ErrRunNotWorking, runID, phase)
	}

	sessionID := topLevelSessionID(runID, hr.Status.SessionID)

	var target herorunv1alpha1.Session
	if err := o.kubeClient.Get(ctx, client.ObjectKey{Namespace: o.namespace(), Name: sessionID}, &target); err != nil {
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf("%w: session %s not found", ErrRunNotWorking, sessionID)
		}
		return fmt.Errorf("getting Session %s: %w", sessionID, err)
	}
	if target.Spec.RunID != "" && target.Spec.RunID != runID {
		return fmt.Errorf("session %s belongs to run %s, not %s", sessionID, target.Spec.RunID, runID)
	}
	if target.Status.Phase.IsTerminal() || target.DeletionTimestamp != nil {
		return fmt.Errorf("%w: session %s is in phase %s", ErrRunNotWorking, sessionID, target.Status.Phase)
	}
	return o.createSessionMessage(ctx, &target, runID, text)
}

func (o *Orchestrator) createSessionMessage(ctx context.Context, target *herorunv1alpha1.Session, runID, text string) error {
	name, err := o.newSessionMessageName(target.Name)
	if err != nil {
		return err
	}
	spec := herorunv1alpha1.SessionMessageSpec{
		SessionName: target.Name,
		RunID:       cmp.Or(target.Spec.RunID, runID),
		Text:        text,
	}
	msg := &herorunv1alpha1.SessionMessage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cmp.Or(target.Namespace, o.namespace()),
			Labels:    spec.Labels(),
		},
		Spec: spec,
	}
	if err := controllerutil.SetControllerReference(target, msg, o.kubeClient.Scheme()); err != nil {
		return fmt.Errorf("setting owner reference on session message: %w", err)
	}
	if err := o.kubeClient.Create(ctx, msg); err != nil {
		return fmt.Errorf("creating session message: %w", err)
	}
	return nil
}

func (o *Orchestrator) newSessionMessageName(targetName string) (string, error) {
	var suffix [2]byte
	if _, err := cryptorand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generating session message name: %w", err)
	}
	ts := max(o.clock().UTC().UnixNano(), 0)
	seq := o.msgSeq.Add(1)
	return fmt.Sprintf("%s-msg-%016x-%06x-%s", targetName, ts, seq, hex.EncodeToString(suffix[:])), nil
}

// SandboxOwnerForSession returns the shared SandboxOwner for a HeroRun-owned
// Session, configured with the run's replacement sandbox options and on-demand
// workspace and portal preparation. It returns (nil, nil) for standalone
// sessions with no RunID.
func (o *Orchestrator) SandboxOwnerForSession(ctx context.Context, obj *herorunv1alpha1.Session) (*session.SandboxOwner, error) {
	if o == nil || obj == nil || obj.Spec.RunID == "" {
		return nil, nil
	}
	ns := cmp.Or(obj.Namespace, o.namespace())
	hrName := cmp.Or(obj.Spec.HeroRunName, sandboxNameForRun(obj.Spec.RunID))
	var hr herorunv1alpha1.HeroRun
	if err := o.kubeClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: hrName}, &hr); err != nil {
		return nil, fmt.Errorf("getting HeroRun %s: %w", hrName, err)
	}
	run, err := HeroRunToRun(&hr)
	if err != nil {
		return nil, fmt.Errorf("converting HeroRun %s: %w", hrName, err)
	}
	run.ID = cmp.Or(run.ID, obj.Spec.RunID)
	run.SandboxName = cmp.Or(run.SandboxName, obj.Spec.SandboxName)
	run.WorkspaceClaimName = cmp.Or(run.WorkspaceClaimName, obj.Spec.WorkspaceClaimName)
	repo, err := o.store.GetRepo(ctx, run.Repo)
	if err != nil {
		return nil, fmt.Errorf("getting repo %s: %w", run.Repo, err)
	}
	var initialSb *session.Sandbox
	if o.harness != nil && run.SandboxName != "" {
		initialSb = o.harness.GetOrAttachSandbox(run.SandboxName)
	}
	return o.getOrCreateSandboxOwner(ctx, repo, run, &hr, initialSb), nil
}

func (o *Orchestrator) getOrCreateSandboxOwner(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun, initialSb *session.Sandbox) *session.SandboxOwner {
	o.runStatesMu.Lock()
	rs := o.runStates[run.ID]
	if rs == nil {
		rs = &runState{}
		o.runStates[run.ID] = rs
	}
	if rs.sandboxOwner == nil {
		rs.sandboxOwner = session.NewSandboxOwner(o.harness)
	}
	owner := rs.sandboxOwner
	o.runStatesMu.Unlock()

	if initialSb != nil && owner.ActiveSandbox() == nil {
		owner.SetActiveSandbox(initialSb)
	}
	sbOpts := newRunWorkflow(o, repo, run, hr).buildCreateSandboxOptions()
	owner.SetCreateSandboxOptions(sbOpts)
	// Set preparer each time a turn worker starts so it uses the current turn's repo and run.
	owner.SetSandboxPreparer(func(ctx context.Context, s *session.Sandbox) error {
		// When a replacement Sandbox pod becomes ready in the Working phase, the service portal
		// controller's pod watch or NRI registration can briefly lag behind pod readiness.
		// While runPreparing retries via the reconciler's RequeueAfter queue, this worker
		// preparer retries prepareSandbox for up to 10 attempts (5 seconds) while the error is a
		// portalBindError before giving up.
		var err error
		for attempt := 0; attempt < 10; attempt++ {
			if attempt > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(500 * time.Millisecond):
				}
			}
			err = o.prepareSandbox(ctx, repo, run, s)
			var pErr *portalBindError
			if err == nil || !errors.As(err, &pErr) {
				break
			}
		}
		return err
	})
	return owner
}

func (o *Orchestrator) namespace() string {
	return o.harness.Namespace()
}

// saveHeroRun writes the run status to the HeroRun custom resource in kube-apiserver,
// updating the object held by the reconciler directly so that concurrent changes result
// in an API conflict error and a clean retry.
func (o *Orchestrator) saveHeroRun(ctx context.Context, hr *herorunv1alpha1.HeroRun, run *state.Run) error {
	hr.Status = HeroRunFromRun(run).Status
	if err := o.kubeClient.Status().Update(ctx, hr); err != nil {
		return fmt.Errorf("updating HeroRun %s status: %w", hr.Name, err)
	}
	return nil
}

// putRun writes a run record to the HeroRun custom resource in kube-apiserver
// and exports a snapshot to run.json on disk for human inspection.
func (o *Orchestrator) putRun(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun, prev state.RunPhase) error {
	if err := o.saveHeroRun(ctx, hr, run); err != nil {
		return err
	}
	if err := o.store.PutRun(ctx, run); err != nil {
		scoped.Log(ctx).V(2).Info("exporting run.json failed", "run", run.ID, "error", err)
	}
	if run != nil && run.Phase.IsTerminal() {
		o.deleteRunIndex(sandboxNameForRun(run.ID))
		o.deleteRunState(run.ID)
	}
	return nil
}

// runEnd describes how a run terminates and the resources to clean up.
type runEnd struct {
	// Phase is the terminal phase to transition the run into.
	Phase state.RunPhase

	// Kind is the event kind recorded on the terminal event.
	Kind string

	// Message is the summary message recorded on the run and terminal event.
	Message string

	// Data goes on the event as additional key-value metadata.
	Data map[string]any

	// Sandbox is deleted when non-nil.
	Sandbox *session.Sandbox
}

func (o *Orchestrator) isManagerCanceled() bool {
	if o == nil {
		return false
	}
	o.managerCtxMu.Lock()
	defer o.managerCtxMu.Unlock()
	return o.managerCtx != nil && o.managerCtx.Err() != nil
}

func (o *Orchestrator) saveTerminalRun(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun, prev state.RunPhase, endTime time.Time, end runEnd) error {
	if err := o.putRun(ctx, repo, run, hr, prev); err != nil {
		return fmt.Errorf("updating terminal run state: %w", err)
	}

	msg := end.Message
	if end.Phase == state.PhaseFailed && end.Kind == "failed" {
		msg = fmt.Sprintf("Run failed: %s", end.Message)
	}
	event := state.Event{
		Time:    endTime,
		Phase:   end.Phase,
		Kind:    end.Kind,
		Message: msg,
		Data:    end.Data,
	}
	if end.Phase == state.PhaseFailed {
		event.Error = end.Message
	}
	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, event); err != nil {
		scoped.Log(ctx).Error(err, "failed to append event", "kind", end.Kind, "run", run.ID)
	}

	o.deleteRunState(run.ID)

	if end.Sandbox != nil {
		if delErr := o.harness.DeleteSandbox(context.WithoutCancel(ctx), end.Sandbox); delErr != nil {
			scoped.Log(ctx).V(2).Info("error deleting sandbox after run ended", "sandbox", run.SandboxName, "error", delErr)
		}
	}
	return nil
}

func runStartTime(run *state.Run) time.Time {
	if run == nil {
		return time.Time{}
	}
	for _, phase := range []state.RunPhase{state.PhasePreparing, state.PhasePending, state.PhaseWorking} {
		if t, ok := run.PhaseTimestamps[phase]; ok && !t.IsZero() {
			return t
		}
	}
	if t, err := state.ParseRunIDTime(run.ID); err == nil && !t.IsZero() {
		return t
	}
	return time.Time{}
}

func buildSystemPrompt(basePrompt string, repo *state.Repo, run *state.Run, goalBody string) string {
	var sb strings.Builder
	if basePrompt != "" {
		sb.WriteString(basePrompt)
		sb.WriteString("\n\n")
	}

	sb.WriteString("# Goal\n\n")
	sb.WriteString(strings.TrimSpace(goalBody))
	sb.WriteString("\n\n")

	sb.WriteString("# Repository Facts\n\n")
	if repo != nil {
		sb.WriteString(fmt.Sprintf("- URL: %s\n", repo.URL))
		if repo.DefaultBranch != "" {
			sb.WriteString(fmt.Sprintf("- Default Branch: %s\n", repo.DefaultBranch))
		}
	}
	if run != nil {
		if branch := run.BranchName(); branch != "" {
			sb.WriteString(fmt.Sprintf("- Branch: %s\n", branch))
		}
		if run.BaseCommit != "" {
			sb.WriteString(fmt.Sprintf("- Base Commit: %s\n", run.BaseCommit))
		}
	}
	sb.WriteString("- Working Directory: /workspace/repo\n\n")
	sb.WriteString("Commit your changes with git before calling open_pull_request; only commits on the branch are pushed, nothing uncommitted is included.\n\n")

	sb.WriteString("# Rules\n\n")
	sb.WriteString("- Never edit .goals/ or the metric script.\n")
	sb.WriteString("- Keep changes focused.\n")
	sb.WriteString("- Call the finish tool with a summary written for maintainers when you have nothing more to do.\n")

	return sb.String()
}

func generateSessionID(runID string) string {
	var sb strings.Builder
	sb.WriteString("run")
	for _, r := range strings.ToLower(runID) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
		}
	}
	id := sb.String()
	if len(id) > 40 {
		id = id[:40]
	}
	return id
}

// RunningSessions returns the names of non-terminal Session resources currently running for runID.
func (o *Orchestrator) RunningSessions(runID string) map[string]bool {
	if o == nil || o.kubeClient == nil || o.harness == nil || runID == "" {
		return nil
	}
	var list herorunv1alpha1.SessionList
	if err := o.kubeClient.List(context.Background(), &list, client.InNamespace(o.namespace())); err != nil {
		return nil
	}
	var running map[string]bool
	for i := range list.Items {
		s := &list.Items[i]
		matchesRun := s.Spec.RunID == runID || (s.Spec.RunID == "" && s.Labels[LabelRun] == runID)
		if !matchesRun || s.Status.Phase.IsTerminal() || s.DeletionTimestamp != nil {
			continue
		}
		if running == nil {
			running = make(map[string]bool)
		}
		running[s.Name] = true
	}
	return running
}
