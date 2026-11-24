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
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/portal"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets/ao"
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

type turnOutcome struct {
	Err      error
	Finished time.Time
	PRTitle  string
	PRBody   string
	Summary  string
}

// Orchestrator reconciles runs in phase Pending and Working using a controller-runtime controller.
type Orchestrator struct {
	store              state.Store
	sessionStore       session.Store
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

	turnWorkersMu sync.Mutex
	turnWorkers   map[string]*turnWorkerHandle
	workerWG      sync.WaitGroup

	pendingMessagesMu sync.Mutex
	pendingMessagesCh chan struct{}

	managerCtxMu sync.Mutex
	managerCtx   context.Context
}

// ErrRunNotWorking indicates that a message cannot be posted because the run has left the working state.
var ErrRunNotWorking = errors.New("run is no longer working")

// runState holds in-memory runtime state shared across sessions of an active run.
type runState struct {
	sandboxOwner *session.SandboxOwner
	turnBudget   *session.TurnBudget
	children     *childGroup
}

// turnWorkerHandle tracks a live in-process turn worker goroutine.
type turnWorkerHandle struct {
	runID  string
	cancel context.CancelFunc
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
		sessionStore:          opts.SessionStore,
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
		turnWorkers:           make(map[string]*turnWorkerHandle),
	}
	return orch, nil
}

func (o *Orchestrator) turnBudget(runID string) *session.TurnBudget {
	if o == nil {
		return nil
	}
	o.runStatesMu.Lock()
	defer o.runStatesMu.Unlock()
	rs := o.runStates[runID]
	if rs == nil {
		return nil
	}
	if rs.turnBudget == nil {
		rs.turnBudget = session.NewTurnBudget(o.maxTurns)
	}
	return rs.turnBudget
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
// runWorkflow.Run (for Pending, Preparing, Selecting, and OpeningPR) or
// workRun, and returns.
func (o *Orchestrator) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := scoped.Log(ctx)

	run, repo, hr, err := o.getRunForReconcile(ctx, req)
	if err != nil {
		return reconcile.Result{}, err
	}
	if run == nil {
		return reconcile.Result{}, nil
	}

	if run.Phase.IsTerminal() {
		o.deleteRunIndex(req.Name)
		o.cancelRunTurnWorkers(run.ID)
		o.deleteRunState(run.ID)
		return reconcile.Result{}, nil
	}

	switch run.Phase {
	case state.PhasePending, state.PhasePreparing, state.PhaseSelecting, state.PhaseOpeningPR:
		return o.reconcileWorkflow(ctx, newRunWorkflow(o, repo, run, hr))

	case state.PhaseWorking:
		if o.sessionStore != nil && o.harness != nil && o.harness.LLMClient() != nil {
			res, err := o.workRun(ctx, repo, run, hr)
			if err != nil {
				log.Error(err, "working run failed", "repo", repo.URL, "run", run.ID)
				if run.Phase.IsTerminal() {
					return reconcile.Result{}, nil
				}
				return reconcile.Result{}, err
			}
			return res, nil
		}
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
	if res.requeueAfter > 0 {
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

// ensureRunState creates the in-memory state for a run if it does not yet exist.
func (o *Orchestrator) ensureRunState(runID string) *runState {
	o.runStatesMu.Lock()
	defer o.runStatesMu.Unlock()
	rs := o.runStates[runID]
	if rs == nil {
		rs = &runState{}
		o.runStates[runID] = rs
	}
	return rs
}

func newTurnOperationID(sessionID string) (string, error) {
	b := make([]byte, 8)
	if _, err := cryptorand.Read(b); err != nil {
		return "", fmt.Errorf("generating operation ID: %w", err)
	}
	return fmt.Sprintf("%s:%x", sessionID, b), nil
}

func (o *Orchestrator) isTurnWorkerRunning(opID string) bool {
	if opID == "" {
		return false
	}
	o.turnWorkersMu.Lock()
	defer o.turnWorkersMu.Unlock()
	return o.turnWorkers[opID] != nil
}

func (o *Orchestrator) reserveTurnWorker(opID, runID string) bool {
	o.turnWorkersMu.Lock()
	defer o.turnWorkersMu.Unlock()
	if o.turnWorkers == nil {
		o.turnWorkers = make(map[string]*turnWorkerHandle)
	}
	for _, w := range o.turnWorkers {
		if w.runID == runID {
			return false
		}
	}
	o.turnWorkers[opID] = &turnWorkerHandle{runID: runID}
	return true
}

func (o *Orchestrator) setTurnWorkerCancel(opID string, cancel context.CancelFunc) {
	o.turnWorkersMu.Lock()
	defer o.turnWorkersMu.Unlock()
	if w := o.turnWorkers[opID]; w != nil {
		w.cancel = cancel
	}
}

func (o *Orchestrator) deleteTurnWorker(opID string) {
	if opID == "" {
		return
	}
	o.turnWorkersMu.Lock()
	defer o.turnWorkersMu.Unlock()
	delete(o.turnWorkers, opID)
}

func (o *Orchestrator) cancelRunTurnWorkers(runID string) {
	o.turnWorkersMu.Lock()
	var cancels []context.CancelFunc
	for _, w := range o.turnWorkers {
		if w.runID == runID && w.cancel != nil {
			cancels = append(cancels, w.cancel)
		}
	}
	o.turnWorkersMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func completedActiveOperation(opID string, outcome turnOutcome) *herorunv1alpha1.ActiveOperation {
	op := &herorunv1alpha1.ActiveOperation{
		ID:   opID,
		Done: true,
	}
	if !outcome.Finished.IsZero() {
		op.FinishedAt = toMetaTime(&outcome.Finished)
	}
	switch {
	case errors.Is(outcome.Err, session.ErrPullRequestRequested):
		op.Result = herorunv1alpha1.OperationResultPullRequest
		op.PRTitle = outcome.PRTitle
		op.PRBody = outcome.PRBody
	case errors.Is(outcome.Err, session.ErrSessionFinished):
		op.Result = herorunv1alpha1.OperationResultFinished
		op.Summary = outcome.Summary
	case outcome.Err == nil:
		op.Result = herorunv1alpha1.OperationResultCompleted
	case errors.Is(outcome.Err, session.ErrMaxTurns):
		op.Result = herorunv1alpha1.OperationResultMaxTurns
	case isCanceled(outcome.Err):
		op.Result = herorunv1alpha1.OperationResultCanceled
		op.Error = outcome.Err.Error()
	default:
		op.Result = herorunv1alpha1.OperationResultFailed
		op.Error = outcome.Err.Error()
	}
	return op
}

func (o *Orchestrator) recordTurnOutcome(ctx context.Context, runID, opID string, outcome turnOutcome) {
	if ctx.Err() != nil || o.isManagerCanceled() {
		return
	}
	key := client.ObjectKey{Namespace: o.namespace(), Name: sandboxNameForRun(runID)}

	const maxAttempts = 5
	for range maxAttempts {
		var hr herorunv1alpha1.HeroRun
		if err := o.kubeClient.Get(ctx, key, &hr); err != nil {
			if !k8serrors.IsNotFound(err) {
				scoped.Log(ctx).Error(err, "failed to get HeroRun to record turn outcome", "run", runID, "operation", opID)
			}
			return
		}
		if state.RunPhase(hr.Status.Phase).IsTerminal() {
			return
		}
		if hr.Status.ActiveOperation == nil || hr.Status.ActiveOperation.ID != opID || hr.Status.ActiveOperation.Done {
			return
		}
		hr.Status.ActiveOperation = completedActiveOperation(opID, outcome)
		now := metav1.NewTime(o.clock().UTC().Truncate(time.Second))
		hr.Status.ObservedAt = &now
		if err := o.kubeClient.Status().Update(ctx, &hr); err != nil {
			if k8serrors.IsConflict(err) {
				continue
			}
			scoped.Log(ctx).Error(err, "failed to record turn outcome on HeroRun", "run", runID, "operation", opID)
			return
		}
		return
	}
}

func (o *Orchestrator) getWorkerContext(ctx context.Context) context.Context {
	o.managerCtxMu.Lock()
	base := o.managerCtx
	o.managerCtxMu.Unlock()
	if base == nil {
		base = context.WithoutCancel(ctx)
	}
	return scoped.WithLog(base, scoped.Log(ctx))
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

func (o *Orchestrator) deliverPendingMessages(ctx context.Context, repo state.RepoKey, runID string, session *session.Session) error {
	o.pendingMessagesMu.Lock()
	defer o.pendingMessagesMu.Unlock()

	msgs, err := o.store.ReadPendingMessages(ctx, repo, runID, session.Name)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}

	var toAdd []llm.Message
	for _, pm := range msgs {
		text := pm.Text
		if pm.From != "" {
			text = fmt.Sprintf("Message from %s: %s", pm.From, pm.Text)
		}
		toAdd = append(toAdd, llm.Message{
			Role:    "user",
			Content: &text,
		})
	}

	// Persist to session first under the lock.
	if err := session.AddMessages(ctx, toAdd...); err != nil {
		return fmt.Errorf("adding pending messages to session: %w", err)
	}

	// Remove pending messages file only after AddMessages succeeds.
	if err := o.store.ClearPendingMessages(ctx, repo, runID, session.Name); err != nil {
		return fmt.Errorf("clearing pending messages: %w", err)
	}
	return nil
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

func (o *Orchestrator) hasPendingMessages(ctx context.Context, repo state.RepoKey, runID string, sessionID string) (bool, error) {
	sessionID = topLevelSessionID(runID, sessionID)
	o.pendingMessagesMu.Lock()
	defer o.pendingMessagesMu.Unlock()
	msgs, err := o.store.ReadPendingMessages(ctx, repo, runID, sessionID)
	if err != nil {
		return false, err
	}
	return len(msgs) > 0, nil
}

// PostMessage enqueues a user message to be delivered to the session of a run before its next model call.
func (o *Orchestrator) PostMessage(ctx context.Context, repo state.RepoKey, runID string, text string) error {
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

	if err := o.appendPendingMessage(ctx, repo, runID, sessionID, state.PendingMessage{Text: text}); err != nil {
		return err
	}

	o.Notify(repo, runID)
	return nil
}

func (o *Orchestrator) sendMessage(ctx context.Context, children *childGroup, repo *state.Repo, run *state.Run, senderSession *session.Session, recipient string, message string) error {
	if senderSession.Metadata.RunID == "" {
		return errors.New("cannot call send_message: session belongs to no run")
	}
	if recipient == senderSession.Name {
		return errors.New("cannot send message to self")
	}

	_, recipientMeta, err := o.sessionStore.LoadSession(ctx, recipient)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return fmt.Errorf("recipient session %q not found", recipient)
		}
		return fmt.Errorf("loading recipient session %q: %w", recipient, err)
	}
	if recipientMeta == nil || recipientMeta.RunID != run.ID {
		return fmt.Errorf("cannot send message to session %q: not in the same run", recipient)
	}

	if recipient != run.SessionID && !children.isRunning(recipient) {
		return fmt.Errorf("recipient session %q is not running", recipient)
	}

	if err := o.appendPendingMessage(ctx, repo.Key(), run.ID, recipient, state.PendingMessage{From: senderSession.Name, Text: message}); err != nil {
		return err
	}

	o.Notify(repo.Key(), run.ID)
	return nil
}

func (o *Orchestrator) appendPendingMessage(ctx context.Context, repo state.RepoKey, runID, sessionID string, msg state.PendingMessage) error {
	o.pendingMessagesMu.Lock()
	defer o.pendingMessagesMu.Unlock()
	if err := o.store.AppendPendingMessage(ctx, repo, runID, sessionID, msg); err != nil {
		return fmt.Errorf("appending pending message: %w", err)
	}
	if o.pendingMessagesCh != nil {
		close(o.pendingMessagesCh)
		o.pendingMessagesCh = nil
	}
	return nil
}

func (o *Orchestrator) checkPendingMessages(ctx context.Context, repo state.RepoKey, runID, sessionID string) (bool, chan struct{}, error) {
	o.pendingMessagesMu.Lock()
	defer o.pendingMessagesMu.Unlock()
	msgs, err := o.store.ReadPendingMessages(ctx, repo, runID, sessionID)
	if err != nil {
		return false, nil, err
	}
	if len(msgs) > 0 {
		return true, nil, nil
	}
	if o.pendingMessagesCh == nil {
		o.pendingMessagesCh = make(chan struct{})
	}
	return false, o.pendingMessagesCh, nil
}

func (o *Orchestrator) waitForEvent(ctx context.Context, children *childGroup, repo *state.Repo, run *state.Run, sess *session.Session, timeout time.Duration) (string, error) {
	if sess.Metadata.RunID == "" {
		return "", errors.New("cannot call wait: session belongs to no run")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	waitDur, cappedByDeadline := timeout, false
	if run.Deadline != nil {
		if rem := run.Deadline.Sub(o.clock().UTC()); rem <= 0 {
			o.Notify(repo.Key(), run.ID)
			return "Wait ended: run deadline passed.", nil
		} else if rem < waitDur {
			waitDur, cappedByDeadline = rem, true
		}
	}

	timer := time.NewTimer(waitDur)
	defer timer.Stop()

	for {
		hasMsg, msgCh, err := o.checkPendingMessages(ctx, repo.Key(), run.ID, sess.Name)
		if err != nil {
			return "", fmt.Errorf("checking pending messages: %w", err)
		}
		var stopped []string
		var childCh chan struct{}
		if children != nil {
			stopped, childCh = children.popStopped(sess.Name)
		}
		if hasMsg || len(stopped) > 0 {
			var parts []string
			if hasMsg {
				parts = append(parts, "A message arrived.")
			}
			if len(stopped) == 1 {
				parts = append(parts, fmt.Sprintf("Child session %s stopped.", stopped[0]))
			} else if len(stopped) > 1 {
				parts = append(parts, fmt.Sprintf("Child sessions stopped: %s.", strings.Join(stopped, ", ")))
			}
			return strings.Join(parts, " "), nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-msgCh:
		case <-childCh:
		case <-timer.C:
			if cappedByDeadline {
				o.Notify(repo.Key(), run.ID)
				return "Wait ended: run deadline passed.", nil
			}
			return fmt.Sprintf("Timed out after %s with no new messages or stopped child sessions.", timeout), nil
		}
	}
}

func (o *Orchestrator) getOrCreateSandboxOwner(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun, initialSb *session.Sandbox) *session.SandboxOwner {
	o.runStatesMu.Lock()
	rs := o.runStates[run.ID]
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

func isCanceled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if status.Code(err) == codes.Canceled {
		return true
	}
	var se interface{ GRPCStatus() *status.Status }
	if errors.As(err, &se) {
		return se.GRPCStatus().Code() == codes.Canceled
	}
	return false
}

func (o *Orchestrator) finishRun(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun, end runEnd) error {
	if ctx.Err() != nil || o.isManagerCanceled() {
		return nil
	}

	prev := run.Phase
	endTime := o.clock().UTC()
	run.SetPhase(end.Phase, endTime)
	if end.Phase == state.PhaseFailed {
		run.Error = end.Message
	}
	return o.saveTerminalRun(ctx, repo, run, hr, prev, endTime, end)
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

	o.cancelRunTurnWorkers(run.ID)
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

func (o *Orchestrator) workRun(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun) (reconcile.Result, error) {
	log := scoped.Log(ctx)

	if ctx.Err() != nil {
		return reconcile.Result{}, nil
	}

	if op := run.ActiveOperation; op != nil && op.Done {
		return reconcile.Result{}, o.handleTurnOutcome(ctx, repo, run, hr, op)
	}

	if o.isManagerCanceled() {
		return reconcile.Result{}, nil
	}

	now := o.clock().UTC()
	if run.Deadline != nil && !now.Before(*run.Deadline) {
		var sb *session.Sandbox
		if o.harness != nil && run.SandboxName != "" {
			sb = o.harness.GetOrAttachSandbox(run.SandboxName)
		}
		var elapsed time.Duration
		if start := runStartTime(run); !start.IsZero() {
			if e := now.Sub(start); e > 0 {
				elapsed = e
			}
		}
		run.ActiveOperation = nil
		if err := o.finishRun(ctx, repo, run, hr, runEnd{
			Phase:   state.PhaseFailed,
			Kind:    "deadline_exceeded",
			Message: "deadline exceeded",
			Data: map[string]any{
				"deadline": run.Deadline.Format(time.RFC3339),
				"elapsed":  elapsed.String(),
			},
			Sandbox: sb,
		}); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	requeueAfter := time.Duration(0)
	if run.Deadline != nil {
		requeueAfter = run.Deadline.Sub(now)
	}

	if op := run.ActiveOperation; op != nil && o.isTurnWorkerRunning(op.ID) {
		log.V(2).Info("run is already actively executing a session", "run", run.ID, "operation", op.ID)
		return reconcile.Result{RequeueAfter: requeueAfter}, nil
	}

	if err := o.startTurn(ctx, repo, run, hr); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: requeueAfter}, nil
}

func (o *Orchestrator) startTurn(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun) error {
	if o.isManagerCanceled() {
		return nil
	}

	o.ensureRunState(run.ID)

	sessionStarted := false
	if run.SessionID == "" {
		run.SessionID = topLevelSessionID(run.ID, "")
		sessionStarted = true
	}

	opID, err := newTurnOperationID(run.SessionID)
	if err != nil {
		return err
	}
	if !o.reserveTurnWorker(opID, run.ID) {
		return nil
	}

	run.ActiveOperation = &state.ActiveOperation{ID: opID}
	if err := o.putRun(ctx, repo, run, hr, run.Phase); err != nil {
		o.deleteTurnWorker(opID)
		return fmt.Errorf("updating run with active operation: %w", err)
	}
	if sessionStarted {
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    o.clock().UTC(),
			Phase:   state.PhaseWorking,
			Kind:    "session_started",
			Message: fmt.Sprintf("Started session %s", run.SessionID),
			Data:    map[string]any{"session_id": run.SessionID},
		}); err != nil {
			scoped.Log(ctx).Error(err, "failed to append session_started event", "run", run.ID)
		}
	}

	workerCtx, cancel := context.WithCancel(o.getWorkerContext(ctx))
	o.setTurnWorkerCancel(opID, cancel)

	o.workerWG.Add(1)
	go func() {
		defer o.workerWG.Done()
		defer cancel()
		defer o.deleteTurnWorker(opID)
		outcome := o.runTurnWorker(workerCtx, repo, run, hr)
		o.recordTurnOutcome(workerCtx, run.ID, opID, outcome)
		o.deleteTurnWorker(opID)
		o.Notify(repo.Key(), run.ID)
	}()
	return nil
}

func (o *Orchestrator) runTurnWorker(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun) turnOutcome {
	o.ensureRunState(run.ID)

	sandboxName := run.SandboxName
	if sandboxName == "" {
		return turnOutcome{
			Err:      errors.New("sandbox name is empty for working run"),
			Finished: o.clock().UTC(),
		}
	}

	var sb *session.Sandbox
	if o.harness != nil {
		sb = o.harness.GetOrAttachSandbox(sandboxName)
	}

	sessionID := run.SessionID

	goalBody := ""
	if run.Goal != nil {
		goalBody = run.Goal.Body
	} else if run.GoalName != "" {
		goalRec, err := o.store.GetGoal(ctx, repo.Key(), run.GoalName)
		if err == nil && goalRec != nil && goalRec.Goal != nil {
			goalBody = goalRec.Goal.Body
		}
	}

	sess, err := o.harness.LoadSession(ctx, o.sessionStore, sessionID)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			sess, err = o.harness.NewSession(ctx, o.sessionStore, sessionID, session.Metadata{
				Repo:      repo.Key().String(),
				RunID:     run.ID,
				CreatedAt: o.clock().UTC(),
			})
			if err != nil {
				return turnOutcome{
					Err:      fmt.Errorf("creating session %s: %w", sessionID, err),
					Finished: o.clock().UTC(),
				}
			}
		} else {
			return turnOutcome{
				Err:      fmt.Errorf("loading session %s: %w", sessionID, err),
				Finished: o.clock().UTC(),
			}
		}
	}

	// Bind sandbox owner to session
	sess.SetSandboxOwner(o.getOrCreateSandboxOwner(ctx, repo, run, hr, sb))

	// Seed system prompt if new session
	if len(sess.Messages()) == 0 {
		sysPrompt := buildSystemPrompt(o.harness.RunOptions().SystemPrompt, repo, run, goalBody)
		if err := sess.AddMessages(ctx, llm.Message{
			Role:    "system",
			Content: &sysPrompt,
		}); err != nil {
			return turnOutcome{
				Err:      fmt.Errorf("seeding system prompt: %w", err),
				Finished: o.clock().UTC(),
			}
		}
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	childCtx, cancelChildren := context.WithCancel(ctx)
	defer cancelChildren()
	children := newChildGroup(childCtx)
	children.setRunning(sessionID, true)
	o.setRunChildren(run.ID, children)
	defer func() {
		children.setRunning(sessionID, false)
		o.setRunChildren(run.ID, nil)
	}()

	events := &orchestratorTurnEvents{
		orch:    o,
		repoKey: repo.Key(),
		runID:   run.ID,
	}

	// A fresh session starts with RunTurn to supply the initial goal prompt.
	// A resumed session (after restart or previous turn) continues from transcript history.
	hasUserMessage := false
	for _, m := range sess.Messages() {
		if m.Role == "user" {
			hasUserMessage = true
			break
		}
	}

	finishTool := ao.NewFinishTool()
	prTool := ao.NewOpenPullRequestTool(ao.OpenPullRequestExpectation{
		Repository: repo.URL,
		Branch:     run.BranchName(),
	})
	startSessionTool := ao.NewStartSessionTool(func(toolCtx context.Context, task string) (string, error) {
		return o.startChildSession(toolCtx, children, repo, run, sess, task)
	})
	listSessionsTool := ao.NewListSessionsTool(func(toolCtx context.Context) ([]ao.SessionInfo, error) {
		return o.listRunSessions(toolCtx, run, children)
	})
	sendMessageTool := ao.NewSendMessageTool(func(toolCtx context.Context, recipient, message string) error {
		return o.sendMessage(toolCtx, children, repo, run, sess, recipient, message)
	})
	waitTool := ao.NewWaitTool(func(toolCtx context.Context, timeout time.Duration) (string, error) {
		return o.waitForEvent(toolCtx, children, repo, run, sess, timeout)
	})
	budget := o.turnBudget(run.ID)
	messageSource := func(ctx context.Context, session *session.Session) error {
		return o.deliverPendingMessages(ctx, repo.Key(), run.ID, session)
	}
	harness := o.harness.WithTurnBudget(budget).WithExtraTools(finishTool, prTool, startSessionTool, listSessionsTool, sendMessageTool, waitTool).WithMessageSource(messageSource)

	var turnErr error
	if hasUserMessage {
		turnErr = harness.ContinueTurn(sessionCtx, sess, events)
	} else {
		turnErr = harness.RunTurn(sessionCtx, sess, "Please work on the goal.", events)
	}

	cancelChildren()
	children.wg.Wait()

	return turnOutcome{
		Err:      turnErr,
		Finished: o.clock().UTC(),
		PRTitle:  prTool.Title,
		PRBody:   prTool.Body,
		Summary:  finishTool.Summary,
	}
}

func (o *Orchestrator) handleTurnOutcome(ctx context.Context, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun, op *state.ActiveOperation) error {
	log := scoped.Log(ctx)

	if run.Phase.IsTerminal() {
		return nil
	}

	attachSandbox := func() *session.Sandbox {
		if o.harness != nil && run.SandboxName != "" {
			return o.harness.GetOrAttachSandbox(run.SandboxName)
		}
		return nil
	}

	switch op.Result {
	case state.OperationResultPullRequest:
		run.PRTitle = op.PRTitle
		run.PRBody = op.PRBody
		run.ActiveOperation = nil
		prev := run.Phase
		now := o.clock().UTC()
		run.SetPhase(state.PhaseSelecting, now)
		if err := o.putRun(ctx, repo, run, hr, prev); err != nil {
			return fmt.Errorf("updating run to Selecting: %w", err)
		}
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    now,
			Phase:   state.PhaseSelecting,
			Kind:    "pull_request_requested",
			Message: "Pull request requested by agent",
		}); err != nil {
			log.Error(err, "failed to append pull_request_requested event", "run", run.ID)
		}
		o.releaseSandbox(run.SandboxName)
		o.Notify(repo.Key(), run.ID)
		return nil

	case state.OperationResultFinished:
		run.Summary = op.Summary
		run.ActiveOperation = nil
		return o.finishRun(ctx, repo, run, hr, runEnd{
			Phase:   state.PhaseNoImprovement,
			Kind:    "finished",
			Message: op.Summary,
			Sandbox: attachSandbox(),
		})

	case state.OperationResultCompleted, state.OperationResultMaxTurns:
		hasPending, err := o.hasPendingMessages(ctx, repo.Key(), run.ID, run.SessionID)
		if err != nil {
			return fmt.Errorf("checking pending messages: %w", err)
		}
		if hasPending && op.Result != state.OperationResultMaxTurns {
			run.ActiveOperation = nil
			if err := o.putRun(ctx, repo, run, hr, run.Phase); err != nil {
				return fmt.Errorf("clearing active operation: %w", err)
			}
			o.Notify(repo.Key(), run.ID)
			return nil
		}
		run.ActiveOperation = nil
		return o.finishRun(ctx, repo, run, hr, runEnd{
			Phase:   state.PhaseNoImprovement,
			Kind:    "no_improvement",
			Message: "session ended without a pull request",
			Sandbox: attachSandbox(),
		})

	case state.OperationResultCanceled:
		if ctx.Err() != nil || o.isManagerCanceled() {
			return nil
		}
		run.ActiveOperation = nil
		if err := o.putRun(ctx, repo, run, hr, run.Phase); err != nil {
			return fmt.Errorf("clearing active operation: %w", err)
		}
		o.Notify(repo.Key(), run.ID)
		return nil

	default:
		if ctx.Err() != nil || o.isManagerCanceled() {
			return nil
		}
		errMsg := fmt.Sprintf("session error: %s", op.Error)
		run.ActiveOperation = nil
		if err := o.finishRun(ctx, repo, run, hr, runEnd{
			Phase:   state.PhaseFailed,
			Kind:    "failed",
			Message: errMsg,
			Sandbox: attachSandbox(),
		}); err != nil {
			return err
		}
		return errors.New(errMsg)
	}
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

// generateChildSessionName derives a child session name from parent, ensuring
// the result is at most 40 alphanumeric characters.
func (o *Orchestrator) generateChildSessionName(parent string) (string, error) {
	prefix := parent
	if len(prefix) > 28 {
		prefix = prefix[:28]
	}
	b := make([]byte, 4)
	if _, err := cryptorand.Read(b); err != nil {
		return "", fmt.Errorf("generating random session ID suffix: %w", err)
	}
	return fmt.Sprintf("%sc%x", prefix, b), nil
}

// startChildSession creates and starts a child session for parentSession within run.
func (o *Orchestrator) startChildSession(ctx context.Context, children *childGroup, repo *state.Repo, run *state.Run, parentSession *session.Session, task string) (string, error) {
	if parentSession.Metadata.RunID == "" {
		return "", errors.New("cannot call start_session: session belongs to no run")
	}

	if o.harness.LLMClient() == nil {
		return "", errors.New("cannot call start_session: no LLM client configured")
	}

	budget := o.turnBudget(run.ID)
	if budget == nil {
		return "", errors.New("cannot call start_session: run has no turn budget")
	}

	if parentSession.Metadata.Depth() >= o.maxSessionDepth {
		return "", fmt.Errorf("depth limit of %d reached; cannot start child session", o.maxSessionDepth)
	}

	childName, err := o.generateChildSessionName(parentSession.Name)
	if err != nil {
		return "", err
	}

	childMeta := parentSession.Metadata.Child(parentSession.Name)
	childMeta.Toolset = parentSession.Metadata.Toolset
	childMeta.CreatedAt = o.clock().UTC()
	childSess, err := o.harness.NewSession(ctx, o.sessionStore, childName, childMeta)
	if err != nil {
		return "", fmt.Errorf("creating child session: %w", err)
	}

	childSess.SetSandboxOwner(parentSession.SandboxOwner())

	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    o.clock().UTC(),
		Phase:   state.PhaseWorking,
		Kind:    "session_started",
		Message: fmt.Sprintf("Started session %s", childName),
		Data: map[string]any{
			"session_id": childName,
			"parent":     parentSession.Name,
		},
	}); err != nil {
		scoped.Log(ctx).Error(err, "failed to append session_started event", "run", run.ID, "session", childName)
	}

	childStartTool := ao.NewStartSessionTool(func(toolCtx context.Context, subTask string) (string, error) {
		return o.startChildSession(toolCtx, children, repo, run, childSess, subTask)
	})
	childListTool := ao.NewListSessionsTool(func(toolCtx context.Context) ([]ao.SessionInfo, error) {
		return o.listRunSessions(toolCtx, run, children)
	})
	childSendTool := ao.NewSendMessageTool(func(toolCtx context.Context, recipient, message string) error {
		return o.sendMessage(toolCtx, children, repo, run, childSess, recipient, message)
	})
	childWaitTool := ao.NewWaitTool(func(toolCtx context.Context, timeout time.Duration) (string, error) {
		return o.waitForEvent(toolCtx, children, repo, run, childSess, timeout)
	})
	childMessageSource := func(ctx context.Context, session *session.Session) error {
		return o.deliverPendingMessages(ctx, repo.Key(), run.ID, session)
	}
	childHarness := o.harness.WithTurnBudget(budget).WithExtraTools(childStartTool, childListTool, childSendTool, childWaitTool, ao.NewFinishTool()).WithMessageSource(childMessageSource)

	children.setRunning(childName, true)
	children.wg.Add(1)
	o.workerWG.Add(1)
	go func() {
		defer o.workerWG.Done()
		defer children.wg.Done()
		defer children.markStopped(parentSession.Name, childName)

		if len(childSess.Messages()) == 0 {
			sysPrompt := buildSystemPrompt(o.harness.RunOptions().SystemPrompt, repo, run, task)
			if err := childSess.AddMessages(children.ctx, llm.Message{
				Role:    "system",
				Content: &sysPrompt,
			}); err != nil {
				scoped.Log(children.ctx).Error(err, "failed to seed system prompt", "session", childName)
				return
			}
		}

		events := &orchestratorTurnEvents{
			orch:    o,
			repoKey: repo.Key(),
			runID:   run.ID,
		}
		if err := childHarness.RunTurn(children.ctx, childSess, task, events); err != nil {
			scoped.Log(children.ctx).Info("child session finished with error", "session", childName, "error", err)
		}
	}()

	return childName, nil
}

func (o *Orchestrator) listRunSessions(ctx context.Context, run *state.Run, children *childGroup) ([]ao.SessionInfo, error) {
	sessions, err := session.ListRunSessions(ctx, o.sessionStore, run.ID)
	if err != nil {
		return nil, err
	}
	result := make([]ao.SessionInfo, len(sessions))
	for i, s := range sessions {
		st := "stopped"
		if s.Name == run.SessionID || children.isRunning(s.Name) {
			st = "running"
		}
		result[i] = ao.SessionInfo{
			Name:   s.Name,
			Parent: s.Metadata.Parent,
			State:  st,
		}
	}
	return result, nil
}

// childGroup coordinates child sessions started within a turn worker.
type childGroup struct {
	ctx     context.Context
	wg      *sync.WaitGroup
	mu      sync.Mutex
	running map[string]bool
	stopped map[string][]string
	notify  chan struct{}
}

func newChildGroup(ctx context.Context) *childGroup {
	return &childGroup{
		ctx:     ctx,
		wg:      &sync.WaitGroup{},
		running: make(map[string]bool),
		stopped: make(map[string][]string),
	}
}

func (g *childGroup) setRunning(name string, running bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if running {
		g.running[name] = true
	} else {
		delete(g.running, name)
	}
}

func (g *childGroup) markStopped(parent, name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.running, name)
	g.stopped[parent] = append(g.stopped[parent], name)
	if g.notify != nil {
		close(g.notify)
		g.notify = nil
	}
}

func (g *childGroup) popStopped(parent string) ([]string, chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if names := g.stopped[parent]; len(names) > 0 {
		delete(g.stopped, parent)
		return names, nil
	}
	if g.notify == nil {
		g.notify = make(chan struct{})
	}
	return nil, g.notify
}

func (g *childGroup) isRunning(name string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.running[name]
}

func (g *childGroup) runningSnapshot() map[string]bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.running) == 0 {
		return nil
	}
	snap := make(map[string]bool, len(g.running))
	for name, active := range g.running {
		if active {
			snap[name] = true
		}
	}
	return snap
}

func (o *Orchestrator) setRunChildren(runID string, children *childGroup) {
	o.runStatesMu.Lock()
	defer o.runStatesMu.Unlock()
	if rs := o.runStates[runID]; rs != nil {
		rs.children = children
	}
}

// RunningSessions returns the names of sessions currently running for runID.
func (o *Orchestrator) RunningSessions(runID string) map[string]bool {
	o.runStatesMu.Lock()
	rs := o.runStates[runID]
	var children *childGroup
	if rs != nil {
		children = rs.children
	}
	o.runStatesMu.Unlock()
	if children == nil {
		return nil
	}
	return children.runningSnapshot()
}

type orchestratorTurnEvents struct {
	orch    *Orchestrator
	repoKey state.RepoKey
	runID   string
}

func (e *orchestratorTurnEvents) AssistantMessage(_ context.Context, _ string) {}

func (e *orchestratorTurnEvents) ApproveToolCall(_ context.Context, _ llm.ToolCall) (bool, error) {
	return true, nil
}

func (e *orchestratorTurnEvents) ToolCallStarted(ctx context.Context, call llm.ToolCall) {
	scoped.Log(ctx).V(1).Info("tool call started", "tool", call.Function.Name)
}

func (e *orchestratorTurnEvents) ToolCallFinished(ctx context.Context, call llm.ToolCall, err error) {
	scoped.Log(ctx).V(1).Info("tool call finished", "tool", call.Function.Name, "error", err)
	if e.orch != nil {
		e.orch.Notify(e.repoKey, e.runID)
	}
}

func (e *orchestratorTurnEvents) MessageAppended(_ context.Context, _ llm.Message) {
	if e.orch != nil {
		e.orch.Notify(e.repoKey, e.runID)
	}
}
