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

// Package sessionapi implements the gRPC SessionService for auto-optimizer.
package sessionapi

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/run"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrBusy is returned when a turn is already in flight for the session.
var ErrBusy = errors.New("session is busy")

// ErrSessionBelongsToRun indicates that a turn cannot be started on the session
// server because the session belongs to a run and its turns are managed by the orchestrator.
var ErrSessionBelongsToRun = errors.New("cannot start turn: session belongs to a run")

// SessionSummary summarizes an agent session for in-process consumers such as the web UI.
type SessionSummary struct {
	Name         string
	Repo         string
	RunID        string
	Toolset      string
	Parent       string
	Ancestors    []string
	Depth        int
	CreatedAt    time.Time
	MessageCount int
	InFlight     bool
}

// ToolCall represents an assistant tool call.
type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

// Message represents a single message in a session transcript.
type Message struct {
	Role       string
	Content    string
	ToolCallID string
	ToolCalls  []ToolCall
}

// SessionDetail represents the full session transcript and metadata.
type SessionDetail struct {
	Summary  SessionSummary
	Messages []Message
}

// ServerOptions configures a SessionService Server.
type ServerOptions struct {
	Store          session.Store
	StateStore     state.Store
	DefaultToolset string
	// Harness is used when all sessions use a single harness (e.g. in tests).
	Harness *session.Harness
	// HarnessFactory resolves or constructs a Harness for a given toolset name.
	// If nil and Harness is provided, HarnessFactory defaults to returning Harness.
	HarnessFactory func(toolsetName string) (*session.Harness, error)
	KubeClient     client.Client
	Namespace      string
}

// Server implements sessionv1.SessionServiceServer.
type Server struct {
	sessionv1.UnimplementedSessionServiceServer

	store          session.Store
	stateStore     state.Store
	defaultToolset string
	harnessFactory func(toolsetName string) (*session.Harness, error)
	kubeClient     client.Client
	namespace      string

	mu             sync.Mutex
	busySessions   map[string]bool
	activeSessions map[string]*session.Session
}

// NewServer creates a new SessionService Server.
func NewServer(opts ServerOptions) (*Server, error) {
	if opts.Store == nil {
		return nil, errors.New("Store is required")
	}

	defaultToolset := opts.DefaultToolset
	if defaultToolset == "" {
		defaultToolset = "basic"
	}

	factory := opts.HarnessFactory
	if factory == nil {
		if opts.Harness != nil {
			factory = func(_ string) (*session.Harness, error) {
				return opts.Harness, nil
			}
		} else {
			return nil, errors.New("either Harness or HarnessFactory must be provided")
		}
	}

	ns := opts.Namespace
	if ns == "" {
		ns = session.DefaultNamespace
	}

	return &Server{
		store:          opts.Store,
		stateStore:     opts.StateStore,
		defaultToolset: defaultToolset,
		harnessFactory: factory,
		kubeClient:     opts.KubeClient,
		namespace:      ns,
		busySessions:   make(map[string]bool),
		activeSessions: make(map[string]*session.Session),
	}, nil
}

// sessionMetadata returns the metadata for a session, checking in-memory active sessions
// first, then loading from the store.
func (s *Server) sessionMetadata(ctx context.Context, name string) (*session.Metadata, error) {
	s.mu.Lock()
	sess, ok := s.activeSessions[name]
	s.mu.Unlock()
	if ok {
		return &sess.Metadata, nil
	}

	_, meta, err := s.store.LoadSession(ctx, name)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "session %q not found", name)
		}
		return nil, status.Errorf(codes.Internal, "loading session %q: %v", name, err)
	}
	if meta == nil {
		meta = &session.Metadata{}
	}
	return meta, nil
}

// lookupParent validates the parent session name and retrieves its metadata.
// It returns NotFound if the parent session does not exist, and Internal for any loading error.
func (s *Server) lookupParent(ctx context.Context, parent string) (*session.Metadata, error) {
	if err := session.ValidateSessionName(parent); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent session name %q: %v", parent, err)
	}
	meta, err := s.sessionMetadata(ctx, parent)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Errorf(codes.NotFound, "parent session %q not found", parent)
		}
		return nil, status.Errorf(codes.Internal, "loading parent session %q: %v", parent, err)
	}
	return meta, nil
}

// CreateSession creates a new agent session.
func (s *Server) CreateSession(ctx context.Context, req *sessionv1.CreateSessionRequest) (*sessionv1.Session, error) {
	log := scoped.Log(ctx)

	name := req.GetName()
	if err := session.ValidateSessionName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}

	toolset := req.GetToolset()
	if toolset == "" {
		toolset = s.defaultToolset
	}
	if _, err := toolsets.Get(toolset); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unknown toolset %q: %v", toolset, err)
	}

	parent := req.GetParent()
	var meta session.Metadata
	if parent != "" {
		parentMeta, err := s.lookupParent(ctx, parent)
		if err != nil {
			return nil, err
		}
		meta = parentMeta.Child(parent)
	}

	harness, err := s.harnessFactory(toolset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolving harness for toolset %q: %v", toolset, err)
	}

	meta.Toolset = toolset
	meta.CreatedAt = time.Now()

	sess, err := harness.NewSession(ctx, s.store, name, meta)
	if err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "session %q already exists or cannot be created: %v", name, err)
	}

	s.mu.Lock()
	s.activeSessions[name] = sess
	s.mu.Unlock()

	log.Info("created session", "session", name, "toolset", toolset, "parent", parent)

	return &sessionv1.Session{
		Name:         sess.Name,
		Toolset:      sess.Metadata.Toolset,
		Parent:       sess.Metadata.Parent,
		CreationTime: timestamppb.New(sess.Metadata.CreatedAt),
		MessageCount: 0,
	}, nil
}

// ListSessions lists all known sessions and their summaries.
func (s *Server) ListSessions(ctx context.Context, _ *sessionv1.ListSessionsRequest) (*sessionv1.ListSessionsResponse, error) {
	summaries, err := s.store.ListSessions(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing sessions: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var sessions []*sessionv1.Session
	for _, sum := range summaries {
		msgCount := sum.MessageCount
		if act, ok := s.activeSessions[sum.Name]; ok {
			msgCount = len(act.Messages())
		}
		sessions = append(sessions, &sessionv1.Session{
			Name:         sum.Name,
			Toolset:      sum.Metadata.Toolset,
			Parent:       sum.Metadata.Parent,
			CreationTime: timestamppb.New(sum.Metadata.CreatedAt),
			MessageCount: int32(msgCount),
		})
	}

	return &sessionv1.ListSessionsResponse{Sessions: sessions}, nil
}

// GetSession returns details and message count for a single session.
func (s *Server) GetSession(ctx context.Context, req *sessionv1.GetSessionRequest) (*sessionv1.Session, error) {
	name := req.GetName()
	if err := session.ValidateSessionName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}

	s.mu.Lock()
	sess, ok := s.activeSessions[name]
	s.mu.Unlock()

	if ok {
		return &sessionv1.Session{
			Name:         sess.Name,
			Toolset:      sess.Metadata.Toolset,
			Parent:       sess.Metadata.Parent,
			CreationTime: timestamppb.New(sess.Metadata.CreatedAt),
			MessageCount: int32(len(sess.Messages())),
		}, nil
	}

	messages, meta, err := s.store.LoadSession(ctx, name)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "session %q not found", name)
		}
		return nil, status.Errorf(codes.Internal, "loading session %q: %v", name, err)
	}

	var metadata session.Metadata
	if meta != nil {
		metadata = *meta
	}

	return &sessionv1.Session{
		Name:         name,
		Toolset:      metadata.Toolset,
		Parent:       metadata.Parent,
		CreationTime: timestamppb.New(metadata.CreatedAt),
		MessageCount: int32(len(messages)),
	}, nil
}

// ListMessages returns all messages for a session.
func (s *Server) ListMessages(ctx context.Context, req *sessionv1.ListMessagesRequest) (*sessionv1.ListMessagesResponse, error) {
	name := req.GetName()
	if err := session.ValidateSessionName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}

	s.mu.Lock()
	sess, ok := s.activeSessions[name]
	s.mu.Unlock()

	var messages []llm.Message
	if ok {
		messages = sess.Messages()
	} else {
		var err error
		messages, _, err = s.store.LoadSession(ctx, name)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "session %q not found", name)
			}
			return nil, status.Errorf(codes.Internal, "loading session %q: %v", name, err)
		}
	}

	var pbMessages []*sessionv1.Message
	for _, m := range messages {
		pbMessages = append(pbMessages, toProtoMessage(m))
	}

	return &sessionv1.ListMessagesResponse{Messages: pbMessages}, nil
}

// checkCanStartTurn checks whether a turn can be started for the named session.
// Sessions that belong to a run are rejected with ErrSessionBelongsToRun.
func (s *Server) checkCanStartTurn(ctx context.Context, name string) error {
	meta, err := s.sessionMetadata(ctx, name)
	if err != nil {
		if status.Code(err) == codes.NotFound || errors.Is(err, session.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta != nil && meta.RunID != "" {
		return ErrSessionBelongsToRun
	}
	return nil
}

// SendMessage appends a user message and runs one turn of the agentic loop.
func (s *Server) SendMessage(req *sessionv1.SendMessageRequest, stream sessionv1.SessionService_SendMessageServer) error {
	ctx := stream.Context()
	log := scoped.Log(ctx)

	name := req.GetName()
	if err := session.ValidateSessionName(name); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}
	text := req.GetText()
	if text == "" {
		return status.Errorf(codes.InvalidArgument, "message text is required")
	}

	if err := s.checkCanStartTurn(ctx, name); err != nil {
		if errors.Is(err, ErrSessionBelongsToRun) {
			return status.Errorf(codes.FailedPrecondition, "cannot start turn: session %q belongs to a run", name)
		}
		return err
	}

	// Only one turn at a time per session.
	s.mu.Lock()
	if s.busySessions[name] {
		s.mu.Unlock()
		return status.Errorf(codes.FailedPrecondition, "session %q is busy", name)
	}
	s.busySessions[name] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.busySessions, name)
		s.mu.Unlock()
	}()

	sess, harness, err := s.getOrLoadSession(ctx, name)
	if err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}

	// If this is a new session without messages, ensure the system prompt is set and stream it.
	if len(sess.Messages()) == 0 {
		if err := harness.EnsureSystemPrompt(ctx, sess); err != nil {
			return status.Errorf(codes.Internal, "ensuring system prompt: %v", err)
		}
		for _, m := range sess.Messages() {
			if err := stream.Send(&sessionv1.SessionEvent{
				Event: &sessionv1.SessionEvent_MessageAppended{
					MessageAppended: &sessionv1.MessageAppended{
						Message: toProtoMessage(m),
					},
				},
			}); err != nil {
				return err
			}
		}
	}

	events := &streamTurnEvents{stream: stream}
	if err := harness.RunTurn(ctx, sess, text, events); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Error(err, "run turn failed", "session", name)
		if sendErr := stream.Send(&sessionv1.SessionEvent{
			Event: &sessionv1.SessionEvent_Error{
				Error: &sessionv1.Error{
					Message: err.Error(),
				},
			},
		}); sendErr != nil {
			log.V(2).Info("failed to stream error event", "error", sendErr)
		}
		return nil
	}

	if events.sendErr != nil {
		return events.sendErr
	}

	return nil
}

type streamTurnEvents struct {
	stream  sessionv1.SessionService_SendMessageServer
	sendErr error
}

func (e *streamTurnEvents) AssistantMessage(_ context.Context, text string) {
	if err := e.stream.Send(&sessionv1.SessionEvent{
		Event: &sessionv1.SessionEvent_TurnFinished{
			TurnFinished: &sessionv1.TurnFinished{
				Text: text,
			},
		},
	}); err != nil && e.sendErr == nil {
		e.sendErr = err
	}
}

func (e *streamTurnEvents) ApproveToolCall(_ context.Context, _ llm.ToolCall) (bool, error) {
	// Tool calls are auto-approved by the server in this issue.
	if e.sendErr != nil {
		return false, e.sendErr
	}
	return true, nil
}

func (e *streamTurnEvents) ToolCallStarted(_ context.Context, call llm.ToolCall) {
	if err := e.stream.Send(&sessionv1.SessionEvent{
		Event: &sessionv1.SessionEvent_ToolCallStarted{
			ToolCallStarted: &sessionv1.ToolCallStarted{
				ToolCall: toProtoToolCall(call),
			},
		},
	}); err != nil && e.sendErr == nil {
		e.sendErr = err
	}
}

func (e *streamTurnEvents) ToolCallFinished(_ context.Context, call llm.ToolCall, err error) {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	if sendErr := e.stream.Send(&sessionv1.SessionEvent{
		Event: &sessionv1.SessionEvent_ToolCallFinished{
			ToolCallFinished: &sessionv1.ToolCallFinished{
				ToolCall: toProtoToolCall(call),
				Error:    errStr,
			},
		},
	}); sendErr != nil && e.sendErr == nil {
		e.sendErr = sendErr
	}
}

func (e *streamTurnEvents) MessageAppended(_ context.Context, msg llm.Message) {
	if err := e.stream.Send(&sessionv1.SessionEvent{
		Event: &sessionv1.SessionEvent_MessageAppended{
			MessageAppended: &sessionv1.MessageAppended{
				Message: toProtoMessage(msg),
			},
		},
	}); err != nil && e.sendErr == nil {
		e.sendErr = err
	}
}

func toProtoMessage(msg llm.Message) *sessionv1.Message {
	var content string
	if msg.Content != nil {
		content = *msg.Content
	}
	pbMsg := &sessionv1.Message{
		Role:       msg.Role,
		Content:    content,
		ToolCallId: msg.ToolCallID,
	}
	for _, tc := range msg.ToolCalls {
		pbMsg.ToolCalls = append(pbMsg.ToolCalls, toProtoToolCall(tc))
	}
	return pbMsg
}

func toProtoToolCall(tc llm.ToolCall) *sessionv1.ToolCall {
	return &sessionv1.ToolCall{
		Id:   tc.ID,
		Type: tc.Type,
		Function: &sessionv1.FunctionCall{
			Name:      tc.Function.Name,
			Arguments: tc.Function.Arguments,
		},
	}
}

// ListRepositories returns all registered repositories.
func (s *Server) ListRepositories(ctx context.Context, _ *sessionv1.ListRepositoriesRequest) (*sessionv1.ListRepositoriesResponse, error) {
	if s.stateStore == nil {
		return &sessionv1.ListRepositoriesResponse{}, nil
	}

	repos, err := s.stateStore.ListRepos(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing repositories: %v", err)
	}

	var pbRepos []*sessionv1.Repository
	for _, r := range repos {
		var regTime *timestamppb.Timestamp
		if !r.RegisteredTime.IsZero() {
			regTime = timestamppb.New(r.RegisteredTime)
		}
		pbRepos = append(pbRepos, &sessionv1.Repository{
			Url:            r.URL,
			Host:           r.Host,
			Owner:          r.Owner,
			Name:           r.Name,
			DefaultBranch:  r.DefaultBranch,
			ForgeKind:      r.ForgeKind,
			RegisteredTime: regTime,
			HeadCommit:     r.HeadCommit,
		})
	}

	return &sessionv1.ListRepositoriesResponse{Repositories: pbRepos}, nil
}

// ListGoals returns goals for a repository.
func (s *Server) ListGoals(ctx context.Context, req *sessionv1.ListGoalsRequest) (*sessionv1.ListGoalsResponse, error) {
	if s.stateStore == nil {
		return &sessionv1.ListGoalsResponse{}, nil
	}

	repoURL := req.GetRepoUrl()
	if repoURL == "" {
		return nil, status.Error(codes.InvalidArgument, "repo_url is required")
	}

	key, err := state.ParseRepoURL(repoURL)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid repo_url %q: %v", repoURL, err)
	}

	goalsList, err := s.stateStore.ListGoals(ctx, key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing goals: %v", err)
	}

	var pbGoals []*sessionv1.GoalRecord
	for _, g := range goalsList {
		var name, kind, schedule, verify string
		if g.Goal != nil {
			name = g.Goal.Name
			kind = string(g.Goal.Kind())
			schedule = g.Goal.Schedule
			verify = g.Goal.Verify
		}
		rec := &sessionv1.GoalRecord{
			Name:       name,
			Kind:       kind,
			Hash:       g.Hash,
			BaseCommit: g.BaseCommit,
			Schedule:   schedule,
			Verify:     verify,
			LastRunId:  g.LastRunID,
		}
		if g.BestValue != nil {
			rec.BestValue = fmt.Sprintf("%v", *g.BestValue)
		}
		pbGoals = append(pbGoals, rec)
	}

	return &sessionv1.ListGoalsResponse{Goals: pbGoals}, nil
}

// ListRuns returns runs for a repository.
func (s *Server) ListRuns(ctx context.Context, req *sessionv1.ListRunsRequest) (*sessionv1.ListRunsResponse, error) {
	repoURL := req.GetRepoUrl()
	if repoURL == "" {
		return nil, status.Error(codes.InvalidArgument, "repo_url is required")
	}

	if s.kubeClient == nil {
		return &sessionv1.ListRunsResponse{}, nil
	}

	runs, err := run.ListRuns(ctx, s.kubeClient, s.namespace, repoURL)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing runs: %v", err)
	}

	var pbRuns []*sessionv1.RunRecord
	for _, r := range runs {
		pbRuns = append(pbRuns, &sessionv1.RunRecord{
			Id:          r.ID,
			RepoUrl:     repoURL,
			GoalName:    r.GoalName,
			Phase:       string(r.Phase),
			BaseCommit:  r.BaseCommit,
			Branch:      r.Branch,
			PrUrl:       r.PRURL,
			SessionId:   r.SessionID,
			SandboxName: r.SandboxName,
			Error:       r.Error,
		})
	}

	return &sessionv1.ListRunsResponse{Runs: pbRuns}, nil
}

// GetRun returns details and events for a run.
func (s *Server) GetRun(ctx context.Context, req *sessionv1.GetRunRequest) (*sessionv1.GetRunResponse, error) {
	if s.kubeClient == nil {
		return nil, status.Error(codes.FailedPrecondition, "kube client not configured")
	}

	runID := req.GetId()
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	runRec, err := run.GetRun(ctx, s.kubeClient, s.namespace, runID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "run %q not found", runID)
		}
		return nil, status.Errorf(codes.Internal, "finding run: %v", err)
	}

	repoURL := runRec.Repo.String()
	if s.stateStore != nil {
		if repo, err := s.stateStore.GetRepo(ctx, runRec.Repo); err == nil && repo != nil && repo.URL != "" {
			repoURL = repo.URL
		}
	}

	var events []state.Event
	if s.stateStore != nil {
		events, err = s.stateStore.ReadEvents(ctx, runRec.Repo, runRec.ID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "reading events for run: %v", err)
		}
	}

	pbRun := &sessionv1.RunRecord{
		Id:          runRec.ID,
		RepoUrl:     repoURL,
		GoalName:    runRec.GoalName,
		Phase:       string(runRec.Phase),
		BaseCommit:  runRec.BaseCommit,
		Branch:      runRec.Branch,
		PrUrl:       runRec.PRURL,
		SessionId:   runRec.SessionID,
		SandboxName: runRec.SandboxName,
		Error:       runRec.Error,
	}

	var pbEvents []*sessionv1.RunEvent
	for _, e := range events {
		var pbTime *timestamppb.Timestamp
		if !e.Time.IsZero() {
			pbTime = timestamppb.New(e.Time)
		}
		pbEvents = append(pbEvents, &sessionv1.RunEvent{
			Time:    pbTime,
			Phase:   string(e.Phase),
			Kind:    e.Kind,
			Message: e.Message,
			Error:   e.Error,
		})
	}

	return &sessionv1.GetRunResponse{
		Run:    pbRun,
		Events: pbEvents,
	}, nil
}

func (s *Server) getOrLoadSession(ctx context.Context, name string) (*session.Session, *session.Harness, error) {
	s.mu.Lock()
	sess := s.activeSessions[name]
	s.mu.Unlock()

	if sess == nil {
		defaultHarness, err := s.harnessFactory(s.defaultToolset)
		if err != nil {
			return nil, nil, fmt.Errorf("resolving default harness: %w", err)
		}
		loaded, err := defaultHarness.LoadSession(ctx, s.store, name)
		if err != nil {
			if errors.Is(err, session.ErrNotFound) {
				loaded, err = defaultHarness.NewSession(ctx, s.store, name, session.Metadata{
					Toolset:   s.defaultToolset,
					CreatedAt: time.Now(),
				})
				if err != nil {
					return nil, nil, fmt.Errorf("creating session %q: %w", name, err)
				}
				scoped.Log(ctx).Info("auto-created session on send", "session", name, "toolset", s.defaultToolset)
			} else {
				return nil, nil, fmt.Errorf("loading session %q: %w", name, err)
			}
		}
		sess = loaded
		s.mu.Lock()
		s.activeSessions[name] = sess
		s.mu.Unlock()
	}

	toolset := sess.Metadata.Toolset
	if toolset == "" {
		toolset = s.defaultToolset
	}
	harness, err := s.harnessFactory(toolset)
	if err != nil {
		return nil, nil, fmt.Errorf("resolving harness for toolset %q: %w", toolset, err)
	}

	return sess, harness, nil
}

// ListSessionSummaries returns summaries for all known sessions, ordered newest first.
func (s *Server) ListSessionSummaries(ctx context.Context) ([]SessionSummary, error) {
	summaries, err := s.store.ListSessions(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing sessions: %w", err)
	}

	sessionRuns, err := s.sessionRunsMap(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result []SessionSummary
	for _, sum := range summaries {
		var fallbackRunID string
		if sessionRuns != nil {
			fallbackRunID = sessionRuns[sum.Name]
		}
		result = append(result, s.sessionSummaryFromStore(sum, fallbackRunID))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	return result, nil
}

func (s *Server) sessionSummaryFromStore(sum session.SessionSummary, fallbackRunID string) SessionSummary {
	msgCount := sum.MessageCount
	if act, ok := s.activeSessions[sum.Name]; ok {
		msgCount = len(act.Messages())
	}
	runID := sum.Metadata.RunID
	if runID == "" {
		runID = fallbackRunID
	}
	return SessionSummary{
		Name:         sum.Name,
		Repo:         sum.Metadata.Repo,
		RunID:        runID,
		Toolset:      sum.Metadata.Toolset,
		Parent:       sum.Metadata.Parent,
		Ancestors:    sum.Metadata.Ancestors,
		Depth:        sum.Metadata.Depth(),
		CreatedAt:    sum.Metadata.CreatedAt,
		MessageCount: msgCount,
		InFlight:     s.busySessions[sum.Name],
	}
}

func (s *Server) sessionRunsMap(ctx context.Context) (map[string]string, error) {
	if s.kubeClient == nil {
		return nil, nil
	}
	var hrList herorunv1alpha1.HeroRunList
	if err := s.kubeClient.List(ctx, &hrList, client.InNamespace(s.namespace)); err != nil {
		return nil, fmt.Errorf("listing HeroRuns for session runs: %w", err)
	}
	sessionRuns := make(map[string]string)
	for i := range hrList.Items {
		hr := &hrList.Items[i]
		runID := hr.Spec.RunID
		if runID == "" {
			runID = strings.TrimPrefix(hr.Name, "run-")
		}
		if hr.Status.SessionID != "" && runID != "" {
			sessionRuns[hr.Status.SessionID] = runID
		}
	}
	return sessionRuns, nil
}

// GetSessionDetail returns full metadata and messages for a given session.
func (s *Server) GetSessionDetail(ctx context.Context, name string) (*SessionDetail, error) {
	if err := session.ValidateSessionName(name); err != nil {
		return nil, fmt.Errorf("invalid session name %q: %w", name, err)
	}

	s.mu.Lock()
	sess, ok := s.activeSessions[name]
	inFlight := s.busySessions[name]
	s.mu.Unlock()

	var messages []llm.Message
	var meta session.Metadata
	if ok {
		meta = sess.Metadata
		messages = sess.Messages()
	} else {
		var loadedMeta *session.Metadata
		var err error
		messages, loadedMeta, err = s.store.LoadSession(ctx, name)
		if err != nil {
			return nil, err
		}
		if loadedMeta != nil {
			meta = *loadedMeta
		}
	}

	var webMsgs []Message
	for _, m := range messages {
		var content string
		if m.Content != nil {
			content = *m.Content
		}
		var tcs []ToolCall
		for _, tc := range m.ToolCalls {
			tcs = append(tcs, ToolCall{
				ID:        tc.ID,
				Name:      tc.Function.Name,
				Arguments: tc.Function.Arguments,
			})
		}
		webMsgs = append(webMsgs, Message{
			Role:       m.Role,
			Content:    content,
			ToolCallID: m.ToolCallID,
			ToolCalls:  tcs,
		})
	}

	runID := meta.RunID
	if runID == "" {
		sessionRuns, err := s.sessionRunsMap(ctx)
		if err != nil {
			return nil, err
		}
		if sessionRuns != nil {
			runID = sessionRuns[name]
		}
	}

	return &SessionDetail{
		Summary: SessionSummary{
			Name:         name,
			Repo:         meta.Repo,
			RunID:        runID,
			Toolset:      meta.Toolset,
			Parent:       meta.Parent,
			Ancestors:    meta.Ancestors,
			Depth:        meta.Depth(),
			CreatedAt:    meta.CreatedAt,
			MessageCount: len(messages),
			InFlight:     inFlight,
		},
		Messages: webMsgs,
	}, nil
}

// CreateNewSession creates a session with the specified name and toolset.
func (s *Server) CreateNewSession(ctx context.Context, name, toolset string) error {
	_, err := s.CreateSession(ctx, &sessionv1.CreateSessionRequest{
		Name:    name,
		Toolset: toolset,
	})
	return err
}

// ListRunSessions returns all sessions belonging to the specified run,
// ordered by depth and creation time, with each session's parent and depth.
func (s *Server) ListRunSessions(ctx context.Context, runID string) ([]SessionSummary, error) {
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run ID is required")
	}

	sessions, err := session.ListRunSessions(ctx, s.store, runID)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]SessionSummary, len(sessions))
	for i, sum := range sessions {
		result[i] = s.sessionSummaryFromStore(sum, runID)
	}

	return result, nil
}

// SessionDepth returns how deep the session is in the session tree.
// A top-level session (with no parent) has depth 0.
// It returns NotFound if the session does not exist.
func (s *Server) SessionDepth(ctx context.Context, name string) (int, error) {
	if err := session.ValidateSessionName(name); err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid session name %q: %v", name, err)
	}

	meta, err := s.sessionMetadata(ctx, name)
	if err != nil {
		return 0, err
	}
	return meta.Depth(), nil
}

// StartTurn starts one agent turn asynchronously in the background.
// If a turn is already running for the session, ErrBusy is returned.
func (s *Server) StartTurn(ctx context.Context, name, text string) error {
	if err := session.ValidateSessionName(name); err != nil {
		return fmt.Errorf("invalid session name %q: %w", name, err)
	}
	if text == "" {
		return errors.New("message text is required")
	}

	if err := s.checkCanStartTurn(ctx, name); err != nil {
		return err
	}

	s.mu.Lock()
	if s.busySessions[name] {
		s.mu.Unlock()
		return ErrBusy
	}
	s.busySessions[name] = true
	s.mu.Unlock()

	sess, harness, err := s.getOrLoadSession(ctx, name)
	if err != nil {
		s.mu.Lock()
		delete(s.busySessions, name)
		s.mu.Unlock()
		return err
	}

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.busySessions, name)
			s.mu.Unlock()
		}()

		turnCtx := context.Background()
		log := scoped.Log(turnCtx)

		if len(sess.Messages()) == 0 {
			if err := harness.EnsureSystemPrompt(turnCtx, sess); err != nil {
				log.Error(err, "ensuring system prompt failed", "session", name)
				return
			}
		}

		events := &autoApprovedTurnEvents{ctx: turnCtx}
		if err := harness.RunTurn(turnCtx, sess, text, events); err != nil {
			log.Error(err, "run turn failed", "session", name)
		}
	}()

	return nil
}

type autoApprovedTurnEvents struct {
	ctx context.Context
}

func (e *autoApprovedTurnEvents) AssistantMessage(ctx context.Context, text string) {
	scoped.Log(ctx).V(2).Info("assistant replied", "text", text)
}

func (e *autoApprovedTurnEvents) ApproveToolCall(_ context.Context, _ llm.ToolCall) (bool, error) {
	return true, nil
}

func (e *autoApprovedTurnEvents) ToolCallStarted(ctx context.Context, call llm.ToolCall) {
	scoped.Log(ctx).V(2).Info("tool call started", "tool", call.Function.Name)
}

func (e *autoApprovedTurnEvents) ToolCallFinished(ctx context.Context, call llm.ToolCall, err error) {
	scoped.Log(ctx).V(2).Info("tool call finished", "tool", call.Function.Name, "error", err)
}

func (e *autoApprovedTurnEvents) MessageAppended(ctx context.Context, msg llm.Message) {
	scoped.Log(ctx).V(3).Info("message appended", "role", msg.Role)
}
