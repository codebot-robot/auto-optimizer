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

package commands

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

// RunSessionOptions specifies options for running an in-process agent session.
type RunSessionOptions struct {
	SessionName string
	Namespace   string
	Toolset     string
	Image       string
	HomeDir     string
	ToolTimeout time.Duration
	StateDir    string
	Prompt      string
	Yolo        bool
}

// InitDefaults sets default values for RunSessionOptions.
func (o *RunSessionOptions) InitDefaults() {
	if o.SessionName == "" {
		o.SessionName = os.Getenv("SESSION_NAME")
		if o.SessionName == "" {
			o.SessionName = "default"
		}
	}
	if o.Namespace == "" {
		o.Namespace = os.Getenv("SANDBOX_NAMESPACE")
		if o.Namespace == "" {
			o.Namespace = "ao-sandboxes"
		}
	}
	if o.Toolset == "" {
		o.Toolset = os.Getenv("TOOLSET")
		if o.Toolset == "" {
			o.Toolset = "basic"
		}
	}
	if o.HomeDir == "" {
		o.HomeDir = os.Getenv("SANDBOX_HOME_DIR")
		if o.HomeDir == "" {
			o.HomeDir = "/home/clawtainer"
		}
	}
	if o.Image == "" {
		o.Image = os.Getenv("SANDBOX_IMAGE")
	}
	if o.ToolTimeout == 0 {
		o.ToolTimeout = session.DefaultToolTimeout
	}
	if o.StateDir == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			userHome = ""
		}
		o.StateDir = filepath.Join(userHome, ".local", "auto-optimizer")
	}
}

// RunSession runs an in-process agent session.
func RunSession(ctx context.Context, opt *RunSessionOptions) error {
	if opt.HomeDir == "" {
		return errors.New("homeDir must not be empty")
	}
	if opt.SessionName == "" {
		return errors.New("sessionName is required")
	}
	if err := session.ValidateSessionName(opt.SessionName); err != nil {
		return fmt.Errorf("invalid sessionName %q: %w", opt.SessionName, err)
	}

	toolset, err := toolsets.Get(opt.Toolset)
	if err != nil {
		return err
	}

	stateDir := opt.StateDir
	if strings.HasPrefix(stateDir, "~/") {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("getting user home directory: %w", err)
		}
		stateDir = filepath.Join(userHome, stateDir[2:])
	}

	var opts session.RunOptions
	opts.InitDefaults()
	opts.Namespace = opt.Namespace
	opts.HomeDir = opt.HomeDir
	opts.ToolTimeout = opt.ToolTimeout
	opts.StateDir = stateDir
	opts.SystemPrompt = toolset.SystemPrompt()

	image := opt.Image
	if image == "" {
		image = toolset.DefaultImage()
	}
	if image == "" {
		image = session.DefaultImage
	}
	opts.Image = image

	llmClient, err := llm.NewFromEnv(opts.ModelName)
	if err != nil {
		return fmt.Errorf("initializing LLM client: %w", err)
	}

	restConfig, err := session.GetRESTConfig()
	if err != nil {
		return fmt.Errorf("getting Kubernetes configuration: %w", err)
	}

	sandboxClient, err := session.NewSandboxClient(restConfig)
	if err != nil {
		return fmt.Errorf("initializing sandbox client: %w", err)
	}

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = opts.ToolTimeout
	toolset.RegisterTools(toolsRegistry)

	sessionStore := session.NewFileStore(stateDir)
	harness := session.NewHarness(llmClient, sandboxClient, toolsRegistry, opts)

	sess, err := harness.LoadSession(ctx, sessionStore, opt.SessionName)
	if errors.Is(err, session.ErrNotFound) {
		sess, err = harness.NewSession(ctx, sessionStore, opt.SessionName, session.Metadata{Toolset: toolset.Name()})
	}
	if err != nil {
		return fmt.Errorf("building session: %w", err)
	}

	events := &consoleEvents{
		yolo:   opt.Yolo,
		reader: bufio.NewReader(os.Stdin),
	}

	if opt.Prompt != "" {
		return runOneShot(ctx, harness, sess, opt.Prompt, events)
	}

	return runREPL(ctx, harness, sess, events)
}

func runOneShot(ctx context.Context, harness *session.Harness, sess *session.Session, prompt string, events session.TurnEvents) error {
	if err := harness.EnsureSystemPrompt(ctx, sess); err != nil {
		return fmt.Errorf("adding system prompt: %w", err)
	}
	return harness.RunTurn(ctx, sess, prompt, events)
}

func runREPL(ctx context.Context, harness *session.Harness, sess *session.Session, events session.TurnEvents) error {
	isNewSession := len(sess.Messages()) == 0

	if err := harness.EnsureSystemPrompt(ctx, sess); err != nil {
		return fmt.Errorf("adding system prompt: %w", err)
	}

	if isNewSession {
		fmt.Println("================================================================================")
		fmt.Println("Welcome to the auto-optimizer session runner!")
		fmt.Printf("Session Name: %s\n", sess.Name)
		fmt.Println("Type your message (or '/exit' or '/quit' to quit):")
		fmt.Println("================================================================================")
	} else {
		fmt.Println("================================================================================")
		fmt.Printf("Resumed session %q with %d messages in history:\n", sess.Name, len(sess.Messages()))
		fmt.Println("================================================================================")
		for _, msg := range sess.Messages() {
			if msg.Role == "user" && msg.Content != nil {
				fmt.Printf("User> %s\n", *msg.Content)
			} else if msg.Role == "assistant" && msg.Content != nil && *msg.Content != "" {
				fmt.Printf("Agent> %s\n", *msg.Content)
			}
		}
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("\nUser> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("reading standard input: %w", err)
		}

		input := strings.TrimSpace(line)
		if input == "" {
			continue
		}
		if strings.EqualFold(input, "/exit") || strings.EqualFold(input, "/quit") {
			return nil
		}

		if err := harness.RunTurn(ctx, sess, input, events); err != nil {
			return err
		}
	}
}

type consoleEvents struct {
	yolo   bool
	reader *bufio.Reader
}

func (e *consoleEvents) AssistantMessage(_ context.Context, text string) {
	fmt.Printf("\nAgent> %s\n", text)
}

func (e *consoleEvents) ApproveToolCall(_ context.Context, call llm.ToolCall) (bool, error) {
	if e.yolo {
		return true, nil
	}

	fmt.Printf("\nExecute tool %s with arguments %s? [y/N]: ", call.Function.Name, call.Function.Arguments)
	line, err := e.reader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("reading approval response: %w", err)
	}
	resp := strings.TrimSpace(strings.ToLower(line))
	return resp == "y" || resp == "yes", nil
}

func (e *consoleEvents) ToolCallStarted(_ context.Context, _ llm.ToolCall) {}

func (e *consoleEvents) ToolCallFinished(_ context.Context, _ llm.ToolCall, _ error) {}

func (e *consoleEvents) MessageAppended(_ context.Context, _ llm.Message) {}
