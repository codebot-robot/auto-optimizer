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
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

// ServeOptions specifies options for starting the Session API server.
type ServeOptions struct {
	Listen      string
	Namespace   string
	Toolset     string
	Image       string
	HomeDir     string
	ToolTimeout time.Duration
	StateDir    string
}

// InitDefaults sets default values for ServeOptions.
func (o *ServeOptions) InitDefaults() {
	if o.Listen == "" {
		o.Listen = os.Getenv("AO_LISTEN")
		if o.Listen == "" {
			o.Listen = "127.0.0.1:8081"
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

// Serve starts the gRPC session service server.
func Serve(ctx context.Context, opt *ServeOptions) error {
	log := klog.FromContext(ctx)

	if opt.HomeDir == "" {
		return errors.New("homedir must not be empty")
	}

	stateDir := opt.StateDir
	if strings.HasPrefix(stateDir, "~/") {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("getting user home directory: %w", err)
		}
		stateDir = filepath.Join(userHome, stateDir[2:])
	}

	var baseOpts session.RunOptions
	baseOpts.InitDefaults()
	baseOpts.Namespace = opt.Namespace
	baseOpts.HomeDir = opt.HomeDir
	baseOpts.ToolTimeout = opt.ToolTimeout
	baseOpts.StateDir = stateDir

	llmClient, err := llm.NewFromEnv(baseOpts.ModelName)
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

	sessionStore := session.NewFileStore(stateDir)

	harnessFactory := func(toolsetName string) (*session.Harness, error) {
		toolset, err := toolsets.Get(toolsetName)
		if err != nil {
			return nil, err
		}

		runOpts := baseOpts
		runOpts.SystemPrompt = toolset.SystemPrompt()

		image := opt.Image
		if image == "" {
			image = toolset.DefaultImage()
		}
		if image == "" {
			image = session.DefaultImage
		}
		runOpts.Image = image

		toolsRegistry := tools.NewRegistry()
		toolsRegistry.ToolTimeout = runOpts.ToolTimeout
		toolset.RegisterTools(toolsRegistry)

		return session.NewHarness(llmClient, sandboxClient, toolsRegistry, runOpts), nil
	}

	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          sessionStore,
		DefaultToolset: opt.Toolset,
		HarnessFactory: harnessFactory,
	})
	if err != nil {
		return fmt.Errorf("initializing session service server: %w", err)
	}

	listener, err := net.Listen("tcp", opt.Listen)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", opt.Listen, err)
	}
	defer func() {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			log.V(2).Info("failed to close listener", "error", closeErr)
		}
	}()

	grpcServer := grpc.NewServer()
	sessionv1.RegisterSessionServiceServer(grpcServer, srv)

	log.Info("session server listening", "listen", opt.Listen, "stateDir", stateDir)

	errCh := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down session server...")
		grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}
