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

package llm

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// defaultTokenSource is used by NewFromEnv to obtain a DefaultTokenSource.
// Tests can override it to avoid requiring real Google Cloud credentials.
var defaultTokenSource = func(ctx context.Context, scope ...string) (oauth2.TokenSource, error) {
	return google.DefaultTokenSource(ctx, scope...)
}

// NewFromEnv returns the ChatClient for modelName: the built-in fake for
// FakeModelEliza (which needs no API key), otherwise the OpenAI-compatible
// client configured from the environment.
//
// An explicit GEMINI_API_KEY takes precedence. When OPENAI_BASE_URL is unset
// and VERTEX_PROJECT is set, Vertex AI mode with Workload Identity is selected
// automatically, using VERTEX_LOCATION (defaulting to "global").
func NewFromEnv(modelName string) (ChatClient, error) {
	if modelName == FakeModelEliza {
		return &ElizaClient{}, nil
	}

	baseURL := os.Getenv("OPENAI_BASE_URL")
	geminiKey := os.Getenv("GEMINI_API_KEY")
	if geminiKey != "" {
		return NewClient(baseURL, geminiKey)
	}

	vertexProject := os.Getenv("VERTEX_PROJECT")
	if baseURL == "" && vertexProject != "" {
		location := os.Getenv("VERTEX_LOCATION")
		if location == "" {
			location = "global"
		}
		endpointURL := VertexBaseURL(vertexProject, location)
		ts, err := defaultTokenSource(context.Background(), "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return nil, fmt.Errorf("llm: failed to create default token source: %w", err)
		}
		return NewClient(endpointURL, "", WithTokenSource(ts))
	}

	openaiKey := os.Getenv("OPENAI_API_KEY")
	if openaiKey != "" {
		return NewClient(baseURL, openaiKey)
	}

	return nil, fmt.Errorf("GEMINI_API_KEY, OPENAI_API_KEY, or VERTEX_PROJECT environment variable is required")
}
