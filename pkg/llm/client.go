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

// Package llm provides an OpenAI-compatible chat client and testing doubles.
// Forked from kubernetes-sigs/agent-sandbox/examples/sandboxed-tools (commit f83f2d87c1aeeb90407d8cfe9a0c1fad74044304).
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"golang.org/x/oauth2"
)

// Client interacts with an OpenAI-compatible API endpoint using net/http.
type Client struct {
	baseURL     string
	apiKey      string
	tokenSource oauth2.TokenSource
	httpClient  *http.Client
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithTokenSource configures the Client to authenticate using an OAuth2 TokenSource.
func WithTokenSource(ts oauth2.TokenSource) ClientOption {
	return func(c *Client) {
		c.tokenSource = ts
	}
}

// WithHTTPClient configures a custom http.Client for the Client.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// VertexBaseURL returns the OpenAI-compatible base URL for Vertex AI.
// When location is empty or "global", it uses the global endpoint URL.
func VertexBaseURL(project, location string) string {
	if location == "" || location == "global" {
		return fmt.Sprintf("https://aiplatform.googleapis.com/v1/projects/%s/locations/global/endpoints/openapi", project)
	}
	return fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/endpoints/openapi", location, project, location)
}

// ChatCompletionRequest represents the request body for the chat completions API.
type ChatCompletionRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Tools    []Tool    `json:"tools,omitempty"`
}

// Message represents a single chat message in the conversation history.
type Message struct {
	// Role is the role of the message sender, identifying the "speaker".
	// Common values: "user", "assistant", "system".
	Role string `json:"role"`

	// Content is the content of the message.
	// This is a pointer, because some models expect an empty string to be passed.
	Content *string `json:"content,omitempty"`

	ToolCalls        []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID       string     `json:"tool_call_id,omitempty"`
	ThoughtSignature string     `json:"thought_signature,omitempty"`
	ExtraContent     any        `json:"extra_content,omitempty"`
}

// Tool defines a tool available for the LLM to call.
type Tool struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

// ToolFunction describes the function specification for a tool.
type ToolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

// ToolCall represents a function call requested by the LLM.
type ToolCall struct {
	ID               string       `json:"id"`
	Type             string       `json:"type"`
	Function         FunctionCall `json:"function"`
	ThoughtSignature string       `json:"thought_signature,omitempty"`
	ExtraContent     any          `json:"extra_content,omitempty"`
}

// FunctionCall contains the name and arguments of the requested function.
type FunctionCall struct {
	Name             string `json:"name"`
	Arguments        string `json:"arguments"`
	ThoughtSignature string `json:"thought_signature,omitempty"`
}

// ChatCompletionResponse represents the response body from the chat completions API.
type ChatCompletionResponse struct {
	ID      string   `json:"id"`
	Choices []Choice `json:"choices"`
}

// Choice represents a single completion choice returned by the model.
type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// NewClient initializes a new OpenAI-compatible LLM client.
func NewClient(baseURL, apiKey string, opts ...ClientOption) (*Client, error) {
	c := &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(c)
	}

	if c.apiKey == "" && c.tokenSource == nil {
		return nil, fmt.Errorf("llm: api key or token source is required")
	}
	if c.baseURL == "" {
		c.baseURL = "https://generativelanguage.googleapis.com/v1beta/openai"
	}
	c.baseURL = strings.TrimSuffix(c.baseURL, "/")

	return c, nil
}

// CreateChatCompletion sends a chat completion request to the API.
func (c *Client) CreateChatCompletion(ctx context.Context, req ChatCompletionRequest) (*ChatCompletionResponse, error) {
	log := scoped.Log(ctx)

	url := c.baseURL + "/chat/completions"
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("llm: failed to marshal chat completion request: %w", err)
	}
	log.V(1).Info("making LLM request", "request", string(bodyBytes))

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("llm: failed to create http request: %w", err)
	}

	bearerToken := c.apiKey
	if c.tokenSource != nil {
		tok, err := c.tokenSource.Token()
		if err != nil {
			return nil, fmt.Errorf("llm: failed to obtain token from token source: %w", err)
		}
		if tok == nil {
			return nil, fmt.Errorf("llm: token source returned nil token")
		}
		bearerToken = tok.AccessToken
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+bearerToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("llm: http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.V(2).Info("failed to close response body", "error", closeErr)
		}
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("llm: failed to read response body: %w", err)
	}
	log.V(1).Info("response from LLM", "status", resp.StatusCode, "body", string(respBody))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("llm: api error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var chatResp ChatCompletionResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return nil, fmt.Errorf("llm: failed to decode response: %w", err)
	}

	return &chatResp, nil
}
