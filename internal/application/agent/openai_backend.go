package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	agentv1 "github.com/keelab/keelmesh/gen/agent/v1"
	khttp "github.com/keelab/keelmesh/internal/transport/http"
)

type OpenAIBackend struct {
	BaseURL string
	APIKey  string
	Model   string
	Client  *khttp.Client
}

func NewOpenAIBackend(baseURL, apiKey, model string, client *khttp.Client) (*OpenAIBackend, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" || apiKey == "" || model == "" {
		return nil, fmt.Errorf("agentcore: OpenAI backend requires base URL, API key, and model")
	}
	if client == nil {
		return nil, fmt.Errorf("agentcore: OpenAI HTTP client is required")
	}
	return &OpenAIBackend{BaseURL: baseURL, APIKey: apiKey, Model: model, Client: client}, nil
}

type openAIChatRequest struct {
	Model    string              `json:"model"`
	Messages []openAIChatMessage `json:"messages"`
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatResponse struct {
	Choices []struct {
		Message openAIChatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (b *OpenAIBackend) Execute(ctx context.Context, request *agentv1.ExecuteTaskRequest) (*agentv1.ExecuteTaskResponse, error) {
	return b.execute(ctx, request, nil)
}

func (b *OpenAIBackend) ExecuteWithEvents(ctx context.Context, request *agentv1.ExecuteTaskRequest, publish func(*agentv1.TaskEvent)) (*agentv1.ExecuteTaskResponse, error) {
	if publish != nil {
		publish(&agentv1.TaskEvent{
			TaskId: request.GetTaskId(),
			State:  "progress",
			Progress: &agentv1.ProgressSnapshot{
				Status: "running", Phase: "model", Preview: "calling model",
			},
		})
	}
	return b.execute(ctx, request, publish)
}

func (b *OpenAIBackend) execute(ctx context.Context, request *agentv1.ExecuteTaskRequest, _ func(*agentv1.TaskEvent)) (*agentv1.ExecuteTaskResponse, error) {
	content := strings.TrimSpace(request.GetContent())
	if content == "" {
		return nil, fmt.Errorf("agentcore: model input is empty")
	}
	payload, err := json.Marshal(openAIChatRequest{
		Model:    b.Model,
		Messages: []openAIChatMessage{{Role: "user", Content: content}},
	})
	if err != nil {
		return nil, fmt.Errorf("agentcore: encode model request: %w", err)
	}
	endpoint := b.BaseURL + "/v1/chat/completions"
	if strings.HasSuffix(b.BaseURL, "/v1") {
		endpoint = b.BaseURL + "/chat/completions"
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("agentcore: build model request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+b.APIKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := khttp.Do[*http.Response](ctx, b.Client, "agent-openai", http.MethodPost, httpRequest, func(_ context.Context, response *http.Response) (*http.Response, error) {
		body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		if err != nil {
			return nil, err
		}
		cloned := new(http.Response)
		*cloned = *response
		cloned.Body = io.NopCloser(bytes.NewReader(body))
		cloned.ContentLength = int64(len(body))
		return cloned, nil
	})
	if err != nil {
		return nil, fmt.Errorf("agentcore: call model: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("agentcore: read model response: %w", err)
	}
	var decoded openAIChatResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("agentcore: decode model response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message := response.Status
		if decoded.Error != nil && decoded.Error.Message != "" {
			message = decoded.Error.Message
		}
		return nil, fmt.Errorf("agentcore: model returned %s: %s", response.Status, message)
	}
	if len(decoded.Choices) == 0 || strings.TrimSpace(decoded.Choices[0].Message.Content) == "" {
		return nil, fmt.Errorf("agentcore: model response contains no assistant content")
	}
	return &agentv1.ExecuteTaskResponse{TaskId: request.GetTaskId(), State: "final", Content: decoded.Choices[0].Message.Content}, nil
}
