package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"pentagentx/pkg/config"
)

const (
	internalTokenHeader = "X-Pentagentx-Internal-Token"
	contentTypeJSON     = "application/json"
)

type TaskClient interface {
	StartTask(ctx context.Context, flowID, taskID int64) (*TaskSnapshot, error)
	ResumeTask(ctx context.Context, flowID, taskID int64, userInput string) (*TaskSnapshot, error)
}

type RunTaskRequest struct {
	FlowID int64 `json:"flow_id"`
	TaskID int64 `json:"task_id"`
}

type ResumeTaskRequest struct {
	FlowID    int64  `json:"flow_id"`
	TaskID    int64  `json:"task_id"`
	UserInput string `json:"user_input"`
}

type TaskSnapshot struct {
	HasInterrupt bool   `json:"-"`
	InterruptMsg string `json:"-"`
	IsCompleted  bool   `json:"-"`
	NextNodes    []string `json:"-"`
}

type httpTaskClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewTaskClient(cfg *config.Config) TaskClient {
	if cfg == nil {
		return nil
	}

	timeout := time.Duration(cfg.LangGraphTimeout) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}

	return &httpTaskClient{
		baseURL: strings.TrimRight(cfg.LangGraphURL, "/"),
		token:   cfg.LangGraphInternalToken,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *httpTaskClient) StartTask(ctx context.Context, flowID, taskID int64) (*TaskSnapshot, error) {
	return c.post(ctx, "/runs/start", RunTaskRequest{
		FlowID: flowID,
		TaskID: taskID,
	})
}

func (c *httpTaskClient) ResumeTask(ctx context.Context, flowID, taskID int64, userInput string) (*TaskSnapshot, error) {
	return c.post(ctx, "/runs/resume", ResumeTaskRequest{
		FlowID:    flowID,
		TaskID:    taskID,
		UserInput: userInput,
	})
}

func (c *httpTaskClient) post(ctx context.Context, path string, payload any) (*TaskSnapshot, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal orchestrator request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator request: %w", err)
	}

	req.Header.Set("Content-Type", contentTypeJSON)
	if c.token != "" {
		req.Header.Set(internalTokenHeader, c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call orchestrator service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("orchestrator service returned status %d", resp.StatusCode)
	}

	// Parse interrupt info from the response
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode orchestrator response: %w", err)
	}

	snapshot := &TaskSnapshot{
		HasInterrupt: false,
		IsCompleted:  false,
	}

	// Check for interrupts
	if interrupts, ok := result["interrupts"].([]any); ok && len(interrupts) > 0 {
		snapshot.HasInterrupt = true
		for _, intr := range interrupts {
			if intrMap, ok := intr.(map[string]any); ok {
				if msg, ok := intrMap["value"].(map[string]any); ok {
					if text, ok := msg["message"].(string); ok {
						snapshot.InterruptMsg = text
					}
				}
			}
		}
	}

	// Parse next nodes
	if next, ok := result["next"].([]any); ok {
		for _, n := range next {
			if s, ok := n.(string); ok {
				snapshot.NextNodes = append(snapshot.NextNodes, s)
			}
		}
	}

	// Determine completion: no interrupts AND no next nodes means graph finished
	// Also check the "task" field from Python's _serialize_snapshot for explicit task_status
	if taskInfo, ok := result["task"].(map[string]any); ok {
		if status, ok := taskInfo["task_status"].(string); ok && status == "completed" {
			snapshot.IsCompleted = true
		}
	}
	// If no explicit status but also no next nodes and no interrupts, mark completed
	if !snapshot.HasInterrupt && len(snapshot.NextNodes) == 0 {
		snapshot.IsCompleted = true
	}

	return snapshot, nil
}
