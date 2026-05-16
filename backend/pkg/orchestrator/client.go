package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"pentagi/pkg/config"
)

const (
	internalTokenHeader = "X-Pentagi-Internal-Token"
	contentTypeJSON     = "application/json"
)

type TaskClient interface {
	StartTask(ctx context.Context, flowID, taskID int64) error
	ResumeTask(ctx context.Context, flowID, taskID int64) error
}

type RunTaskRequest struct {
	FlowID int64 `json:"flow_id"`
	TaskID int64 `json:"task_id"`
}

type httpTaskClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewTaskClient(cfg *config.Config) TaskClient {
	if cfg == nil || !cfg.LangGraphEnabled {
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

func (c *httpTaskClient) StartTask(ctx context.Context, flowID, taskID int64) error {
	return c.post(ctx, "/runs/start", RunTaskRequest{
		FlowID: flowID,
		TaskID: taskID,
	})
}

func (c *httpTaskClient) ResumeTask(ctx context.Context, flowID, taskID int64) error {
	return c.post(ctx, "/runs/resume", RunTaskRequest{
		FlowID: flowID,
		TaskID: taskID,
	})
}

func (c *httpTaskClient) post(ctx context.Context, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal orchestrator request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create orchestrator request: %w", err)
	}

	req.Header.Set("Content-Type", contentTypeJSON)
	if c.token != "" {
		req.Header.Set(internalTokenHeader, c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call orchestrator service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("orchestrator service returned status %d", resp.StatusCode)
	}

	return nil
}
