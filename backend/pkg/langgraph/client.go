package langgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Client interface {
	StartRun(ctx context.Context, req StartRunRequest) (StartRunResponse, error)
	ResumeRun(ctx context.Context, req ResumeRunRequest) (ResumeRunResponse, error)
	CancelRun(ctx context.Context, req CancelRunRequest) error
	GetState(ctx context.Context, contextID string) (StateSnapshot, error)
}

type HTTPClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPClient(baseURL string, timeout time.Duration) (*HTTPClient, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("LangGraph URL is empty")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	return &HTTPClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: timeout},
	}, nil
}

func (c *HTTPClient) StartRun(ctx context.Context, req StartRunRequest) (StartRunResponse, error) {
	var resp StartRunResponse
	err := c.doJSON(ctx, http.MethodPost, "/runs/start", req, &resp)
	return resp, err
}

func (c *HTTPClient) ResumeRun(ctx context.Context, req ResumeRunRequest) (ResumeRunResponse, error) {
	var resp ResumeRunResponse
	err := c.doJSON(ctx, http.MethodPost, "/runs/resume", req, &resp)
	return resp, err
}

func (c *HTTPClient) CancelRun(ctx context.Context, req CancelRunRequest) error {
	return c.doJSON(ctx, http.MethodPost, "/runs/cancel", req, nil)
}

func (c *HTTPClient) GetState(ctx context.Context, contextID string) (StateSnapshot, error) {
	var resp StateSnapshot
	err := c.doJSON(ctx, http.MethodGet, "/state/"+contextID, nil, &resp)
	return resp, err
}

func (c *HTTPClient) doJSON(ctx context.Context, method, path string, body any, out any) error {
	var reqBody *bytes.Reader
	if body == nil {
		reqBody = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("LangGraph API returned %s", resp.Status)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
