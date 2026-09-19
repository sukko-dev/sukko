package restpublish

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Client is a REST publish client for POST /api/v1/publish.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// Request is the JSON body for a publish call.
type Request struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// Response is the JSON body returned on successful publish.
type Response struct {
	Status  string `json:"status"`
	Channel string `json:"channel"`
	Mid     string `json:"mid"` // stable message identity of the published message (the ingress record's identity, ADR-0018)
}

// AuthConfig holds authentication credentials for publish requests.
type AuthConfig struct {
	Token  string // JWT via Authorization: Bearer header
	APIKey string // X-API-Key header
}

// NewClient creates a REST publish client targeting the given base URL.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

// Publish sends a publish request and returns the parsed response.
func (c *Client) Publish(ctx context.Context, req Request, auth AuthConfig) (*Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("rest publish: marshal request: %w", err)
	}

	statusCode, respBody, err := c.do(ctx, body, auth, "application/json")
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("rest publish: HTTP %d: %s", statusCode, string(respBody))
	}

	var resp Response
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("rest publish: unmarshal response: %w", err)
	}

	return &resp, nil
}

// PublishRaw sends a raw body for error-case testing (bad JSON, oversized body, wrong content type).
// Returns the HTTP status code, response body, and any transport error.
func (c *Client) PublishRaw(ctx context.Context, body []byte, auth AuthConfig, contentType string) (statusCode int, respBody []byte, err error) {
	return c.do(ctx, body, auth, contentType)
}

func (c *Client) do(ctx context.Context, body []byte, auth AuthConfig, contentType string) (statusCode int, respBody []byte, err error) {
	url := c.baseURL + "/api/v1/publish"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, fmt.Errorf("rest publish: create request: %w", err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if auth.Token != "" {
		req.Header.Set("Authorization", "Bearer "+auth.Token)
	}
	if auth.APIKey != "" {
		req.Header.Set("X-API-Key", auth.APIKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("rest publish: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("rest publish: read response: %w", err)
	}

	return resp.StatusCode, respBody, nil
}
