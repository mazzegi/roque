package http_client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mazzegi/roque/message"
)

func New(baseURL string) *Client {
	return &Client{
		httpClient: &http.Client{},
		baseURL:    baseURL,
	}
}

type Client struct {
	httpClient *http.Client
	baseURL    string
}

func (clt *Client) WriteContext(ctx context.Context, topic message.Topic, msgs ...[]byte) error {
	buf := bytes.Buffer{}
	err := json.NewEncoder(&buf).Encode(msgs)
	if err != nil {
		return fmt.Errorf("json.encode: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, clt.baseURL+"write", &buf)
	if err != nil {
		return fmt.Errorf("http.new-request to %q: %w", clt.baseURL+"write", err)
	}
	q := req.URL.Query()
	q.Add("topic", string(topic))
	req.URL.RawQuery = q.Encode()

	resp, err := clt.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http.client.do: %w", err)
	}
	defer resp.Body.Close()

	respBs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status-code %s: %s", resp.Status, string(respBs))
	}

	return nil
}

func (clt *Client) ReadContext(ctx context.Context, clientID string, topic message.Topic, limit int, wait time.Duration) ([]message.Message, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, clt.baseURL+"read", nil)
	if err != nil {
		return nil, fmt.Errorf("http.new-request to %q: %w", clt.baseURL+"read", err)
	}
	q := req.URL.Query()
	q.Add("topic", string(topic))
	q.Add("clientid", clientID)
	q.Add("limit", fmt.Sprintf("%d", limit))
	q.Add("wait", fmt.Sprintf("%d", wait.Milliseconds()))
	req.URL.RawQuery = q.Encode()

	resp, err := clt.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http.client.do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBs, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status-code %s: %s", resp.Status, string(respBs))
	}

	var msgs []message.Message
	err = json.NewDecoder(resp.Body).Decode(&msgs)
	if err != nil {
		return nil, fmt.Errorf("json.decode: %w", err)
	}
	return msgs, nil
}

func (clt *Client) CommitContext(ctx context.Context, clientID string, topic message.Topic, idx int) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, clt.baseURL+"commit", nil)
	if err != nil {
		return fmt.Errorf("http.new-request to %q: %w", clt.baseURL+"commit", err)
	}
	q := req.URL.Query()
	q.Add("topic", string(topic))
	q.Add("clientid", clientID)
	q.Add("idx", fmt.Sprintf("%d", idx))
	req.URL.RawQuery = q.Encode()

	resp, err := clt.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http.client.do: %w", err)
	}
	defer resp.Body.Close()

	respBs, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status-code %s: %s", resp.Status, string(respBs))
	}

	return nil
}
