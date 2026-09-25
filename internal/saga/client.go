package saga

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
)

// CommandClient calls the command service over HTTP. The saga is a separate process
// and coordinates other services through their APIs - it does not reach into their
// databases.
type CommandClient struct {
	baseURL string
	http    *http.Client
}

// NewCommandClient builds a client pointed at the command service base URL.
func NewCommandClient(baseURL string, hc *http.Client) *CommandClient {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &CommandClient{baseURL: baseURL, http: hc}
}

// Pay records a payment on the order (real PaymentReceived event).
func (c *CommandClient) Pay(ctx context.Context, orderID uuid.UUID, amount int64, ref string) error {
	return c.post(ctx, fmt.Sprintf("/orders/%s/pay", orderID), map[string]any{
		"amount": amount, "payment_ref": ref,
	})
}

// Cancel cancels the order (used as compensation).
func (c *CommandClient) Cancel(ctx context.Context, orderID uuid.UUID, reason string) error {
	return c.post(ctx, fmt.Sprintf("/orders/%s/cancel", orderID), map[string]any{
		"reason": reason,
	})
}

func (c *CommandClient) post(ctx context.Context, path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("command service %s -> %d: %s", path, resp.StatusCode, string(msg))
	}
	return nil
}
