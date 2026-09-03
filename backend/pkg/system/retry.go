package system

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	// retryAttempts is the total number of tries for a request that fails at
	// the transport level or with a retryable status before giving up.
	retryAttempts = 4
	// retryBackoff is the base delay between attempts; it doubles each try.
	retryBackoff = 800 * time.Millisecond
)

// retryRoundTripper transparently retries requests whose connections drop
// before an HTTP response is received, as well as requests rejected with a
// transient status (429 / 500 / 502 / 503 / 504). LLM API gateways commonly
// throttle bursts of agent traffic with 429s and randomly reset non-browser
// TLS connections; a short backoff-and-retry recovers both transparently.
// Non-transient statuses (4xx other than 429) and cancelled or expired
// contexts are never retried.
type retryRoundTripper struct {
	base     http.RoundTripper
	attempts int
	backoff  time.Duration
}

func newRetryRoundTripper(base http.RoundTripper) *retryRoundTripper {
	return &retryRoundTripper{
		base:     base,
		attempts: retryAttempts,
		backoff:  retryBackoff,
	}
}

func (rt *retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var (
		resp *http.Response
		err  error
	)

	for attempt := 1; attempt <= rt.attempts; attempt++ {
		attemptReq := req
		if attempt > 1 {
			attemptReq = req.Clone(req.Context())
			if req.GetBody != nil {
				body, bodyErr := req.GetBody()
				if bodyErr != nil {
					return nil, bodyErr
				}
				attemptReq.Body = body
			} else if req.Body != nil {
				// Body cannot be replayed safely; return the last failure.
				return resp, err
			}
		}

		resp, err = rt.base.RoundTrip(attemptReq)

		retryable := false
		reason := ""
		if err != nil {
			retryable = isRetryableTransportError(err)
			reason = err.Error()
		} else {
			retryable = isRetryableStatus(resp.StatusCode)
			reason = http.StatusText(resp.StatusCode)
		}

		if !retryable || attempt == rt.attempts {
			return resp, err
		}

		// Draining and closing the rejected response frees the connection
		// for reuse by the next attempt.
		if resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			resp = nil
		}

		delay := rt.backoff * time.Duration(1<<(attempt-1))
		logrus.WithContext(req.Context()).WithFields(logrus.Fields{
			"attempt": attempt,
			"url":     req.URL.Host + req.URL.Path,
			"delay":   delay,
			"reason":  reason,
		}).Warn("transient request failure, retrying")

		select {
		case <-time.After(delay):
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}

	return resp, err
}

func isRetryableStatus(status int) bool {
	return status == http.StatusTooManyRequests ||
		status == http.StatusInternalServerError ||
		status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func isRetryableTransportError(err error) bool {
	if err == nil {
		return false
	}
	// The caller (or its overall client timeout) has already given up;
	// a retry on a dead context would fail immediately.
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}
