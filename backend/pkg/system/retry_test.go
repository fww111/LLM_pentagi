package system

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type fakeRoundTripper struct {
	failures int   // number of initial attempts that fail
	err      error // error to return on failing attempts
	calls    int
	bodies   []string // captured body of each attempt
}

func (f *fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	f.calls++
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		req.Body.Close()
		f.bodies = append(f.bodies, string(b))
	}
	if f.calls <= f.failures {
		return nil, f.err
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader("ok")),
		Header:     http.Header{},
	}, nil
}

func fastRetry() *retryRoundTripper {
	rt := newRetryRoundTripper(http.DefaultTransport)
	rt.backoff = time.Millisecond
	return rt
}

func TestRetryRoundTripperRecoversFromTransportFailures(t *testing.T) {
	fake := &fakeRoundTripper{failures: 2, err: errors.New("connection reset by peer")}
	rt := fastRetry()
	rt.base = fake

	req, _ := http.NewRequest("POST", "https://example.com/v1/chat", bytes.NewBufferString(`{"q":1}`))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString(`{"q":1}`)), nil
	}

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if fake.calls != 3 {
		t.Fatalf("expected 3 calls (2 failures + 1 success), got %d", fake.calls)
	}
	for i, body := range fake.bodies {
		if body != `{"q":1}` {
			t.Fatalf("attempt %d: body not replayed correctly: %q", i+1, body)
		}
	}
}

func TestRetryRoundTripperGivesUpAfterMaxAttempts(t *testing.T) {
	fake := &fakeRoundTripper{failures: 100, err: errors.New("connection reset by peer")}
	rt := fastRetry()
	rt.base = fake

	req, _ := http.NewRequest("POST", "https://example.com/v1/chat", bytes.NewBufferString(`{}`))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(`{}`)), nil }

	if _, err := rt.RoundTrip(req); err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if fake.calls != retryAttempts {
		t.Fatalf("expected %d calls, got %d", retryAttempts, fake.calls)
	}
}

func TestRetryRoundTripperDoesNotRetryContextCanceled(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &fakeRoundTripper{failures: 100, err: context.Canceled}
	rt := fastRetry()
	rt.base = fake

	req, _ := http.NewRequest("POST", "https://example.com/v1/chat", nil)
	req = req.WithContext(cancelled)

	if _, err := rt.RoundTrip(req); err == nil {
		t.Fatal("expected error")
	}
	if fake.calls != 1 {
		t.Fatalf("cancelled request must not be retried, got %d calls", fake.calls)
	}
}

func TestRetryRoundTripperDoesNotRetryContextDeadline(t *testing.T) {
	fake := &fakeRoundTripper{failures: 100, err: context.DeadlineExceeded}
	rt := fastRetry()
	rt.base = fake

	req, _ := http.NewRequest("POST", "https://example.com/v1/chat", nil)

	if _, err := rt.RoundTrip(req); err == nil {
		t.Fatal("expected error")
	}
	if fake.calls != 1 {
		t.Fatalf("deadline-exceeded request must not be retried, got %d calls", fake.calls)
	}
}

func TestRetryRoundTripperNoReplayWithoutGetBody(t *testing.T) {
	fake := &fakeRoundTripper{failures: 100, err: errors.New("connection reset")}
	rt := fastRetry()
	rt.base = fake

	// Body without GetBody: not safe to replay, first failure returns error.
	req, _ := http.NewRequest("POST", "https://example.com/v1/chat", io.NopCloser(strings.NewReader(`{}`)))
	req.ContentLength = -1

	if _, err := rt.RoundTrip(req); err == nil {
		t.Fatal("expected error")
	}
	if fake.calls != 1 {
		t.Fatalf("non-replayable body must not be retried, got %d calls", fake.calls)
	}
}
