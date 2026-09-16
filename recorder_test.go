package main

// Tests for the forensic recorder. The property under test throughout is the
// one the design turns on: the archive cannot slow down or fail the relay.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor polls a condition rather than sleeping a fixed interval, so a slow
// machine does not turn a passing test into a flake and a fast one does not pay
// for the slow machine.
func waitFor(t *testing.T, why string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", why)
}

// A nil recorder is the no-configuration case and every method must survive it,
// because main.go builds one from an environment variable that is usually
// unset.
func TestNilRecorderIsSafe(t *testing.T) {
	var r *Recorder
	r.Record([]byte(`{"id":"x"}`))
	r.Close()
}

// The happy path: what starling delivered is what postgrest receives, byte for
// byte, with the headers that make the insert cheap.
func TestRecorderPostsTheEnvelope(t *testing.T) {
	var got atomic.Value
	var ctype, prefer atomic.Value
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			got.Store(string(b))
			ctype.Store(r.Header.Get("Content-Type"))
			prefer.Store(r.Header.Get("Prefer"))
			w.WriteHeader(http.StatusCreated)
		}),
	)
	defer srv.Close()

	m := NewMetrics()
	r := NewRecorder(srv.URL, "", m, NewLogger())
	defer r.Close()

	body := `{"id":"m1","body":"hello"}`
	r.Record([]byte(body))
	waitFor(
		t,
		"the post",
		func() bool { return m.RecorderPosted.With("ok").Value() == 1 },
	)

	if got.Load() != body {
		t.Fatalf("archive got %q, want %q", got.Load(), body)
	}
	if ctype.Load() != "application/json" {
		t.Fatalf("Content-Type = %q", ctype.Load())
	}
	if prefer.Load() != "return=minimal" {
		t.Fatalf("Prefer = %q", prefer.Load())
	}
}

// The caller owns its buffer. A record that changes after it was queued is
// worse than no record, so Record copies.
func TestRecorderCopiesTheCallersBuffer(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			got.Store(string(b))
			w.WriteHeader(http.StatusCreated)
		}),
	)
	defer srv.Close()

	m := NewMetrics()
	r := NewRecorder(srv.URL, "", m, NewLogger())
	defer r.Close()

	buf := []byte(`{"id":"first"}`)
	r.Record(buf)
	copy(buf, []byte(`{"id":"SECOND"}`))
	waitFor(
		t,
		"the post",
		func() bool { return m.RecorderPosted.With("ok").Value() == 1 },
	)
	if got.Load() != `{"id":"first"}` {
		t.Fatalf(
			"archive got %q; the caller's later write reached it",
			got.Load(),
		)
	}
}

// The property the whole design exists for: an archive that hangs must not hold
// up the relay. Record is called from the request path and must return
// immediately even when nothing is draining.
func TestRecordNeverBlocks(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-block
		}),
	)
	defer srv.Close()
	defer close(block)

	m := NewMetrics()
	r := NewRecorder(srv.URL, "", m, NewLogger())
	defer r.Close()

	// Far more than the queue holds, against a server that answers nothing.
	start := time.Now()
	for i := 0; i < recorderQueueDepth*2; i++ {
		r.Record([]byte(`{"id":"x"}`))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf(
			"Record blocked: %d calls took %s",
			recorderQueueDepth*2,
			elapsed,
		)
	}
	if m.RecorderDropped.With("queue_full").Value() == 0 {
		t.Fatal("a full queue should drop and count, not grow")
	}
}

// A transient failure is retried. One 500 then success, and the message reaches
// the archive rather than being lost to a single bad moment.
func TestRecorderRetriesTransientFailure(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
		}),
	)
	defer srv.Close()

	m := NewMetrics()
	r := NewRecorder(srv.URL, "", m, NewLogger())
	defer r.Close()

	r.Record([]byte(`{"id":"retry"}`))
	waitFor(
		t,
		"the retry to land",
		func() bool { return m.RecorderPosted.With("ok").Value() == 1 },
	)
	if calls.Load() != 2 {
		t.Fatalf("server saw %d calls, want 2", calls.Load())
	}
	if m.RecorderRetries.With().Value() != 1 {
		t.Fatalf(
			"retries = %d, want 1",
			m.RecorderRetries.With().Value(),
		)
	}
}

// A refusal that will not change is not retried. Spending four attempts on a
// permanent 400 wastes the budget a transient failure would have needed.
func TestRecorderDoesNotRetryPermanentRefusal(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		}),
	)
	defer srv.Close()

	m := NewMetrics()
	r := NewRecorder(srv.URL, "", m, NewLogger())
	defer r.Close()

	r.Record([]byte(`{"id":"bad"}`))
	waitFor(
		t,
		"the refusal",
		func() bool { return m.RecorderDropped.With("rejected").Value() == 1 },
	)
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf(
			"server saw %d calls; a 400 must not be retried",
			calls.Load(),
		)
	}
}

// Persistent failure ends in a drop and a counter, not in a goroutine retrying
// forever. Giving up is a first-class outcome here.
func TestRecorderGivesUpAndCounts(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}),
	)
	defer srv.Close()

	m := NewMetrics()
	r := NewRecorder(srv.URL, "", m, NewLogger())
	defer r.Close()

	r.Record([]byte(`{"id":"doomed"}`))
	waitFor(
		t,
		"exhaustion",
		func() bool { return m.RecorderDropped.With("exhausted").Value() == 1 },
	)
	if calls.Load() != int64(recorderAttempts) {
		t.Fatalf(
			"server saw %d calls, want %d",
			calls.Load(),
			recorderAttempts,
		)
	}
}

// No URL means no archive, and that must be visible as nil rather than as an
// object that quietly discards everything.
func TestNoURLMeansNoRecorder(t *testing.T) {
	if r := NewRecorder("", "", NewMetrics(), NewLogger()); r != nil {
		t.Fatal("an unconfigured recorder should be nil")
	}
}

// A disabled recorder queues nothing. The seam exists so the switch can become
// a flipr flag without touching the request path.
func TestDisabledRecorderQueuesNothing(t *testing.T) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("a disabled recorder posted")
		}),
	)
	defer srv.Close()

	m := NewMetrics()
	r := NewRecorder(srv.URL, "", m, NewLogger())
	defer r.Close()
	r.enabled = func() bool { return false }

	r.Record([]byte(`{"id":"x"}`))
	time.Sleep(100 * time.Millisecond)
	if m.RecorderQueued.With().Value() != 0 {
		t.Fatal("a disabled recorder queued a message")
	}
}

// Backoff must grow, stay under the cap, and vary. The jitter is what stops a
// postgrest restart from being met by every queued message at the same instant.
func TestBackoffIsBoundedAndJittered(t *testing.T) {
	for attempt := 1; attempt <= 8; attempt++ {
		for i := 0; i < 50; i++ {
			d := backoffFor(attempt)
			if d < 0 || d > recorderMaxDelay {
				t.Fatalf(
					"attempt %d produced %s, outside [0, "+
						"%s]",
					attempt,
					d,
					recorderMaxDelay,
				)
			}
		}
	}
	distinct := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		distinct[backoffFor(4)] = true
	}
	if len(distinct) < 5 {
		t.Fatalf(
			"backoff produced %d distinct values in 50 draws; "+
				"jitter is not working",
			len(distinct),
		)
	}
}

// Close is called from shutdown paths, which is exactly where a second caller
// turns up.
func TestCloseIsIdempotent(t *testing.T) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusCreated)
		}),
	)
	defer srv.Close()
	r := NewRecorder(srv.URL, "", NewMetrics(), NewLogger())
	r.Close()
	r.Close()
}

// A token is read from a file, never taken from the environment directly. A
// missing file is not fatal: an unauthenticated postgrest is a valid local
// configuration.
func TestTokenComesFromAFile(t *testing.T) {
	path := t.TempDir() + "/token"
	if err := writeFile(path, "  s3cret\n"); err != nil {
		t.Fatal(err)
	}
	if got := readToken(path); got != "s3cret" {
		t.Fatalf("readToken = %q, want %q", got, "s3cret")
	}
	if got := readToken(""); got != "" {
		t.Fatalf("readToken(\"\") = %q, want empty", got)
	}
	if got := readToken(path + "-absent"); got != "" {
		t.Fatalf(
			"a missing token file should read as empty, got %q",
			got,
		)
	}
}

// writeFile is a test helper, kept here so the production file has no
// filesystem writer in it at all.
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
