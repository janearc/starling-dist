package main

// The forensic record. Every envelope starling delivers is also posted to
// postgrest, so that what agents said to each other survives the pods that said
// it.
//
// Why this exists. In August a teaming effort produced 1,094 messages between
// twenty-two agents, and the only reason any of it can be read today is that
// somebody thought to tar up a directory afterwards. Nine of those agents left
// no other trace at all.
//
// A relay that does not write down what it relayed is a relay whose history is
// an accident.
//
// The recorder MUST never be able to stop the relay. That is the whole design
// constraint and everything below follows from it.
//
// Recording is off the request path, the queue is bounded, a full queue drops
// rather than blocks, retries are bounded, and a postgrest that is down or slow
// or wrong costs starling nothing but a counter.
//
// An agent's message is delivered whether or not anyone is writing it down; the
// reverse -- messages failing because the archive is unavailable -- would make
// the forensics tool the outage.
//
// Why it still retries. Fire-and-forget is about the caller, not about giving
// up on the first packet loss. A single transient failure should not lose a
// message from the record, so there is exponential backoff with jitter, bounded
// to a few attempts.
//
// No retry loop in this estate is unbounded, and this one ends in a drop and a
// counter rather than in a goroutine that never finishes.

import (
	"bytes"
	"context"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// How much of postgrest's unavailability starling is willing to absorb before
// it starts dropping. Sized to a burst rather than to an outage: if postgrest
// is down for a minute the record loses a minute, which is the correct trade
// against holding messages in memory.
const (
	recorderQueueDepth = 512
	recorderAttempts   = 4
	recorderBaseDelay  = 200 * time.Millisecond
	recorderMaxDelay   = 5 * time.Second
	recorderTimeout    = 5 * time.Second
	recorderDrainGrace = 3 * time.Second
)

// Recorder posts delivered envelopes to postgrest.
//
// A nil *Recorder is a working no-op: every method is nil-safe, so a starling
// with no recorder configured needs no branch at the call site. Recording is
// optional infrastructure and the code that calls it should not have to know
// that.
type Recorder struct {
	url     string
	token   string
	client  *http.Client
	queue   chan []byte
	metrics *Metrics
	log     *Logger

	// enabled is read per message rather than at startup, so the switch can
	// become a flipr flag without changing anything here. Today it is set
	// once from the environment; the seam is the point.
	enabled func() bool

	stopOnce sync.Once
	done     chan struct{}
	wg       sync.WaitGroup

	// Cancelled by Close, and the parent of every request context.
	//
	// Without it a shutdown waits out the full client timeout of whatever
	// post is in flight -- which is precisely the case where the archive is
	// already unresponsive, so starling would be slowest to stop exactly
	// when the thing holding it up is the thing that has failed.
	ctx    context.Context
	cancel context.CancelFunc
}

// readToken loads a bearer token from a file. The path comes from the
// environment, never the token itself: a secret in an environment variable is a
// secret in `ps`, in a crash dump, and in every child process starling ever
// spawns.
//
// Missing or unreadable is not fatal -- an unauthenticated postgrest is a valid
// local configuration and a wrong one fails visibly on the first post.
func readToken(path string) string {
	if path == "" {
		return ""
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// NewRecorder builds a recorder, or returns nil when none is configured.
// Returning nil rather than a disabled object is deliberate: the zero
// configuration is "no archive", and that should be visible in main.go rather
// than hidden behind an object that silently does nothing.
func NewRecorder(url, tokenPath string, m *Metrics, log *Logger) *Recorder {
	if url == "" {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Recorder{
		ctx:     ctx,
		cancel:  cancel,
		url:     url,
		token:   readToken(tokenPath),
		client:  &http.Client{Timeout: recorderTimeout},
		queue:   make(chan []byte, recorderQueueDepth),
		metrics: m,
		log:     log,
		enabled: func() bool { return true },
		done:    make(chan struct{}),
	}
	r.wg.Add(1)
	go r.run()
	return r
}

// Record hands an envelope to the recorder and returns immediately.
//
// This is called from the request path, so it does exactly two things that can
// take time: nothing, and nothing. A full queue is a drop with a counter, which
// is the behaviour that keeps a slow archive from becoming a slow relay.
func (r *Recorder) Record(raw []byte) {
	if r == nil || !r.enabled() {
		return
	}
	// Copy: the caller owns its buffer and may reuse it, and a record that
	// changes after it was queued is worse than no record.
	buf := make([]byte, len(raw))
	copy(buf, raw)
	select {
	case r.queue <- buf:
		r.metrics.RecorderQueued.With().Inc()
	default:
		r.metrics.RecorderDropped.With("queue_full").Inc()
	}
}

// run drains the queue until Close. One worker, not a pool: postgrest inserts
// are cheap and ordering within the archive is worth more than throughput here.
func (r *Recorder) run() {
	defer r.wg.Done()
	for {
		select {
		case raw := <-r.queue:
			r.deliver(raw)
		case <-r.done:
			// Drain what is already queued, briefly, so a clean
			// shutdown does not throw away messages that were
			// accepted a moment ago.
			deadline := time.After(recorderDrainGrace)
			for {
				select {
				case raw := <-r.queue:
					r.deliver(raw)
				case <-deadline:
					return
				default:
					return
				}
			}
		}
	}
}

// deliver posts one envelope, retrying transient failures and then giving up.
// Giving up is a first-class outcome: a message that cannot be archived is
// counted and dropped, never queued forever and never retried into a live lock.
func (r *Recorder) deliver(raw []byte) {
	for attempt := 0; attempt < recorderAttempts; attempt++ {
		if attempt > 0 {
			r.metrics.RecorderRetries.With().Inc()
			// Sleep, but wake immediately on shutdown. A backoff
			// that ignores Close turns a five second stop into a
			// thirty second one.
			select {
			case <-time.After(backoffFor(attempt)):
			case <-r.ctx.Done():
				r.metrics.RecorderDropped.With("shutdown").Inc()
				return
			}
		}
		if r.ctx.Err() != nil {
			r.metrics.RecorderDropped.With("shutdown").Inc()
			return
		}
		status, err := r.post(raw)
		switch {
		case err == nil && status < 300:
			r.metrics.RecorderPosted.With("ok").Inc()
			return
		case err == nil && !retryableStatus(status):
			// A 400 will be a 400 next time. Retrying a permanent
			// refusal wastes attempts that a transient failure
			// might have needed.
			r.metrics.RecorderDropped.With("rejected").Inc()
			r.log.Warn(
				"postgrest refused a record",
				map[string]any{"status": status},
			)
			return
		}
	}
	r.metrics.RecorderDropped.With("exhausted").Inc()
	r.log.Warn(
		"gave up archiving a message",
		map[string]any{"attempts": recorderAttempts},
	)
}

// post makes one attempt and reports the status. Errors and statuses are
// returned rather than logged here so that deliver decides what is worth a
// line: one log per abandoned message, not one per attempt.
func (r *Recorder) post(raw []byte) (int, error) {
	ctx, cancel := context.WithTimeout(r.ctx, recorderTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		r.url,
		bytes.NewReader(raw),
	)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Ask postgrest not to send the row back. The archive is write-only
	// from starling's side and the response body is pure cost.
	req.Header.Set("Prefer", "return=minimal")
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// retryableStatus reports whether a status is worth another attempt. Server
// errors and rate limits are; anything the caller got wrong is not.
func retryableStatus(status int) bool {
	return status >= 500 || status == http.StatusTooManyRequests ||
		status == http.StatusRequestTimeout
}

// backoffFor is exponential with full jitter, capped. Jitter is not decoration:
//
// starling posts one record per message, so a postgrest restart would otherwise
// produce a synchronised retry from every queued message at the same instant --
// the thundering herd the estate forbids, aimed at a service that has just come
// back up.
func backoffFor(attempt int) time.Duration {
	d := recorderBaseDelay << (attempt - 1)
	if d > recorderMaxDelay {
		d = recorderMaxDelay
	}
	return time.Duration(rand.Int63n(int64(d) + 1))
}

// Close stops the worker and waits for the short drain. Safe on a nil recorder
// and safe to call twice, because shutdown paths are exactly where a second
// caller shows up.
func (r *Recorder) Close() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		close(r.done)
		// Drain first, then cut. The worker gets its grace period to
		// finish what is already queued; anything still in flight after
		// that is abandoned rather than waited on.
		go func() {
			time.Sleep(recorderDrainGrace)
			r.cancel()
		}()
	})
	r.wg.Wait()
	r.cancel()
}
