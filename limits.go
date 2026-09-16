package main

// Keeping starling standing, and noticing when something is trying not to let
// it.
//
// The threat is an accident, NOT an attacker. Ruled 2026-09-01: the realistic
// failure is an agent clever enough, possibly by accident, to overwhelm an http
// service -- a retry loop that does not know it is a retry loop, rather than
// somebody probing. That frame is what shapes everything here.
//
// Ordering is the part that is easy to get wrong. Verification costs a
// TokenReview against the API server, so a limiter that only runs after
// authentication turns starling into an amplifier pointed at the API server --
// a much worse outcome than starling itself falling over.
//
// So there are two layers and the cheap one is first:
//
//   1. before auth   a global concurrency cap. No identity, no API calls.
//   2. after auth    a token bucket per pod, which is where policy lives.
//
// The limiter is also a sensor. A 429 counted per sender is exactly the spike
// signal this service exists to produce: an agent stuck in a loop shows up as a
// rising refusal rate on one label before it shows up as anything else. So
// refusals are counted by identity, not just enforced.

import (
	"net/http"
	"sync"
	"time"
)

const (
	// How many requests may be in flight across all callers. Generous
	// relative to a handful of agents, low enough that a runaway cannot
	// exhaust goroutines or memory.
	//
	// Long-polls are excluded -- see inflight below -- because a held
	// connection is the normal resting state here, not load.
	maxInFlight = 64

	// Per-pod token bucket: burst allows a normal exchange, rate is the
	// sustained ceiling. An agent posting steadily faster than this is not
	// having a conversation.
	burstPerPod  = 20
	refillPerPod = 30 * time.Second / 20 // 20 messages per 30s sustained

	// The hard transport limit, enforced while reading. A limit checked
	// after the body is in memory has already lost -- the point is to
	// refuse a 500MB POST before it is 500MB of resident memory.
	maxRequestBytes = 64 << 10 // 64 KiB

	// The semantic limit on the text of one murmur -- one message through
	// the relay. Not a resource guard: agents move bulk through git, and a
	// message is for coordination.
	//
	// It closes a propagation path, since pasting wholesale is how register
	// travelled in August and a bounded message has to be summarised
	// instead.
	//
	// WHY 1 KiB, AND WHY IT USED TO BE 8. The first number was picked
	// before anybody had measured a real exchange.
	//
	// On 2026-09-01 six peer messages between two agent sessions were
	// measured at 2.5 to 4.5 KB each -- each one longer than the entire
	// first generation of that project's handoff document -- and every
	// reply to those six was a 170-byte delivery acknowledgement.
	//
	// Nobody read them. An 8 KiB cap refused none of the six, and a cap
	// that admits everything it was measured against is not a cap.
	//
	// So the ceiling has to sit under the smallest message that went
	// unread, or it does not bind. 2 KiB clears 2.5 KB by a fifth, which
	// the same message with its opening paragraph trimmed walks straight
	// through, and the cap is decorative again by next week.
	//
	// 1 KiB is about 150 words: a paragraph, six times the acknowledgement
	// that was actually read, and room for a claim, its evidence and where
	// to look.
	//
	// A sender with more to say than that has something to push and a ref
	// to send.
	maxBodyBytes = 1 << 10 // 1 KiB

	// The assertion form is capped field by field, and that is the part
	// that does the work. A single combined budget lets a sender spend all
	// of it on one field and write the paragraph anyway, which is the thing
	// being removed.
	//
	// Three separate ceilings mean a claim is a sentence whatever else is
	// going on.
	//
	// They sum to 992 bytes, under the 1 KiB body cap on purpose: neither
	// form is the cheap way to say more than the other.
	maxClaimBytes = 240 // a sentence
	// a log line, an error, a diff hunk -- not a file
	maxEvidenceBytes = 512
	maxLocationBytes = 240 // a repo, a ref, a path, a url
)

// inflight is the pre-auth concurrency cap. Deliberately not a rate limiter:
// rate limiting before identity means keying on an IP, which in a cluster is
// shared and tells you nothing. A concurrency cap needs no key at all.
type inflight struct {
	slots chan struct{}
}

// newInflight builds a cap of n concurrent requests.
func newInflight(n int) *inflight {
	return &inflight{slots: make(chan struct{}, n)}
}

// acquire takes a slot, reporting whether one was free. Never blocks: a queued
// request is a request holding memory, and refusing immediately is both cheaper
// and more informative than making a stuck agent wait.
func (i *inflight) acquire() bool {
	select {
	case i.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release returns a slot.
func (i *inflight) release() {
	select {
	case <-i.slots:
	default:
	}
}

// bucket is one pod's token bucket.
type bucket struct {
	tokens float64
	last   time.Time
}

// Throttle is the post-auth, per-identity limiter.
type Throttle struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	burst   float64
	refill  time.Duration
	now     func() time.Time
}

// NewThrottle builds a limiter with the given burst and refill interval.
func NewThrottle(burst int, refill time.Duration) *Throttle {
	return &Throttle{
		buckets: map[string]*bucket{},
		burst:   float64(burst),
		refill:  refill,
		now:     time.Now,
	}
}

// Allow reports whether this pod may make a request now, consuming a token if
// so. Keyed on the pod name, which is established from the token rather than
// supplied -- so unlike an IP-keyed limiter this cannot be evaded by
// reconnecting, and unlike a header-keyed one it cannot be evaded at all.
func (t *Throttle) Allow(pod string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	b, ok := t.buckets[pod]
	if !ok {
		b = &bucket{tokens: t.burst, last: now}
		t.buckets[pod] = b
	}

	// Refill for elapsed time, capped at burst.
	if t.refill > 0 {
		gained := float64(now.Sub(b.last)) / float64(t.refill)
		if gained > 0 {
			b.tokens += gained
			if b.tokens > t.burst {
				b.tokens = t.burst
			}
			b.last = now
		}
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Forget drops a pod's bucket. Called when a channel is retired, so a fleet
// that has been running for weeks does not accumulate an entry per session that
// ever existed.
func (t *Throttle) Forget(pod string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.buckets, pod)
}

// limitBody caps a request body while it is being read, and returns the reader
// to use. The 64 KiB ceiling is on the WIRE; the 8 KiB message limit is checked
// after parsing, because "your message is too long" and "your request was
// absurd" deserve different answers.
func limitBody(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
}
