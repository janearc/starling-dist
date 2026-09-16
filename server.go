package main

// The HTTP surface. Six endpoints, and the order things happen in matters more
// than any of them individually.
//
// The middleware order is the design:
//
//   1. inflight cap    no identity needed, no API calls made. First because
//                      verification costs a TokenReview, and a limiter that
//                      only runs after auth turns starling into an amplifier
//                      pointed at the API server.
//   2. body cap        enforced while reading. A limit checked after the body
//                      is in memory has already lost.
//   3. authenticate    establishes the pod from its token. Everything below
//                      this line knows who is calling, as a fact.
//   4. throttle        per-pod, keyed on that established identity -- so it
//                      cannot be evaded by reconnecting, which is the usual
//                      weakness of an IP-keyed limiter.
//
// Every refusal is counted by sender. The limiter is a sensor as much as a
// guard: an agent stuck in a retry loop shows up as a rising refusal rate on
// one label long before it shows up as anything else.

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Aliases resolves an operator-facing name to a pod. Kept behind an interface
// because the real implementation reads flipr, and a test should not need one.
//
// Resolution never travels back to the caller: an agent addresses "ingest" and
// is not told which pod that is. Not for secrecy -- an agent pod has no inbound
// surface, so knowing buys nothing -- but because a name an agent can read is a
// role it can inhabit.
type Aliases interface {
	Resolve(name string) (pod string, ok bool)
}

// StaticAliases is a fixed table, for tests and for running without flipr.
type StaticAliases map[string]string

// Resolve looks the name up.
func (s StaticAliases) Resolve(name string) (string, bool) {
	p, ok := s[name]
	return p, ok
}

// Server is starling.
type Server struct {
	store    *Store
	verifier Verifier
	aliases  Aliases
	metrics  *Metrics
	recorder *Recorder
	throttle *Throttle
	inflight *inflight

	// Bodies already seen, for echo detection. Bounded: this is a signal,
	// not an archive, and an unbounded map keyed on message content is a
	// slow leak with an obvious cause.
	echoMu   sync.Mutex
	echoSeen map[string]time.Time

	// Pods with a long-poll currently open. One per pod: there is exactly
	// one poller in a pod, so a second concurrent poll is a bug rather than
	// demand.
	pollMu sync.Mutex
	polls  map[string]bool

	now func() time.Time

	// How long an inbox long-poll may be held. Bounded here rather than
	// trusted from the request: a caller asking to wait an hour would hold
	// a connection and a goroutine for an hour.
	maxWait time.Duration
}

// beginPoll claims the single poll slot for a pod, reporting whether it was
// free. Not a queue: a caller that already has a poll open and opens another is
// wrong, and making it wait would hide that rather than surface it.
func (s *Server) beginPoll(pod string) bool {
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	if s.polls[pod] {
		return false
	}
	s.polls[pod] = true
	return true
}

// endPoll releases a pod's poll slot.
func (s *Server) endPoll(pod string) {
	s.pollMu.Lock()
	delete(s.polls, pod)
	s.pollMu.Unlock()
}

// NewServer wires the pieces together.
func NewServer(store *Store, v Verifier, a Aliases, m *Metrics) *Server {
	return &Server{
		store:    store,
		verifier: v,
		aliases:  a,
		metrics:  m,
		throttle: NewThrottle(burstPerPod, refillPerPod),
		inflight: newInflight(maxInFlight),
		polls:    map[string]bool{},
		echoSeen: map[string]time.Time{},
		now:      time.Now,
		maxWait:  30 * time.Second,
	}
}

// SetRecorder attaches the forensic recorder. Set after construction rather
// than passed to NewServer because recording is optional and every test would
// otherwise have to say so.
func (s *Server) SetRecorder(r *Recorder) { s.recorder = r }

// Handler builds the mux.
//
// What is behind the capacity cap and what is NOT. An earlier version wrapped
// the whole mux, which was wrong twice over.
//
// It put /health, /metrics and /api behind the same 64 slots as everything
// else, so under load monitoring goes Dark first -- the worst possible
// ordering, since the endpoints that explain a problem stop answering exactly
// when there is a problem to explain.
//
// And it held a slot for the entire duration of an /inbox long-poll, which is
// thirty seconds by design, so the fleet would hard-stop at around 64 pollers
// doing nothing but waiting.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Unauthenticated and uncapped. A health check that needs credentials
	// reports unhealthy when the API server hiccups; one that needs a free
	// slot reports unhealthy when the service is busy, which is when you
	// need it.
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /api", s.handleAPI)

	// Short authenticated requests: capacity-capped, because they are quick
	// and a flood of them is what the cap exists for.
	mux.Handle("POST /channel", s.capacity(s.authenticated(s.handleClaim)))
	mux.Handle("POST /ticket", s.capacity(s.authenticated(s.handleTicket)))
	mux.Handle(
		"POST /message",
		s.capacity(s.authenticated(s.handleMessage)),
	)
	mux.Handle(
		"POST /heartbeat",
		s.capacity(s.authenticated(s.handleHeartbeat)),
	)

	// The long-poll is NOT capacity-capped: holding a connection open is
	// its correct resting state, not load, and counting it against a cap
	// sized for bursts makes waiting look like pressure.
	//
	// It is bounded instead by one concurrent poll per pod, which is the
	// true shape -- there is exactly one poller in a pod, so a second
	// concurrent poll is a bug rather than demand.
	mux.Handle("POST /inbox", s.authenticated(s.handleInbox))

	return mux
}

// capacity is the pre-auth concurrency cap. Refuses immediately rather than
// queueing: a queued request is a request holding memory, and a stuck agent
// learns nothing from being made to wait.
func (s *Server) capacity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.inflight.acquire() {
			// No identity yet, so this cannot be attributed. That
			// is the price of refusing before authentication and it
			// is the right trade.
			writeErr(
				w,
				http.StatusServiceUnavailable,
				"starling is at capacity",
			)
			return
		}
		defer s.inflight.release()
		next.ServeHTTP(w, r)
	})
}

// authHandler is a handler that knows who is calling.
type authHandler func(http.ResponseWriter, *http.Request, Identity)

// authenticated establishes identity, then applies the per-pod throttle.
//
// The body cap lives here rather than in capacity, because every route that
// reads a body is authenticated and /inbox is no longer wrapped by capacity. A
// cap that only applies to some POST routes is a cap somebody will walk through
// without noticing.
func (s *Server) authenticated(next authHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limitBody(w, r)
		tok := BearerToken(r.Header)
		if tok == "" {
			s.metrics.AuthFailed.With("no_token").Inc()
			writeErr(w, http.StatusUnauthorized, "no bearer token")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		id, err := s.verifier.Verify(ctx, tok)
		if err != nil {
			if errors.Is(err, ErrUnauthenticated) {
				s.metrics.AuthFailed.With("rejected").Inc()
				writeErr(
					w,
					http.StatusUnauthorized,
					"token not accepted",
				)
				return
			}
			// Could not reach the API server. FAIL, do not pass: an
			// unverified sender is the thing starling exists to
			// make impossible.
			s.metrics.AuthFailed.With("unavailable").Inc()
			writeErr(
				w,
				http.StatusServiceUnavailable,
				"cannot verify identity right now",
			)
			return
		}

		if !s.throttle.Allow(id.Pod) {
			s.metrics.Throttled.With(id.Pod).Inc()
			writeErr(w, http.StatusTooManyRequests, "slow down")
			return
		}

		next(w, r, id)
	})
}

// handleClaim opens a channel for a session.
func (s *Server) handleClaim(
	w http.ResponseWriter,
	r *http.Request,
	id Identity,
) {
	var in struct {
		Note string `json:"note"`
	}
	if !decode(w, r, &in) {
		return
	}
	// The rate limit is NOT reset here, and an earlier version of this
	// handler did reset it.
	//
	// That was evadable: Allow runs before this handler, so an agent could
	// spend nineteen tokens on /message, spend the twentieth on /channel,
	// and have the reset mint a fresh full bucket.
	//
	// Sustained rate was then bounded by round-trip latency rather than by
	// policy -- and reachable by accident, since a crash-looping wrapper
	// that claims at startup would never be throttled at all.
	//
	// So the bucket is a property of the POD across time, not of a session.
	// A new session inheriting a depleted bucket recovers in a few seconds
	// at the refill rate, which is the right cost for the guarantee.
	info, err := s.store.ClaimChannel(id, in.Note)
	if err != nil {
		writeErr(
			w,
			http.StatusInternalServerError,
			"could not claim a channel",
		)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"channel": info.Channel,
		"pod":     info.Pod,
		"pod_uid": info.UID,
	})
}

// handleTicket opens a channel between this session and a peer, or restarts the
// idle clock on the one they already have.
//
// The requester does not consent on the peer's behalf and does not need to: a
// ticket grants reachability, not attention. The peer still has to read its
// mail and does not have to answer.
//
// Asking it first is not available -- an idle Claude session is blocked on a
// read with no loop and no timer, so the only agents that could consent are the
// ones already busy.
func (s *Server) handleTicket(
	w http.ResponseWriter,
	r *http.Request,
	id Identity,
) {
	var in struct {
		To string `json:"to"`
	}
	if !decode(w, r, &in) {
		return
	}
	if strings.TrimSpace(in.To) == "" {
		writeErr(w, http.StatusBadRequest, "no peer named")
		return
	}

	mine, err := s.store.CurrentChannel(id.Pod)
	if err != nil {
		writeErr(
			w,
			http.StatusConflict,
			"claim a channel before asking for a ticket",
		)
		return
	}

	pod := in.To
	if resolved, ok := s.aliases.Resolve(in.To); ok {
		pod = resolved
	}
	peer, err := s.store.CurrentChannel(pod)
	if err != nil {
		writeErr(
			w,
			http.StatusNotFound,
			fmt.Sprintf("no live session for %q", in.To),
		)
		return
	}

	tk, err := s.store.OpenTicket(mine, peer)
	if err != nil {
		writeErr(
			w,
			http.StatusInternalServerError,
			"could not open a ticket",
		)
		return
	}
	s.metrics.TicketsIssued.With(id.Pod).Inc()

	// `to` is the name the caller used, echoed back, NOT the pod it
	// resolved to. Resolution never travels back to an agent: a name an
	// agent can read is a role it can inhabit, and a ticket reply would be
	// a tidy place to leak one.
	writeJSON(w, http.StatusOK, map[string]any{
		"ticket":             tk.ID,
		"to":                 in.To,
		"expires_in_seconds": int(ticketIdleTTL.Seconds()),
	})
}

// handleMessage accepts a message, decides what to do with it, and delivers it.
func (s *Server) handleMessage(
	w http.ResponseWriter,
	r *http.Request,
	id Identity,
) {
	var in Murmur
	if !decode(w, r, &in) {
		s.metrics.Dropped.With(id.Pod, "malformed").Inc()
		return
	}

	if reason, advice, ok := validate(in); !ok {
		// The label is the short reason; the advice goes to the caller
		// only.
		//
		// An earlier version put the whole sentence in the label, which
		// read badly in a legend and would have forked into a second
		// series for the same condition the moment anybody reworded the
		// advice. Metric labels are identifiers, not messages.
		s.metrics.Dropped.With(id.Pod, reason).Inc()
		msg := reason
		if advice != "" {
			msg = reason + ": " + advice
		}
		writeErr(w, http.StatusBadRequest, msg)
		return
	}

	// Resolve the alias. An unknown name and a pod with no live session are
	// different answers -- one is a typo, the other is a dead agent -- so
	// they are counted and reported separately.
	pod := in.To
	if resolved, ok := s.aliases.Resolve(in.To); ok {
		pod = resolved
	}
	target, err := s.store.CurrentChannel(pod)
	if err != nil {
		s.metrics.Dropped.With(id.Pod, "no_live_channel").Inc()
		writeErr(
			w,
			http.StatusNotFound,
			fmt.Sprintf("no live session for %q", in.To),
		)
		return
	}

	sender, err := s.store.CurrentChannel(id.Pod)
	if err != nil {
		s.metrics.Dropped.With(id.Pod, "sender_has_no_channel").Inc()
		writeErr(
			w,
			http.StatusConflict,
			"claim a channel before sending",
		)
		return
	}

	// The ticket is looked up by the two stamped channel ids -- the
	// sender's, established from its token, and the recipient's, resolved
	// by starling. Nothing here reads a value the sender wrote.
	//
	// That is what keeps a ticket from becoming a field a sender writes
	// about itself, which would be the `from` problem arriving by a side
	// door and looking like session management.
	_, err = s.store.UseTicket(sender.Channel, target.Channel)
	if err != nil {
		switch {
		case errors.Is(err, ErrNoTicket):
			s.metrics.Dropped.With(id.Pod, "no_ticket").Inc()
			writeErr(
				w,
				http.StatusConflict,
				fmt.Sprintf(
					"no ticket for %q; ask for one first",
					in.To,
				),
			)
		case errors.Is(err, ErrTicketExpired):
			// Distinct from never having had one, because the two
			// callers need to do different things about it.
			s.metrics.Dropped.With(id.Pod, "ticket_expired").Inc()
			writeErr(
				w,
				http.StatusConflict,
				fmt.Sprintf(
					"the ticket for %q closed after "+
						"thirty minutes idle; ask "+
						"for another",
					in.To,
				),
			)
		default:
			// The store failed. Counted apart from the two refusals
			// above and answered with a 503, because telling a
			// caller to ask for a ticket it already has would send
			// it round a loop that cannot end.
			s.metrics.Dropped.With(id.Pod, "ticket_unreadable").
				Inc()
			writeErr(
				w,
				http.StatusServiceUnavailable,
				"could not check the ticket",
			)
		}
		return
	}

	if s.seenBefore(in.text()) {
		// Not a refusal. Echo is a signal about how a conversation is
		// going, not an error, and dropping it would hide the thing
		// worth seeing.
		s.metrics.Echoed.With(id.Pod).Inc()
	}

	env := Envelope{
		ID: newMessageID(),
		TS: s.now().UTC().Format(time.RFC3339Nano),
		From: EnvSender{
			Pod:     id.Pod,
			PodUID:  id.UID,
			Channel: sender.Channel,
		},
		// Both are kept: the alias is what a person reads, the pod is
		// what happened.
		To:      pod,
		ToAlias: in.To,
		Kind:    in.Kind,
		// Carried through as three fields rather than flattened into
		// the body. Flattening here would put the narrative field back
		// in the one place it matters most, which is the record a later
		// reader works from.
		Body:      in.Body,
		Claim:     in.Claim,
		Evidence:  in.Evidence,
		Location:  in.Location,
		InReplyTo: in.InReplyTo,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		writeErr(
			w,
			http.StatusInternalServerError,
			"could not encode the envelope",
		)
		return
	}
	if err := s.store.Deliver(target.Channel, raw); err != nil {
		s.metrics.Dropped.With(id.Pod, "delivery_failed").Inc()
		writeErr(w, http.StatusServiceUnavailable, "could not deliver")
		return
	}

	s.metrics.Accepted.With(id.Pod).Inc()
	s.metrics.Delivered.With(id.Pod, pod).Inc()

	// Archive after delivery, never before, and never in a way that can
	// fail this request. The message is the agent's; the record is ours.
	s.recorder.Record(raw)

	writeJSON(w, http.StatusOK, map[string]string{"id": env.ID})
}

// handleInbox long-polls for mail. Whose inbox comes from the token, so there
// is no parameter that could point somewhere else.
func (s *Server) handleInbox(
	w http.ResponseWriter,
	r *http.Request,
	id Identity,
) {
	var in struct {
		Channel     string `json:"channel"`
		WaitSeconds int    `json:"wait_seconds"`
	}
	if !decode(w, r, &in) {
		return
	}
	ch, err := s.store.CurrentChannel(id.Pod)
	if err != nil {
		writeErr(w, http.StatusConflict, "claim a channel first")
		return
	}

	// The channel is proof, not an address, and the difference is the whole
	// reason this parameter is tolerable.
	//
	// It does not select an inbox. Whose inbox this is still follows from
	// the token and nothing else; a wrong channel is a refusal, never a
	// redirect, so there is no value here that reads somebody else's mail.
	//
	// What it establishes is that the caller is the session that claimed.
	//
	// Before this, a new session in a recycled pod could skip claiming and
	// poll, and CurrentChannel -- keyed on the pod name alone -- would hand
	// it the previous occupant's channel and its mail. Retirement was
	// voluntary, because starling had no signal that a session had changed.
	//
	// Now it has one: only the session holding the current channel id can
	// read the current channel, and a session that never claimed holds
	// nothing.
	if in.Channel != ch.Channel {
		writeErr(
			w,
			http.StatusConflict,
			"stale or missing channel; claim before polling",
		)
		return
	}

	// One concurrent poll per pod. There is exactly one poller in a pod, so
	// a second simultaneous poll is a bug -- a restarted poller whose
	// predecessor has not noticed, or a caller looping without waiting for
	// its own reply.
	//
	// Refusing it costs the caller nothing correct and stops one confused
	// pod from holding connections open in a loop.
	//
	// This replaces counting long-polls against the global capacity cap,
	// which made a fleet of waiting pollers look like load and hard-stopped
	// it at 64.
	if !s.beginPoll(id.Pod) {
		writeErr(
			w,
			http.StatusConflict,
			"a poll is already open for this pod",
		)
		return
	}
	defer s.endPoll(id.Pod)

	wait := time.Duration(in.WaitSeconds) * time.Second
	if wait > s.maxWait {
		wait = s.maxWait
	}

	deadline := s.now().Add(wait)
	for {
		msgs, err := s.store.Take(ch.Channel, 32)
		if err != nil {
			writeErr(
				w,
				http.StatusInternalServerError,
				"could not read the inbox",
			)
			return
		}
		if len(msgs) > 0 || !s.now().Before(deadline) {
			writeRaw(w, msgs)
			return
		}
		// Polling rather than a notification channel, deliberately.
		// Four agents and a quarter-second tick is nothing, and a
		// condition variable per channel is machinery to maintain for a
		// latency nobody can feel.
		select {
		case <-r.Context().Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// handleHeartbeat records that a poller is alive.
func (s *Server) handleHeartbeat(
	w http.ResponseWriter,
	r *http.Request,
	id Identity,
) {
	ch, err := s.store.CurrentChannel(id.Pod)
	if err != nil {
		writeErr(w, http.StatusConflict, "no channel to report on")
		return
	}
	if err := s.store.Heartbeat(ch.Channel); err != nil {
		writeErr(
			w,
			http.StatusInternalServerError,
			"could not record the heartbeat",
		)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleHealth reports whether starling can do its job, by doing part of it.
//
// NOT A 200 generator. A health check does the work, and reports healthy,
// degraded or down. So this touches the store.
//
// It deliberately does NOT check the API server: verification failing is
// degraded, not down, and a health check that goes red because Kubernetes is
// briefly busy causes a restart that fixes nothing.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	chans, err := s.store.LiveChannels()
	status := "healthy"
	code := http.StatusOK
	detail := ""
	if err != nil {
		status, code = "down", http.StatusServiceUnavailable
		detail = "store unreadable: " + err.Error()
	}
	// version rides on every answer, healthy or not: the audit that
	// compares what runs to what is committed reads it here, and a down pod
	// that cannot say what it is built from is the harder of the two to
	// diagnose.
	writeJSON(w, code, map[string]any{
		"status":   status,
		"channels": len(chans),
		"detail":   detail,
		"version":  version,
	})
}

// handleMetrics publishes the instrument panel, refreshing the gauges that are
// derived from current state rather than incremented as things happen.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.refreshGauges()
	w.Header().
		Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(s.metrics.Render()))
}

// refreshGauges recomputes per-channel gauges at scrape time. Reset first, so a
// channel that has gone away stops reporting a stale depth forever.
func (s *Server) refreshGauges() {
	chans, err := s.store.LiveChannels()
	if err != nil {
		return
	}
	s.metrics.InboxDepth.Reset()
	s.metrics.HeartbeatAge.Reset()
	now := s.now()
	for _, c := range chans {
		if d, err := s.store.Depth(c.Channel); err == nil {
			s.metrics.InboxDepth.With(c.Pod).Set(float64(d))
		}
		s.metrics.HeartbeatAge.With(c.Pod).
			Set(now.Sub(c.LastHeartbeat).Seconds())
	}
	s.metrics.Channels.With().Set(float64(len(chans)))

	// Read-only, like the rest of this function: expiry is applied on use
	// and on the next claim, so a scrape never writes to the store.
	if tickets, err := s.store.LiveTickets(); err == nil {
		s.metrics.TicketsOpen.With().Set(float64(len(tickets)))
	}
}

// descriptor is the compiled FileDescriptorSet, built by bin/gen.sh and served
// at /api. Committed rather than generated at startup so the binary always
// matches the protos it was built from, and so a container needs no toolchain.
//
//go:embed descriptor.binpb
var descriptor []byte

// handleAPI publishes the schema, as a descriptor rather than as prose.
//
// This is the estate's convention -- flipr and hm both return a
// FileDescriptorSet as application/octet-stream -- and the reason is that a
// consumer can READ it.
//
// Hand-written JSON describing the endpoints is legible to a person and useless
// to anything else: it cannot be validated against, it drifts from the protos
// the moment somebody edits one, and nothing can discover a service from it.
//
// The descriptor cannot drift, because gen.sh builds it from the same protos
// that generate the code.
func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(descriptor)
}

// EnvSender is the stamped sender on a stored envelope.
type EnvSender struct {
	Pod     string `json:"pod"`
	PodUID  string `json:"pod_uid"`
	Channel string `json:"channel"`
}

// Envelope is what starling stores and delivers. Both message forms appear
// here, and exactly one of them is ever populated, because validate refused
// anything carrying both before it got this far.
type Envelope struct {
	ID        string    `json:"id"`
	TS        string    `json:"ts"`
	From      EnvSender `json:"from"`
	To        string    `json:"to"`
	ToAlias   string    `json:"to_alias"`
	Kind      string    `json:"kind"`
	Body      string    `json:"body,omitempty"`
	Claim     string    `json:"claim,omitempty"`
	Evidence  string    `json:"evidence,omitempty"`
	Location  string    `json:"location,omitempty"`
	InReplyTo string    `json:"in_reply_to,omitempty"`
}

// Murmur is one message through the relay, as posted. Note what is absent: it
// has no sender field and never will, because identity is stamped from the
// token rather than written by the claimant.
//
// Two forms, and no narrative field in either. A murmur carries a short body,
// or it carries an assertion -- a claim, its evidence and where to look -- and
// never both.
//
// That is the whole of the property: a schema is a filter that needs no model,
// and a shape with no slot the right size for rhetoric has nowhere to put any.
type Murmur struct {
	To        string `json:"to"`
	Kind      string `json:"kind"`
	Body      string `json:"body"`
	InReplyTo string `json:"in_reply_to"`
	Claim     string `json:"claim"`
	Evidence  string `json:"evidence"`
	Location  string `json:"location"`
}

// asserts reports whether this murmur uses the assertion form, which is true
// the moment any of the three fields is filled in.
//
// Deliberately not "all three": a sender that supplies evidence and no claim
// has chosen the form and got it wrong, and should be told which field is
// missing rather than have the whole message read as a body it did not send.
func (m Murmur) asserts() bool {
	return strings.TrimSpace(m.Claim) != "" ||
		strings.TrimSpace(m.Evidence) != "" ||
		strings.TrimSpace(m.Location) != ""
}

// text returns everything the sender wrote, for echo detection. All the fields
// together rather than the body alone, because otherwise moving a paragraph
// from body into claim makes the same words look like a new message.
func (m Murmur) text() string {
	return m.Body + "\x00" + m.Claim + "\x00" +
		m.Evidence + "\x00" + m.Location
}

// validate applies the mechanical rules, which throw out most of what should be
// thrown out at no cost and with no judgement. Smoothing is a later layer; this
// one needs no model and cannot be wrong in an interesting way.
//
// Returns a short reason suitable as a metric label, and advice for the caller
// which is deliberately NOT part of the label.
//
// WHAT THIS DOES NOT DO: decide which form a given kind ought to use. A rule
// like "an answer must be an assertion" is arguable, and an arguable rule in
// the mechanical layer is a rule somebody argues with in a pull request until
// it is gone.
//
// Both forms are bounded and neither has room for a paragraph of doctrine, so
// which one to use is the sender's business.
func validate(m Murmur) (reason, advice string, ok bool) {
	switch {
	case strings.TrimSpace(m.To) == "":
		return "no_recipient", "", false
	case !validKind(m.Kind):
		return "unknown_kind", "one of question, answer, notice", false
	}

	hasBody := strings.TrimSpace(m.Body) != ""
	switch {
	case !hasBody && !m.asserts():
		return "empty_body", "", false
	case hasBody && m.asserts():
		// Allowing both would give narrative its field back under
		// another name: the claim becomes a headline and the body
		// becomes the essay, which is the shape being removed.
		return "two_forms",
			"a body or a claim with its evidence and location, " +
				"not both", false
	}

	if m.asserts() {
		switch {
		case strings.TrimSpace(m.Claim) == "":
			return "no_claim",
				"evidence and a location need something " +
					"they are evidence for", false
		case strings.TrimSpace(m.Location) == "":
			// An assertion with nowhere to go and look for yourself
			// is the thing this relay exists to stop propagating.
			return "no_location",
				"say where to look: a repo, a ref, a path " +
					"or a url", false
		case len(m.Claim) > maxClaimBytes:
			return "claim_too_long",
				"a claim is a sentence; the rest is " +
					"evidence", false
		case len(m.Evidence) > maxEvidenceBytes:
			return "evidence_too_long",
				"quote the line, not the file", false
		case len(m.Location) > maxLocationBytes:
			return "location_too_long",
				"a ref or a path, not a description of one",
				false
		}
		return "", "", true
	}

	if len(m.Body) > maxBodyBytes {
		// The advice matters: "too long" without a remedy invites a
		// retry at the same length. Push it to /exchange and send the
		// branch name.
		return "body_too_long", "push it and send the ref", false
	}
	return "", "", true
}

// validKind checks the closed set. A string rather than an enum so an unknown
// kind is a droppable message rather than a schema migration -- and the drop is
// counted, so a consumer using a kind nobody implemented is visible.
func validKind(k string) bool {
	switch k {
	case "question", "answer", "notice":
		return true
	}
	return false
}

// seenBefore reports whether this exact body has passed through before, and
// records it. Echo is the mechanism by which register actually spread in
// August: the same text forwarded between agents until everyone was using it.
//
// Hashed rather than stored, and pruned, because this is a signal and not an
// archive.
func (s *Server) seenBefore(body string) bool {
	sum := sha256.Sum256([]byte(body))
	key := hex.EncodeToString(sum[:8])

	s.echoMu.Lock()
	defer s.echoMu.Unlock()

	now := s.now()
	_, seen := s.echoSeen[key]
	s.echoSeen[key] = now

	if len(s.echoSeen) > 4096 {
		cutoff := now.Add(-1 * time.Hour)
		for k, t := range s.echoSeen {
			if t.Before(cutoff) {
				delete(s.echoSeen, k)
			}
		}
	}
	return seen
}

// decode reads a JSON body, rejecting unknown fields.
//
// This is where the contract is enforced. A payload carrying `from` fails to
// decode and never reaches code that could be tempted to trust it.
//
// Rejecting is not the same as ignoring: an ignored field still gets written by
// some future client, still appears in a log, and looks authoritative by the
// time anyone notices.
func decode(w http.ResponseWriter, r *http.Request, into any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		writeErr(
			w,
			http.StatusBadRequest,
			"malformed request: "+err.Error(),
		)
		return false
	}
	return true
}

// writeJSON sends a JSON reply.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr sends an error, stating the reason. A refusal with no stated reason
// is indistinguishable from a bug.
func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// writeRaw sends already-encoded envelopes without re-marshalling them.
func writeRaw(w http.ResponseWriter, msgs [][]byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"messages":[`))
	for i, m := range msgs {
		if i > 0 {
			_, _ = w.Write([]byte(","))
		}
		_, _ = w.Write(m)
	}
	_, _ = w.Write([]byte(`]}`))
}
