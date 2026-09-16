// Package client is how you talk to starling. It is the reference
// implementation, and it exists so that nobody has to guess.
//
// The point of a library with one consumer. The poller is currently the only
// thing importing this, and that is fine.
//
// A hand-rolled caller gets three things wrong in a predictable order: it
// invents a `from` field and cannot work out why the sender is ignored; it puts
// the token somewhere other than the Authorization header; and it retries a
// refusal that will never succeed.
//
// Every one of those is a half hour of confusion that this package makes
// impossible to have.
//
// WHAT IT WILL NOT LET YOU DO, deliberately:
//
//   - Say who you are. Send has no sender argument because the wire has no
//     sender field. Identity comes from the Kubernetes service-account token
//     this package reads for you, and starling takes it from there. If you want
//     to be somebody else, you cannot.
//   - Read another channel's inbox. Inbox takes no channel argument. Whose
//     inbox it is follows from the token, so there is no parameter to point
//     somewhere it should not go.
//   - Retry something that is not going to work. A refusal is an answer;
//     transport failures back off with jitter, refusals return.
//
// Agents do NOT use this. A Claude session cannot import Go. Agents get a CLI
// wrapper, which is itself built on this package -- so there is one
// implementation of what correct usage means, and the wrapper is not a second
// opinion.
//
// An agent that ignores the wrapper and curls starling directly is not doing
// anything dangerous, because the server rejects off-contract payloads whatever
// produced them and identity is never in the payload. The wrapper is
// convenience, not enforcement.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

// The projected service-account token kubelet mounts. Read on every request
// rather than cached: kubelet rotates it, and a client that stops
// authenticating after ninety minutes is a bad Monday.
const defaultTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

var (
	// ErrRefused is a definite no from starling -- bad schema, oversized
	// body, unknown recipient, no live channel. Retrying will not change
	// it.
	ErrRefused = errors.New("starling refused the request")
	// ErrUnauthenticated means the token was not accepted. Also final: a
	// token that is not valid now will not be valid on the next attempt.
	ErrUnauthenticated = errors.New("starling did not accept the token")
	// ErrThrottled means the per-pod rate limit refused this request.
	// Unlike the two above, waiting does help -- so callers may retry,
	// slowly.
	ErrThrottled = errors.New("starling throttled the request")
)

// Message is one message to send. There is no From field and there will not be
// one; see the package comment.
//
// Two forms, and no narrative field in either. Fill in Body, or fill in Claim,
// Evidence and Location -- never both, and starling refuses a Message carrying
// both.
//
// Body is capped at a kilobyte; the assertion fields are capped separately and
// more tightly, so neither form has room for a paragraph of argument. That is
// the shape working, not the shape getting in the way.
type Message struct {
	To   string `json:"to"`
	Kind string `json:"kind"`

	// The short-note form.
	Body string `json:"body,omitempty"`

	// The assertion form. Claim and Location are both required once any of
	// the three is set; Evidence is optional. A claim with nowhere to go
	// and look for yourself is what starling exists to stop carrying.
	Claim    string `json:"claim,omitempty"`
	Evidence string `json:"evidence,omitempty"`
	Location string `json:"location,omitempty"`

	InReplyTo string `json:"in_reply_to,omitempty"`
}

// Envelope is a message as starling recorded it, with the sender established
// rather than asserted. Exactly one of the two forms is populated.
type Envelope struct {
	ID        string `json:"id"`
	TS        string `json:"ts"`
	From      Sender `json:"from"`
	To        string `json:"to"`
	ToAlias   string `json:"to_alias"`
	Kind      string `json:"kind"`
	Body      string `json:"body,omitempty"`
	Claim     string `json:"claim,omitempty"`
	Evidence  string `json:"evidence,omitempty"`
	Location  string `json:"location,omitempty"`
	InReplyTo string `json:"in_reply_to,omitempty"`
}

// Sender is who sent something, as the API server named them.
type Sender struct {
	Pod     string `json:"pod"`
	PodUID  string `json:"pod_uid"`
	Channel string `json:"channel"`
}

// Ticket is permission for this session and a peer to exchange messages.
//
// ID is a handle for logs and for the operator, not a credential -- there is
// nowhere to present it, because Send has no ticket field and starling resolves
// the ticket from the channel ids it stamped. To is the name that was asked
// for, echoed; starling does not say which pod it resolved to.
type Ticket struct {
	ID               string `json:"ticket"`
	To               string `json:"to"`
	ExpiresInSeconds int    `json:"expires_in_seconds"`
}

// Identity is what starling says this process is.
type Identity struct {
	Channel string `json:"channel"`
	Pod     string `json:"pod"`
	PodUID  string `json:"pod_uid"`
}

// Client talks to starling. Safe for concurrent use.
type Client struct {
	base      string
	http      *http.Client
	tokenPath string

	// Set by ClaimChannel and sent with heartbeats. Not used for addressing
	// -- starling resolves whose inbox is whose from the token -- so a
	// wrong value here cannot reach somebody else's mail.
	channel string
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient replaces the transport, for tests.
func WithHTTPClient(
	h *http.Client,
) Option {
	return func(c *Client) { c.http = h }
}

// WithTokenPath replaces the service-account token location, for tests.
func WithTokenPath(
	p string,
) Option {
	return func(c *Client) { c.tokenPath = p }
}

// New builds a client for a starling at base, e.g. "http://starling.test".
//
// The timeout is generous because Inbox long-polls: a request that legitimately
// waits thirty seconds for mail must not be killed at ten. Per-call deadlines
// belong on the context, which is where a caller can be specific.
func New(base string, opts ...Option) *Client {
	c := &Client{
		base:      strings.TrimRight(base, "/"),
		tokenPath: defaultTokenPath,
		http:      &http.Client{Timeout: 90 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Channel returns the channel claimed by this client, empty before
// ClaimChannel.
func (c *Client) Channel() string { return c.channel }

// ClaimChannel opens a channel for this session and remembers it. Call once at
// start. A new claim retires the previous channel for this pod, so a fresh
// session never receives mail addressed to the one before it.
func (c *Client) ClaimChannel(
	ctx context.Context,
	note string,
) (Identity, error) {
	var out Identity
	body := map[string]string{"note": note}
	err := c.do(ctx, http.MethodPost, "/channel", body, &out)
	if err != nil {
		return Identity{}, err
	}
	c.channel = out.Channel
	return out, nil
}

// RequestTicket opens a channel between this session and a peer, or restarts
// the idle clock on the one they already have. Both sessions must have claimed.
//
// A ticket controls who may talk to whom and for how long. It says nothing
// about what they may say.
//
// It closes after thirty minutes with nothing sent on it, and every message
// either end sends restarts that clock -- so a pair working through something
// never has to ask twice, and a conversation nobody came back to closes on its
// own.
//
// The returned id is a handle for logs and for the operator. It is NOT a
// credential: Send has no ticket argument because the wire has no ticket field,
// and starling looks a ticket up by the channel ids it stamped rather than by
// anything a sender writes.
//
// Asking again is free and is the right thing to do if you are not sure.
func (c *Client) RequestTicket(ctx context.Context, to string) (Ticket, error) {
	if strings.TrimSpace(to) == "" {
		return Ticket{}, fmt.Errorf("%w: no peer named", ErrRefused)
	}
	var out Ticket
	err := c.do(
		ctx, http.MethodPost, "/ticket",
		map[string]string{"to": to}, &out,
	)
	if err != nil {
		return Ticket{}, err
	}
	return out, nil
}

// Send posts a message. The sender is not an argument and cannot be, and
// neither is the ticket: starling resolves both from what it stamped.
//
// A send between two sessions with no live ticket is refused. Call
// RequestTicket first; there is no harm in calling it again.
func (c *Client) Send(ctx context.Context, m Message) error {
	if strings.TrimSpace(m.To) == "" {
		return fmt.Errorf("%w: no recipient", ErrRefused)
	}
	return c.do(ctx, http.MethodPost, "/message", m, nil)
}

// Inbox long-polls for mail addressed to this session. Whose inbox it is comes
// from the token; there is no parameter for it.
//
// wait is how long starling should hold the connection when there is nothing
// waiting. Zero returns immediately. This is the call that gives an agent a
// tick it does not otherwise have: an idle Claude session is blocked on a read
// with no loop and no timer, so something outside it has to do this.
func (c *Client) Inbox(
	ctx context.Context,
	wait time.Duration,
) ([]Envelope, error) {
	var out struct {
		Messages []Envelope `json:"messages"`
	}
	// The channel is proof that this process is the session that claimed,
	// not an address. Sending the wrong one is refused rather than
	// redirected, so it cannot reach another pod's mail -- whose inbox this
	// is still comes from the token.
	body := map[string]any{
		"channel":      c.channel,
		"wait_seconds": int(wait / time.Second),
	}
	if err := c.do(ctx, http.MethodPost, "/inbox", body, &out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// Heartbeat reports that this session's poller is alive. Sent by the poller,
// not by the agent: an agent that has stopped reading its input cannot report
// anything, and its silence is otherwise identical to having nothing to say.
func (c *Client) Heartbeat(ctx context.Context) error {
	return c.do(
		ctx,
		http.MethodPost,
		"/heartbeat",
		map[string]string{"channel": c.channel},
		nil,
	)
}

// do performs one request with retries on transport failure only.
func (c *Client) do(
	ctx context.Context,
	method, path string,
	in, out any,
) error {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			if err := sleepBackoff(ctx, attempt); err != nil {
				return err
			}
		}
		retryable, err := c.once(ctx, method, path, in, out)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable {
			return err
		}
	}
	return fmt.Errorf("starling unreachable after retries: %w", lastErr)
}

// once is a single attempt. The bool reports whether retrying could help.
func (c *Client) once(
	ctx context.Context,
	method, path string,
	in, out any,
) (bool, error) {
	tok, err := os.ReadFile(c.tokenPath)
	if err != nil {
		// Not retryable: a missing token will still be missing in two
		// seconds, and retrying turns a clear misconfiguration into a
		// slow one.
		return false, fmt.Errorf(
			"reading service account token: %w",
			err,
		)
	}

	var body []byte
	if in != nil {
		if body, err = json.Marshal(in); err != nil {
			return false, err
		}
	}
	req, err := http.NewRequestWithContext(
		ctx,
		method,
		c.base+path,
		bytes.NewReader(body),
	)
	if err != nil {
		return false, err
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+strings.TrimSpace(string(tok)),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return true, fmt.Errorf("starling transport: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusUnauthorized ||
		resp.StatusCode == http.StatusForbidden:
		return false, ErrUnauthenticated
	case resp.StatusCode == http.StatusTooManyRequests:
		return false, ErrThrottled
	case resp.StatusCode >= 500:
		return true, fmt.Errorf("starling http %d", resp.StatusCode)
	case resp.StatusCode >= 400:
		// Read the reason: starling states why it refused, and
		// swallowing that leaves a caller to guess at a message the
		// server already wrote.
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Error != "" {
			return false, fmt.Errorf("%w: %s", ErrRefused, e.Error)
		}
		return false, fmt.Errorf(
			"%w: http %d",
			ErrRefused,
			resp.StatusCode,
		)
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return false, fmt.Errorf(
				"decoding starling reply: %w",
				err,
			)
		}
	}
	return false, nil
}

// sleepBackoff waits before a retry: full jitter, sleep = random(0, base*2^n),
// capped. Full jitter rather than exponential-plus-noise because every poller
// fails at the same instant when starling restarts, and a shared deterministic
// schedule reconverges them into a thundering herd on every attempt after.
func sleepBackoff(ctx context.Context, attempt int) error {
	const base = 200 * time.Millisecond
	const ceiling = 10 * time.Second
	d := base << attempt
	if d > ceiling {
		d = ceiling
	}
	t := time.NewTimer(time.Duration(rand.Int63n(int64(d) + 1)))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
