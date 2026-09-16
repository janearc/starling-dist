package main

// Tests for the HTTP surface.
//
// The ones worth reading are the refusals. Accepting a good message is easy to
// get right and easy to test; what this service is FOR is refusing things, and
// each refusal below is a property somebody could remove without any obvious
// test going red.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// fakeVerifier resolves a token to an identity with no cluster involved.
type fakeVerifier map[string]Identity

// Verify answers from the fake's table of tokens, and refuses any other.
func (f fakeVerifier) Verify(
	_ context.Context,
	token string,
) (Identity, error) {
	id, ok := f[token]
	if !ok {
		return Identity{}, ErrUnauthenticated
	}
	return id, nil
}

// unreachableVerifier stands in for an API server that cannot be reached --
// distinct from one that says no.
type unreachableVerifier struct{}

// Verify times out, as it would with the API server unreachable.
func (unreachableVerifier) Verify(context.Context, string) (Identity, error) {
	return Identity{}, context.DeadlineExceeded
}

// testServer builds a server with two known agents and an alias table.
func testServer(t *testing.T) (*Server, http.Handler) {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	v := fakeVerifier{
		"tok-opus-0": {
			Pod:            "opus-0",
			UID:            "uid-opus-0",
			ServiceAccount: "sa",
		},
		"tok-fable-1": {
			Pod:            "fable-1",
			UID:            "uid-fable-1",
			ServiceAccount: "sa",
		},
	}
	s := NewServer(
		store,
		v,
		StaticAliases{"ingest": "fable-1"},
		NewMetrics(),
	)
	return s, s.Handler()
}

// post makes an authenticated request and returns the response.
func post(
	h http.Handler,
	path, token string,
	body any,
) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	r := httptest.NewRequest(http.MethodPost, path, &buf)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// claim opens a channel for a pod and returns its id, which polling now needs
// as proof that the caller is the session that claimed.
func claim(t *testing.T, h http.Handler, token string) string {
	t.Helper()
	w := post(h, "/channel", token, map[string]string{"note": "test"})
	if w.Code != http.StatusOK {
		t.Fatalf("claim failed: %d %s", w.Code, w.Body.String())
	}
	var out struct {
		Channel string `json:"channel"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("claim reply: %v", err)
	}
	return out.Channel
}

// ticket opens a channel between the caller and a peer, which a send now
// requires. Returns the handle, which nothing accepts back -- it is here so a
// test can assert on what the reply says rather than to be presented anywhere.
func ticket(t *testing.T, h http.Handler, token, to string) string {
	t.Helper()
	w := post(h, "/ticket", token, map[string]string{"to": to})
	if w.Code != http.StatusOK {
		t.Fatalf(
			"ticket to %s failed: %d %s",
			to,
			w.Code,
			w.Body.String(),
		)
	}
	var out struct {
		Ticket string `json:"ticket"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("ticket reply: %v", err)
	}
	return out.Ticket
}

// inbox polls with a channel and decodes the envelopes.
func inbox(
	t *testing.T,
	h http.Handler,
	token, channel string,
	wait int,
) []Envelope {
	t.Helper()
	w := post(
		h,
		"/inbox",
		token,
		map[string]any{"channel": channel, "wait_seconds": wait},
	)
	var out struct{ Messages []Envelope }
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return out.Messages
}

// THE WHOLE POINT. A payload carrying `from` must be REJECTED, not ignored.
// An ignored field gets written by some future client, appears in a log,
// and looks authoritative by the time anyone notices.
func TestASenderCannotWriteItsOwnName(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	body := map[string]any{
		"to": "fable-1", "kind": "question", "body": "who am i",
		"from": "someone-else",
	}
	w := post(h, "/message", "tok-opus-0", body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf(
			"a payload with a `from` field must be refused, got "+
				"%d: %s",
			w.Code,
			w.Body.String(),
		)
	}
}

// The envelope's sender is the pod the token authenticated as, stamped by
// starling, whatever the body says.
func TestTheStampedSenderIsTheAuthenticatedPod(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	fableCh := claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	if w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "question", "body": "did the CRS handling change",
	}); w.Code != http.StatusOK {
		t.Fatalf("send: %d %s", w.Code, w.Body.String())
	}

	msgs := inbox(t, h, "tok-fable-1", fableCh, 0)
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	got := msgs[0]
	if got.From.Pod != "opus-0" || got.From.PodUID != "uid-opus-0" {
		t.Errorf("sender = %+v, want the authenticated pod", got.From)
	}
}

// The alias is what a person reads; the pod is what happened. Losing
// either makes a record that cannot answer one of the two questions.
func TestAnAliasResolvesAndBothNamesAreKept(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	fableCh := claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "ingest")

	post(h, "/message", "tok-opus-0", map[string]string{
		"to": "ingest", "kind": "question", "body": "hello",
	})
	msgs := inbox(t, h, "tok-fable-1", fableCh, 0)
	if len(msgs) != 1 {
		t.Fatalf("alias did not resolve; got %d messages", len(msgs))
	}
	if msgs[0].To != "fable-1" || msgs[0].ToAlias != "ingest" {
		t.Errorf(
			"to=%q alias=%q; both should be recorded",
			msgs[0].To,
			msgs[0].ToAlias,
		)
	}
}

// There is no parameter for whose inbox this is, so there is nothing to
// point at somebody else. Verified by the shape of the API: opus-0 asking
// for mail gets its own, never fable-1's, with no way to ask otherwise.
func TestInboxIsNotAddressable(t *testing.T) {
	_, h := testServer(t)
	opusCh := claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "for fable only",
	})

	if msgs := inbox(t, h, "tok-opus-0", opusCh, 0); len(msgs) != 0 {
		t.Errorf(
			"opus-0 received %d message(s) addressed to fable-1",
			len(msgs),
		)
	}
}

// A request with no token is refused as unauthenticated.
func TestNoTokenIsRefused(t *testing.T) {
	_, h := testServer(t)
	if w := post(h, "/message", "", map[string]string{"to": "x", "kind": "notice", "body": "y"}); w.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", w.Code)
	}
}

// A token the verifier does not know is refused as unauthenticated.
func TestAnUnknownTokenIsRefused(t *testing.T) {
	_, h := testServer(t)
	if w := post(h, "/message", "not-a-token", map[string]string{"to": "x", "kind": "notice", "body": "y"}); w.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", w.Code)
	}
}

// FAIL CLOSED. If the API server cannot be reached, starling must refuse.
// Admitting an unverified sender is the one thing this service exists to
// prevent, so degrading into it is worse than being down -- and 503 says
// "come back", where 401 would wrongly tell a valid caller its token is bad.
func TestAnUnverifiableCallerIsRefusedNotAdmitted(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := NewServer(
		store,
		unreachableVerifier{},
		StaticAliases{},
		NewMetrics(),
	)
	w := post(
		s.Handler(),
		"/message",
		"any-token",
		map[string]string{"to": "x", "kind": "notice", "body": "y"},
	)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf(
			"code = %d, want 503 when identity cannot be "+
				"established",
			w.Code,
		)
	}
}

// "Too long" without a remedy invites a retry at the same length.
func TestAnOversizedBodyIsRefusedWithAdvice(t *testing.T) {
	s, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": strings.Repeat("x", maxBodyBytes+1),
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "send the ref") {
		t.Errorf(
			"refusal should say what to do instead, got: %s",
			w.Body.String(),
		)
	}
	// The metric label must be the short reason only. A sentence in a label
	// reads badly in a legend and forks into a new series if reworded.
	if !strings.Contains(s.metrics.Render(), `reason="body_too_long"`) {
		t.Errorf(
			"drop label should be the short reason, got:\n%s",
			s.metrics.Render(),
		)
	}
}

// The cap is pinned to a measurement, not to a round number, so this test
// fails if somebody raises it back over the thing it was set to refuse.
//
// 2026-09-01: six peer messages between two agent sessions, 2.5 to 4.5 KB
// each, every reply a 170-byte acknowledgement. The old 8 KiB ceiling
// admitted all six. The smallest of them is the number that matters -- a cap
// above it refuses nothing that was ever sent.
func TestTheCapRefusesTheMessagesThatWereActuallyMeasured(t *testing.T) {
	const smallestMeasured = 2500
	if maxBodyBytes >= smallestMeasured {
		t.Fatalf(
			"maxBodyBytes = %d; a cap at or above %d admits "+
				"every message the cap exists to refuse",
			maxBodyBytes,
			smallestMeasured,
		)
	}

	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": strings.Repeat("x", smallestMeasured),
	})
	if w.Code != http.StatusBadRequest {
		t.Errorf(
			"code = %d, want 400 for a message the size of the "+
				"ones nobody read",
			w.Code,
		)
	}
}

// The transport cap, distinct from the message cap: a limit checked after
// the body is in memory has already lost.
func TestAnAbsurdRequestIsRefusedWhileReading(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	huge := strings.Repeat("x", maxRequestBytes*2)
	r := httptest.NewRequest(
		http.MethodPost,
		"/message",
		strings.NewReader(
			`{"to":"a","kind":"notice","body":"`+huge+`"}`,
		),
	)
	r.Header.Set("Authorization", "Bearer tok-opus-0")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code == http.StatusOK {
		t.Errorf("a request past the transport cap must not succeed")
	}
}

// --- the assertion form: a shape with nowhere for narrative to live ----------

// The three fields must survive to the recipient and to the record. Flatten
// them into the body on the way through and the narrative field is back in
// the one place it matters most, which is what a later reader works from.
func TestAnAssertionIsCarriedAsThreeFieldsNotFlattened(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	fableCh := claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	if w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "answer",
		"claim":    "the CRS handling changed",
		"evidence": "reproject() now takes an explicit srid",
		"location": "kingfisher-dev@4aaeb08 geo/reproject.go",
	}); w.Code != http.StatusOK {
		t.Fatalf("send: %d %s", w.Code, w.Body.String())
	}

	msgs := inbox(t, h, "tok-fable-1", fableCh, 0)
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	got := msgs[0]
	if got.Claim != "the CRS handling changed" {
		t.Errorf("claim = %q", got.Claim)
	}
	if got.Evidence == "" || got.Location == "" {
		t.Errorf(
			"evidence=%q location=%q; both must survive delivery",
			got.Evidence,
			got.Location,
		)
	}
	if got.Body != "" {
		t.Errorf(
			"body = %q; an assertion must not be flattened into "+
				"a body",
			got.Body,
		)
	}
}

// Allowing both gives narrative its field back under another name: the claim
// becomes a headline and the body becomes the essay.
func TestAMessageCarriesOneFormOrTheOther(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice",
		"body":     "and here is what i think about all of it, at length",
		"claim":    "the reindex is done",
		"location": "kingfisher-dev",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf(
			"code = %d, want 400 when a message carries both forms",
			w.Code,
		)
	}
	if !strings.Contains(w.Body.String(), "not both") {
		t.Errorf(
			"refusal should say which rule was broken, got: %s",
			w.Body.String(),
		)
	}
}

// An assertion nobody can go and check is exactly what this relay exists to
// stop propagating, and requiring the field is how that stays mechanical.
func TestAnAssertionWithNowhereToLookIsRefused(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "answer",
		"claim":    "the whole approach is wrong and we should start again",
		"evidence": "it feels wrong",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf(
			"code = %d, want 400 for a claim with no location",
			w.Code,
		)
	}
	if !strings.Contains(w.Body.String(), "where to look") {
		t.Errorf(
			"refusal should say what is missing, got: %s",
			w.Body.String(),
		)
	}
}

// Any of the three fields chooses the form, so a sender that got it half
// right is told which field is missing rather than having its message read
// as a body it did not send.
func TestEvidenceWithoutAClaimIsRefusedAsSuch(t *testing.T) {
	s, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "answer", "evidence": "panic: nil map", "location": "haho-dev",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", w.Code)
	}
	if !strings.Contains(s.metrics.Render(), `reason="no_claim"`) {
		t.Errorf(
			"the drop should be counted as no_claim:\n%s",
			s.metrics.Render(),
		)
	}
}

// One combined budget would let a sender spend all of it on one field and
// write the paragraph anyway, which is the thing being removed. Each field
// has to refuse on its own.
func TestTheAssertionFieldsAreCappedSeparately(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	for _, tc := range []struct {
		field string
		size  int
	}{
		{"claim", maxClaimBytes + 1},
		{"evidence", maxEvidenceBytes + 1},
		{"location", maxLocationBytes + 1},
	} {
		body := map[string]string{
			"to": "fable-1", "kind": "answer",
			"claim": "a short claim", "location": "somewhere-dev",
		}
		body[tc.field] = strings.Repeat("x", tc.size)
		if w := post(h, "/message", "tok-opus-0", body); w.Code != http.StatusBadRequest {
			t.Errorf(
				"%s at %d bytes returned %d, want 400",
				tc.field,
				tc.size,
				w.Code,
			)
		}
	}

	// And the three together stay under the body cap, so neither form is
	// the cheap way to say more than the other.
	if total := maxClaimBytes + maxEvidenceBytes + maxLocationBytes; total > maxBodyBytes {
		t.Errorf(
			"the assertion fields sum to %d, above the %d body cap",
			total,
			maxBodyBytes,
		)
	}
}

// Echo is hashed over every field the sender wrote. Hashing the body alone
// would make the same words look new the moment they changed field.
func TestMovingAParagraphIntoAClaimIsStillEcho(t *testing.T) {
	s, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	body := map[string]string{
		"to": "fable-1", "kind": "answer",
		"claim": "the same words", "location": "somewhere-dev",
	}
	if w := post(h, "/message", "tok-opus-0", body); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := post(h, "/message", "tok-opus-0", body); w.Code != http.StatusOK {
		t.Fatal("a repeated assertion must still be delivered")
	}
	if !strings.Contains(
		s.metrics.Render(),
		`starling_messages_echoed_total{sender="opus-0"} 1`,
	) {
		t.Errorf(
			"a repeated assertion was not counted as echo:\n%s",
			s.metrics.Render(),
		)
	}
}

// A message with neither a body nor an assertion is refused.
func TestAnEmptyMessageIsRefusedInEitherForm(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	if w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice",
	}); w.Code != http.StatusBadRequest {
		t.Errorf(
			"code = %d, want 400 for a message with neither form",
			w.Code,
		)
	}
}

// A kind outside the schema's list is refused, not delivered under a guess.
func TestAnUnknownKindIsDropped(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "proclamation", "body": "hear ye",
	})
	if w.Code != http.StatusBadRequest {
		t.Errorf("code = %d, want 400 for an unknown kind", w.Code)
	}
}

// A typo and a dead agent deserve different answers.
func TestAddressingSomebodyWithNoLiveSessionIsDistinct(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "question", "body": "you there",
	})
	if w.Code != http.StatusNotFound {
		t.Errorf(
			"code = %d, want 404 when the recipient has no live "+
				"channel",
			w.Code,
		)
	}
}

// A pod must claim a channel before it can send.
func TestSendingBeforeClaimingIsRefused(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-fable-1")
	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "question", "body": "hi",
	})
	if w.Code != http.StatusConflict {
		t.Errorf(
			"code = %d, want 409 when the sender has no channel",
			w.Code,
		)
	}
}

// The inheritance property, end to end through the API rather than only in
// the store.
func TestANewSessionDoesNotReceiveTheOldOnesMail(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "for the first session",
	})

	// fable-1 restarts and claims again before ever reading.
	newCh := claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	if msgs := inbox(t, h, "tok-fable-1", newCh, 0); len(msgs) != 0 {
		t.Errorf("the new session inherited %d message(s)", len(msgs))
	}
}

// The limiter is a sensor as much as a guard: a stuck agent shows up as a
// rising refusal rate on one label before it shows up as anything else.
func TestThrottleRefusesAndIsCounted(t *testing.T) {
	s, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	got429 := false
	for i := 0; i < burstPerPod+5; i++ {
		w := post(h, "/message", "tok-opus-0", map[string]string{
			"to": "fable-1", "kind": "notice", "body": "spam",
		})
		if w.Code == http.StatusTooManyRequests {
			got429 = true
			break
		}
	}
	if !got429 {
		t.Fatal("no request was throttled past the burst")
	}
	if !strings.Contains(
		s.metrics.Render(),
		`starling_requests_throttled_total{sender="opus-0"}`,
	) {
		t.Error(
			"a throttle refusal must be attributed to the sender " +
				"in metrics",
		)
	}
}

// Echo is a signal about how a conversation is going, not an error.
// Dropping it would hide the thing worth seeing.
func TestEchoIsCountedRatherThanRefused(t *testing.T) {
	s, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	body := map[string]string{
		"to":   "fable-1",
		"kind": "notice",
		"body": "the exact same words",
	}
	if w := post(h, "/message", "tok-opus-0", body); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := post(h, "/message", "tok-opus-0", body); w.Code != http.StatusOK {
		t.Fatal("a repeated body must still be delivered")
	}
	if !strings.Contains(
		s.metrics.Render(),
		`starling_messages_echoed_total{sender="opus-0"} 1`,
	) {
		t.Errorf("echo was not counted:\n%s", s.metrics.Render())
	}
}

// /health reads the store and reports healthy with the build version, rather
// than answering 200 for being up.
func TestHealthDoesWorkRatherThanReturningTwoHundred(t *testing.T) {
	_, h := testServer(t)
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d", w.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["status"] != "healthy" {
		t.Errorf("status = %v", out["status"])
	}
	// The build sha is what lets an audit tie a running pod to a commit; a
	// /health without it passes every probe and answers no question.
	if out["version"] != version {
		t.Errorf("version = %v, want %q", out["version"], version)
	}
	if v, _ := out["version"].(string); v == "" {
		t.Error(
			"version is empty; the ldflags stamp has nothing to " +
				"land on",
		)
	}
	if _, ok := out["channels"]; !ok {
		t.Error(
			"health must report something it had to look up, not " +
				"a bare ok",
		)
	}
}

// A family with no series still emits HELP and TYPE. Without that, a fresh
// starling omits the metric entirely and a dashboard reads "No data" for a
// metric that exists and is legitimately zero -- indistinguishable from one
// that was renamed or never shipped.
func TestMetricsDeclaresFamiliesBeforeAnythingHappens(t *testing.T) {
	_, h := testServer(t)
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	body := w.Body.String()
	for _, name := range []string{
		"starling_messages_accepted_total",
		"starling_messages_dropped_total",
		"starling_messages_delivered_total",
		"starling_messages_echoed_total",
		"starling_requests_throttled_total",
		"starling_inbox_depth",
		"starling_heartbeat_age_seconds",
	} {
		if !strings.Contains(body, "# TYPE "+name+" ") {
			t.Errorf(
				"family %s is not declared on a fresh server",
				name,
			)
		}
	}
}

// /api serves the compiled contract, a real descriptor set that names
// starling.v1.
func TestApiServesTheDescriptor(t *testing.T) {
	_, h := testServer(t)
	r := httptest.NewRequest(http.MethodGet, "/api", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("code = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("content-type = %q, want application/octet-stream", ct)
	}
	// It must be a real FileDescriptorSet, not an empty embed that
	// compiles.
	if w.Body.Len() < 1000 {
		t.Errorf(
			"/api returned %d bytes; the descriptor did not get "+
				"embedded",
			w.Body.Len(),
		)
	}
	if !bytes.Contains(w.Body.Bytes(), []byte("starling.v1")) {
		t.Error(
			"/api descriptor does not mention the starling.v1 " +
				"package",
		)
	}
}

// A heartbeat from a claimed channel is accepted.
func TestHeartbeatRecordsLiveness(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	if w := post(h, "/heartbeat", "tok-opus-0", nil); w.Code != http.StatusNoContent {
		t.Errorf("code = %d, want 204", w.Code)
	}
}

// A long poll on an empty inbox returns as soon as a message arrives, not at
// its timeout.
func TestInboxLongPollReturnsWhenMailArrives(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	fableCh := claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	done := make(chan int, 1)
	go func() {
		done <- len(inbox(t, h, "tok-fable-1", fableCh, 5))
	}()

	time.Sleep(300 * time.Millisecond)
	post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "arriving mid-poll",
	})

	select {
	case n := <-done:
		if n != 1 {
			t.Errorf("long poll returned %d messages, want 1", n)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("long poll did not return after mail arrived")
	}
}

// --- regression tests for the 2026-09-02 review
// -------------------------------
//
// One per finding. A fix without a test is a fix that lasts until somebody
// tidies the code that happened to implement it.

// FINDING 1, the high one. handleClaim used to call throttle.Forget so a
// fresh session started with a full bucket. Allow runs BEFORE the handler,
// so an agent could spend nineteen tokens on /message, spend the twentieth
// on /channel, and have the reset mint twenty more -- sustained rate bounded
// by round-trip latency rather than by policy.
//
// Reachable by accident, which is what makes it worth a test: a wrapper
// crash-looping on startup claims every time and is never throttled.
func TestClaimingDoesNotResetTheRateLimit(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	// Exhaust the bucket.
	for i := 0; i < burstPerPod+2; i++ {
		post(h, "/message", "tok-opus-0", map[string]string{
			"to": "fable-1", "kind": "notice", "body": "filling the bucket",
		})
	}
	if w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "should be refused",
	}); w.Code != http.StatusTooManyRequests {
		t.Fatalf("bucket did not empty: got %d", w.Code)
	}

	// Claim, which used to refill it. The claim itself may be throttled;
	// either way what must NOT happen is a full bucket afterwards.
	post(
		h,
		"/channel",
		"tok-opus-0",
		map[string]string{"note": "reset attempt"},
	)

	if w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "after claiming",
	}); w.Code != http.StatusTooManyRequests {
		t.Errorf(
			"claiming refilled the bucket: got %d, want 429",
			w.Code,
		)
	}
}

// FINDING 3. Retirement used to be voluntary: CurrentChannel is keyed on the
// pod name alone, so a new session in a recycled pod that skipped claiming
// would be handed the previous occupant's channel and its mail.
func TestAnUnclaimedSessionCannotDrainItsPredecessor(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	oldCh := claim(t, h, "tok-fable-1")

	post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "for the first session",
	})

	// A new session starts in the same pod -- same token, same identity --
	// and polls without claiming. It holds no channel, so it has no proof.
	if msgs := inbox(t, h, "tok-fable-1", "", 0); len(msgs) != 0 {
		t.Errorf(
			"a session that never claimed drained %d message(s)",
			len(msgs),
		)
	}
	// And it cannot guess its way in with a stale id either.
	if msgs := inbox(t, h, "tok-fable-1", oldCh+"x", 0); len(msgs) != 0 {
		t.Errorf("a wrong channel id returned %d message(s)", len(msgs))
	}
}

// FINDING 2. capacity used to wrap the whole mux, so /health, /metrics and
// /api shared the 64 slots with everything else -- monitoring goes dark
// exactly when there is something to explain.
func TestMonitoringIsNotBehindTheCapacityCap(t *testing.T) {
	s, h := testServer(t)

	// Occupy every slot.
	for i := 0; i < maxInFlight; i++ {
		if !s.inflight.acquire() {
			t.Fatalf("could not fill the cap; stopped at %d", i)
		}
	}
	t.Cleanup(func() {
		for i := 0; i < maxInFlight; i++ {
			s.inflight.release()
		}
	})

	for _, path := range []string{"/health", "/metrics", "/api"} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Errorf(
				"%s returned %d at capacity; monitoring must "+
					"not be capped",
				path,
				w.Code,
			)
		}
	}

	// And a capped route genuinely still refuses, so the test above is not
	// passing because the cap stopped working.
	if w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "x", "kind": "notice", "body": "y",
	}); w.Code != http.StatusServiceUnavailable {
		t.Errorf(
			"capped route returned %d at capacity, want 503",
			w.Code,
		)
	}
}

// A pod has one poller. A second concurrent poll is a bug -- a restarted
// poller whose predecessor has not noticed -- and refusing it stops one
// confused pod holding connections open in a loop.
func TestOnlyOnePollPerPod(t *testing.T) {
	_, h := testServer(t)
	ch := claim(t, h, "tok-opus-0")

	started := make(chan struct{})
	go func() {
		close(started)
		inbox(t, h, "tok-opus-0", ch, 2)
	}()
	<-started
	time.Sleep(200 * time.Millisecond)

	w := post(
		h,
		"/inbox",
		"tok-opus-0",
		map[string]any{"channel": ch, "wait_seconds": 0},
	)
	if w.Code != http.StatusConflict {
		t.Errorf("second concurrent poll returned %d, want 409", w.Code)
	}
}

// --- tickets: who may talk to whom, and for how long -------------------------
//
// A ticket says nothing about what two agents may say. The tests that cover
// that are the shape and the cap above; these cover reachability and the clock.

// Two pods that hold no ticket between them cannot message, and the refusal
// is counted as no_ticket.
func TestSendingWithoutATicketIsRefused(t *testing.T) {
	s, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "we have not been introduced",
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("code = %d, want 409 with no ticket", w.Code)
	}
	if !strings.Contains(s.metrics.Render(), `reason="no_ticket"`) {
		t.Errorf(
			"the refusal was not counted as no_ticket:\n%s",
			s.metrics.Render(),
		)
	}
}

// A channel between two agents is not a one-way street. Issuing two
// half-tickets would mean an agent could be talked at without being able to
// reply, which is the shape of the August exchanges rather than a fix for
// them.
func TestATicketOpensTheChannelInBothDirections(t *testing.T) {
	_, h := testServer(t)
	opusCh := claim(t, h, "tok-opus-0")
	fableCh := claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	if w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "question", "body": "did the CRS handling change",
	}); w.Code != http.StatusOK {
		t.Fatalf("opus to fable: %d %s", w.Code, w.Body.String())
	}
	// fable never asked for anything and can still answer.
	if w := post(h, "/message", "tok-fable-1", map[string]string{
		"to": "opus-0", "kind": "answer",
		"claim": "it changed", "location": "kingfisher-dev geo/reproject.go",
	}); w.Code != http.StatusOK {
		t.Fatalf("fable to opus: %d %s", w.Code, w.Body.String())
	}

	if n := len(inbox(t, h, "tok-fable-1", fableCh, 0)); n != 1 {
		t.Errorf("fable received %d, want 1", n)
	}
	if n := len(inbox(t, h, "tok-opus-0", opusCh, 0)); n != 1 {
		t.Errorf("opus received %d, want 1", n)
	}
}

// A ticket unused for longer than ticketIdleTTL no longer opens the channel.
func TestATicketClosesAfterThirtyMinutesIdle(t *testing.T) {
	s, h := testServer(t)
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	s.store.now = func() time.Time { return now }

	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	now = now.Add(ticketIdleTTL + time.Second)
	w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "still there?",
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("code = %d, want 409 on an expired ticket", w.Code)
	}
	// Expired and never-issued need different answers: one caller has to
	// ask, the other has to know its conversation lapsed.
	if !strings.Contains(s.metrics.Render(), `reason="ticket_expired"`) {
		t.Errorf(
			"an expired ticket was counted as something else:\n%s",
			s.metrics.Render(),
		)
	}
	if !strings.Contains(w.Body.String(), "idle") {
		t.Errorf(
			"the refusal should say the ticket lapsed, got: %s",
			w.Body.String(),
		)
	}
}

// Idle means idle, not elapsed. A pair working through something for two
// hours must not have to keep asking for permission to continue.
func TestEveryMessageRestartsTheIdleClock(t *testing.T) {
	s, h := testServer(t)
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	s.store.now = func() time.Time { return now }

	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	for i := 0; i < 3; i++ {
		now = now.Add(ticketIdleTTL - time.Minute)
		if w := post(h, "/message", "tok-opus-0", map[string]string{
			"to": "fable-1", "kind": "notice", "body": fmt.Sprintf("still working, %d", i),
		}); w.Code != http.StatusOK {
			t.Fatalf(
				"message %d at %v: %d %s",
				i,
				now,
				w.Code,
				w.Body.String(),
			)
		}
	}
}

// THE PROPERTY THIS PIECE HAS TO KEEP. A ticket a sender presents is a field
// a sender writes about itself, which is the `from` problem arriving by a
// side door dressed as session management. So the wire has no ticket field
// and a payload carrying one is rejected rather than ignored.
func TestATicketIsNotAFieldASenderWrites(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	handle := ticket(t, h, "tok-opus-0", "fable-1")

	w := post(h, "/message", "tok-opus-0", map[string]any{
		"to": "fable-1", "kind": "notice", "body": "presenting my papers",
		"ticket": handle,
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf(
			"a payload with a `ticket` field must be refused, "+
				"got %d: %s",
			w.Code,
			w.Body.String(),
		)
	}
}

// Resolution never travels back to an agent: a name an agent can read is a
// role it can inhabit, and a ticket reply would be a tidy place to leak one.
func TestATicketReplyDoesNotNameTheResolvedPod(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	w := post(h, "/ticket", "tok-opus-0", map[string]string{"to": "ingest"})
	if w.Code != http.StatusOK {
		t.Fatalf("ticket: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "fable-1") {
		t.Errorf(
			"the ticket reply named the pod behind the alias: %s",
			w.Body.String(),
		)
	}
	var out struct {
		Ticket           string `json:"ticket"`
		To               string `json:"to"`
		ExpiresInSeconds int    `json:"expires_in_seconds"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.To != "ingest" {
		t.Errorf("to = %q, want the name the caller used", out.To)
	}
	if out.ExpiresInSeconds != int(ticketIdleTTL.Seconds()) {
		t.Errorf(
			"expires_in_seconds = %d, want %d",
			out.ExpiresInSeconds,
			int(ticketIdleTTL.Seconds()),
		)
	}
}

// The inheritance property again, applied to conversations rather than to
// mail. The peer that agreed to talk is gone; a new occupant of the slot
// inherits neither its inbox nor its introductions.
func TestATicketDoesNotOutliveTheSessionThatHeldIt(t *testing.T) {
	s, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	if live, err := s.store.LiveTickets(); err != nil || len(live) != 1 {
		t.Fatalf("tickets = %d, err = %v; want 1", len(live), err)
	}

	// fable-1 restarts.
	claim(t, h, "tok-fable-1")

	if live, err := s.store.LiveTickets(); err != nil || len(live) != 0 {
		t.Errorf(
			"tickets = %d after the peer restarted, err = %v; "+
				"want 0",
			len(live),
			err,
		)
	}
	if w := post(h, "/message", "tok-opus-0", map[string]string{
		"to": "fable-1", "kind": "notice", "body": "carrying on where we left off",
	}); w.Code != http.StatusConflict {
		t.Errorf("code = %d, want 409 after the peer restarted", w.Code)
	}
}

// An agent that asks again because it lost track is asking for the thing it
// already has. A second id would leave two records for one pair.
func TestAskingAgainDoesNotMintASecondTicket(t *testing.T) {
	s, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")

	first := ticket(t, h, "tok-opus-0", "fable-1")
	// And from the other end, which keys to the same pair.
	second := ticket(t, h, "tok-fable-1", "opus-0")
	if first != second {
		t.Errorf(
			"ticket ids %q and %q differ; a pair has one ticket",
			first,
			second,
		)
	}
	if live, _ := s.store.LiveTickets(); len(live) != 1 {
		t.Errorf("tickets = %d, want 1", len(live))
	}
}

// A ticket cannot be asked for toward a pod with no live session.
func TestATicketNeedsAPeerWithALiveSession(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	if w := post(h, "/ticket", "tok-opus-0", map[string]string{"to": "fable-1"}); w.Code != http.StatusNotFound {
		t.Errorf(
			"code = %d, want 404 for a peer with no live session",
			w.Code,
		)
	}
}

// A pod must claim a channel before it can ask for a ticket.
func TestAskingForATicketBeforeClaimingIsRefused(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-fable-1")
	if w := post(h, "/ticket", "tok-opus-0", map[string]string{"to": "fable-1"}); w.Code != http.StatusConflict {
		t.Errorf(
			"code = %d, want 409 when the caller holds no channel",
			w.Code,
		)
	}
}

// Who starts conversations is a topology signal in its own right, and the
// requester label is stamped rather than chosen.
func TestTicketsAreCountedAndGauged(t *testing.T) {
	_, h := testServer(t)
	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	body := w.Body.String()
	if !strings.Contains(
		body,
		`starling_tickets_issued_total{requester="opus-0"} 1`,
	) {
		t.Errorf(
			"the ticket was not attributed to the requester:\n%s",
			body,
		)
	}
	if !strings.Contains(body, "starling_tickets_open 1") {
		t.Errorf(
			"the open-ticket gauge did not report the pair:\n%s",
			body,
		)
	}
}

// What bounds the bucket. Nothing sweeps while the fleet is perfectly idle,
// which is a handful of small records; the next session start clears them.
func TestIdleTicketsAreSweptOnTheNextClaim(t *testing.T) {
	s, h := testServer(t)
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	s.store.now = func() time.Time { return now }

	claim(t, h, "tok-opus-0")
	claim(t, h, "tok-fable-1")
	ticket(t, h, "tok-opus-0", "fable-1")

	now = now.Add(ticketIdleTTL + time.Minute)
	if n := countTickets(t, s.store); n != 1 {
		t.Fatalf(
			"the expired ticket vanished without a claim; found %d",
			n,
		)
	}

	// A session starting anywhere is the moment starling is certain
	// something ended, so it is where the sweep runs.
	claim(t, h, "tok-opus-0")
	if n := countTickets(t, s.store); n != 0 {
		t.Errorf("%d expired ticket(s) survived a claim", n)
	}
}

// countTickets counts every ticket record in the store, expired or not, which
// LiveTickets deliberately will not do.
func countTickets(t *testing.T, s *Store) int {
	t.Helper()
	n := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTickets).
			ForEach(func(_, _ []byte) error {
				n++
				return nil
			})
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}
