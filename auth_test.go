package main

// Tests for the only thing that matters: that a caller is who the API server
// says it is, and that every way of not knowing that results in a refusal.
//
// These stand a fake API server up rather than mocking the verifier, because
// the parts most likely to be wrong are the ones that talk to it -- what counts
// as authenticated, which failures are worth retrying, and whether a token
// without pod claims is accepted.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAPI stands in for the Kubernetes API server. reply decides what the
// TokenReview call returns, and calls counts how many times it was asked --
// which is how the cache and the retry policy are observed.
type fakeAPI struct {
	srv   *httptest.Server
	calls atomic.Int32
}

// newFakeAPI starts a server whose TokenReview handler is the supplied
// function.
func newFakeAPI(
	t *testing.T,
	handler func(token string, w http.ResponseWriter),
) *fakeAPI {
	t.Helper()
	f := &fakeAPI{}
	f.srv = httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.calls.Add(1)
			var req tokenReviewRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			handler(req.Spec.Token, w)
		}),
	)
	t.Cleanup(f.srv.Close)
	return f
}

// authenticatedAs writes a TokenReview reply for a real pod token.
func authenticatedAs(w http.ResponseWriter, pod, uid string) {
	var out tokenReviewResponse
	out.Status.Authenticated = true
	out.Status.User.Username = "system:serviceaccount:local:default"
	out.Status.User.Extra = map[string][]string{
		extraPodName: {pod},
		extraPodUID:  {uid},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(out)
}

// testVerifier builds a verifier pointed at a fake API server, with this
// process's own "service account token" in a temp file.
func testVerifier(t *testing.T, f *fakeAPI) *K8sVerifier {
	t.Helper()
	dir := t.TempDir()
	tok := filepath.Join(dir, "token")
	if err := os.WriteFile(tok, []byte("starlings-own-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &K8sVerifier{
		host:      f.srv.URL,
		client:    f.srv.Client(),
		tokenPath: tok,
		cache:     map[string]cacheEntry{},
		now:       time.Now,
	}
}

// The identity is the pod and uid the API server's TokenReview names, never
// anything the caller says about itself.
func TestVerifyReturnsThePodTheAPIServerNames(t *testing.T) {
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {
		authenticatedAs(w, "agent-2", "uid-agent-2")
	})
	id, err := testVerifier(
		t,
		f,
	).Verify(context.Background(), "an-agent-token")
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if id.Pod != "agent-2" {
		t.Errorf("pod = %q, want agent-2", id.Pod)
	}
	if id.UID != "uid-agent-2" {
		t.Errorf("uid = %q, want the pod uid from the claims", id.UID)
	}
}

// A token the API server does not authenticate yields no identity.
func TestAnUnauthenticatedTokenIsRefused(t *testing.T) {
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {
		var out tokenReviewResponse
		out.Status.Authenticated = false
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(out)
	})
	_, err := testVerifier(
		t,
		f,
	).Verify(context.Background(), "a-made-up-token")
	if err == nil {
		t.Fatal("a token the API server rejects must not verify")
	}
}

// THE SUBTLE ONE. A plain service-account token authenticates perfectly
// well and is not a pod. Accepting it would give an identity with no pod
// name, and every downstream label would silently be the empty string.
func TestATokenWithoutPodClaimsIsRefused(t *testing.T) {
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {
		var out tokenReviewResponse
		out.Status.Authenticated = true
		out.Status.User.Username = "system:serviceaccount:local:default"
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(out)
	})
	_, err := testVerifier(
		t,
		f,
	).Verify(context.Background(), "a-bare-sa-token")
	if err == nil {
		t.Fatal(
			"a token that authenticates but names no pod is not " +
				"an identity here",
		)
	}
}

// A blank token is refused here, and costs the API server nothing.
func TestAnEmptyTokenNeverReachesTheAPIServer(t *testing.T) {
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {
		authenticatedAs(w, "agent-0", "uid")
	})
	v := testVerifier(t, f)
	if _, err := v.Verify(context.Background(), "   "); err == nil {
		t.Fatal("an empty token must be refused")
	}
	if n := f.calls.Load(); n != 0 {
		t.Errorf(
			"asked the API server %d times about an empty token; "+
				"want 0",
			n,
		)
	}
}

// The cache exists so a chatty session does not cause a TokenReview per
// message. Without it, four agents in conversation would triple the API
// server's load for no new information.
func TestAVerifiedTokenIsNotRevalidatedEveryMessage(t *testing.T) {
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {
		authenticatedAs(w, "agent-1", "uid-1")
	})
	v := testVerifier(t, f)
	for i := 0; i < 5; i++ {
		if _, err := v.Verify(context.Background(), "same-token"); err != nil {
			t.Fatalf("verify %d: %v", i, err)
		}
	}
	if n := f.calls.Load(); n != 1 {
		t.Errorf(
			"called the API server %d times for one token; want 1",
			n,
		)
	}
}

// A cached verification lasts verifyCacheTTL, then the token is checked
// again, so a pod that is gone stops being believed.
func TestTheCacheExpires(t *testing.T) {
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {
		authenticatedAs(w, "agent-1", "uid-1")
	})
	v := testVerifier(t, f)
	now := time.Now()
	v.now = func() time.Time { return now }

	if _, err := v.Verify(context.Background(), "t"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(verifyCacheTTL + time.Second)
	if _, err := v.Verify(context.Background(), "t"); err != nil {
		t.Fatal(err)
	}
	if n := f.calls.Load(); n != 2 {
		t.Errorf("called %d times across the TTL boundary; want 2", n)
	}
}

// Guards the obvious catastrophe: a cache keyed on anything shared would
// hand one agent another's identity.
func TestDifferentTokensAreCachedSeparately(t *testing.T) {
	f := newFakeAPI(t, func(token string, w http.ResponseWriter) {
		switch token {
		case "token-a":
			authenticatedAs(w, "agent-0", "uid-0")
		default:
			authenticatedAs(w, "agent-3", "uid-3")
		}
	})
	v := testVerifier(t, f)
	a, err := v.Verify(context.Background(), "token-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := v.Verify(context.Background(), "token-b")
	if err != nil {
		t.Fatal(err)
	}
	if a.Pod == b.Pod {
		t.Fatalf(
			"two tokens resolved to the same pod (%q); the cache "+
				"is keyed wrong",
			a.Pod,
		)
	}
}

// A 503 from the API server is retried with backoff rather than refusing the
// caller for a moment of cluster load.
func TestATransientFailureIsRetried(t *testing.T) {
	var n atomic.Int32
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {
		if n.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		authenticatedAs(w, "agent-2", "uid-2")
	})
	id, err := testVerifier(t, f).Verify(context.Background(), "tok")
	if err != nil {
		t.Fatalf("a 503 should be retried, not fatal: %v", err)
	}
	if id.Pod != "agent-2" {
		t.Errorf("pod = %q after retry", id.Pod)
	}
}

// A 403 means starling's own RBAC is wrong. Asking again cannot fix it,
// and retrying turns a clear deployment fault into a slow one.
func TestARefusalIsNotRetried(t *testing.T) {
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {
		w.WriteHeader(http.StatusForbidden)
	})
	v := testVerifier(t, f)
	if _, err := v.Verify(context.Background(), "tok"); err == nil {
		t.Fatal("a 403 must fail")
	}
	if n := f.calls.Load(); n != 1 {
		t.Errorf("retried a 403 %d times; want 1 attempt", n)
	}
}

// The property worth stating in a test: no fallback. An unverified sender
// is the thing this service exists to make impossible, so being unable to
// verify must refuse rather than guess.
func TestVerificationFailsRatherThanDegradingWhenTheAPIServerIsGone(
	t *testing.T,
) {
	f := newFakeAPI(t, func(_ string, w http.ResponseWriter) {})
	v := testVerifier(t, f)
	f.srv.Close()
	if _, err := v.Verify(context.Background(), "tok"); err == nil {
		t.Fatal(
			"with no API server reachable, verification must " +
				"fail closed",
		)
	}
}

// The Authorization header yields a bearer token only in the forms RFC 7235
// allows, and nothing otherwise.
func TestBearerToken(t *testing.T) {
	cases := []struct {
		header string
		want   string
		why    string
	}{
		{"Bearer abc", "abc", "the ordinary case"},
		{
			"bearer abc",
			"abc",
			"the scheme is case-insensitive per RFC 7235",
		},
		{
			"Bearer   abc  ",
			"abc",
			"surrounding whitespace is not part of the token",
		},
		{"", "", "no header"},
		{"abc", "", "a bare value is not a bearer credential"},
		{
			"Basic abc",
			"",
			"a different scheme is not a bearer credential",
		},
		{"Bearer", "", "the scheme alone carries no token"},
		{"Bearer ", "", "an empty token is empty"},
	}
	for _, c := range cases {
		h := http.Header{}
		if c.header != "" {
			h.Set("Authorization", c.header)
		}
		if got := BearerToken(h); got != c.want {
			t.Errorf(
				"BearerToken(%q) = %q, want %q -- %s",
				c.header,
				got,
				c.want,
				c.why,
			)
		}
	}
}

// A cancelled context ends a backoff wait at once.
func TestBackoffRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepBackoff(ctx, 3); err == nil {
		t.Fatal(
			"a cancelled context must abandon the wait rather " +
				"than sleeping it out",
		)
	}
}
