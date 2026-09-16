package main

// Establishing who is calling, which is the only thing this service is really
// for. Everything else -- schema, dropping, delivery, counters -- is worth
// nothing if the sender is whoever the sender says it is.
//
// HOW: every pod mounts a projected service-account token, signed by the API
// server, carrying the pod's name and uid as claims. The caller presents it as
// a bearer token and starling asks the API server whether it is real, via
// TokenReview. The answer includes the pod name and uid in user.extra.
//
// Why NOT parse the jwt ourselves: we would then have to fetch and rotate the
// cluster's signing keys, verify the signature, check expiry and audience, and
// be right about all of it. TokenReview is the API server answering a question
// it is authoritative about.
//
// The cost is one call per new token, which the cache below reduces to roughly
// one per session.
//
// WHY NOT client-go: it is tens of megabytes of dependency to make one POST.
// flipr's restraint about gRPC applies here for the same reason -- the whole
// interaction is a JSON object in and a JSON object out.
//
// No fallback. If the API server cannot be reached, verification fails. It does
// not pass, it does not use a cached-forever answer, it does not trust the
// token's own claims because they look plausible.
//
// An unverified sender is exactly the thing this service exists to make
// impossible, so degrading into accepting one would be worse than being down.

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// Where kubelet puts this pod's own credentials and the cluster CA.
	saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	saCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

	// The extra-claim keys the API server returns for a bound token. These
	// are the whole point: the pod name is assigned by the API server
	// before the container starts, so it is a fact rather than a claim.
	extraPodName = "authentication.kubernetes.io/pod-name"
	extraPodUID  = "authentication.kubernetes.io/pod-uid"

	// How long a verified answer is reused. Deliberately short relative to
	// a token's lifetime: the cache exists so a chatty session does not
	// cause a TokenReview per message, not to avoid revalidating a
	// long-lived session.
	verifyCacheTTL = 5 * time.Minute
)

// ErrUnauthenticated is returned when a token is well-formed but the API server
// does not recognise it. Distinct from a transport failure, because the two
// call for opposite responses: reject the caller, or fail the service.
var ErrUnauthenticated = errors.New("token not authenticated by the API server")

// Identity is who a caller is, as established rather than as asserted. Every
// field here came from the API server.
type Identity struct {
	// Pod name, e.g. "agent-2". The hostname a StatefulSet assigned.
	Pod string
	// Pod uid. Distinguishes this occupant of the slot from the previous
	// one, which is the generation identifier August did not have.
	UID string
	// The service account, kept for logs. Not identity: every agent pod
	// shares the default account, so this does not distinguish them.
	ServiceAccount string
}

// Verifier turns a bearer token into an Identity. An interface so tests can
// establish identities without a cluster, and so the real implementation is
// never the only way to construct one.
type Verifier interface {
	Verify(ctx context.Context, token string) (Identity, error)
}

// cacheEntry is a verified identity and when it stops being reusable.
type cacheEntry struct {
	id      Identity
	expires time.Time
}

// K8sVerifier asks the API server. Safe for concurrent use.
type K8sVerifier struct {
	host   string // https://kubernetes.default
	client *http.Client
	// This pod's own token, for authenticating the TokenReview call itself.
	// Read on every use rather than cached: kubelet rotates it, and a
	// service that stops authenticating after ninety minutes is a bad
	// Monday.
	tokenPath string

	mu    sync.Mutex
	cache map[string]cacheEntry

	// Injectable for tests.
	now func() time.Time
}

// NewK8sVerifier builds a verifier that trusts the cluster CA and nothing else.
// Returns an error rather than falling back to an insecure client: a verifier
// that skips certificate checks would authenticate whatever answered.
func NewK8sVerifier(host string) (*K8sVerifier, error) {
	ca, err := os.ReadFile(saCAPath)
	if err != nil {
		return nil, fmt.Errorf("reading cluster CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New(
			"cluster CA at " + saCAPath + " is not usable PEM",
		)
	}
	return &K8sVerifier{
		host:      host,
		tokenPath: saTokenPath,
		cache:     map[string]cacheEntry{},
		now:       time.Now,
		client: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs:    pool,
					MinVersion: tls.VersionTLS12,
				},
			},
		},
	}, nil
}

// Verify establishes who presented this token, using a cached answer when one
// is still fresh.
func (v *K8sVerifier) Verify(
	ctx context.Context,
	token string,
) (Identity, error) {
	if strings.TrimSpace(token) == "" {
		return Identity{}, ErrUnauthenticated
	}
	if id, ok := v.cached(token); ok {
		return id, nil
	}
	id, err := v.review(ctx, token)
	if err != nil {
		return Identity{}, err
	}
	v.remember(token, id)
	return id, nil
}

// cached returns a previously verified identity if it has not expired.
func (v *K8sVerifier) cached(token string) (Identity, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	e, ok := v.cache[token]
	if !ok || v.now().After(e.expires) {
		return Identity{}, false
	}
	return e.id, true
}

// remember stores a verified identity for verifyCacheTTL.
func (v *K8sVerifier) remember(token string, id Identity) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cache[token] = cacheEntry{
		id:      id,
		expires: v.now().Add(verifyCacheTTL),
	}
}

// tokenReviewRequest is the API object, hand-written because it is nine fields.
type tokenReviewRequest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Spec       struct {
		Token string `json:"token"`
	} `json:"spec"`
}

// tokenReviewResponse is the part of the reply we read.
type tokenReviewResponse struct {
	Status struct {
		Authenticated bool `json:"authenticated"`
		User          struct {
			Username string              `json:"username"`
			Extra    map[string][]string `json:"extra"`
		} `json:"user"`
		Error string `json:"error"`
	} `json:"status"`
}

// review performs the TokenReview call, with backoff and jitter on transport
// failure. Retries only transport and 5xx: a 401 or a negative review is an
// answer, and repeating the question does not change it.
func (v *K8sVerifier) review(
	ctx context.Context,
	token string,
) (Identity, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			if err := sleepBackoff(ctx, attempt); err != nil {
				return Identity{}, err
			}
		}
		id, retryable, err := v.reviewOnce(ctx, token)
		if err == nil {
			return id, nil
		}
		lastErr = err
		if !retryable {
			return Identity{}, err
		}
	}
	return Identity{}, fmt.Errorf(
		"tokenreview failed after retries: %w",
		lastErr,
	)
}

// reviewOnce is a single TokenReview attempt. The bool reports whether a retry
// could plausibly succeed.
func (v *K8sVerifier) reviewOnce(
	ctx context.Context,
	token string,
) (Identity, bool, error) {
	own, err := os.ReadFile(v.tokenPath)
	if err != nil {
		return Identity{}, false, fmt.Errorf(
			"reading own service account token: %w",
			err,
		)
	}

	var body tokenReviewRequest
	body.APIVersion = "authentication.k8s.io/v1"
	body.Kind = "TokenReview"
	body.Spec.Token = token
	buf, err := json.Marshal(body)
	if err != nil {
		return Identity{}, false, err
	}

	url := v.host + "/apis/authentication.k8s.io/v1/tokenreviews"
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewReader(buf),
	)
	if err != nil {
		return Identity{}, false, err
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+strings.TrimSpace(string(own)),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.client.Do(req)
	if err != nil {
		return Identity{}, true, fmt.Errorf(
			"tokenreview transport: %w",
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return Identity{}, true, fmt.Errorf(
			"tokenreview http %d",
			resp.StatusCode,
		)
	}
	if resp.StatusCode != http.StatusCreated &&
		resp.StatusCode != http.StatusOK {
		// 403 here means starling's own RBAC is wrong, which is a
		// deployment fault and not the caller's. Say so plainly rather
		// than reporting it as an authentication failure and sending
		// someone hunting the wrong problem.
		return Identity{}, false, fmt.Errorf(
			"tokenreview refused with http %d -- check "+
				"starling's RBAC for tokenreviews",
			resp.StatusCode,
		)
	}

	var out tokenReviewResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Identity{}, false, fmt.Errorf(
			"decoding tokenreview: %w",
			err,
		)
	}
	if !out.Status.Authenticated {
		return Identity{}, false, ErrUnauthenticated
	}

	id := Identity{
		ServiceAccount: out.Status.User.Username,
		Pod:            firstExtra(out.Status.User.Extra, extraPodName),
		UID:            firstExtra(out.Status.User.Extra, extraPodUID),
	}
	// A token that authenticates but carries no pod claims is not a pod
	// token -- it is a plain service-account token, which any number of
	// things could hold. Refuse it: an identity without a pod is not an
	// identity here.
	if id.Pod == "" || id.UID == "" {
		return Identity{}, false, fmt.Errorf(
			"%w: token has no pod binding",
			ErrUnauthenticated,
		)
	}
	return id, false, nil
}

// firstExtra reads one value out of the API server's extra-claims map.
func firstExtra(extra map[string][]string, key string) string {
	if vs, ok := extra[key]; ok && len(vs) > 0 {
		return vs[0]
	}
	return ""
}

// sleepBackoff waits before a retry: full jitter, sleep = random(0, base*2^n),
// capped.
//
// Full jitter rather than exponential-plus-noise because four pollers
// long-polling the same service will fail at the same instant when it restarts,
// and a shared deterministic schedule reconverges them into a thundering herd
// on every subsequent attempt.
func sleepBackoff(ctx context.Context, attempt int) error {
	const base = 100 * time.Millisecond
	const cap = 5 * time.Second
	d := base << attempt
	if d > cap {
		d = cap
	}
	wait := time.Duration(rand.Int63n(int64(d) + 1))
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// BearerToken pulls the token out of an Authorization header. Returns empty
// for anything that is not exactly a bearer credential; being lenient here
// would mean guessing at what a caller meant to present.
func BearerToken(h http.Header) string {
	v := h.Get("Authorization")
	const prefix = "Bearer "
	if len(v) <= len(prefix) ||
		!strings.EqualFold(v[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(v[len(prefix):])
}
