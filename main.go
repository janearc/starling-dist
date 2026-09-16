package main

// starling -- the relay agents talk through instead of talking to each other.
//
// Kept short on purpose. Everything interesting lives in auth.go, store.go,
// server.go, limits.go and metrics.go; this file wires them together, starts a
// listener, and stops cleanly. If this file grows, something has been put in
// the wrong place.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

// version is the short git commit this binary was built from. The Dockerfile
// stamps it with -X main.version=$version; until 2026-09-03 nothing declared
// the variable, so the stamp landed on nothing and the build's provenance was
// lost at the moment it was recorded.
//
// "dev" means the binary was built outside that path and cannot be traced to a
// commit, and /health says so rather than hiding it.
var version = "dev"

// Defaults, all overridable by environment so a manifest can set them without a
// rebuild. No flags: a container's configuration is its environment, and having
// both is two places to look when something is wrong.
const (
	defaultAddr      = ":8099"
	defaultStorePath = "/var/lib/starling/starling.db"
	defaultAPIServer = "https://kubernetes.default"
)

// env reads a variable with a fallback.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// nameTmuxWindow sets the tmux window title when running inside one, so a
// person can find the service among their windows. Best effort and silent on
// failure: this is a convenience for a human looking at a screen, and a service
// must not fail to start because a terminal multiplexer was not interested.
func nameTmuxWindow(name string) {
	if os.Getenv("TMUX") == "" {
		return
	}
	_ = exec.Command("tmux", "rename-window", name).Run()
}

// main opens the store and the identity verifier, refusing to start
// without either, then serves until it is told to stop.
func main() {
	log := NewLogger()
	nameTmuxWindow("starling")

	addr := env("STARLING_ADDR", defaultAddr)
	storePath := env("STARLING_STORE", defaultStorePath)
	apiServer := env("STARLING_APISERVER", defaultAPIServer)

	store, err := OpenStore(storePath)
	if err != nil {
		log.Error(
			"cannot open the store; refusing to start",
			map[string]any{"path": storePath, "err": err.Error()},
		)
		os.Exit(1)
	}
	defer store.Close()

	// Fail to start rather than start unable to verify. A starling that
	// cannot establish who is calling is not a degraded starling, it is a
	// service that would have to either refuse everything or trust
	// everything, and both are worse than being visibly down.
	verifier, err := NewK8sVerifier(apiServer)
	if err != nil {
		log.Error(
			"cannot build the identity verifier; refusing to start",
			map[string]any{"err": err.Error()},
		)
		os.Exit(1)
	}

	// Aliases come from flipr, which is not wired yet. Empty until it is:
	// addressing falls back to pod names, which works and is honest, rather
	// than pretending a table exists.
	aliases := StaticAliases{}

	srv := NewServer(store, verifier, aliases, NewMetrics())

	// The forensic record. Absent by default: with no URL configured
	// starling relays and writes nothing down, which is the honest zero
	// rather than a half-configured archive that looks present and holds
	// nothing.
	recorder := NewRecorder(
		env("STARLING_RECORDER_URL", ""),
		env("STARLING_RECORDER_TOKEN_FILE", ""),
		srv.metrics, log)
	if recorder == nil {
		log.Warn(
			"no forensic recorder configured; messages are "+
				"relayed but not archived",
			nil,
		)
	} else {
		log.Info("archiving to postgrest", map[string]any{
			"url": env("STARLING_RECORDER_URL", ""),
		})
	}
	srv.SetRecorder(recorder)
	defer recorder.Close()

	httpSrv := &http.Server{
		Addr:    addr,
		Handler: srv.Handler(),
		// ReadHeaderTimeout guards the slow-header case.
		//
		// There is deliberately NO ReadTimeout or WriteTimeout: /inbox
		// long-polls for up to thirty seconds by design, and a write
		// timeout would kill exactly the requests that are working
		// correctly.
		//
		// The body size cap and the in-flight limit are what bound
		// resource use here instead.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	go func() {
		log.Info(
			"listening",
			map[string]any{"addr": addr, "store": storePath},
		)
		if err := httpSrv.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Error(
				"listener stopped",
				map[string]any{"err": err.Error()},
			)
			os.Exit(1)
		}
	}()

	// Shut down on a signal, giving in-flight long-polls a moment to
	// finish.
	//
	// A poller whose connection is cut mid-wait retries with backoff and
	// loses nothing, so this is politeness rather than correctness -- but a
	// service that exits mid-request writes confusing lines into somebody's
	// log.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	sig := <-stop
	log.Info("shutting down", map[string]any{"signal": fmt.Sprint(sig)})

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Warn(
			"shutdown did not complete cleanly",
			map[string]any{"err": err.Error()},
		)
	}
	log.Info("stopped", nil)
}
