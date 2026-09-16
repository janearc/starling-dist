package main

// Instrumentation. This is the reason starling is worth building rather than a
// side effect of having built it. Metrics on agent communication were a stated
// requirement of the design, not an addition (2026-09-01).
//
// The labels are stamped, NOT claimed, and that is what makes these numbers
// mean anything.
//
// A per-agent counter whose label the agent chooses is worthless: in August an
// agent replying under several names would have appeared in a dashboard as
// several healthy agents, and every panel would have looked fine.
//
// Here `sender` comes from a Kubernetes token the API server signed, so the
// series is the agent whether or not the agent agrees.
//
// Hand-rolled, matching the estate. flipr, haho and delightd all write their
// own /metrics and only metricsd takes client_golang. That keeps the dependency
// list at bbolt and protobuf, which is easy to defend for a service whose
// failure mode is "agents stop talking".
//
// Families are declared even when empty. A family with no series still emits
// its HELP and TYPE lines.
//
// This matters more than it looks: a scrape of a freshly started starling would
// otherwise omit the metric entirely, so a dashboard panel reads "No data" for
// a metric that exists and is legitimately zero, which is indistinguishable
// from a metric that was renamed or never shipped.

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value reads the current total.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge is a value that goes both ways.
type Gauge struct {
	mu sync.Mutex
	v  float64
}

// Set replaces the value.
func (g *Gauge) Set(v float64) {
	g.mu.Lock()
	g.v = v
	g.mu.Unlock()
}

// Value reads the current value.
func (g *Gauge) Value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.v
}

// family is one metric name plus every labelled series under it.
type family struct {
	name   string
	help   string
	typ    string // "counter" or "gauge"
	labels []string

	mu sync.Mutex
	// encoded labels -> nothing; counters live below
	series  map[string][]uint64
	counter map[string]*Counter
	gauge   map[string]*Gauge
}

// Registry holds every family starling exposes.
type Registry struct {
	mu       sync.Mutex
	families []*family
	byName   map[string]*family
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{byName: map[string]*family{}}
}

// declare registers a family so it appears in /metrics whether or not it has
// series yet. Called once per metric at startup; declaring twice is a
// programming error rather than a runtime condition, so it panics.
func (r *Registry) declare(name, help, typ string, labels []string) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[name]; dup {
		panic("starling: metric declared twice: " + name)
	}
	f := &family{
		name: name, help: help, typ: typ, labels: labels,
		counter: map[string]*Counter{},
		gauge:   map[string]*Gauge{},
	}
	r.byName[name] = f
	r.families = append(r.families, f)
	return f
}

// NewCounter declares a counter family.
func (r *Registry) NewCounter(name, help string, labels ...string) *CounterVec {
	return &CounterVec{f: r.declare(name, help, "counter", labels)}
}

// NewGauge declares a gauge family.
func (r *Registry) NewGauge(name, help string, labels ...string) *GaugeVec {
	return &GaugeVec{f: r.declare(name, help, "gauge", labels)}
}

// CounterVec is a counter family; With selects a series.
type CounterVec struct{ f *family }

// With returns the counter for these label values, in the order the family
// declared its label names.
func (cv *CounterVec) With(values ...string) *Counter {
	key := encodeLabels(cv.f.labels, values)
	cv.f.mu.Lock()
	defer cv.f.mu.Unlock()
	c, ok := cv.f.counter[key]
	if !ok {
		c = &Counter{}
		cv.f.counter[key] = c
	}
	return c
}

// GaugeVec is a gauge family; With selects a series.
type GaugeVec struct{ f *family }

// With returns the gauge for these label values.
func (gv *GaugeVec) With(values ...string) *Gauge {
	key := encodeLabels(gv.f.labels, values)
	gv.f.mu.Lock()
	defer gv.f.mu.Unlock()
	g, ok := gv.f.gauge[key]
	if !ok {
		g = &Gauge{}
		gv.f.gauge[key] = g
	}
	return g
}

// Reset drops every series in a gauge family. Used before republishing gauges
// that are derived from current state (inbox depth per channel), so a channel
// that has gone away stops reporting a stale depth forever.
func (gv *GaugeVec) Reset() {
	gv.f.mu.Lock()
	gv.f.gauge = map[string]*Gauge{}
	gv.f.mu.Unlock()
}

// encodeLabels renders label pairs into the Prometheus exposition form, which
// doubles as the series key. Sorted by declaration order, not alphabetically:
// the caller passes values positionally and a reordering here would silently
// mislabel every series.
func encodeLabels(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		v := ""
		if i < len(values) {
			v = values[i]
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(v))
		b.WriteString(`"`)
	}
	b.WriteByte('}')
	return b.String()
}

// escapeLabelValue escapes the three characters the text format reserves.
// Without this, a pod name containing a quote would produce a scrape that fails
// to parse -- and the failure would be attributed to starling being down.
func escapeLabelValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}

// Render writes the whole registry in Prometheus text exposition format.
func (r *Registry) Render() string {
	r.mu.Lock()
	fams := make([]*family, len(r.families))
	copy(fams, r.families)
	r.mu.Unlock()

	var b strings.Builder
	for _, f := range fams {
		fmt.Fprintf(&b, "# HELP %s %s\n", f.name, f.help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", f.name, f.typ)

		f.mu.Lock()
		keys := make([]string, 0, len(f.counter)+len(f.gauge))
		for k := range f.counter {
			keys = append(keys, k)
		}
		for k := range f.gauge {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if c, ok := f.counter[k]; ok {
				fmt.Fprintf(
					&b,
					"%s%s %d\n",
					f.name,
					k,
					c.Value(),
				)
			}
			if g, ok := f.gauge[k]; ok {
				fmt.Fprintf(
					&b,
					"%s%s %g\n",
					f.name,
					k,
					g.Value(),
				)
			}
		}
		f.mu.Unlock()
	}
	return b.String()
}

// Metrics is starling's instrument panel. Every field here answers a question
// somebody would otherwise have to read a transcript to answer.
type Metrics struct {
	reg *Registry

	// The spike panel. Rate per sender is the thing to alarm on: a burst is
	// normal, sustained is not.
	Accepted *CounterVec // sender
	// Refusals, by reason. A rising drop rate on one sender is a signal
	// about that agent before anybody reads a message.
	Dropped *CounterVec // sender, reason
	// Delivered, by pair. An agent that starts talking to everyone rather
	// than to its reducer is a topology change, and topology changes are
	// what teaming failures look like from outside.
	Delivered *CounterVec // sender, recipient
	// Echo: the same body forwarded again. This is the mechanism by which
	// register actually spread in August, and it is a hash comparison.
	Echoed *CounterVec // sender
	// Throttle refusals. The limiter as sensor: a stuck agent shows up here
	// first.
	Throttled *CounterVec // sender
	// Authentication outcomes, so a broken RBAC or an expired token is
	// visible as something other than silence.
	AuthFailed *CounterVec // reason

	// Mail waiting, per pod. An agent that has stopped reading is as
	// interesting as one that will not stop writing.
	//
	// Labelled "agent", NOT "pod". Prometheus attaches its own `pod` label
	// at scrape time -- the pod being scraped, which is starling itself --
	// and a colliding label from the target is renamed `exported_pod`.
	//
	// So a panel querying {{pod}} would show "starling-7f475c48f-dgmfq" on
	// every series instead of naming the agent. Found by reading a real
	// scrape;
	//
	// invisible in the raw /metrics output, where the label is exactly what
	// you wrote.
	//
	// Labelled by agent, not by channel. Channel ids were a label here and
	// that was wrong twice.
	//
	// /metrics is unauthenticated, so it published an internal identifier
	// to anything that could reach the port, and a label per session grows
	// without bound, which is the cardinality failure that makes a
	// dashboard slow and then useless.
	InboxDepth *GaugeVec // pod
	// Seconds since a poller last reported. An agent whose poller died
	// stops heartbeating, and that absence is a panel rather than a
	// silence.
	HeartbeatAge *GaugeVec // pod
	// How many sessions exist at all.
	Channels *GaugeVec

	// Tickets opened, by the session that asked. Who starts conversations
	// is a topology signal in its own right: an agent that opens a ticket
	// to everybody is doing something different from one that talks to its
	// peer.
	TicketsIssued *CounterVec // requester
	// Pairs currently able to talk. Rising without messages rising means
	// agents are asking for channels and then not using them.
	TicketsOpen *GaugeVec

	// The forensic record. These are about the archive, not about the
	// relay, and they are separate families on purpose: a dashboard must be
	// able to show that messages are flowing while the archive is failing,
	// because that is the state the design deliberately allows.
	RecorderQueued  *CounterVec // envelopes handed to the recorder
	RecorderPosted  *CounterVec // outcome
	RecorderDropped *CounterVec // reason
	RecorderRetries *CounterVec // attempts beyond the first
}

// NewMetrics declares every family up front, so a freshly started starling
// exposes all of them at zero rather than growing its /metrics as things
// happen. See the note at the top about why empty families still appear.
func NewMetrics() *Metrics {
	r := NewRegistry()
	return &Metrics{
		reg: r,
		Accepted: r.NewCounter(
			"starling_messages_accepted_total",
			"Messages accepted from a sender.",
			"sender",
		),
		Dropped: r.NewCounter(
			"starling_messages_dropped_total",
			"Messages refused, by reason.",
			"sender",
			"reason",
		),
		Delivered: r.NewCounter(
			"starling_messages_delivered_total",
			"Messages delivered, by sender and recipient.",
			"sender",
			"recipient",
		),
		Echoed: r.NewCounter(
			"starling_messages_echoed_total",
			"Messages whose body had been sent before.",
			"sender",
		),
		Throttled: r.NewCounter(
			"starling_requests_throttled_total",
			"Requests refused by the per-pod rate limit.",
			"sender",
		),
		AuthFailed: r.NewCounter(
			"starling_auth_failures_total",
			"Requests that could not be authenticated, by reason.",
			"reason",
		),
		InboxDepth: r.NewGauge(
			"starling_inbox_depth",
			"Messages waiting in an agent's inbox.",
			"agent",
		),
		HeartbeatAge: r.NewGauge(
			"starling_heartbeat_age_seconds",
			"Seconds since an agent's poller last reported.",
			"agent",
		),
		Channels: r.NewGauge(
			"starling_channels",
			"Sessions with a claimed channel.",
		),

		TicketsIssued: r.NewCounter(
			"starling_tickets_issued_total",
			"Tickets opened, by the session that asked.",
			"requester",
		),
		TicketsOpen: r.NewGauge(
			"starling_tickets_open",
			"Pairs of sessions currently able to exchange "+
				"messages.",
		),

		RecorderQueued: r.NewCounter(
			"starling_recorder_queued_total",
			"Envelopes handed to the forensic recorder.",
		),
		RecorderPosted: r.NewCounter(
			"starling_recorder_posted_total",
			"Envelopes written to the archive, by outcome.",
			"outcome",
		),
		RecorderDropped: r.NewCounter(
			"starling_recorder_dropped_total",
			"Envelopes the archive did not receive, by reason.",
			"reason",
		),
		RecorderRetries: r.NewCounter(
			"starling_recorder_retries_total",
			"Archive post attempts beyond the first.",
		),
	}
}

// Render produces the /metrics body.
func (m *Metrics) Render() string { return m.reg.Render() }
