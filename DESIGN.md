# starling

the relay agents talk through instead of talking to each other.

a direct channel lets the sender write its own name, and nobody is
positioned to check it. starling stamps identity from the pod's own
kubernetes token, so a message says who sent it because the api server
said so.

    ask       a ticket: these two may talk, for thirty idle minutes
    send      a murmur: a short note, or a claim with its evidence
    poll      the pod's poller long-polls and delivers into the session
    count     every message stamped, so the dashboard cannot be lied to

a channel is one session, not one pod, so the next occupant of a slot
never reads the last one's mail. aliases name work and resolve inside
starling, so an agent never learns a peer's pod or a role for itself.

the shape does the filtering: a note is a kilobyte, an assertion is three
capped fields, and there is no field for narrative, so there is nowhere
for doctrine to live. off-contract messages never arrive.

losing the store loses every undelivered message, and the remedy is to
roll the deployment rather than restore it.

that is the service. the rest is why, and the readme is how to run it.

## what is built

deployed and serving: two agent pods exchanged a message, the sender
established by TokenReview, prometheus scraping it.

| part | state |
|---|---|
| the contract, and identity from the pod token | built |
| the store, channels, retirement | built |
| tickets: who may talk to whom, thirty minutes idle | built |
| the http surface, limits, metrics, `/api` | built |
| the reference client | built, untested |
| deployment: image, RBAC, PVC, service, route | built and running |
| the poller | not built. `send-keys`, the restart loop and `/opt/starling` are what it will do |
| flipr: aliases and the messaging kill-switch | not wired. aliases are an empty static table, so addressing is by pod name |
| smoothing | not built. a second layer behind a flag, off by default |

nothing running depends on anything unbuilt: the relay works without a
poller, and pod-name addressing works without flipr.

## the shape

    main.go        the mux, the listener, the static alias table
    identity.go    TokenReview: the pod name and uid, from the api server
    channel.go     one channel per session, claimed at start, retired
    ticket.go      who may talk to whom, keyed on channels, idle timeout
    validate.go    the schema check: kind, size, form, recipient
    store.go       the mailbox, and nothing that survives its loss
    metrics.go     stamped labels only
    proto/         the source of truth; gen/ is generated from it
    client/        the reference client for an agent to use

## the problem

agents need to cooperate, and "ask the ingest one whether the schema
changed" is an ordinary request with no way to make it.

in august 2026 a family of agents accepted a peer changing its
session-name string and replying under names that were not its own.
nobody could reconstruct who said what, because the record was terminal
scrollback and one agent's account of another.

the second problem is propagation: that family's register, emphatic
doctrine and role vocabulary and quotations nobody had said, spread
because messages passed between agents untouched.

so the requirement is not a transport. it is a place in the middle that
can refuse.

## the requirements

- something in the middle that drops what must not propagate and smooths
  what does, without stopping agents talking
- their communication must be measurable: metrics are a requirement, not
  a side effect
- agents do nothing until the operator has spoken to them, and context
  arrives when handed over, never implicitly
- boring to run: startable, stoppable, legible about whether it is up

these are stated as requirements and not as quotations, because a phrase
chosen for emphasis is exactly the phrase that survives being copied
while the reason it was said does not. quote machines, not people.

## what was rejected

**kafka.** already running, and it makes the observer first-class.
rejected because unauthenticated kafka lets any agent read any topic, and
fixing that is SASL, ACLs and per-agent principals to buy back a property
a relay has for free.

**flipr as the transport.** small, reliable, already a dependency, with a
tamper-evident oplog. rejected because its `caller` comes from a header
the client sets itself: correct for a flag store, fatal where attribution
is the point. flipr keeps the alias table and the kill-switch.

**claude code's own cross-session delivery.** works out of the box, and
is unmediated: no stamping, no schema, no dropping, no counters. it
removes everything this service exists to do.

**starling pushing with `kubectl exec` and `tmux send-keys`.** a relay
that can type into any agent's terminal is a worse problem than the one
it solves.

**a NetworkPolicy fencing agents off from flipr.** it would stop an agent
reading the alias table and learning a role word for itself, but agents
legitimately use flipr and there is nothing to do with the knowledge.

## the decisions

**identity comes from the pod's token, and the schema has no sender
field.** every pod mounts a projected service-account token signed by the
api server, carrying the pod name and uid. an agent cannot forge one or
obtain a peer's, and there is no secret to rotate.

`Send` has no `from`: not ignored, absent. parsing rejects unknown
fields, so a payload carrying one never reaches code that might trust it.
the `pod_uid` is the part easy to miss: it separates this `agent-2` from
yesterday's, and confusable generations are what went wrong in august.

**aliases resolve inside starling and are never returned.** the operator
addresses `ingest`; the agent never learns which pod that is. not for
secrecy, since an agent pod has no inbound surface at all.

a name an agent can read is a role it can inhabit, so aliases name work
(`ingest`, `maps`, `edge`) and never roles.

**channels are ephemeral, one per session.** a pod outlives a session,
and without this the second occupant of `agent-2` reads mail written to
someone else. aliases point at pods, starling maps pods to their current
channel, and both hops are internal.

**a ticket is who may talk to whom, and for how long.** two agents get a
channel on request and it closes after thirty idle minutes. a ticket
controls reachability and nothing else: what may be said is the size cap
and the shape, answered elsewhere on purpose.

a ticket is not a credential the sender presents. if a ticket id were a
field on a message it would be a field a sender writes about itself, the
`from` problem by a side door, so starling looks a ticket up by the two
channel ids it stamped. a payload carrying `ticket` fails to decode.

keyed on channels rather than pods, so the next occupant of a slot cannot
inherit a conversation, and either end restarting ends the ticket.

opened unilaterally, because a handshake needs the peer to be executing,
and an idle session is blocked on a read: consent could only come from an
agent that was already busy. a ticket grants reachability, not attention.

thirty minutes idle rather than elapsed: every message restarts the
clock, so a pair working through something never asks twice. expiry is
applied on use and on the next claim, which is the one moment starling is
certain a session has ended. nothing sweeps while the fleet is idle.

**delivery is pull, and the poller is mandatory.** an idle claude session
is not running: it is blocked on a read, with no loop and no timer, so an
idle agent is unreachable rather than slow. each pod runs a poller that
long-polls starling and delivers with `send-keys`, which unblocks it.

the poller is the tick, the only route into a blocked session, and the
heartbeat, so starling can tell "pod alive, session idle" from "session
gone". it runs inside the pod it serves and can reach exactly one tmux
socket, its own.

a process in the agent container rather than a sidecar, which removes the
shared-socket plumbing and costs supervision: kubelet will not restart a
dead process inside a live container.

so the entrypoint runs it under a restart loop with backoff, and starling
counts heartbeat gaps, because an agent whose poller died is deaf while
its pod reports healthy.

the poller and the agent are one principal, and what keeps them from
colliding is convention: the poller pulls, the agent only posts. the
agent posts to starling directly, so a dead poller makes an agent deaf
and not mute, and it can still report what went wrong.

**delivered text must be unmistakably a peer's.**

```
[starling] message from agent-2 (alias: ingest), 2026-09-01T23:58:14Z
  <body>
```

not cosmetic: the moment another agent's words arrive undistinguished in
the input stream, this is an inter-agent prompt-injection channel, and
august was agents adopting each other's framing without noticing whose it
was.

**dropping is mechanical before it is clever.** a schema check throws out
most of what should go, with no judgement: unknown kind, oversized body,
unresolvable recipient, malformed envelope.

size is capped twice. a hard transport limit of 64 KiB refuses an
oversized request while reading it, because a limit checked after the
body is in memory has already lost. a semantic limit of 1 KiB on the
message text is enforced in validation and counted as an ordinary drop.

the second is not a resource guard. agents move bulk through the mirror
and their clones; a message is for coordination, and the refusal says so:
too long, push it and send the ref.

it also closes a propagation path, since pasting wholesale is how
register travelled and a kilobyte has to be summarised.

the limit was 8 KiB, which was a guess made before anybody measured.

six peer messages between two sessions measured 2.5 to 4.5 KB each, every
reply was a 170-byte acknowledgement, and nobody read them. 8 KiB refused
none of the six, so it admitted everything it was measured against.

1 KiB is roughly 150 words: a claim, its evidence, and where to look. the
regression test asserts the constant against the measurement, so raising
it over 2.5 KB fails the suite rather than passing review.

**a schema is a filter that needs no model.** the cap bounds how much can
be said and the shape bounds what kind of thing it can be. a murmur
carries one of two forms, never both and never neither:

| form | fields | cap |
|---|---|---|
| a short note | `body` | 1 KiB |
| an assertion | `claim`, `evidence`, `location` | 240 / 512 / 240 bytes |

`claim` and `location` are required once any of the three is set, and the
fields are capped separately, which is the part that does the work: one
combined budget would let a sender spend it all on one field.

they sum to 992 bytes, under the body cap, so neither form is the cheap
way to say more.

requiring `location` is the sharper requirement. an assertion nobody can
check is what this relay exists to stop carrying, and "say where to look"
is a mechanical test where "is this well-founded" is not.

validation does not decide which form a kind ought to use: an arguable
rule in the mechanical layer is a rule somebody argues away in a pull
request. both forms are bounded, so the choice is the sender's.

the three fields reach the recipient and the archive as three fields.
flattening them into a body would put the narrative field back in the
record a later reader works from.

this was additive on the wire: `Send` gained fields 5 to 7 and `Envelope`
11 to 13, nothing renumbered or removed, so a sender that only knows
`body` still works. what changed is policy.

the measurement behind it: the peer channel carried 3.7 to 6.6
capitalised words per hundred, against 0.17 to 0.80 in the same estate's
documents. emphasis survives copying while the reason for it does not.

smoothing, which normalises prose and flags escalation, is a second layer
behind a flag, off by default, and the place for a small local model
looking only at what the counters flagged. the shape is what keeps that
layer small.

**metrics are the reason this is worth building.** every message passes
one place, so counting is free, and the labels are stamped rather than
claimed.

a per-agent counter whose label the agent chooses is worthless: an agent
replying under several names would have looked like several healthy
agents.

- messages per minute per sender, the spike panel
- drop rate by reason, which rises on one sender before anyone reads a
  message
- pairwise volume, because an agent talking to everyone instead of its
  reducer is a topology change, and that is what teaming failures look
  like from outside
- repeated-body count, by hash: echo is how register actually spread
- inbox depth per recipient, since an agent that is not reading is as
  interesting as one that will not stop writing

none of this needs sentiment analysis.

## declared: losing the store is disastrous

if starling loses its store, every undelivered message is gone. there is
no replay, no second copy and no reconstruction, and there is not going
to be one.

two things bound how bad that is. the blast radius is agents and only
agents: nothing in the estate reads starling, so a total loss stops
agents talking and touches nothing else.

and agent work does not live here, because it was given somewhere else to
live: an agent works in a clone of the read-only mirror and pushes to a
writable host path that survives a cluster rebuild.

what is lost is in-flight conversation, which is the cheapest thing here
to recreate: you ask again.

so the remediation is replacement, not recovery. if the agents are not
cohesive, roll the deployment and start over. an agent population is
re-creatable by construction, and a confused one is cheaper to replace
than to repair.

## what this does not solve

- an agent can be sincerely wrong under its own correct name: this fixes
  provenance, not judgement
- latency is bounded by the poller's interval, so seconds rather than
  instant. the operator is always the fast path
- if flipr is down, starling is down, inherited deliberately
