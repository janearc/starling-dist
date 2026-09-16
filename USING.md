# Using starling

The relay agents talk through instead of talking to each other.

An agent posts a message; starling establishes who sent it from the pod's
Kubernetes token, checks it against a schema, drops what should not propagate,
delivers it, and counts everything on the way past.

**A sender cannot write its own name.** There is no `from` field, and a payload
carrying one is rejected rather than ignored. Identity comes from the projected
service-account token every pod already mounts — signed by the API server,
naming the pod, unobtainable by any other pod.

## Run it

    STARLING_ADDR=:8099 \
    STARLING_STORE=/var/lib/starling/starling.db \
    STARLING_APISERVER=https://kubernetes.default \
      ./starling

All three have those values as defaults. It refuses to start if it cannot open
the store or build the identity verifier, because a starling that cannot
establish who is calling would have to either refuse everyone or trust everyone.

    go build ./...          # build
    go test ./...           # test
    sh bin/gen.sh           # regenerate; the tree must be clean after

## Endpoints

| | |
|---|---|
| `POST /channel` | claim a channel for this session; retires the pod's previous one |
| `POST /ticket` | `{to}` — permission for these two sessions to talk, thirty minutes idle |
| `POST /message` | `{to, kind, in_reply_to}` and one message form — no sender field exists |
| `POST /inbox` | `{channel, wait_seconds}` — long-poll for this session's mail |
| `POST /heartbeat` | the poller reporting that it is alive |
| `GET /health` | healthy, degraded or down, by touching the store |
| `GET /metrics` | prometheus text |
| `GET /api` | the compiled FileDescriptorSet |

The five `POST`s need `Authorization: Bearer <the pod's service-account token>`.
The three `GET`s do not, and are not behind the concurrency cap: a health check
that fails when the service is busy fails when you need it.

`channel` on `/inbox` is **proof, not an address**. Whose inbox it is still
follows from the token; a wrong channel is refused rather than redirected.

## Two message forms, and no narrative field in either

A message carries one of these, never both and never neither:

| form | fields | cap |
|---|---|---|
| a short note | `body` | 1 KiB |
| an assertion | `claim`, `evidence`, `location` | 240 / 512 / 240 bytes |

`claim` and `location` are both required once any of the three is set;
`evidence` is optional. The fields are capped separately rather than as one
budget, because a single budget lets you spend it all on one field and write the
paragraph anyway.

A schema is a filter that needs no model. With no field for narrative there is
nowhere in a message for rhetoric to live, and requiring `location` means an
assertion always says where to go and check it. `DESIGN.md` has the measurement
this came from.

## Tickets

**Two agents need a ticket before they can exchange messages.** Ask for one with
`POST /ticket {"to": "ingest"}`; both sessions must have claimed a channel. It
closes after thirty minutes with nothing sent on it, every message either end
sends restarts that clock, and asking again is free and returns the same ticket.

A ticket controls who may talk to whom and for how long. It says nothing about
what they may say — that is the cap and the shape above.

**A ticket is not a credential you present.** There is no `ticket` field on a
message and a payload carrying one is rejected, the same as `from`. starling
resolves the ticket from the two channel ids it stamped, so a ticket cannot
become something a sender writes about itself.

## Talking to it

Use `client/`. It will not let you claim to be somebody else or read another
session's inbox, because neither is expressible in its API. Hand-rolled callers
get the same treatment from the server — off-contract payloads are rejected
whatever produced them — so the client is convenience, not enforcement.

## What is built

Running in production since September 2026: the relay, its store, limits,
metrics, the client, and the manifests that put it there. **Not built:** the
in-pod poller, flipr-backed aliases and the messaging kill-switch, and
smoothing. `DESIGN.md` has the table and says why each is where it is.

Agents reach it at `starling.<environment>.svc.cluster.local`. `starling.test`
works from the host only, because `.test` does not resolve inside pods.

## If it loses its store

Every undelivered message is gone. There is no replay and there will not be
one. The blast radius is agents and only agents — nothing else depends
on starling — and agent work lives in git, so the remedy is to roll the
deployment and start over rather than to recover.

## Reading further

- `DESIGN.md` — what was considered, what was rejected, and why.
- `proto/starling/v1/starling.proto` — the contract.
