# operating starling

for the person who has been woken up. start here, not at `DESIGN.md`.

starling runs in a pod, in the namespace named for its environment. it
cannot run outside one by design: it needs the cluster CA to verify caller
identity and refuses to start without it.

one caveat that affects the agents it serves: `.test` does not resolve
inside pods, so agents use `starling.<environment>.svc.cluster.local` and
`starling.test` works from the host only. the fix is a CoreDNS rewrite,
which is cluster-wide rather than starling's.

## is it up

    curl -s http://starling.test/health | jq
    {"status":"healthy","channels":2,"detail":"","version":"98d36f9"}

| status | meaning | what to do |
|---|---|---|
| `healthy` | the store opened and answered | nothing |
| `down` | the store is unreadable | the store is gone, below |
| no answer | the process is not running, or the pod is not scheduled | `kubectl -n <env> get pods -l app.kubernetes.io/name=starling` |

the endpoint reads the store rather than returning 200, so healthy means
something. it does not check the api server on purpose: being unable to
verify identity is degraded, not down, and a health check that goes red
when kubernetes is briefly busy causes a restart that fixes nothing.

`version` is the short commit, stamped by the Dockerfile's build arg. a
`version` of `dev` cannot be traced to a commit, and an audit should
treat that as a failure.

## condition: everything is being refused

work out which refusal it is first. they have different causes and only
one of them is starling's fault.

verify, and what each means:

| refusal | means | what you can do |
|---|---|---|
| `401 token not accepted` | the caller's token is not valid, usually a pod with no projected token or a caller outside the cluster | check the pod has one under `/var/run/secrets/kubernetes.io/serviceaccount/` |
| `503 cannot verify identity right now` | starling cannot reach the api server, so it refuses everyone | look at the api server, not at starling; `starling_auth_failures_total{reason="unavailable"}` |
| `tokenreview refused with http 403` | starling's own RBAC is wrong: it needs `create` on `tokenreviews` | fix the deployment; restarting cannot help |
| `429 slow down` | one pod is over its rate limit, usually an agent looping | `starling_requests_throttled_total` by sender, whose label the agent cannot choose |
| `503 starling is at capacity` | 64 concurrent short requests | something is hammering it; `/health` and `/metrics` are not behind the cap |

the implications: refusing everyone when identity cannot be verified is
the one thing this service exists to do. it is correct behaviour, and the
fix is never a restart.

## condition: an agent is not receiving mail

verify in this order, because each rules out the next:

    starling_heartbeat_age_seconds       by pod: above a few minutes, the
                                        poller is dead and the agent is
                                        unreachable while looking healthy
    starling_inbox_depth                 by pod: climbing with a stale
                                        heartbeat confirms the poller;
                                        zero means nothing was sent
    starling_messages_dropped_total      by sender and reason

what the reasons mean:

- `no_live_channel`: the recipient never claimed, or has restarted
- `body_too_long`: somebody sent a diff instead of a reference. the cap is
  1 KiB, set from measured traffic of 2.5 to 4.5 KB, and the refusal says
  what to do instead: push it and send the ref
- `two_forms`, `no_claim`, `no_location`, `claim_too_long`,
  `evidence_too_long`, `location_too_long`: a caller has not been updated
  to the message shape, and the refusal names the field
- `no_ticket`: the two sessions have never been introduced, so the sender
  asks for a ticket first
- `ticket_expired`: they were, and nobody said anything for thirty
  minutes. asking again reopens it

a session that never claimed holds no channel and is refused with `409
stale or missing channel`. that is the design: it stops a new occupant of
a recycled pod draining its predecessor's mail.

the implications: none of the shape refusals mean anything is broken, and
a caller stuck on `no_ticket` in a loop is one that has not learned to
ask.

## condition: two agents cannot reach each other

verify: both sessions have claimed a channel, or the answer is `404 no
live session` for the peer and `409 claim a channel` for the caller.

what you can do: the sender asks for a ticket.

    curl -s -H "Authorization: Bearer $TOKEN" \
      -d '{"to":"ingest"}' http://starling.test/ticket

`starling_tickets_open` is how many pairs can talk now, and
`starling_tickets_issued_total` is by requester.

open climbing while `starling_messages_accepted_total` stays flat means
agents ask for channels and then do not use them, which is worth asking
what prompts them.

the implications: tickets die with either session, so a pod restart ends
every conversation it was part of and a rollout is followed by a burst of
`no_ticket` while agents reintroduce themselves.

## condition: the store is gone

verify: `/health` says `down` with the store unreadable.

what you can do: replace it, not recover it.

    kubectl -n <environment> rollout restart deploy/starling

the implications: every undelivered message is gone and there is no
replay. that is affordable because nothing else depends on starling and
agent work lives in git; what is lost is in-flight conversation, which
you recreate by asking again.

do not build a restore path. if agents are incoherent, roll the
deployment and start over: an agent population is re-creatable by
construction, and a confused one is cheaper to replace than to repair.

## how to build

    game check          # gofmt, vet, the tests
    game build          # starling into bin/, stamped with the commit

## how to deploy and bounce

    bin/deploy.sh <environment>
    kubectl -n <environment> scale deploy/starling --replicas=1
    kubectl -n <environment> scale deploy/starling --replicas=0
    kubectl -n <environment> logs deploy/starling

starling is one deployment, so scaling it is starting and stopping it.

the environment is a file under `kube/environments/` naming the cluster,
the context and the edge port; `local.env` is the example to copy.

the script states its target and refuses a mismatch or a dirty tree.

then it builds the image with the commit hash as tag and version, imports
it, substitutes `${COMMIT}` and `${ENV}` into the manifests, applies them,
waits for the rollout, and reads `/health` back by name, failing unless it
reports the hash.

the manifest carries placeholders and never a literal hash. before the
script, the hash was typed by hand, which is how a manifest came to pin
an old commit while the tree moved on.

## what it depends on

- the kubernetes api server, for TokenReview. unreachable means starling
  refuses everyone, loudly, by design
- its own PVC, per the condition above
- flipr, not yet: when aliases and the kill-switch are wired, flipr being
  down will mean starling is down. it is not wired today

## where to look

- the grafana board, if the environment has one. spike panel first
- `kubectl -n <environment> logs deploy/starling`, json lines
- `curl http://starling.test/api` for the contract
