# starling, built static and shipped on nothing.
#
# Two stages. The builder has the toolchain; the runtime has a binary, a CA
# bundle and a user. There is no shell in the final image, which is not a
# hardening pose -- it is that starling needs nothing else, so anything else
# would be there by accident rather than by decision.
#
# CGO_ENABLED=0 because bbolt and protobuf are pure Go and a static binary
# removes an entire class of "works in the builder, not in the runtime" that is
# tedious to diagnose from inside a pod.
FROM golang:1.26 AS build
WORKDIR /src

# Dependencies first, so a source edit does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# -trimpath so the binary does not carry build-host paths, and the version
# stamped from the build arg so `starling` in a pod can be traced to a commit.
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /starling .

# The tests run in the image build. A container that compiles but fails its own
# tests should not become an image: finding that out at deploy time means the
# cluster is where you learn it.
RUN go test ./...

FROM gcr.io/distroless/static-debian12:nonroot

# The CA bundle is the only thing starling needs from a distribution. It talks
# TLS to the API server, verifying against the CLUSTER CA that kubelet projects
# into the pod -- not against these roots -- but a missing bundle turns other
# outbound TLS into a confusing failure, and it costs nothing.
COPY --from=build /starling /starling

# nonroot in distroless is uid 65532. The manifest sets runAsUser explicitly as
# well, so this is belt and braces rather than the only thing standing between
# starling and root.
USER nonroot:nonroot

# The store lives on a PVC mounted here. The directory must exist and be
# writable by the running uid or startup fails at step one -- see OPERATION.md.
ENV STARLING_STORE=/var/lib/starling/starling.db
ENV STARLING_ADDR=:8099

EXPOSE 8099
ENTRYPOINT ["/starling"]
