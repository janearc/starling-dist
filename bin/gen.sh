#!/bin/sh
# regenerate wire code from the protos.
#
# gen-freshness rule: run this, and the tree must be clean afterwards. A diff
# after generation means the committed code and the contract disagree, and the
# contract wins.
#
# THE PIN IS ENFORCED, NOT SUGGESTED, and the reasoning is flipr's -- copied
# deliberately rather than reinvented. protoc-gen-go stamps its own version into
# every file it writes, so a plugin a minor version off rewrites the whole tree
# for reasons that have nothing to do with the contract, and gen-freshness then
# fails in a way that looks like a schema change and is not.
#
# Installed to a REPO-LOCAL .tools rather than $HOME/go/bin because other repos
# here pin different versions of the same plugin, and a shared $HOME/go/bin
# makes whichever ran last the winner. A `command -v` guard pins nothing: it
# accepts whatever happens to be installed, which is the failure this avoids.
set -e
cd "$(dirname "$0")/.."

# Matches google.golang.org/protobuf in go.mod. Move them together.
PROTOC_GEN_GO_VERSION=v1.36.12
TOOLS="$PWD/.tools"

if [ "$("$TOOLS/protoc-gen-go" --version 2>/dev/null)" != "protoc-gen-go $PROTOC_GEN_GO_VERSION" ]; then
  echo "installing protoc-gen-go $PROTOC_GEN_GO_VERSION into .tools"
  GOPRIVATE=github.com/janearc GOBIN="$TOOLS" \
    go install "google.golang.org/protobuf/cmd/protoc-gen-go@$PROTOC_GEN_GO_VERSION"
fi

export PATH="$TOOLS:$PATH"
buf lint
buf generate

# The descriptor served at /api. The estate's convention is that /api returns a
# compiled FileDescriptorSet as application/octet-stream, not prose -- flipr and
# hm both do this -- so a consumer can read the schema rather than be told about
# it. Committed, because it is served by an embed and the binary must match the
# protos it was built from.
buf build -o descriptor.binpb
