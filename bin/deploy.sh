#!/bin/sh
# deploy.sh <environment> -- build starling at the committed tree, import it
# into that environment's cluster, and roll it out. The ONE way starling
# reaches a cluster. Before this script existed the image was built by hand, imported by hand, and its
# hash typed into the manifest, which is how the manifest came to pin a
# commit that the tree had moved past.
#
# THE ENVIRONMENT IS THE ONLY INPUT: kube/environments/<environment>.env names
# the cluster, the kubectl context and the edge port, and the script states
# that target and refuses if the context is not in the kubeconfig or does not
# agree with the cluster. The image tag is the commit hash,
# substituted into the manifest's ${COMMIT} placeholder at apply and stamped
# into the binary as its version, so /health reports the same hash the pod
# runs. Refuses a dirty tree: a hash over uncommitted changes lies.
set -eu

REPO="$(cd "$(dirname "$0")/.." && pwd)"
ENVNAME="${1:-}"
if [ -z "$ENVNAME" ]; then
  echo "usage: bin/deploy.sh <environment>   (one of: $(ls "$REPO/kube/environments" | sed 's/\.env$//' | tr '\n' ' '))" >&2
  exit 2
fi
ENVFILE="$REPO/kube/environments/$ENVNAME.env"
[ -f "$ENVFILE" ] || { echo "deploy: no such environment: $ENVFILE" >&2; exit 2; }
# shellcheck disable=SC1090
. "$ENVFILE"
for v in ENV CLUSTER CTX EDGE_PORT; do
  eval "val=\${$v:-}"
  [ -n "$val" ] || { echo "deploy: $ENVFILE does not set $v" >&2; exit 2; }
done

cd "$REPO"
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "deploy: the tree is dirty. Commit first: the image tag is the commit hash." >&2
  exit 1
fi

echo "== target: environment $ENV, cluster $CLUSTER, context $CTX, edge port $EDGE_PORT =="
kubectl config get-contexts -o name | grep -qx "$CTX" || { echo "deploy: context $CTX is not in the kubeconfig; refusing" >&2; exit 1; }
[ "$CTX" = "k3d-$CLUSTER" ] || { echo "deploy: $ENVFILE names context $CTX for cluster $CLUSTER; k3d derives k3d-$CLUSTER; refusing" >&2; exit 1; }

SHA="$(git rev-parse --short=7 HEAD)"
IMG="starling:$SHA"

echo "== building $IMG (the tests run inside the build; a failing suite refuses the image) =="
docker build --build-arg VERSION="$SHA" -t "$IMG" "$REPO"

echo "== importing into $CLUSTER =="
k3d image import "$IMG" -c "$CLUSTER"

echo "== applying manifests to $CTX =="
# every manifest carries ${ENV} for its namespace, and the deployment also
# carries ${COMMIT} for its image
for f in 00-rbac 10-pvc 20-deployment 30-service 40-ingressroute; do
  sed -e "s/\${COMMIT}/$SHA/g" -e "s/\${ENV}/$ENV/g" "$REPO/kube/$f.yaml" |
    kubectl --context "$CTX" apply -f -
done

echo "== waiting for rollout =="
kubectl --context "$CTX" -n "$ENV" rollout status deployment/starling --timeout=180s

echo "== verify: the service answers by NAME through this environment's edge with the version this tree built =="
sleep 1
got="$(curl -s --max-time 5 "http://starling.test:${EDGE_PORT}/health" || true)"
echo "$got"
printf '%s' "$got" | grep -q "\"version\":\"$SHA\"" || { echo "deploy: starling.test:$EDGE_PORT does not report $SHA" >&2; exit 1; }
echo "deployed $IMG to $ENV"
