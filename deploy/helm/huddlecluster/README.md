# HuddleCluster Helm Chart

Deploys a HuddleCluster master (Deployment, or a StatefulSet when
`master.ha.enabled=true`) plus an agent DaemonSet, using the same
image as `deploy/docker/Dockerfile`.

## Verification status

- `helm lint` and `helm template` (including the HA+TLS combined
  render path, and the TLS-without-`existingSecret` validation guard)
  pass cleanly.
- **Deployed and verified against a real cluster (Docker Desktop
  Kubernetes, v1.36.1, single-replica master + agent DaemonSet, no
  TLS/auth):** `helm install` succeeded, both Pods reached
  `Running`/`1/1 Ready` (readiness/liveness probes against
  `/v1/health` passing), the PVC bound, and — most importantly — the
  agent Pod actually joined the master over real cluster networking
  and was heartbeating (`GET /v1/nodes` showed `status: alive`,
  `heartbeat_count` incrementing) when queried through
  `kubectl port-forward`.
- **Not yet verified:** HA mode (`master.ha.enabled=true`,
  StatefulSet + peer discovery), TLS (`master.tls.enabled=true`),
  and the `--features`/`master.features` wiring, against a real
  cluster. These render correctly (`helm template`) and the
  underlying mechanisms are each independently tested elsewhere
  (HA and TLS both have real end-to-end tests outside Kubernetes —
  see `tests/test_cluster_ha.py`, `tests/test_master_tls.py`), but the
  *combination* of "these features" + "running inside a real
  StatefulSet/Kubernetes Secret mount" hasn't specifically been
  exercised yet. If you use these, keep an eye on `kubectl logs` and
  `kubectl describe pod` the first time.

## Install

```bash
# Build and push your image first (see deploy/docker/Dockerfile)
helm install my-huddlecluster deploy/helm/huddlecluster \
  --set image.repository=your-registry/huddlecluster \
  --set image.tag=4.13.0
```

For a local trial against Docker Desktop's built-in Kubernetes (no
registry push needed — this is exactly the path that was verified
above):

```bash
docker build -f deploy/docker/Dockerfile -t huddlecluster:4.13.0 .
kubectl create namespace huddle-test
helm install my-huddlecluster deploy/helm/huddlecluster \
  --namespace huddle-test \
  --set image.repository=huddlecluster \
  --set image.tag=4.13.0 \
  --set image.pullPolicy=IfNotPresent
kubectl get pods -n huddle-test
kubectl port-forward -n huddle-test svc/my-huddlecluster-master 7070:7070
# in another terminal: curl http://localhost:7070/v1/nodes
```

Note: `image.pullPolicy=Never` looks like the "correct" choice for a
purely local image, but Docker Desktop's containerd-backed Kubernetes
was observed rejecting it (`ErrImageNeverPull`) even when the image
was genuinely present locally — `IfNotPresent` worked correctly.

With a values file (recommended for anything beyond a quick trial):

```bash
helm install my-huddlecluster deploy/helm/huddlecluster -f my-values.yaml
```

See `values.yaml` for every option, with inline comments. The
highlights:

```yaml
master:
  tls:
    enabled: true
    existingSecret: my-tls-cert   # kubectl create secret tls my-tls-cert --cert=... --key=...
  auth:
    apiKeys:
      - key: "a-real-secret-not-this"
        role: admin
  features:
    circuit_breaker: { trip_threshold: 0.5 }
    autoscaler: { min_nodes: 3, max_nodes: 10 }
  ha:
    enabled: true
    replicaCount: 3
```

## HA mode caveat

`master.ha.enabled=true` runs a StatefulSet with `replicaCount`
independent master Pods, each computing its own `node_id`/`peers` at
container startup from `$HOSTNAME` (which StatefulSet sets to
`<name>-<ordinal>`) and the headless Service's per-Pod DNS names. This
is the standard Kubernetes pattern for this kind of peer discovery,
and it renders correctly, but — per "Verification status" above — it
hasn't specifically been deployed and watched elect a leader inside a
real cluster yet. Worth doing before trusting it in production.

## Uninstall

```bash
helm uninstall my-huddlecluster
```

PersistentVolumeClaims aren't deleted automatically by Helm (this is
standard Helm/Kubernetes behavior, not chart-specific) — delete them
separately if you want the state gone too:

```bash
kubectl delete pvc -l app.kubernetes.io/instance=my-huddlecluster
```