# HuddleCluster Helm Chart

Deploys a HuddleCluster master (Deployment, or a StatefulSet when
`master.ha.enabled=true`) plus an agent DaemonSet, using the same
image as `deploy/docker/Dockerfile`.

## Verify before real use

This chart was built and reviewed carefully, but **`helm` wasn't
available in the environment it was developed in** (network-restricted
sandbox — binary downloads from `get.helm.sh`/GitHub releases were
blocked). It was validated by:

- Manually checking every template's `{{`/`}}` brace balance
- Manually rendering the default-values path by hand and confirming
  the result is valid YAML
- Extracting the embedded shell wrapper scripts (all conditional
  branches combined) and syntax-checking them with `sh -n`
- Extracting the embedded Python snippets and syntax-checking them
  with `ast.parse`

That's meaningfully more scrutiny than an untested chart, but it is
**not** a substitute for actually running:

```bash
helm lint deploy/helm/huddlecluster
helm template test-release deploy/helm/huddlecluster | kubectl apply --dry-run=client -f -
```

on a machine with `helm` installed, before deploying this anywhere
real. Please do that first — if something doesn't render correctly,
it's most likely an indentation issue in one of the `templates/*.yaml`
files, since that's the class of bug this kind of manual review is
weakest at catching (Go template whitespace control — `{{-`/`-}}` — is
easy to get subtly wrong and hard to verify without actually running
the templating engine).

## Install

```bash
# Build and push your image first (see deploy/docker/Dockerfile)
helm install my-huddlecluster deploy/helm/huddlecluster \
  --set image.repository=your-registry/huddlecluster \
  --set image.tag=4.13.0
```

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
but it's the most complex part of this chart and the part most worth
double-checking with `helm template` + a real cluster before trusting
it — see the wrapper script in `templates/master.yaml`.

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