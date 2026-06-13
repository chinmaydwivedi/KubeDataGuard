# KubeDataGuard MVP Runbook

## Prerequisites

- Docker runtime
- Docker Compose plugin
- Optional: kind and kubectl for operator testing
- Make

This machine was verified with Docker CLI + Colima rather than Docker Desktop.

Useful runtime checks:

```sh
colima status
docker info
docker compose version
```

Compose reports are written through this host mount:

```text
${HOME}/.kubedataguard/reports:/workspace/reports
```

## No-Docker Proof

```sh
make demo-local
```

This runs a deterministic local proof without Postgres, Redpanda, OpenSearch, Docker, or third-party Python clients.

Expected result:

```text
report_type: kubedataguard-local-proof
passed: true
before.existence.status: DriftDetected
before.aggregate.status: DriftDetected
repair.repaired: 5 with the default count
after.existence.status: Healthy
after.aggregate.status: Healthy
```

What it proves:

- missing derived records are detected
- stale derived records are detected
- aggregate count/revenue drift is detected
- repair reindexes candidate records from source truth
- post-repair existence and aggregate checks are clean
- reports preserve source watermark and stream offset range

## Kubernetes Operator

Build the Go operator:

```sh
make operator-build
```

Run Go unit tests:

```sh
make operator-test
```

The controller supports two modes:

- default synthetic mode for smoke testing status patches
- job-backed mode with `dataguard.io/checker-mode=job`

In job-backed mode, the operator creates Kubernetes Jobs for each `Invariant` generation or scheduled `checkIntervalSeconds` slot. The Job runs the Python checker, writes the full report to the worker report store, writes compact `status.json` and `repair-input.json` into a ConfigMap, and the operator patches `Invariant.status` from `status.json`.

If that status is `DriftDetected` and an auto-approved `RepairPolicy` exists, the operator creates a repair Job for the same check ID. The repair Job reads compact repair input from the checker ConfigMap, reindexes missing/stale records from Postgres into OpenSearch, verifies the invariant, writes a repair status ConfigMap, and the operator patches `Invariant.status` from the repair result.

Build and run the operator in-cluster:

```sh
make operator-image
kind load docker-image kubedataguard-operator:latest --name kubedataguard
kubectl apply -f k8s/operator.yaml
```

Synthetic drift annotation:

```yaml
metadata:
  annotations:
    dataguard.io/synthetic-drift-count: "3"
```

Expected behavior:

```text
0 or absent -> status.phase=Healthy
positive value -> status.phase=DriftDetected
```

The synthetic path was runtime-verified against a local kind cluster.

```sh
kind create cluster --name kubedataguard --wait 180s
kubectl apply -f k8s/crds
kubectl apply -f examples/commerce-consistency.yaml
go run ./cmd/dataguard-operator --metrics-bind-address=0 --health-probe-bind-address=:8083
```

In another shell:

```sh
kubectl get invariant
kubectl annotate invariant paid-orders-indexed dataguard.io/synthetic-drift-count=3 --overwrite
kubectl get invariant paid-orders-indexed -o jsonpath='{.status.phase} {.status.healthy} {.status.driftCount}'
kubectl annotate invariant paid-orders-indexed dataguard.io/synthetic-drift-count=0 --overwrite
```

## Job-Backed Operator Proof

Start the Compose data systems first:

```sh
make up
```

Build the checker image and load it into kind:

```sh
docker compose build dataguard
kind load docker-image kubedataguard-dataguard:latest --name kubedataguard
```

Apply CRDs and the example resources:

```sh
kubectl apply -f k8s/crds
kubectl apply -f examples/commerce-consistency.yaml
```

Run the operator locally against kind:

```sh
go run ./cmd/dataguard-operator --metrics-bind-address=0 --health-probe-bind-address=:8083
```

The example invariants already opt into job-backed mode:

```yaml
metadata:
  annotations:
    dataguard.io/checker-mode: job
    dataguard.io/checker-image: kubedataguard-dataguard:latest
```

Useful inspection commands:

```sh
kubectl get invariant -o wide
kubectl get jobs,pods,configmaps -l app.kubernetes.io/name=kubedataguard -o wide
kubectl get configmaps -l app.kubernetes.io/name=kubedataguard
kubectl get configmap <report-configmap-name> -o jsonpath='{.data.status\.json}'
kubectl get configmap <repair-configmap-name> -o jsonpath='{.data.status\.json}'
```

For scheduled checks, names include a time-slot check ID such as `g3-t29493510` instead of only `g3`.

To force a new checker generation after creating drift:

```sh
kubectl patch invariant paid-orders-indexed --type=merge -p '{"spec":{"maxLagSeconds":0}}'
kubectl patch invariant paid-orders-aggregate --type=merge -p '{"spec":{"maxLagSeconds":0}}'
```

To restore the example SLO after repair:

```sh
kubectl patch invariant paid-orders-indexed --type=merge -p '{"spec":{"maxLagSeconds":60,"severity":"critical"}}'
kubectl patch invariant paid-orders-aggregate --type=merge -p '{"spec":{"maxLagSeconds":60,"severity":"critical"}}'
```

Runtime-verified result:

```text
generation 5 after make demo-drift:
  paid-orders-indexed checker report: DriftDetected, drift_count=8
  operator created dataguard-repair-paid-orders-indexed-g5
  repair Job reindexed 8 missing documents
  paid-orders-indexed repair report: Healthy, drift_count=0

fresh aggregate generation after repair:
  paid-orders-aggregate: Healthy, drift_count=0

restored maxLagSeconds=60:
  both invariants: Healthy, drift_count=0
```

Bounded freshness was also runtime-verified:

```text
paid-orders-freshness:
  guarantee: boundedFreshness
  checkedRecords: 41
  phase: Healthy
  sourceLSN: present
  sourceWatermark: present
  streamOffsetStart: 0
  streamOffsetEnd: 250
```

Freshness drift and repair were runtime-verified through the operator:

```text
generation 4 after make drift-freshness:
  checker report: DriftDetected, drift_count=5
  operator selected RepairPolicy reindex-stale-paid-orders
  repair report: Healthy, drift_count=0, sloBreachCount=5

generation 5 after restoring maxLagSeconds=60:
  paid-orders-freshness: Healthy, checkedRecords=41, drift_count=0
```

## Failure Taxonomy Proofs

Checker failure should not be reported as data drift. This was verified by temporarily pointing an invariant at an unreachable Postgres endpoint:

```sh
kubectl annotate invariant paid-orders-indexed \
  dataguard.io/postgres-dsn='postgresql://dataguard:dataguard@host.docker.internal:5999/dataguard' \
  --overwrite
kubectl patch invariant paid-orders-indexed --type=merge -p '{"spec":{"severity":"warning"}}'
kubectl get invariant paid-orders-indexed -o wide
```

Expected phase:

```text
CheckFailed
```

Restore:

```sh
kubectl annotate invariant paid-orders-indexed dataguard.io/postgres-dsn-
kubectl patch invariant paid-orders-indexed --type=merge -p '{"spec":{"severity":"critical","maxLagSeconds":60}}'
```

Repair verification failure should not be reported as successful repair. This was verified with a temporary aggregate `RepairPolicy` using `reindex-records`; the repair Job completed, but verification still found aggregate drift, so the invariant moved to:

```text
RepairFailed
```

The temporary policy was removed after the proof, the data was repaired, and both invariants were restored to `Healthy`.

## Start The Stack

```sh
make up
```

Services:

- Postgres: `localhost:5432`
- Redpanda Kafka API: `localhost:19092`
- Redpanda Console: `http://localhost:8080`
- OpenSearch: `http://localhost:9200`

## Initialize

```sh
make init
```

This creates:

- Postgres tables
- Redpanda topic
- OpenSearch index

For a clean slate:

```sh
docker compose run --rm dataguard init --reset
```

## Happy Path Demo

```sh
make seed
make index
make check
make check-aggregate
make check-freshness
```

Expected result:

```text
healthy: true
drift_count: 0
```

## Drift Demo

```sh
make seed
make drift
make check
make check-aggregate
```

This generates orders and runs the indexer with:

```text
--skip-every-paid 5
```

Expected result:

```text
healthy: false
missing > 0
```

## Freshness Drift Demo

```sh
make seed
make index
make drift-freshness
make check-freshness
```

This mutates a few already-indexed paid orders so the OpenSearch `indexed_at` evidence is older than the Postgres `updated_at` source evidence.

Expected result:

```text
healthy: false
freshness_violations > 0
```

To repair and verify freshness directly:

```sh
make repair-freshness
```

Expected result after verification:

```text
healthy: true
drift_count: 0
freshness_breaches may remain as historical SLO evidence
```

Reports are written to:

```text
${HOME}/.kubedataguard/reports/latest.json
${HOME}/.kubedataguard/reports/drift-*.json
${HOME}/.kubedataguard/reports/drift-*.md
```

## Repair Demo

```sh
make repair
make check
```

Expected result after verification:

```text
healthy: true
drift_count: 0
```

Note: `make repair` reads `latest.json`. If you run `make check-aggregate` before repair, rerun `make check` first so `latest.json` contains the missing order IDs for the existence repair.

## Manual Commands

```sh
docker compose run --rm dataguard init --reset
docker compose run --rm dataguard generate --count 100 --paid-ratio 0.75
docker compose run --rm dataguard index --max-messages 100 --skip-every-paid 10
docker compose run --rm dataguard inject-freshness-drift --count 5
docker compose run --rm dataguard check --invariant existence --max-lag-seconds 0 --write-report
docker compose run --rm dataguard check --invariant aggregate --max-lag-seconds 0 --write-report
docker compose run --rm dataguard check --invariant freshness --max-lag-seconds 60 --write-report
docker compose run --rm dataguard repair --verify
docker compose run --rm dataguard repair --verify --verify-invariant freshness --max-lag-seconds 60
```

## Reading A Report

Reports are not only pass/fail output. Key fields:

- `status`: compact phase for automation, such as `Healthy` or `DriftDetected`
- `kubernetes_status`: compact payload copied into `Invariant.status` by the job-backed operator
- `guarantee`: the consistency claim checked by this run
- `observation_window`: the time and lag boundary used to decide which records are eligible
- `observation_window.source_lsn`: the Postgres WAL position observed during freshness checks when available
- `observation_window.stream_offset_start` and `stream_offset_end`: the event range associated with the check when available
- `counterexamples`: short proof objects for missing, stale, or aggregate drift
- `missing`, `stale`, `aggregate_mismatches`, `freshness_violations`: detailed current drift classes
- `freshness_breaches`: historical freshness SLO misses preserved for evidence after current repair convergence

## Troubleshooting

If OpenSearch fails to start, check host memory and Docker resource limits.

If Docker works but Compose cannot create a bind mount from `Documents`, use the existing report mount under `${HOME}/.kubedataguard/reports`. Colima may not be allowed to bind-mount protected macOS folders.

If Redpanda is not reachable, inspect:

```sh
docker compose logs redpanda-0
```

If reports show all records missing, confirm the indexer consumed from the same topic and group:

```sh
docker compose logs dataguard
```
