# KubeDataGuard

## One-Line Idea

KubeDataGuard is a Kubernetes-native data consistency SLO operator for checking, explaining, and repairing drift between source-of-truth systems and derived data stores.

## Current Maturity

This repository is a research-grade vertical-slice MVP, not a production-ready general-purpose data platform yet.

What is implemented and verified:

- Postgres -> Redpanda/Kafka -> OpenSearch demo pipeline.
- Hardcoded commerce invariants for existence, aggregate consistency, and bounded freshness.
- A first generic query invariant for Postgres source queries and OpenSearch JSON target queries.
- A Go/controller-runtime operator that schedules checker and repair Jobs.
- Compact Kubernetes status handoff through ConfigMaps.
- Full JSON/Markdown reports written to the worker report store and referenced from status.
- Source-of-truth reindex repair for missing/stale/freshness drift in the commerce demo.

What is intentionally not solved yet:

- Arbitrary databases and target stores beyond the first Postgres/OpenSearch generic path.
- Large-scale DBLog-style chunked, resumable scans.
- Production object-store report persistence such as S3/GCS/MinIO.
- Kubernetes Secret-backed connection loading from `DataSource` resources.
- General repair strategies such as Kafka replay, cache invalidation, and analytics backfill.

## Research-Informed Novelty Wedge

The ecosystem already has excellent infrastructure operators and data-quality tools. CloudNativePG manages PostgreSQL lifecycle and failover. Strimzi manages Kafka. KubeDB and KubeBlocks manage many databases and day-2 operations. Great Expectations and Soda validate data quality. OpenLineage records lineage metadata.

KubeDataGuard should not compete with any of those directly.

Its sharper contribution is:

```text
Kubernetes-native reconciliation of cross-system derived-data correctness.
```

That means the project should become the missing layer between infrastructure health and data quality:

- Infrastructure operators ask: is the database, stream, or index healthy?
- Data-quality tools ask: does a dataset satisfy expectations?
- KubeDataGuard asks: does this derived view still correctly represent that source of truth within a declared SLO?

The most novel version of this project is not "a checker." It is a data-correctness control plane with:

- `DataSource`, `DerivedView`, `Invariant`, and `RepairPolicy` CRDs
- lineage-bound invariants that know how data is supposed to move
- `Invariant.status` as the compact Kubernetes truth
- durable drift reports as evidence
- repair provenance showing exactly what was changed and why
- checker failure states that are distinct from data-drift states
- OpenTelemetry traces for check and repair cycles
- optional OpenLineage emission so the data ecosystem can see the same lineage graph

## Demo Story

The first demo should run in a few commands:

```sh
make demo-local # no-Docker proof: drift -> report -> repair -> clean verification
make up       # starts Postgres, Redpanda, Redpanda Console, and OpenSearch
make seed     # resets the demo and inserts 50 orders, most marked paid
make drift    # commits some paid events without indexing them
make check    # reports missing paid orders with order IDs and source details
make repair   # reindexes missing/stale orders from Postgres and verifies
make check    # produces a clean report
make demo-freshness-drift # proves bounded-freshness drift with source/target timestamps
```

That is the project in miniature: a derived view becomes untrustworthy, KubeDataGuard proves exactly how, then repairs and verifies it.

The `demo-local` target remains useful as a fast proof path without external services. The full Compose path has also been runtime-verified with Docker CLI + Colima, Postgres, Redpanda, and OpenSearch.

## First Principles

Modern applications rarely keep data in one place. A single user action can write to Postgres, emit an event to Kafka, update Redis, index a document into Elasticsearch, and update analytics tables in ClickHouse. Each copy exists for a performance or workflow reason, but each copy can become wrong.

The fundamental problem:

```text
If the same business fact exists in multiple systems, how do we know those systems still agree?
```

Traditional monitoring says:

```text
Is Postgres up?
Is Kafka up?
Is Elasticsearch responding?
Is latency acceptable?
```

KubeDataGuard asks a deeper question:

```text
Is the data correct enough for the business promise we made?
```

The project is built around four ideas:

1. Source data
   - The first durable record of a business fact.
   - Example: `orders` table in Postgres.

2. Derived data
   - A transformed, copied, cached, indexed, or aggregated view.
   - Example: `orders` index in Elasticsearch.

3. Invariant
   - A rule that must remain true.
   - Example: every paid order in Postgres must appear in the search index within 60 seconds.

4. Repair
   - A controlled action that makes a bad derived view trustworthy again.
   - Example: replay Kafka events, reindex missing records, invalidate Redis keys, or run a backfill job.

## DDIA Concepts

This project is strongly connected to:

- Replication and replication lag
- Derived data and denormalized views
- Stream processing
- Batch backfills
- Encoding and evolution
- Transactions and the absence of distributed atomicity
- Fault tolerance
- Observability and operability

The key DDIA insight is that many useful data systems are eventually consistent, but "eventually" is not an engineering plan. KubeDataGuard turns eventual consistency into explicit, measurable SLOs.

## Academic Foundation

The research-backed version of KubeDataGuard should be more formal than a table-diff tool.

The consistency-model papers push the project toward named guarantees instead of generic "drift." An invariant should declare which claim it protects:

- Existence: every qualifying source record appears in the derived view.
- Equality: selected source fields and target fields agree.
- Aggregate: counts, sums, or grouped totals agree inside a tolerance.
- Freshness: the derived output is no older than the allowed lag window.
- Client-centric consistency: read-your-writes and monotonic-read claims hold for a traced client/session.
- Causal consistency: if fact B depends on fact A, the derived system must not expose B without A.

DBLog is the most important systems paper for the MVP. It shows why snapshot-plus-log CDC needs watermarks, chunk boundaries, and resumable scans. That should change the design of KubeDataGuard reports and status: checks should eventually record `sourceWatermark`, `eventOffset`, `snapshotWindow`, `consumerGroup`, `lagSeconds`, and `replayScope`. Without those fields, a repair can say "I fixed rows" but cannot prove which part of history was actually covered.

The polyglot-persistence papers sharpen the novelty claim. The problem is not only that Postgres, Kafka, OpenSearch, Redis, and ClickHouse exist. The problem is that business truth leaks across them, and each store has different query semantics, failure modes, indexes, and freshness behavior. KubeDataGuard's CRDs are valuable only if they can describe those heterogeneous links without hiding the evidence.

Elle changes the reporting philosophy. A good report should not say only:

```text
Invariant failed: 12 missing records
```

It should give a counterexample:

```text
order-123 was committed in Postgres at source version 981,
published to topic order-events at offset 4412,
but was absent from OpenSearch after the 60 second lag window.
```

GITER and the Kubernetes operator model reinforce the final control-plane shape:

```text
spec = declared consistency promise
status = compact observed truth
report = durable evidence
repair = controlled reconciliation action
```

DCaaS is the conceptual ancestor: data correctness policy should live outside application code. KubeDataGuard updates that idea for Kubernetes, CDC pipelines, polyglot stores, and SLO-driven repair.

## Kubernetes Angle

Kubernetes already has a powerful reconciliation model:

```text
desired state -> controller observes actual state -> controller acts -> status is updated
```

KubeDataGuard applies this model to data correctness:

```text
desired consistency -> operator observes actual data -> operator detects drift -> operator repairs or reports
```

Instead of only declaring deployments and services, teams declare data contracts as Kubernetes resources.

## Implementation Language Direction

Use a hybrid architecture:

```text
Go/controller-runtime operator
  |
  |-- watches DataSource, DerivedView, Invariant, RepairPolicy
  |-- schedules checker/repair Jobs
  |-- writes compact Kubernetes status
  |
Python checker/repair workers
  |
  |-- connect to Postgres, Kafka/Redpanda, OpenSearch, Redis, ClickHouse
  |-- run invariant logic
  |-- emit verbose evidence reports
  |-- emit compact status payloads for the operator
```

Python is the right language for the local MVP and data-system integrations because the iteration loop is fast, client libraries are good, and invariant logic is easier to evolve. Go is the right language for the long-term Kubernetes operator because controller-runtime, CRD status handling, leader election, watches, work queues, and Kubernetes API machinery are strongest there.

The important boundary is the report contract. Python workers now emit:

- verbose JSON/Markdown evidence reports
- compact `kubernetes_status` payloads shaped like `Invariant.status`

That means the Go controller can remain thin: reconcile resources, run workers, store report references, and update status.

Current closed-loop operator path:

- `cmd/dataguard-operator`: controller-runtime manager entrypoint
- `internal/controller`: unstructured `Invariant` reconciler
- watches `dataguard.io/v1alpha1` `Invariant` resources
- supports `dataguard.io/checker-mode=job`
- creates checker Jobs per `Invariant` generation or scheduled check interval
- runs the Python checker against Postgres, Redpanda/Kafka, and OpenSearch
- stores compact `status.json` and repair input in a report ConfigMap
- writes full JSON/Markdown reports to the worker report store and keeps only the report reference in status
- if drift is detected, finds an auto-approved `RepairPolicy`
- creates a repair Job for allowed `reindex-records` repair
- repair Job reindexes from Postgres into OpenSearch and verifies the invariant
- copies compact checker or repair `status.json` into `Invariant.status`
- still keeps synthetic mode available for controller smoke tests

This proves the Kubernetes-native control loop: the operator reconciles a declared data SLO, a Job performs the data check, a policy-approved repair Job fixes drift, and the CRD status reflects the verified consistency state.

## Core Custom Resources

### DataSource

Describes a system that stores data.

```yaml
apiVersion: dataguard.io/v1alpha1
kind: DataSource
metadata:
  name: orders-postgres
spec:
  type: postgres
  connectionSecret: orders-postgres-secret
  database: commerce
```

### DerivedView

Describes how one data store is derived from another.

```yaml
apiVersion: dataguard.io/v1alpha1
kind: DerivedView
metadata:
  name: orders-search-index
spec:
  sourceRef: orders-postgres
  target:
    type: elasticsearch
    connectionSecret: search-secret
    index: orders
  pipeline:
    type: kafka
    topic: order-events
```

### Invariant

Describes the rule that must hold.

```yaml
apiVersion: dataguard.io/v1alpha1
kind: Invariant
metadata:
  name: paid-orders-indexed
spec:
  derivedViewRef: orders-search-index
  type: existence
  checkIntervalSeconds: 300
  maxLagSeconds: 60
  sourceQuery: |
    select id from orders where status = 'paid'
  targetQuery: |
    select id from orders_search where status = 'paid'
```

The first executable generic query invariant is intentionally narrower:

```yaml
apiVersion: dataguard.io/v1alpha1
kind: Invariant
metadata:
  name: paid-orders-query-check
spec:
  derivedViewRef: orders-search-index
  type: query
  checkIntervalSeconds: 300
  maxLagSeconds: 60
  keyField: id
  compareFields:
    - status
    - amount_cents
    - currency
    - version
  sourceQuery: |
    select id, status, amount_cents, currency, version, updated_at
    from orders
    where status = 'paid'
  targetQuery: |
    {"query":{"term":{"status":"paid"}},"size":10000}
```

### RepairPolicy

Describes what may happen when an invariant fails.

```yaml
apiVersion: dataguard.io/v1alpha1
kind: RepairPolicy
metadata:
  name: reindex-missing-paid-orders
spec:
  invariantRef: paid-orders-indexed
  approvalRequired: false
  actions:
    - type: reindex-records
      batchSize: 500
```

## Reconciliation Loop

The mature operator loop:

```text
1. Watch DataSource, DerivedView, Invariant, and RepairPolicy objects.
2. Load connection details from Kubernetes Secrets.
3. Run source and target checks.
4. Compare results.
5. Classify drift:
   - missing data
   - stale data
   - duplicate data
   - aggregate mismatch
   - schema incompatibility
6. Update Invariant status.
7. Emit metrics and traces.
8. Trigger repair if policy allows it.
9. Verify repair.
10. Record the outcome.
```

The implemented checker loop today is:

```text
Invariant CRD
  |
  |-- Go operator sees dataguard.io/checker-mode=job
  |-- computes checkID from generation or checkIntervalSeconds time slot
  |-- creates dataguard-check-<invariant>-<checkID> Job
  |-- Python checker connects to Postgres, Redpanda/Kafka, and OpenSearch
  |-- Python checker writes full report to REPORT_DIR
  |-- Python checker writes compact status.json and repair-input.json into a ConfigMap
  |-- Go operator copies status.json into Invariant.status
  |-- Go operator requeues scheduled invariants for the next check interval
```

This path has been runtime-verified on kind while the data systems run through Docker Compose.

## Invariant Evidence Model

An invariant is not just a query pair. The durable model should be:

```text
Invariant =
  scope
  + consistency claim
  + observation window
  + allowed lag
  + evidence
  + repair policy
```

Evidence should be concrete enough for a human or controller to reproduce the finding:

- source object ids and versions
- target object ids and versions
- source transaction timestamp or LSN when available
- stream topic, partition, and offset when available
- source snapshot window or CDC watermark
- observed lag in seconds
- comparison outcome: missing, stale, duplicate, aggregate mismatch, or unknown
- checker result: complete, partial, failed, or timed out
- repair provenance: action, input ids, output ids, and verification result

Kubernetes status should stay compact:

```text
Healthy
DriftDetected
CheckFailed
Repairing
RepairFailed
Unknown
```

The full JSON/Markdown report should carry the counterexamples and replay boundaries.

## MVP

Build the smallest convincing version:

```text
Postgres -> Kafka/Redpanda -> Elasticsearch/OpenSearch
```

Use case:

```text
Every paid order in Postgres must exist in the search index within 60 seconds.
```

Components:

- Demo API that writes orders to Postgres and emits order events
- Indexer worker that consumes events and writes Elasticsearch/OpenSearch documents
- KubeDataGuard checker that compares Postgres and the index
- Repair worker that reindexes missing records
- Docker Compose for local development
- Kubernetes CRDs after the Compose demo works

## Implemented Local MVP

This folder now contains a runnable local MVP skeleton.

Files:

- `docker-compose.yml`: Postgres, Redpanda, Redpanda Console, OpenSearch, and the `dataguard` CLI container
- `Dockerfile`: Python CLI image
- `requirements.txt`: Postgres, Kafka, and OpenSearch client libraries
- `src/dataguard`: local MVP implementation
- `src/dataguard/local_demo.py`: no-Docker proof of drift detection, aggregate mismatch, repair, and clean verification
- `tests`: pure invariant unit tests
- `docs/ARCHITECTURE.md`: deeper system design
- `docs/RUNBOOK.md`: commands for running the demo
- `docs/KNOWLEDGE_BASE.md`: official docs consulted
- `k8s/crds`: first Kubernetes CRD sketches
- `examples/commerce-consistency.yaml`: example declarative consistency policy
- `cmd/dataguard-operator`: Go/controller-runtime operator entrypoint
- `internal/controller`: synthetic and job-backed `Invariant` reconciliation
- `Dockerfile.operator`: Go operator image
- `k8s/operator.yaml`: in-cluster operator Deployment/RBAC sketch

Implemented commands:

```sh
python -m dataguard.cli init
python -m dataguard.cli generate
python -m dataguard.cli index
python -m dataguard.cli inject-freshness-drift
python -m dataguard.cli check --invariant existence
python -m dataguard.cli check --invariant aggregate
python -m dataguard.cli check --invariant freshness
python -m dataguard.cli check --invariant query --source-query "..." --target-query "..."
python -m dataguard.cli check-job --invariant existence
python -m dataguard.cli repair
python -m dataguard.cli repair-job
python -m dataguard.cli demo-local
```

With Docker Compose:

```sh
make demo-local
make up
make seed
make drift
make drift-freshness
make check
make check-aggregate
make check-freshness
make repair
make repair-freshness
make check
```

Verified Compose behavior on this machine:

```text
make demo-drift
  generated 50 orders
  indexed 42 events
  deliberately skipped 8 paid orders
  existence invariant: DriftDetected, drift_count=8
  aggregate invariant: DriftDetected, source count=41, target count=33

make demo-repair
  repaired 8 missing orders
  post-repair existence invariant: Healthy
  post-repair aggregate invariant: Healthy
```

Verified Kubernetes checker behavior on this machine:

```text
kind + local Go operator + checker Jobs + Compose data systems

generation 2 after deliberate drift:
  paid-orders-indexed: DriftDetected, drift_count=8
  paid-orders-aggregate: DriftDetected, drift_count=2

generation 3 after repair:
  paid-orders-indexed: Healthy, drift_count=0
  paid-orders-aggregate: Healthy, drift_count=0

generation 4 after restoring the 60-second SLO spec:
  paid-orders-indexed: Healthy, drift_count=0
  paid-orders-aggregate: Healthy, drift_count=0
```

Verified Kubernetes repair behavior on this machine:

```text
generation 5 after deliberate drift:
  checker Job found 8 missing paid orders
  operator found RepairPolicy reindex-missing-paid-orders
  operator created dataguard-repair-paid-orders-indexed-g5
  repair Job reindexed 8 missing OpenSearch documents from Postgres
  repair Job verified paid-orders-indexed Healthy

fresh aggregate generation after repair:
  paid-orders-aggregate: Healthy, drift_count=0

restored 60-second SLO generations:
  both invariants: Healthy, drift_count=0
```

Verified failure taxonomy behavior on this machine:

```text
checker failure:
  unreachable Postgres endpoint -> checker Job failed -> Invariant phase CheckFailed
  restored endpoint -> next generation Healthy

repair verification failure:
  aggregate drift + temporary reindex RepairPolicy -> repair Job completed
  verification still found aggregate mismatch -> Invariant phase RepairFailed
  policy removed + data repaired -> next generation Healthy
```

Verified bounded freshness behavior on this machine:

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

Verified freshness drift controls in the local path:

```text
make demo-freshness-drift
  indexed paid orders first
  injected 5 target documents whose indexed_at was older than source updated_at
  freshness invariant: DriftDetected, drift_count=5

make repair-freshness
  reindexed the freshness violation candidates from Postgres
  post-repair freshness invariant: Healthy
  report preserved historical freshness SLO breach evidence separately
```

Verified freshness drift repair through the kind operator:

```text
paid-orders-freshness generation 4:
  checker Job: DriftDetected, drift_count=5
  RepairPolicy: reindex-stale-paid-orders
  repair Job: Healthy, drift_count=0, sloBreachCount=5

paid-orders-freshness generation 5 after restoring maxLagSeconds=60:
  Healthy, checkedRecords=41, drift_count=0, sloBreachCount=5
```

Compose reports are written to:

```text
${HOME}/.kubedataguard/reports
```

Current no-Docker proof:

```text
report_type: kubedataguard-local-proof
before existence status: DriftDetected
before aggregate status: DriftDetected
repair action: reindex-records-from-source
after existence status: Healthy
after aggregate status: Healthy
```

Current report shape:

- `status`: compact control-plane phase such as `Healthy` or `DriftDetected`
- `guarantee`: the semantic claim being checked
- `observation_window`: check time, target read time, max lag, eligible source boundary, source LSN, stream topic, and stream offset range
- `kubernetes_status`: compact status payload used by the job-backed operator
- `checkID`: generation or scheduled interval identifier used to make repeated checks idempotent
- `counterexamples`: compact evidence for missing, stale, or aggregate-mismatch violations
- `missing`, `stale`, `aggregate_mismatches`, and `freshness_violations`: detailed current drift classes
- `freshness_breaches`: historical bounded-freshness SLO misses that are preserved as evidence but do not make the current state unrecoverably unhealthy

## Failure Scenarios To Demonstrate

- Indexer is down for two minutes
- Kafka consumer commits an offset but fails before indexing
- Elasticsearch rejects a document because of a mapping/schema change
- Duplicate event causes duplicate derived rows
- Backfill misses a date partition
- Redis cache contains stale data after source update

## Failure Taxonomy

The system distinguishes data drift from checker/repair failure.

Data and pipeline failures:

- Missing derived record
- Stale derived record
- Duplicate derived record
- Aggregate mismatch
- Replication lag beyond SLO
- Schema or mapping rejection

Checker failures:

- Source unavailable
- Target unavailable
- Checker crashes mid-scan
- Partial scan
- Slow scan exceeds check interval
- Credentials or Kubernetes Secret missing

Repair failures:

- Repair crashes midway
- Repair writes duplicates
- Repair uses stale source data
- Repair succeeds mechanically but verification still fails
- Repair loops without reducing drift

Expected behavior:

- Never mark an invariant healthy after an incomplete check.
- Separate "checker failed" from "data drift detected."
- Make repairs idempotent.
- Verify after every repair.
- Store compact status in Kubernetes and verbose reports externally.

## Milestones

### Milestone 1: Runtime-Verified Local Demo

- Implemented no-Docker proof path with deterministic local source and derived data
- Runtime-verified Docker Compose with Postgres, Redpanda, OpenSearch, and the Python checker/repair CLI
- Generate demo orders: verified
- Detect missing indexed orders: verified
- Detect aggregate count/revenue drift: verified
- Print JSON/Markdown drift reports: verified
- Repair and verify the invariant: verified

### Milestone 2: Report and Status Shape

- Keep JSON/Markdown reports for local CLI
- Keep ConfigMap handoff compact in Kubernetes: `status.json` plus repair input, not full report blobs
- Define `Invariant.status` fields for Kubernetes:
  - `healthy`
  - `phase`
  - `guarantee`
  - `checkStatus`
  - `driftCount`
  - `lastCheckedAt`
  - `observationWindow`
  - `counterexampleCount`
  - `reportRef`
  - `observedGeneration`
  - `checkID`
  - `checkIntervalSeconds`
  - `nextCheckAfter`

### Milestone 3: More Invariant Types

- Implemented aggregate checks: paid order count and revenue total
- Implemented bounded freshness checks using source `updated_at`, target `indexed_at`, Postgres LSN, and Kafka offset evidence
- Implemented freshness drift injection and freshness repair verification
- Implemented first generic Postgres/OpenSearch query invariant path
- Keep existence as the simplest invariant

### Milestone 4: Failure Taxonomy Tests

- Implemented source unavailable checker failure -> `CheckFailed`
- Implemented verification-still-drifting repair failure -> `RepairFailed`
- Next: target unavailable
- Next: checker timeout
- Next: repair Job crash before report publication
- Next: repair retry/idempotency
- Next: stale/corrupted target document

### Milestone 5: Second Source/Derived Pair

- Add Postgres -> Redis cache freshness, or Postgres -> ClickHouse aggregate table
- Prove the CRD abstraction is not tailored only to OpenSearch

### Milestone 6: Operator Shape

- Define CRDs
- Implemented first controller-runtime skeleton
- Implemented synthetic `Invariant` reconciliation
- Implemented job-backed `Invariant` reconciliation
- Implemented checker Job creation and compact ConfigMap status handoff
- Implemented scheduled checker Jobs through `spec.checkIntervalSeconds`
- Implemented repair Job creation from auto-approved `RepairPolicy`
- Implemented repair result ConfigMap handoff and verified status update
- Added operator Dockerfile and in-cluster Deployment/RBAC sketch
- Enabled CRD `status` subresource
- Update `.status` on `Invariant`
- Run on kind/minikube

### Milestone 7: Observability

- Prometheus metrics:
  - `dataguard_invariant_healthy`
  - `dataguard_drift_records`
  - `dataguard_repair_attempts_total`
  - `dataguard_repair_success_total`
- OpenTelemetry traces around check and repair cycles
- Grafana dashboard

### Milestone 8: Advanced Checks

- Schema compatibility checks
- Sampled checks for large tables
- Shard-aware checks

## Stretch Ideas

- Admission webhook that blocks deploying a pipeline without invariants
- GitOps workflow for consistency SLOs
- Slack/GitHub/Jira incident integration
- Human approval for high-risk repairs
- Multi-tenant invariant isolation
- "Explain drift" page that shows likely root cause
