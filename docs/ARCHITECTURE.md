# KubeDataGuard MVP Architecture

## System Boundary

The MVP protects one derived view:

```text
Postgres orders table -> Redpanda orders.events topic -> OpenSearch orders index
```

There are now three execution modes:

- `demo-local`: deterministic no-Docker proof that models Postgres source rows and OpenSearch documents in memory.
- Compose demo: real Postgres, Redpanda, and OpenSearch integration.
- Kubernetes job-backed demo: a Go operator in kind creates Python checker and repair Jobs that connect to the Compose data systems and publish report ConfigMaps.

All three modes use the same invariant/report semantics. The local proof is not the final runtime target; it is a fast, dependency-light way to prove the control loop.

The declared consistency SLO:

```text
Every paid order committed in Postgres must appear in the OpenSearch orders index after the allowed lag window.
```

## Why This Is The Right First Slice

This slice contains the core DDIA problem without drowning in infrastructure:

- Postgres is the system of record.
- Redpanda/Kafka is the replication log.
- OpenSearch is derived data for search.
- The indexer is the asynchronous pipeline.
- The checker measures whether the derived view is trustworthy.
- The repair command rebuilds missing or stale records from the source of truth.

## Deep System Design Analysis

The project's real boundary is not a set of containers. It is a correctness boundary:

```text
source-of-truth commit -> asynchronous propagation -> derived representation -> user-facing read path
```

KubeDataGuard exists because each arrow can fail independently.

### Source Of Truth

Postgres is the authority for the first MVP. A paid order is considered real once the source transaction commits. The checker should therefore avoid treating Kafka or OpenSearch as authoritative during repair; both are downstream representations.

### Asynchronous Boundary

Redpanda/Kafka represents the replication log between the source and derived view. This boundary creates the central DDIA tradeoff:

```text
lower write latency and looser coupling
in exchange for replication lag, replay complexity, and possible derived-data drift
```

The MVP deliberately simulates the dangerous case where a consumer commits an offset but fails to write OpenSearch. In production, that can make metrics look normal while the derived store is semantically wrong.

### Observation Window

The checker must avoid judging data that is still inside the allowed lag window. Every report therefore needs an observation window:

```text
checked_at
target_read_at
max_lag_seconds
eligible_records_before
source_watermark
stream_topic
source_lsn
stream_offset_start
stream_offset_end
completeness
```

This is the difference between a casual diff and an SLO check. A record updated five seconds ago may be legitimately absent if the SLO allows sixty seconds of lag. A record updated ten minutes ago is a violation if it is still absent.

### Invariant Layers

The first three invariant layers are intentionally complementary:

- Existence and field equality catch missing or stale individual records.
- Aggregate consistency catches count and revenue mismatches that might be visible in analytics or dashboards.
- Bounded freshness catches source updates that the derived view has not observed within the declared lag window.

This mirrors real systems: user-facing search may need record-level correctness, business dashboards often need aggregate correctness, and replicated read paths need freshness evidence.

Freshness has two evidence classes:

- Current freshness violations mean the derived view has not observed a source update yet. These are actionable and repairable by reindexing or replay.
- Freshness SLO breaches mean the target eventually observed the source update, but too late. These are preserved as historical SLO evidence, but they do not keep the current Kubernetes status permanently unhealthy after repair converges the view.

### Control-Plane Status

The Kubernetes controller keeps status compact:

```text
Healthy
DriftDetected
CheckFailed
Repairing
RepairFailed
Unknown
```

The status is for automation. The report is for evidence. Mixing those two would either make Kubernetes status too noisy or make reports too weak.

## Data Flow

```text
generate command
  |
  |-- insert order into Postgres
  |-- write event into order_events_outbox
  |-- publish event to Redpanda
  |
index command
  |
  |-- consume Redpanda events
  |-- write documents to OpenSearch
  |
check command
  |
  |-- query paid orders from Postgres
  |-- mget same ids from OpenSearch
  |-- classify missing/stale/aggregate/freshness drift
  |-- preserve source LSN and stream offset evidence
  |
repair command
  |
  |-- read latest drift report
  |-- fetch source rows from Postgres
  |-- reindex missing/stale documents

operator check-job path
  |
  |-- watch Invariant resources
  |-- create one checker Job per Invariant generation
  |-- run the Python checker in Kubernetes
  |-- write status.json and report.json into a ConfigMap
  |-- patch Invariant.status from status.json

operator repair-job path
  |
  |-- observe DriftDetected checker report
  |-- find auto-approved RepairPolicy
  |-- create one repair Job per Invariant generation
  |-- read checker report ConfigMap
  |-- reindex missing/stale records from Postgres
  |-- verify the invariant
  |-- write repair status.json and report.json into a ConfigMap
  |-- patch Invariant.status from repair status.json
```

## Failure Demonstration

The indexer supports:

```text
--skip-every-paid N
```

This simulates a realistic pipeline bug:

```text
The consumer commits the event offset but fails to update the derived store.
```

From the outside, the event stream may look consumed, but the search index is wrong. This is precisely why uptime monitoring is insufficient.

The CLI also supports:

```text
inject-freshness-drift
```

This simulates a bounded-freshness failure without changing count or amount fields:

```text
source updated_at is moved to an eligible past time
target indexed_at is moved before that source update
freshness checker reports target has not observed the source update
repair reindexes those order ids from Postgres
post-repair freshness check returns Healthy while preserving historical SLO breach evidence
```

The no-Docker proof simulates the same class of error:

```text
source rows contain 20 paid orders
derived index misses every 5th paid order
one indexed document has stale amount/version fields
existence+fieldEquality detects record drift
aggregate detects count and revenue drift
repair upserts missing/stale records from source truth
verification returns Healthy
```

## Invariant Logic

The implemented invariants are intentionally layered:

```text
paid-orders-indexed
paid-orders-aggregate
paid-orders-freshness
```

They check:

- existence: paid source order exists in target index
- field equality: status, amount, currency, and version match
- aggregate equality: paid order count and revenue total match
- bounded freshness: source `updated_at` has been observed by target `indexed_at`
- freshness eligibility: only checks source records older than `max_lag_seconds`

For bounded freshness, `indexed_at < updated_at` is current drift because the derived document predates the source update. `indexed_at - updated_at > max_lag_seconds` is recorded as an SLO breach because it proves the update arrived late, but the derived view is currently caught up.

## Formal Invariant Contract

The mature invariant contract should separate what is being promised from how it is observed:

```text
scope:
  which source rows/events/documents are eligible

claim:
  existence, field equality, aggregate equality, bounded freshness, or session guarantee

observation window:
  source snapshot bounds, CDC watermark, stream offset range, and target read time

allowed uncertainty:
  max lag, tolerated aggregate delta, ignored tombstone age, or sample confidence

evidence:
  concrete source and target ids, versions, offsets, timestamps, and counterexamples
```

This avoids a common trap: a checker that finds differences but cannot explain whether it scanned a complete, meaningful slice of history.

The first report can start simple, but it should evolve toward these fields:

```text
source_watermark
source_snapshot_started_at
source_snapshot_finished_at
stream_topic
stream_partition
stream_offset_start
stream_offset_end
target_read_at
observed_lag_seconds
counterexamples
repair_scope
```

## Repair Logic

The first repair is source-of-truth reindexing:

```text
missing/stale order ids -> fetch from Postgres -> write to OpenSearch
freshness violation order ids -> fetch from Postgres -> write to OpenSearch
```

This avoids trusting the event stream during repair. Later versions can repair by replaying Kafka offsets or rebuilding whole partitions.

## Kubernetes Control Plane

The local commands map directly to operator reconciliation:

```text
DataSource    -> Postgres connection
DerivedView   -> OpenSearch index derived through Redpanda
Invariant     -> paid-orders-indexed rule
RepairPolicy  -> reindex missing/stale orders
```

The controller loop now performs the first closed-loop version of what the CLI proved:

```text
observe desired invariant
measure actual state
write status
repair if policy allows
verify
```

The first repair strategy is implemented for `reindex-records`. Later repair strategies should cover Kafka replay, Redis invalidation, and analytics backfill.

## Current Closed-Loop Operator

```text
Invariant CRD
  |
  |-- Go/controller-runtime watch
  |-- unstructured reconciler
  |-- deterministic checker Job for this generation
  |-- Python checker runs against Postgres/Redpanda/OpenSearch
  |-- report ConfigMap stores report.json and status.json
  |-- DriftDetected status selects a RepairPolicy
  |-- deterministic repair Job reads the checker report
  |-- Python repair reindexes and verifies
  |-- repair ConfigMap stores report.json and status.json
  |-- status subresource patch copies checker or repair status.json into Invariant.status
  |
Invariant.status
```

The controller creates or ensures:

- checker ServiceAccount
- checker Role with ConfigMap get/create/patch/update
- checker RoleBinding
- checker Job
- report ConfigMap handoff
- repair Job when an allowed RepairPolicy exists
- repair result ConfigMap handoff

The job-backed path is enabled with:

```text
dataguard.io/checker-mode: job
```

The checker image is selected with:

```text
dataguard.io/checker-image: kubedataguard-dataguard:latest
```

The synthetic path is still useful for smoke tests and supports:

```text
dataguard.io/synthetic-drift-count: "3"
```

If the value is absent or zero, the reconciler writes `Healthy`. If the value is positive, it writes `DriftDetected`.

The runtime-verified job path is:

```text
generation 2 after deliberate drift:
  paid-orders-indexed -> DriftDetected, drift_count=8
  paid-orders-aggregate -> DriftDetected, drift_count=2

generation 3 after repair:
  paid-orders-indexed -> Healthy, drift_count=0
  paid-orders-aggregate -> Healthy, drift_count=0

generation 4 after restoring the example 60-second SLO:
  paid-orders-indexed -> Healthy, drift_count=0
  paid-orders-aggregate -> Healthy, drift_count=0
```

The runtime-verified repair path is:

```text
generation 5 after deliberate drift:
  paid-orders-indexed checker report -> DriftDetected, drift_count=8
  RepairPolicy reindex-missing-paid-orders selected
  repair Job reindexed 8 records from Postgres into OpenSearch
  repair verification -> Healthy, drift_count=0

fresh aggregate check after repair:
  paid-orders-aggregate -> Healthy, drift_count=0
```

That keeps the Go controller focused on Kubernetes reconciliation and keeps the Python worker focused on data-system correctness.
