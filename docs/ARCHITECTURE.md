# KubeDataGuard MVP Architecture

## System Boundary

The MVP protects three derived-view shapes:

```text
Postgres orders table -> Redpanda orders.events topic -> OpenSearch orders index
Postgres orders table -> Redpanda orders.events topic -> Redis order cache
Postgres orders table -> ClickHouse orders_analytics table
```

There are now three execution modes:

- `demo-local`: deterministic no-Docker proof that models Postgres source rows and OpenSearch documents in memory.
- Compose demo: real Postgres, Redpanda, OpenSearch, Redis, and optional ClickHouse integration.
- Kubernetes job-backed demo: kind runs Postgres, Redpanda, OpenSearch, Redis, ClickHouse, the Go operator, and scheduled Python checker/repair Jobs in-cluster.

All three modes use the same invariant/report semantics. The local proof is not the final runtime target; it is a fast, dependency-light way to prove the control loop.

The declared consistency SLOs:

```text
Every paid order committed in Postgres must appear in the OpenSearch orders index after the allowed lag window.
Every paid order committed in Postgres must be reflected in the Redis cache after the allowed lag window.
The ClickHouse analytics table must match paid-order count and revenue aggregates after a backfill.
```

## Why This Is The Right First Slice

This slice contains the core DDIA problem without drowning in infrastructure:

- Postgres is the system of record.
- Redpanda/Kafka is the replication log.
- OpenSearch is derived data for search.
- Redis is derived data for low-latency cache reads.
- ClickHouse is derived data for analytics aggregates.
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
cdc_frontier
completeness
```

This is the difference between a casual diff and an SLO check. A record updated five seconds ago may be legitimately absent if the SLO allows sixty seconds of lag. A record updated ten minutes ago is a violation if it is still absent.

### Invariant Layers

The first invariant layers are intentionally complementary:

- Existence and field equality catch missing or stale individual records.
- Aggregate consistency catches count and revenue mismatches that might be visible in analytics or dashboards.
- Bounded freshness catches source updates that OpenSearch or Redis has not observed within the declared lag window.
- Prefix-bucket checksum narrowing catches large source/target mismatches without fetching every row first.

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
  |-- optionally compare checksum buckets before fetching exact row counterexamples
  |-- preserve source LSN, outbox, stream offset, and target applied-offset evidence
  |-- write JSON/Markdown reports locally and optionally to S3/MinIO
  |
repair command
  |
  |-- read latest drift report
  |-- fetch source rows from Postgres
  |-- reindex missing/stale documents

operator check-job path
  |
  |-- watch Invariant, DataSource, and DerivedView resources
  |-- create one checker Job per Invariant generation or scheduled check interval
  |-- run the Python checker in Kubernetes
  |-- write full report to the worker report store or S3/MinIO
  |-- write compact status.json and repair-input.json into a ConfigMap
  |-- patch Invariant.status from status.json

operator repair-job path
  |
  |-- observe DriftDetected checker report
  |-- find explicitly allowed RepairPolicy
  |-- create one repair Job per Invariant generation
  |-- read checker repair-input ConfigMap
  |-- emit reconciliation requests or direct-reindex in unsafe demo mode
  |-- verify the invariant
  |-- write repair status.json and compact repair input into a ConfigMap
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
paid-orders-checksum
paid-orders-freshness
paid-orders-redis-cache-freshness
paid-orders-clickhouse-aggregate
```

They check:

- existence: paid source order exists in target index
- field equality: status, amount, currency, and version match
- checksum: source and target bucket fingerprints match before drilling into mismatched ID prefixes
- aggregate equality: paid order count and revenue total match
- bounded freshness: source `updated_at` has been observed by target `indexed_at`
- freshness eligibility: only checks source records older than `max_lag_seconds`
- analytics aggregate: ClickHouse paid-order count and revenue match the Postgres source

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
cdc_frontier_status
outbox_published_count
kafka_partition_offsets
target_applied_offsets
target_read_at
observed_lag_seconds
counterexamples
repair_scope
```

## Reconciliation Logic

The safe production model is owner-executed reconciliation:

```text
missing/stale order ids -> emit reconcile request -> owning service/pipeline transforms and writes the derived view
```

That keeps business transformation logic in the system that owns it. KubeDataGuard should provide evidence and trigger a reconciliation workflow; it should not normally bypass the pipeline and write raw source rows into a derived store.

The local demo still supports source-of-truth reindexing:

```text
missing/stale order ids -> fetch from Postgres -> write to OpenSearch
freshness violation order ids -> fetch from Postgres -> write to OpenSearch
```

Direct reindexing is useful for a controlled vertical-slice proof, but the operator fences automatic direct reindex behind `approvalRequired: false` and `dataguard.io/allow-unsafe-direct-reindex: "true"`. Later versions should dispatch reconciliation events, call owner webhooks, replay Kafka offsets, or rebuild whole partitions.

## Kubernetes Control Plane

The local commands map directly to operator reconciliation:

```text
DataSource    -> Postgres connection
DerivedView   -> OpenSearch index, Redis cache, or ClickHouse analytics table
Invariant     -> paid-orders-indexed, paid-orders-freshness, paid-orders-redis-cache-freshness, paid-orders-clickhouse-aggregate
RepairPolicy  -> emit reconciliation, Kafka replay, webhook, cache invalidation, ClickHouse backfill, or explicitly approved direct repair
```

The controller loop now performs the first closed-loop version of what the CLI proved:

```text
observe desired invariant
measure actual state
write status
trigger reconciliation if policy allows
verify
```

The first direct repair strategy is implemented for `reindex-records`, but automatic direct writes are unsafe by default. The safer worker path can emit reconciliation-event JSONL, publish Kafka replay requests, call owner webhooks, emit Redis invalidation requests, or emit ClickHouse backfill requests.

## Current Closed-Loop Operator

```text
Invariant CRD
  |
  |-- Go/controller-runtime watch on Invariant, DataSource, DerivedView
  |-- unstructured reconciler
  |-- deterministic checker Job for this generation/checkID
  |-- Python checker runs against Postgres/Redpanda/OpenSearch
  |-- report ConfigMap stores status.json and repair-input.json
  |-- DriftDetected status selects an explicitly allowed RepairPolicy
  |-- deterministic repair Job reads the checker report
  |-- Python repair emits reconciliation events or direct-reindexes in unsafe demo mode
  |-- repair ConfigMap stores status.json and repair-input.json
  |-- status subresource patch copies semantic checker or repair status into Invariant.status
  |
Invariant.status
```

The controller creates or ensures:

- checker ServiceAccount
- checker Role with ConfigMap get/create/patch/update
- checker RoleBinding
- checker Job
- compact report ConfigMap handoff
- repair Job when an allowed RepairPolicy exists
- repair result ConfigMap handoff

Full drift evidence is not stored in `Invariant.status` or ConfigMap payloads. The worker writes complete JSON/Markdown artifacts to `REPORT_DIR` and, when configured with `REPORT_STORE=s3`, uploads the same evidence to an S3-compatible store. Kubernetes status carries the compact phase plus `reportRef`.

Before a job-backed check starts, the controller resolves:

```text
Invariant.spec.derivedViewRef
  -> DerivedView.spec.sourceRef
  -> DataSource.spec.connectionSecret
  -> worker POSTGRES_DSN secretKeyRef

DerivedView.spec.target.connectionSecret
  -> worker OPENSEARCH_URL secretKeyRef
  -> or worker REDIS_URL secretKeyRef

DerivedView.spec.pipeline.connectionSecret
  -> worker KAFKA_BOOTSTRAP_SERVERS secretKeyRef
```

The operator injects `valueFrom.secretKeyRef` into the checker and repair Jobs. It does not copy credential values into CRD status, ConfigMaps, or annotations. Annotation/default connection values still exist as a local-development fallback, but the CRD path is now the primary Kubernetes path.

`DataSource`, `DerivedView`, and referenced Secrets are watched topology resources. A `DerivedView` change enqueues the `Invariant`s that reference it. A `DataSource` change first finds derived views with that `sourceRef`, then enqueues the invariants attached to those derived views. A Secret change now fans out through matching DataSource and DerivedView references, so rotated credentials trigger fresh checker Jobs instead of waiting for the next interval.

Scheduled invariants use `spec.checkIntervalSeconds`. The controller computes a `checkID` from the invariant generation and the current interval slot. That gives repeated checks without duplicate Jobs inside the same interval.

The first generic query path is intentionally scoped:

```text
Postgres sourceQuery
keyset-paginated source scan by keyField
optional source scan checkpoint
OpenSearch JSON targetQuery
keyField join
optional compareFields equality
```

The older commerce checks remain as optimized hardcoded demo invariants. The `query` invariant type is the first step toward making `sourceQuery` and `targetQuery` executable API fields rather than documentation-only fields. Source scans now wrap the declared query as a subquery, order by `keyField`, and fetch pages with `sourceScanPageSize`.

When `sourceCheckpointId` is set, bounded scans persist checkpoint state in the worker report store after each page and again at the end of the run. The checkpoint stores the query hash, check ID, key field, first and last scanned key, source watermark, source LSN, page count, row count, and stop reason. That lets the next run resume from the last processed key instead of restarting a large scan. The commerce path now has a first WAL/outbox/Kafka/target-applied-offset frontier, but this is still not full DBLog semantics: there is no logical decoding stream, no crash-exact snapshot-plus-log recovery, and a resumed suffix scan is marked `partial` rather than pretending it is a complete source/target snapshot.

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
