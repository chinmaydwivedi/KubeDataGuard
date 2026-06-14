# Progress: KubeDataGuard

## Current Status

Status: Docker/Colima runtime demo and closed-loop Go/controller-runtime operator are implemented and verified; the architecture has been hardened against the first major critique, now has optional S3/MinIO-compatible evidence storage, keyset-paginated source scans, persisted source-scan checkpoints, Secret-backed DataSource/DerivedView connection resolution, DataSource/DerivedView watch fan-out to dependent Invariants, a Kubernetes-native demo stack manifest, and a first CDC frontier proof across Postgres WAL/outbox, Kafka offsets, and OpenSearch applied-offset evidence.

The project now has Docker Compose infrastructure, Python CLI commands, invariant logic, evidence-bearing reports, optional durable report publication, keyset source scan evidence, scan checkpoint state, aggregate consistency checks, CDC frontier evidence, a deterministic no-Docker proof path, repair logic, tests, Kubernetes CRDs, a Go/controller-runtime reconciler that launches Python checker and repair Jobs with Secret-backed connection env vars and topology-change fan-out, example resources, a kind-native Postgres/Redpanda/OpenSearch demo stack, and deeper docs.

## Decisions Made

- This is the first project to implement.
- Start with Docker Compose before a full Kubernetes operator.
- Start with one invariant type: existence.
- Start with one source and one derived store:
  - Source: Postgres
  - Event log: Redpanda/Kafka
  - Derived store: OpenSearch
- Use Python for the local MVP.
- Use Go/controller-runtime for the long-term Kubernetes operator.
- Include CRD sketches early; build the data-integrated controller only after the local loop is runtime-verified.
- Keep external-service imports lazy so the no-Docker proof can run without Postgres/Kafka/OpenSearch client packages.
- Use a hybrid implementation model: Go operator for Kubernetes reconciliation, Python checker/repair workers for data integrations and invariant logic.
- Treat Kubernetes as the control plane, not the row-level data plane.
- Do not auto-run direct derived-store writes unless a `RepairPolicy` explicitly opts into unsafe direct reindex.
- Store full drift evidence outside Kubernetes; use Kubernetes status for compact phase and report references only.

## Demo Story

The five-minute GitHub demo should look like this:

```sh
make up       # starts Postgres, Redpanda, Redpanda Console, and OpenSearch
make seed     # resets the demo and inserts 50 orders, most marked paid
make drift    # simulates an indexer bug that commits some paid events without indexing them
make check    # reports missing paid orders with order IDs and source details
make repair   # reindexes missing/stale orders from Postgres and verifies the invariant
make check    # produces a clean report
```

This sequence is the value proposition: the user sees a derived view drift from the source of truth, gets a concrete report, repairs from the source of truth, and verifies the data is trustworthy again.

Current runnable proof on this machine:

```sh
make demo-local
```

This produces:

```text
report_type: kubedataguard-local-proof
before existence: DriftDetected
before aggregate: DriftDetected
repair: reindex-records-from-source
after existence: Healthy
after aggregate: Healthy
```

## Implemented Files

- `docker-compose.yml`
- `Dockerfile`
- `go.mod`
- `go.sum`
- `Makefile`
- `requirements.txt`
- `src/dataguard/config.py`
- `src/dataguard/db.py`
- `src/dataguard/events.py`
- `src/dataguard/search.py`
- `src/dataguard/invariants.py`
- `src/dataguard/reporting.py`
- `src/dataguard/generator.py`
- `src/dataguard/indexer.py`
- `src/dataguard/checker.py`
- `src/dataguard/repair.py`
- `src/dataguard/local_demo.py`
- `src/dataguard/k8s_report.py`
- `src/dataguard/cli.py`
- `tests/test_invariants.py`
- `docs/ARCHITECTURE.md`
- `docs/RUNBOOK.md`
- `docs/KNOWLEDGE_BASE.md`
- `k8s/crds/*.yaml`
- `examples/commerce-consistency.yaml`
- `cmd/dataguard-operator/main.go`
- `internal/controller/invariant_controller.go`
- `internal/controller/job_handoff.go`
- `internal/controller/invariant_controller_test.go`

## Latest Deep-System-Design Pass

Implemented in this pass:

- Added report `status` values such as `Healthy` and `DriftDetected`.
- Added explicit `guarantee` labels for invariant semantics.
- Added `observation_window` evidence:
  - check time
  - target read time
  - max lag
  - eligible source boundary
  - source watermark
  - stream topic
- Added `counterexamples` for missing, stale, and aggregate drift.
- Added aggregate invariant logic for paid-order count and paid revenue total.
- Added `dataguard check --invariant aggregate`.
- Added `make check-aggregate`.
- Updated the Invariant CRD sketch with phase, guarantee, check status, observation window, and counterexample count.
- Updated the example consistency policy with `paid-orders-aggregate`.

## Latest Execution Pass

Implemented now:

- Added `dataguard demo-local`.
- Added `make demo-local`.
- Added deterministic local source rows and derived-index drift.
- Added a proof payload with:
  - source summary
  - derived-before summary
  - existence and aggregate reports before repair
  - repair provenance
  - derived-after summary
  - existence and aggregate reports after repair
- Added stream offset start/end evidence to observation windows.
- Made CLI external integrations lazy-loaded so the no-Docker proof does not require unavailable service client packages.
- Added tests for stream offset evidence and the local proof.
- Added `kubernetes_status` payloads to reports so Go/controller-runtime code can update `Invariant.status` without parsing verbose evidence.

## Earlier Operator Skeleton Pass

Implemented now:

- Added Go module for the Kubernetes operator.
- Added controller-runtime manager entrypoint.
- Added unstructured `Invariant` reconciler.
- Added synthetic status builder.
- Added support for `dataguard.io/synthetic-drift-count`.
- Enabled the CRD `status` subresource.
- Extended CRD status schema with:
  - `checkedRecords`
  - `observedGeneration`
  - `observationWindow.checkedAt`
  - `observationWindow.targetReadAt`
  - `observationWindow.streamOffsetStart`
  - `observationWindow.streamOffsetEnd`
- Added `make operator-test`.
- Added `make operator-build`.

Current behavior:

```text
Invariant without synthetic drift -> status.phase=Healthy
Invariant with dataguard.io/synthetic-drift-count > 0 -> status.phase=DriftDetected
```

This proved the Kubernetes reconciliation/status path before the job-backed data integration was added.

## Latest Runtime Verification Pass

Implemented and verified now:

- Installed Docker CLI runtime through Homebrew:
  - `colima`
  - `docker`
  - `docker-compose`
  - `kind`
- Started Colima with Docker runtime.
- Verified Docker with `docker run --rm hello-world`.
- Patched Compose report persistence to avoid the macOS protected `Documents` bind mount:
  - reports now mount to `${HOME}/.kubedataguard/reports`
- Ran the full Compose stack:
  - Postgres
  - Redpanda
  - Redpanda Console
  - OpenSearch
  - `dataguard` CLI image
- Runtime-verified drift detection:
  - generated 50 orders
  - deliberately skipped 8 paid orders in the indexer
  - existence invariant detected 8 missing derived records
  - aggregate invariant detected count and revenue drift
- Runtime-verified repair:
  - reindexed 8 missing orders from Postgres
  - post-repair existence invariant was `Healthy`
  - post-repair aggregate invariant was `Healthy`
- Runtime-verified Kubernetes skeleton:
  - created kind cluster `kubedataguard`
  - applied all DataGuard CRDs
  - applied example commerce resources
  - ran the Go operator locally against kind
  - observed `Invariant.status` update to `Healthy`
  - annotated an invariant with `dataguard.io/synthetic-drift-count=3`
  - observed `Invariant.status.phase=DriftDetected`
  - reset annotation to `0` and observed `Healthy` again

## Latest Kubernetes Job Handoff Pass

Implemented and verified now:

- Added `dataguard check-job`.
- Added Kubernetes ConfigMap report publishing from the Python checker.
- Added `src/dataguard/k8s_report.py` using the in-cluster service account token and Kubernetes API.
- Added `dataguard.io/checker-mode=job` support on `Invariant` resources.
- Added Go controller logic that:
  - creates one deterministic checker Job per `Invariant` generation
  - creates checker ServiceAccount, Role, and RoleBinding
  - passes Postgres, Redpanda/Kafka, OpenSearch, topic, index, invariant, max lag, namespace, ConfigMap, and generation into the Job
  - waits for the Job's report ConfigMap
  - decodes `status.json`
  - patches `Invariant.status`
  - distinguishes running/check-failed states from data drift
- Added idempotence guard so completed report status is not re-patched repeatedly.
- Updated example invariants to opt into job-backed mode.

Runtime-verified against kind plus the Compose data systems:

```text
Invariant generation 2 -> checker Jobs -> report ConfigMaps -> status DriftDetected
paid-orders-indexed: 8 drift records
paid-orders-aggregate: 2 aggregate mismatches

repair from Postgres -> Invariant generation 3 -> checker Jobs -> status Healthy

restore example spec -> Invariant generation 4 -> checker Jobs -> status Healthy
```

The project has crossed the important line: the operator is no longer only a local CLI plus a synthetic Kubernetes skeleton. It now uses Kubernetes reconciliation to run real data checks and update CRD status from real reports.

## Latest Kubernetes Repair Handoff Pass

Implemented and verified now:

- Added `dataguard repair-job`.
- Added ConfigMap repair-input reading from the Python Kubernetes API helper.
- Refactored repair logic so the same function can repair from:
  - local `latest.json`
  - in-cluster checker repair input
- Added Go controller logic that:
  - detects `DriftDetected` checker reports
  - finds an explicitly allowed `RepairPolicy`
  - creates one deterministic repair Job per `Invariant` generation
  - passes the checker report ConfigMap into the repair Job
  - lets the repair Job reindex missing/stale records in controlled unsafe demo mode
  - verifies the invariant after repair
  - reads the repair result ConfigMap before the stale drift report
  - patches `Invariant.status` to `Healthy` or `RepairFailed`
- Added `Repairing` and `RepairFailed` lifecycle status builders.
- Added tests for repair Job construction, auto-repair policy gating, and repair payload handling.

Runtime-verified against kind plus the Compose data systems:

```text
Invariant generation 5 -> checker Jobs -> paid-orders-indexed drift detected
operator found RepairPolicy reindex-missing-paid-orders
operator created dataguard-repair-paid-orders-indexed-g5
repair Job reindexed 8 missing paid orders
repair Job verified paid-orders-indexed Healthy
operator patched paid-orders-indexed status from repair ConfigMap

fresh aggregate generation 6 -> Healthy
restored default 60-second SLO generations -> both Healthy
```

This is now the first closed-loop version of KubeDataGuard:

```text
declare invariant -> detect drift -> run allowed repair -> verify -> update status
```

## Latest Failure Taxonomy Pass

Implemented and verified now:

- Added explicit repair result construction for Kubernetes repair Jobs.
- Added unit coverage for:
  - repair verification success -> `Healthy`
  - repair verification still drifting -> `RepairFailed`
  - checker Job failure status -> `CheckFailed`
  - repair running status -> `Repairing`
  - repair failure status -> `RepairFailed`
- Runtime-verified checker failure:
  - temporarily pointed `paid-orders-indexed` at an unreachable Postgres port
  - checker Job failed before publishing a report
  - operator set `Invariant.status.phase=CheckFailed`
  - restored the real Postgres DSN and verified the next generation returned to `Healthy`
- Runtime-verified repair verification failure:
  - temporarily added an aggregate `RepairPolicy` using `reindex-records`
  - created aggregate drift
  - repair Job completed but verification still found aggregate mismatch
  - repair Job published `phase=RepairFailed`
  - operator copied `RepairFailed` into `Invariant.status`
  - removed the temporary policy, repaired the data, restored the default 60-second SLO, and verified both invariants `Healthy`

This hardens the control plane distinction:

```text
data drift != checker failure != repair failure
```

## Latest Bounded Freshness Pass

Implemented and verified now:

- Added `freshness` as a first-class invariant mode.
- Added bounded freshness comparison logic:
  - source `updated_at`
  - target `indexed_at`
  - observed lag seconds
  - max lag seconds
  - missing target evidence represented as freshness lag violation
- Added CDC evidence fields:
  - Postgres `pg_current_wal_lsn()` as `sourceLSN`
  - source timestamp watermark
  - Kafka topic offset start/end
  - stream topic
- Added `dataguard check --invariant freshness`.
- Added Kubernetes `check-job --invariant freshness`.
- Added `make check-freshness`.
- Added `paid-orders-freshness` example `Invariant`.
- Updated the Invariant CRD status schema with `observationWindow.sourceLSN`.
- Updated Markdown report rendering for freshness violations and source LSN.
- Updated Redpanda external advertised listener to `host.docker.internal:19092` so kind pods can read Kafka metadata/offsets.
- Added unit tests for healthy and violating freshness reports.
- Added Go test proving the operator passes `freshness` to checker Jobs.

Runtime-verified:

```text
Compose freshness check:
  status: Healthy
  guarantee: boundedFreshness
  sourceLSN present
  Kafka offset range present

kind operator freshness check:
  Invariant: paid-orders-freshness
  checkedRecords: 41
  phase: Healthy
  guarantee: boundedFreshness
  sourceLSN: present
  streamOffsetStart: 0
  streamOffsetEnd: 250
```

## Latest Freshness Drift/Repair Pass

Implemented the next phase: freshness is now intentionally breakable and repairable.

New behavior:

- Added `inject-freshness-drift`.
- Added `make drift-freshness`.
- Added `make demo-freshness-drift`.
- Added `make repair-freshness`.
- Local repair can now verify `existence`, `aggregate`, or `freshness`.
- Repair now treats `freshness_violations` as source-of-truth reindex candidates.
- Added a freshness `RepairPolicy` to the example resources; it now defaults to approval-required reconciliation events.
- Split freshness evidence into:
  - `freshness_violations`: current actionable drift
  - `freshness_breaches`: historical bounded-freshness SLO misses

Design reason:

```text
A late derived update proves the SLO was breached, but it should not keep the current Kubernetes status unhealthy forever after the derived view has caught up.
```

Current repair semantics:

```text
freshness violation order ids
  -> local demo can fetch source rows from Postgres and reindex OpenSearch documents
  -> safer production path emits reconciliation requests for the owning pipeline
  -> verify paid-orders-freshness
  -> preserve historical SLO breach evidence separately in the report
```

Runtime-verified:

```text
Compose:
  make demo-freshness-drift -> DriftDetected, drift_count=5
  make repair-freshness -> Healthy, drift_count=0, slo_breach_count=5

kind operator:
  paid-orders-freshness generation 4 checker -> DriftDetected, drift_count=5
  generation 4 repair Job -> Healthy, drift_count=0, sloBreachCount=5
  restored generation 5 maxLagSeconds=60 -> Healthy, checkedRecords=41, drift_count=0
```

## Latest Architecture Hardening Pass

Implemented the first response to the architectural critique.

What changed:

- README now positions this as an evidence-first consistency engine with an optional Kubernetes control-plane wrapper, not a system that uses Kubernetes as a row-level data plane.
- Added `spec.checkIntervalSeconds` to `Invariant`.
- Added scheduled checker Jobs with `checkID` values derived from generation and interval slot.
- Added `checkID`, `checkIntervalSeconds`, and `nextCheckAfter` status fields.
- Added the first generic query invariant path:
  - Postgres `sourceQuery`
  - OpenSearch JSON `targetQuery`
  - declared `keyField`
  - optional `compareFields`
- Changed Kubernetes ConfigMap handoff to compact payloads:
  - `status.json`
  - `repair-input.json`
  - full reports stay in the worker report store and are referenced from status
- Added `Dockerfile.operator`.
- Added `k8s/operator.yaml` with operator Deployment/RBAC.
- Added `make operator-image`.
- Added `paid-orders-query-check` example `Invariant`.
- Removed the OpenSearch `size: 10000` correctness cap by paginating target queries with `search_after`.
- Changed checked-in `RepairPolicy` examples to `approvalRequired: true` and `emit-reconcile-events`.
- Added unsafe opt-in fencing for automatic direct `reindex-records` repair.
- Added `emit-reconcile-events` worker repair mode.
- Reduced Kubernetes status churn by ignoring telemetry-only changes such as `checkID`, `reportRef`, and checked row count when semantic health has not changed.

What this fixes:

```text
sourceQuery/targetQuery are no longer purely decorative for the new query invariant type.
SLO checks can now repeat without spec edits.
ConfigMaps no longer carry full drift report blobs.
The Go operator can be built and deployed as a real in-cluster component.
OpenSearch target checks no longer silently stop at 10,000 documents.
Repair defaults no longer teach automatic direct writes into a derived store.
Repeated healthy scheduled checks do not rewrite Invariant.status just to publish telemetry.
```

Verification for this hardening pass:

```text
PYTHONPATH=src python3 -m unittest discover -s tests
  Passed: 18 tests

python3 -m py_compile $(find src -name '*.py' | sort)
  Passed

go test ./...
  Passed

ruby YAML parse for Compose, CRDs, operator manifest, and examples
  Passed

make demo-local
  Passed: drift -> evidence -> repair -> healthy verification

docker compose build dataguard
  Passed

go build -o /tmp/dataguard-operator ./cmd/dataguard-operator
  Passed
```

Verification note:

```text
make operator-image was started, but the legacy Docker build process became silent for several minutes while compiling the operator image and was interrupted manually. This was not a source compile failure; local go build and go test both passed.
```

## Latest Evidence Store Pass

Implemented the next response to the Kubernetes-as-data-plane critique.

What changed:

- Added report-store settings:
  - `REPORT_STORE=local|s3`
  - `REPORT_BUCKET`
  - `REPORT_PREFIX`
  - `REPORT_S3_ENDPOINT_URL`
  - `AWS_REGION`
- Added `ReportArtifacts` so report writing returns:
  - local JSON path
  - local Markdown path
  - local latest path
  - durable report ref
  - durable Markdown ref
  - durable latest ref
- Kept local reports as the default and as the first write path.
- Added optional S3-compatible publication for JSON, Markdown, and `latest.json`.
- Added optional MinIO services to Docker Compose.
- Added `make up-object-store`.
- Added `make check-s3`.
- Wired operator-created checker and repair Jobs with report-store env configuration from annotations.
- Left credentials out of CRD fields; production should use pod identity, mounted Secrets, or platform-level env injection.

What this fixes:

```text
Full evidence reports no longer have to live only on an ephemeral worker filesystem.
Invariant.status.reportRef can point at durable s3:// evidence.
ConfigMaps remain compact handoff objects instead of becoming row-level evidence blobs.
```

Verification for this evidence-store pass:

```text
PYTHONPATH=src python3 -m unittest discover -s tests
  Passed: 20 tests

python3 -m py_compile $(find src -name '*.py' | sort)
  Passed

go test ./...
  Passed

ruby YAML parse for Compose, CRDs, operator manifest, and examples
  Passed

docker compose build dataguard
  Passed

go build -o /tmp/dataguard-operator ./cmd/dataguard-operator
  Passed

make up-object-store && make seed && make drift && make check-s3
  Passed: checker reported DriftDetected with report ref s3://kubedataguard-reports/local/drift-*.json

docker compose --profile object-store run --rm --entrypoint /bin/sh minio-mc -c 'mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc ls local/kubedataguard-reports/local'
  Passed: drift JSON, drift Markdown, and latest.json present in MinIO
```

What remains intentionally open:

```text
generic query support is currently Postgres -> OpenSearch only
first commerce CDC frontier proof is implemented, but full DBLog logical decoding, CDC-bound generic scans, and crash-exact scan recovery still need implementation
object-store reports still need retention, encryption, lifecycle policy, and compaction
production repair dispatch beyond local JSONL reconciliation events remains future work
```

## Latest Source Scan Pass

Implemented the first response to the full-table-scan critique.

What changed:

- Added keyset-paginated Postgres source scans for generic `query` invariants.
- The checker now wraps `sourceQuery` as a subquery, orders by declared `keyField`, and fetches pages using `sourceScanPageSize`.
- Added CLI flags:
  - `--source-scan-page-size`
  - `--source-resume-after-key`
- Added `spec.sourceScanPageSize` to the `Invariant` CRD.
- Operator-created query checker Jobs now pass `--source-scan-page-size`.
- Added source scan evidence into `observation_window.source_scan`:
  - mode
  - key field
  - page size
  - pages read
  - rows read
  - first key
  - last key
  - resume-after key
  - completion marker
- Added Markdown report rendering for source scan evidence.
- Kept this explicitly narrower than DBLog: this pass added scan evidence and resume boundaries before persisted checkpoint storage or source-log watermark reconciliation existed.

What this fixes:

```text
Generic Postgres source checks no longer require one full fetchall() call.
Reports can now prove how the source scan was chunked.
The report contains a last_key that can be used as a resume boundary for manual or future automated continuation.
```

Verification for this source-scan pass:

```text
PYTHONPATH=src python3 -m unittest discover -s tests
  Passed: 22 tests

python3 -m py_compile $(find src -name '*.py' | sort)
  Passed

go test ./...
  Passed

ruby YAML parse for Compose, CRDs, operator manifest, and examples
  Passed

docker compose build dataguard
  Passed

go build -o /tmp/dataguard-operator ./cmd/dataguard-operator
  Passed

docker compose run --rm dataguard check --invariant query --max-lag-seconds 0 --source-scan-page-size 10 ...
  Passed: query invariant report included source_scan mode=keyset, page_size=10, pages=5, rows=41
```

What remains intentionally open:

```text
source scan checkpoints now persist the last processed key, but not a full source snapshot proof
generic query source scan watermarks are not yet tied to Kafka/CDC offsets
parallel chunk scans and Merkle/checksum comparison are not implemented yet
```

Runtime-verified:

```text
Compose generic query check:
  checked_records: 41
  phase: Healthy
  drift_count: 0

kind in-cluster operator:
  Deployment kubedataguard-operator rolled out
  paid-orders-indexed: Healthy, checkID g11-t5937886, interval 300
  paid-orders-aggregate: Healthy, checkID g10-t5937886, interval 300
  paid-orders-freshness: Healthy, checkID g6-t5937886, interval 300
  paid-orders-query-check: Healthy, checkID g1-t5937886, interval 300
  query ConfigMap keys: status.json, repair-input.json
```

## Latest Checkpoint Pass

Implemented the next response to the full-table-scan critique: bounded source scans can now persist progress.

What changed:

- Added `src/dataguard/checkpoints.py`.
- Added local and S3-compatible checkpoint storage under `scan-checkpoints/`.
- Added CLI flags:
  - `--source-checkpoint-id`
  - `--source-reset-checkpoint`
  - `--source-max-pages`
- Added `spec.sourceCheckpointId` and `spec.sourceMaxPages` to the `Invariant` CRD.
- Operator-created query checker Jobs now pass checkpoint ID and max-page controls.
- Source scan evidence now includes:
  - query hash
  - checkpoint ID
  - checkpoint ref
  - loaded checkpoint summary when resuming
- Checkpoints record:
  - query hash
  - check ID
  - key field
  - first and last scanned key
  - source watermark
  - source LSN
  - page count
  - row count
  - stop reason
- A checkpoint created for one query is rejected if reused for a different query hash.

What this fixes:

```text
Large query checks can be intentionally bounded per Job.
A later run can resume from the persisted last_key instead of restarting from the beginning.
The checkpoint lives in the worker report store, not in Kubernetes status or a large ConfigMap blob.
```

What remains intentionally open:

```text
Resumed suffix scans are marked partial; they are not represented as complete snapshots.
The commerce path has a first WAL/outbox/Kafka/target-applied-offset frontier, but generic resumed scans are not yet bound to that frontier.
There is no parallel chunk scheduler or Merkle/checksum narrowing yet.
```

Verification for this checkpoint pass:

```text
PYTHONPATH=src python3 -m unittest discover -s tests
  Passed: 24 tests

python3 -m py_compile $(find src -name '*.py' | sort)
  Passed

go test ./...
  Passed

ruby YAML parse for Compose, CRDs, operator manifest, and examples
  Passed

docker compose build dataguard
  Passed

go build -o /tmp/dataguard-operator ./cmd/dataguard-operator
  Passed

docker compose run --rm dataguard check --invariant query --source-max-pages 2 --source-checkpoint-id demo-paid-orders-query ...
  Passed: report check_status=partial, completed=false, stop_reason=max_pages, rows=20
  Passed: local checkpoint stored under scan-checkpoints/demo-paid-orders-query.json

docker compose run --rm dataguard check --invariant query --source-checkpoint-id demo-paid-orders-query ...
  Passed: loaded_checkpoint present, resume_after_key used, checkpoint advanced to complete

REPORT_STORE=s3 ... docker compose run --rm dataguard check --invariant query --source-max-pages 1 --source-checkpoint-id demo-paid-orders-query-s3 ...
  Passed: reportRef=s3://kubedataguard-reports/local-checkpoints/drift-*.json
  Passed: MinIO contains scan-checkpoints/demo-paid-orders-query-s3.json
```

## Latest Secret Resolution Pass

Implemented the next response to the "demo annotations are not production wiring" critique.

What changed:

- Added `DataSource` and `DerivedView` GVKs to the Go controller.
- The job-backed reconciler now resolves:
  - `Invariant.spec.derivedViewRef`
  - `DerivedView.spec.sourceRef`
  - `DataSource.spec.connectionSecret`
  - `DerivedView.spec.target.connectionSecret`
  - `DerivedView.spec.pipeline.connectionSecret`
- Checker and repair Jobs now receive connection variables through `valueFrom.secretKeyRef` when CRD resources provide Secrets:
  - `POSTGRES_DSN`
  - `OPENSEARCH_URL`
  - `KAFKA_BOOTSTRAP_SERVERS`
- Non-secret routing still comes from CRDs:
  - `ORDERS_INDEX`
  - `ORDER_EVENTS_TOPIC`
- Added CRD fields for explicit secret key names:
  - `DataSource.spec.connectionSecretKey`, default `dsn`
  - `DerivedView.spec.target.connectionSecretKey`, default `url`
  - `DerivedView.spec.pipeline.bootstrapServersKey`, default `bootstrapServers`
- Added demo Secrets to `examples/commerce-consistency.yaml`.
- Updated operator RBAC to read `datasources` and `derivedviews`.
- Kept annotation/default connection values as a local-development fallback.

What this fixes:

```text
DataSource and DerivedView are now runtime inputs to the operator, not just documentation sketches.
Connection credentials no longer need to be copied into annotations for the Kubernetes path.
Credential values do not enter CRD status, report ConfigMaps, or operator logs through this resolver.
```

What remains intentionally open:

```text
Secret rotation behavior is delegated to the next scheduled Job; there is no immediate restart/refresh mechanism.
Report-store credentials still rely on pod environment/platform identity rather than a ReportStore CRD.
```

Verification for this Secret-resolution pass:

```text
PYTHONPATH=src python3 -m unittest discover -s tests
  Passed: 24 tests

python3 -m py_compile $(find src -name '*.py' | sort)
  Passed

go test ./...
  Passed

ruby YAML parse for Compose, CRDs, operator manifest, and examples
  Passed

docker compose build dataguard
  Passed

go build -o /tmp/dataguard-operator ./cmd/dataguard-operator
  Passed

kubectl apply -f k8s/crds && kubectl apply -f examples/commerce-consistency.yaml
  Passed: demo Secrets, DataSource, DerivedView, Invariants, and RepairPolicies applied

local /tmp/dataguard-operator run against kind
  Passed: created checker Jobs and patched paid-orders-indexed + paid-orders-query-check to Healthy

kubectl get job dataguard-check-paid-orders-indexed-g12-t5938044 -o jsonpath=...
  Passed: POSTGRES_DSN came from orders-postgres-secret/dsn
  Passed: KAFKA_BOOTSTRAP_SERVERS came from orders-kafka-secret/bootstrapServers
  Passed: OPENSEARCH_URL came from orders-opensearch-secret/url
  Passed: ORDERS_INDEX and ORDER_EVENTS_TOPIC remained non-secret CRD routing values

Linux/arm64 operator image from verified binary loaded into kind
  Passed: deployment kubedataguard-operator successfully rolled out and became Ready 1/1

Note: the standard `Dockerfile.operator` legacy Docker build path hit local disk/legacy-builder issues during this pass. The live kind rollout used a Linux/arm64 operator binary built with Go and packaged into `kubedataguard-operator:latest`.
```

## Latest Watch Fan-Out Pass

Implemented the next control-plane wiring step after Secret-backed connection resolution.

What changed:

- The Go controller now watches `DataSource` resources and maps a changed source to affected `Invariant`s through `DerivedView.spec.sourceRef`.
- The Go controller now watches `DerivedView` resources and maps a changed derived view directly to `Invariant.spec.derivedViewRef`.
- Fan-out returns deterministic reconcile requests and deduplicates affected invariants.
- Added unit tests proving:
  - a `DerivedView` update reconciles only invariants that reference that view
  - a `DataSource` update reconciles invariants attached to all derived views fed by that source
  - unrelated derived views and invariants are not enqueued

What this fixes:

```text
DataSource and DerivedView are not only runtime inputs; they are watched topology resources.
A connection/topology change can wake the affected consistency checks without editing each Invariant.
The operator is closer to the Kubernetes model: related desired-state objects now drive reconciliation.
```

What remains intentionally open:

```text
Secret rotation does not immediately enqueue dependent Invariants because the operator does not watch Secrets.
RepairPolicy changes are still picked up on the next Invariant reconcile or scheduled check.
Report-store credentials still rely on pod environment/platform identity rather than a ReportStore CRD.
```

Verification for this watch fan-out pass:

```text
PYTHONPATH=src python3 -m unittest discover -s tests
  Passed: 24 tests

python3 -m py_compile $(find src -name '*.py' | sort)
  Passed

go test ./...
  Passed

ruby YAML parse for Compose, CRDs, operator manifest, and examples
  Passed

go build -o /tmp/dataguard-operator ./cmd/dataguard-operator
  Passed
```

## Latest Kubernetes-Native Demo Stack Pass

Implemented the next phase after topology watch fan-out: remove the remaining `host.docker.internal` dependency from the intended kind demo.

What changed:

- Added `k8s/demo-stack.yaml` with in-cluster demo deployments and Services for:
  - `dataguard-postgres`
  - `dataguard-redpanda`
  - `dataguard-opensearch`
- Updated `examples/commerce-consistency.yaml` Secrets to use in-cluster service DNS:
  - `postgresql://dataguard:dataguard@dataguard-postgres:5432/dataguard`
  - `http://dataguard-opensearch:9200`
  - `dataguard-redpanda:9092`
- Added Makefile targets:
  - `kind-create`
  - `k8s-demo-images`
  - `k8s-demo-stack`
  - `k8s-demo-resources`
  - `k8s-demo-seed`
  - `k8s-demo-drift`
  - `k8s-demo-force-drift-check`
  - `k8s-demo-reset-slo`
  - `k8s-demo-wait-checks`
  - `k8s-demo-status`
  - `k8s-demo`
- The seed and drift steps now run from Kubernetes pods using the same checker image and in-cluster service DNS.
- Fixed two manifest issues found during live validation:
  - OpenSearch port names must be at most 15 characters.
  - Redpanda must use the image entrypoint with `redpanda start` arguments, matching the Compose path.

What this fixes:

```text
The intended Kubernetes demo no longer depends on Compose services reachable through host.docker.internal.
The DataSource, DerivedView, Invariant, RepairPolicy, checker Jobs, operator, and demo data systems can all live in one kind cluster.
The demo is closer to a real platform control-plane proof, while still using ephemeral demo storage.
```

What remains intentionally open:

```text
The demo data systems use emptyDir storage and single-node deployments; this is not a production database/operator install.
The full runtime proof still depends on local Docker/Colima availability.
Production repair strategies beyond owner-executed events and unsafe demo reindex remain future work.
```

Runtime verification for this Kubernetes-native demo pass:

```text
kind delete cluster --name kubedataguard && make k8s-demo
  Passed

Data stack rollout:
  dataguard-postgres: Ready 1/1
  dataguard-redpanda: Ready 1/1
  dataguard-opensearch: Ready 1/1

In-cluster seed pod:
  init --reset succeeded
  generated 50 orders

In-cluster drift pod:
  processed 50 events
  indexed 42 documents
  deliberately skipped 8 paid orders

Operator/checker result:
  paid-orders-indexed: DriftDetected, driftCount=8
  paid-orders-aggregate: DriftDetected, driftCount=2
  paid-orders-freshness: Healthy, driftCount=0
  paid-orders-query-check: Healthy, driftCount=0

All checker Jobs completed after the final status settled.
```

## First Build Target

Runtime-verify the local runnable demo:

```text
Postgres orders table
Demo API/order generator
Redpanda topic
Indexer worker
OpenSearch index
Consistency checker
Repair command
```

Invariant:

```text
Every paid order in Postgres must exist in OpenSearch within 60 seconds.
```

## Verification Completed

- `PYTHONPATH=src python3 -m unittest discover -s tests`
  - Passed: 16 tests
- `go test ./...`
  - Passed
- `go build ./cmd/dataguard-operator`
  - Passed
- `python3 -m py_compile $(find src -name '*.py' | sort)`
  - Passed
- `PYTHONPATH=src python3 -m dataguard.cli demo-local --count 10 --skip-every-paid 5`
  - Passed
  - Detected 2 missing orders and 1 stale document
  - Detected aggregate count and revenue mismatch
  - Repaired 3 records from source truth
  - Verified existence and aggregate invariants healthy
- In-memory Markdown rendering for local proof report
  - Passed
- Ruby YAML parsing for Compose, CRDs, and examples
  - Passed
- `docker run --rm hello-world`
  - Passed
- `make up`
  - Passed
- `make init`
  - Passed after replacing the protected `Documents` bind mount with `${HOME}/.kubedataguard/reports`
- `make demo-drift`
  - Passed
  - Generated 50 orders
  - Indexed 42 events
  - Deliberately skipped 8 paid orders
  - Existence check reported `DriftDetected` with 8 missing orders
- `make check-aggregate`
  - Passed before repair
  - Reported aggregate count drift: source 41, target 33
  - Reported aggregate revenue drift
- `make demo-repair`
  - Passed
  - Repaired 8 missing orders
  - Post-repair existence verification was `Healthy`
- `make check-aggregate`
  - Passed after repair
  - Post-repair aggregate verification was `Healthy`
- `kind create cluster --name kubedataguard --wait 180s`
  - Passed
- `kubectl apply -f k8s/crds`
  - Passed
- `kubectl apply -f examples/commerce-consistency.yaml`
  - Passed
- Local operator run against kind:
  - Passed
  - Reconciled sample invariants to `Healthy`
  - Reconciled synthetic drift annotation to `DriftDetected`
- `docker compose build dataguard`
  - Passed
- `kind load docker-image kubedataguard-dataguard:latest --name kubedataguard`
  - Passed
- Job-backed operator run against kind:
  - Passed
  - Created checker Jobs for `paid-orders-indexed` and `paid-orders-aggregate`
  - Checker Jobs connected from kind to Compose Postgres, Redpanda, and OpenSearch through `host.docker.internal`
  - Checker Jobs wrote compact ConfigMaps
  - Operator patched `Invariant.status` from ConfigMap status
  - Drifted data produced `DriftDetected`
  - Repaired data produced `Healthy`
- Repair-backed operator run against kind:
  - Passed
  - Drifted data triggered `dataguard-repair-paid-orders-indexed-g5`
  - Repair Job reindexed 8 missing OpenSearch documents from Postgres
  - Repair Job wrote `dataguard-repair-report-paid-orders-indexed-g5`
  - Operator patched `paid-orders-indexed` to `Healthy`
  - Fresh aggregate check after repair produced `Healthy`
  - Restored default `maxLagSeconds=60` SLO and both invariants remained `Healthy`
- Failure taxonomy run against kind:
  - Passed
  - Unreachable Postgres checker generation produced `CheckFailed`
  - Restored checker generation produced `Healthy`
  - Temporary aggregate reindex repair policy produced `RepairFailed` after verification still found drift
  - Removed temporary policy and restored both invariants to `Healthy`
- Bounded freshness run:
  - Passed
  - `make check-freshness` produced `Healthy` bounded-freshness report with `sourceLSN`
  - kind operator created `dataguard-check-paid-orders-freshness-g3`
  - `paid-orders-freshness` ended `Healthy` with 41 checked records
  - status included `sourceLSN`, `sourceWatermark`, `streamOffsetStart`, and `streamOffsetEnd`
- Freshness drift/repair run:
  - Passed
  - `make demo-freshness-drift` injected 5 current freshness violations
  - `make repair-freshness` reindexed those 5 orders and verified freshness `Healthy`
  - post-repair report preserved `slo_breach_count=5`
  - kind operator generation 4 detected 5 freshness violations
  - operator selected `reindex-stale-paid-orders`
  - repair Job produced `Healthy`, `driftCount=0`, `sloBreachCount=5`
  - restored generation 5 default SLO produced `Healthy`, `checkedRecords=41`, `driftCount=0`
- Architecture hardening run:
  - Passed
  - generic query invariant against Compose produced `Healthy`, `checked_records=41`, `drift_count=0`
  - `docker compose build dataguard` passed
  - `make operator-image` passed
  - in-cluster `kubedataguard-operator` Deployment rolled out
  - scheduled checker Jobs used `checkID` values like `g1-t5937886`
  - all four invariants were `Healthy`
  - query report ConfigMap contained compact `status.json` and `repair-input.json` keys
- Kubernetes-native demo stack run:
  - Passed from a fresh kind cluster
  - Postgres, Redpanda, and OpenSearch ran inside kind
  - seed and drift worker pods ran inside kind
  - operator checker Jobs used in-cluster service DNS from DataSource/DerivedView Secrets
  - `paid-orders-indexed` reported `DriftDetected`, `driftCount=8`
  - `paid-orders-aggregate` reported `DriftDetected`, `driftCount=2`

## Verification Not Completed

- A Kubernetes-native Postgres/Redpanda/OpenSearch demo stack has been added; production-grade stateful storage and Helm/operator installs are intentionally out of scope for the demo.
- Source-of-truth reindex repair is implemented for existence and freshness drift. Broader repair strategies such as Kafka replay, cache invalidation, and aggregate backfill are not implemented yet.
- Secret-backed worker env wiring and DataSource/DerivedView watch fan-out are implemented; immediate Secret rotation fan-out is not implemented yet.
- The generic query runner is intentionally limited to Postgres source queries and OpenSearch JSON target queries.
- S3/MinIO-compatible report persistence is implemented; retention, encryption, lifecycle policy, and compaction are still open.

## Intermediate Roadmap Before Mature Operator

Do not jump directly from Compose verification to a full controller. Build these layers first:

1. Runtime-verify the local demo.
   - No-Docker proof: implemented and verified.
   - Compose proof: implemented and verified with Postgres, Redpanda, OpenSearch, drift detection, aggregate detection, and repair.
   - Runtime wiring issue found and fixed: Colima cannot bind-mount the protected macOS `Documents` path, so reports are mounted from `${HOME}/.kubedataguard/reports`.

2. Stabilize report persistence.
   - For the local CLI, keep writing JSON/Markdown reports.
   - For Compose on this machine, reports are written through `${HOME}/.kubedataguard/reports:/workspace/reports`.
   - For Kubernetes, ConfigMaps now carry compact `status.json` and `repair-input.json`.
   - For Kubernetes, store summary fields in the `Invariant.status` subresource:
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

3. Expand second invariant type: aggregate check.
   - Implemented: paid order count and paid revenue total in Postgres must match OpenSearch aggregation.
   - Next: add aggregate-specific drift scenarios and repair guidance.

4. Add failure taxonomy tests.
   - Simulate source unavailable, target unavailable, checker crash, repair retry, duplicate repair, stale target document, and corrupted target document.

5. Add lag-aware freshness.
   - Track source `updated_at`, event publish time, index time, and observed lag.
   - This maps directly to DDIA replication lag and becomes the more academic invariant.

6. Add a second source/derived pair.
   - Recommended: Postgres -> Redis cache freshness, or Postgres -> ClickHouse aggregate table.
   - Purpose: prove `DataSource`, `DerivedView`, `Invariant`, and `RepairPolicy` are real abstractions, not accidental names for the OpenSearch demo.

7. Build the Kubernetes controller.
   - Initial synthetic reconciler: implemented.
   - Job-backed checker reconciliation: implemented and runtime-verified.
   - Repair Job reconciliation from `RepairPolicy`: implemented and runtime-verified for `reindex-records`.
   - Scheduled checker Jobs: implemented.
   - Operator image/deployment sketch: implemented.
   - Next: emit metrics and events.

## Research-Backed Model Upgrades

The paper set changes the project from a useful demo into a credible data-correctness control plane.

## Latest CDC Frontier Pass

Implemented now:

- Added Kafka publish metadata to the Postgres outbox:
  - topic
  - partition
  - offset
- Added OpenSearch indexing metadata for events consumed from Kafka:
  - event id
  - event type
  - topic
  - partition
  - offset
- Added `cdc_frontier` to report observation windows and `cdcFrontier` to Kubernetes status.
- Added a bounded-window proof builder that classifies the frontier as:
  - `bounded`
  - `partial`
  - `unavailable`
- The proof combines:
  - Postgres WAL LSN
  - source timestamp watermark
  - eligible outbox count
  - published/unpublished outbox count
  - per-partition published outbox offsets
  - Kafka beginning/end offsets
  - OpenSearch applied-offset evidence
  - target read time
- Added unit tests for:
  - preserving CDC frontier evidence in reports and Kubernetes status
  - bounded frontier when outbox, Kafka, and target evidence cover the same events
  - partial frontier when outbox events are unpublished
  - partial frontier when Kafka offsets lag outbox offsets
  - partial frontier when target applied offsets lag outbox offsets
  - partial frontier when published outbox events have no Kafka offset evidence

Runtime-verified against the existing kind demo stack:

```text
reseeding produced 50 order events
drift injection processed 50 events
indexer wrote 42 documents
indexer deliberately skipped 8 paid events after committing Kafka offsets

paid-orders-indexed:
  generation: 4
  observedGeneration: 4
  phase: DriftDetected
  driftCount: 8
  cdcFrontier.status: bounded
  source.lsn: present
  outbox.published_count: 50
  outbox.unpublished_count: 0
  outbox.offset_recorded_count: 50
  stream.event_count: 100
  stream.offset_end: 100
  target.offset_recorded_count: 42
  target.max_applied_offset: 99

paid-orders-aggregate:
  generation: 4
  observedGeneration: 4
  phase: DriftDetected
  driftCount: 2
  cdcFrontier.status: bounded

paid-orders-freshness:
  generation: 3
  observedGeneration: 3
  phase: DriftDetected
  driftCount: 8
```

Important interpretation:

```text
The frontier being bounded does not mean the data is healthy.
It means the moving-target excuse has been reduced: eligible source events were published, Kafka exposed the offsets, and the target had visible applied-offset evidence up to the frontier.
The invariant still reports drift because eight paid orders are missing from OpenSearch.
```

Still not full DBLog:

- no Postgres logical decoding slot
- no exact snapshot-plus-log handoff protocol
- no crash-exact resume after checker failure
- no Merkle/checksum narrowing for huge histories
- no proof that max target offset implies gap-free application for arbitrary derived views

### CDC Evidence

DBLog makes watermarks and replay boundaries non-optional for the mature design.

The first commerce path now carries these fields in `observation_window.cdc_frontier`. The mature design still needs to generalize them beyond the demo pipeline:

- `sourceWatermark`
- `sourceSnapshotStartedAt`
- `sourceSnapshotFinishedAt`
- `streamTopic`
- `streamPartition`
- `streamOffset`
- `consumerGroup`
- `observedLagSeconds`
- `replayScope`

These fields let the project prove which part of source history was checked or repaired.

### Consistency Semantics

The consistency-model papers imply that each invariant should name its guarantee:

- `existence`
- `fieldEquality`
- `aggregate`
- `boundedFreshness`
- `readYourWrites`
- `monotonicReads`
- `causalVisibility`

The local MVP should implement them in this order:

1. `existence`
2. `aggregate`
3. `boundedFreshness`
4. session/client guarantees later

### Counterexample Reports

Elle implies that failed checks should produce a small, explanatory counterexample history.

For KubeDataGuard, that means a violation should include:

- the source row id and version
- the source commit or update time
- the event topic and offset if available
- the expected derived key
- the observed derived state
- the exact SLO window that was violated

### Spec/Status Discipline

GITER and the Kubernetes operator pattern reinforce this split:

- `spec`: desired data-correctness promise
- `status`: compact observed truth
- external report: durable evidence and counterexamples
- repair policy: allowed reconciliation action

## Failure Taxonomy

KubeDataGuard must be correct about its own behavior before it can credibly monitor data correctness.

### Pipeline/Data Failures

- Missing derived record: source row exists but target document is absent.
- Stale derived record: target document exists but fields differ from source.
- Duplicate derived record: target has more than one representation for a business key.
- Aggregate mismatch: counts or totals differ between source and derived stores.
- Replication lag: target is behind beyond the declared SLO.
- Schema/mapping rejection: target rejects a record due to incompatible field shape.

### Checker Failures

- Source unavailable during scan.
- Target unavailable during scan.
- Checker crashes mid-scan.
- Partial scan produces an incomplete report.
- Slow scan exceeds the check interval.
- Credentials or Kubernetes Secret are missing/rotated.

Expected behavior:

- Do not mark the invariant healthy from an incomplete check.
- Record check failure separately from data drift.
- Preserve the previous known-good status until a complete check finishes.
- Emit enough error detail for a human to distinguish "checker failed" from "data is inconsistent."

### Repair Failures

- Repair action crashes midway.
- Repair writes duplicate documents.
- Repair updates target from stale source data.
- Repair succeeds but verification still fails.
- Repair loops repeatedly without reducing drift.

Expected behavior:

- Repairs must be idempotent.
- Every repair must be followed by verification.
- Failed repair should update status and stop before uncontrolled retries.
- High-risk repairs should eventually require approval.

## Next Actions

1. Expand failure taxonomy coverage:
   - target unavailable
   - checker timeout
   - repair Job crash before report publication
   - repair retry/idempotency
   - stale/corrupt target document
2. Replace demo Deployments with production-grade stateful installs for Postgres, Redpanda, and OpenSearch.
3. Add repair strategies beyond source-of-truth reindexing:
   - Kafka replay
   - Redis invalidation
   - ClickHouse aggregate backfill
4. Add a second source/derived pair to prove the CRD abstraction beyond OpenSearch.
5. Add Secret rotation fan-out or document that scheduled checks are the refresh boundary.
6. Add Prometheus/OpenTelemetry metrics and traces for checks, repairs, frontier status, and SLO breaches.
7. Harden report retention, encryption, lifecycle policy, and compaction.

## Open Questions

### Blocking

Current answer:

- Store compact truth in `Invariant.status`.
- Store verbose JSON/Markdown reports externally.
- Controller ConfigMaps now carry compact `status.json` plus `repair-input.json`.
- `reportRef` now points at the worker report store path for full evidence reports.
- S3/MinIO-compatible report and checkpoint storage is implemented.
- Secret-backed DataSource/DerivedView connection resolution is implemented for worker Jobs.
- DataSource/DerivedView watch fan-out to dependent Invariants is implemented.
- Next blocking decisions: retention/lifecycle policy, encryption posture, and Secret rotation semantics.

### Next Sprint

- What is the next invariant type?

Current answer:

- Aggregate checks are now implemented for paid-order count and revenue total.
- Lag-aware freshness is implemented for the commerce demo.
- First generic Postgres/OpenSearch query invariant is implemented.
- First keyset-paginated source scan path is implemented.
- Persisted scan checkpoints are implemented for bounded keyset scans.
- Secret-backed `DataSource` and `DerivedView` resolution is implemented for the job-backed operator.
- `DataSource` and `DerivedView` topology changes now enqueue affected `Invariant`s.
- Next: production-grade stateful installs, checksum/Merkle narrowing, and a second source/derived pair.

### Future

- Should the first full operator be Go/controller-runtime or Python/Kopf?

Closed answer:

- Use Go/controller-runtime for the real operator.
- Keep Python for the local demo and fast invariant experimentation.

## Knowledge Checkpoint

The important idea to preserve:

```text
KubeDataGuard is not monitoring uptime. It is monitoring whether derived data is still correct relative to declared consistency SLOs.
```
