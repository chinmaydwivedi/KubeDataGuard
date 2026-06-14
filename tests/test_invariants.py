import os
import json
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from dataguard.invariants import (
    compare_paid_order_aggregates,
    compare_paid_order_freshness,
    compare_paid_orders_to_index,
    compare_query_results,
    summarize_paid_orders,
)
from dataguard.cli import build_repair_job_result
from dataguard.checker import build_cdc_frontier
from dataguard.checkpoints import (
    load_scan_checkpoint,
    save_scan_checkpoint,
    update_checkpoint_report_ref,
)
from dataguard.db import execute_source_query_keyset
from dataguard.local_demo import run_local_proof
from dataguard.observability import prometheus_metrics
from dataguard.repair import repair_from_payload
from dataguard.reporting import write_report_artifacts
from dataguard.search import execute_target_query


class PaidOrdersIndexedInvariantTests(unittest.TestCase):
    def test_healthy_when_every_paid_order_matches(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            }
        ]
        indexed = {"order-1": dict(source[0])}

        report = compare_paid_orders_to_index(source, indexed)

        self.assertTrue(report.healthy)
        self.assertEqual(report.drift_count, 0)
        self.assertEqual(report.status, "Healthy")
        self.assertEqual(report.guarantee, "existence+fieldEquality")

    def test_missing_when_paid_order_is_absent(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            }
        ]

        report = compare_paid_orders_to_index(source, {})

        self.assertFalse(report.healthy)
        self.assertEqual(report.drift_count, 1)
        self.assertEqual(report.missing[0]["order_id"], "order-1")
        self.assertEqual(report.status, "DriftDetected")
        self.assertEqual(report.counterexamples[0]["type"], "missing")

    def test_observation_window_preserves_stream_offsets(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            }
        ]

        report = compare_paid_orders_to_index(
            source,
            {},
            stream_topic="orders.events",
            stream_offset_start=10,
            stream_offset_end=15,
        )
        window = report.to_dict()["observation_window"]

        self.assertEqual(window["stream_topic"], "orders.events")
        self.assertEqual(window["stream_offset_start"], 10)
        self.assertEqual(window["stream_offset_end"], 15)

    def test_observation_window_preserves_cdc_frontier(self):
        cdc_frontier = {
            "mode": "postgres-wal+outbox+kafka",
            "status": "bounded",
            "source": {"lsn": "0/16B6C50"},
        }
        report = compare_paid_orders_to_index(
            [{"id": "order-1", "status": "paid", "amount_cents": 1200, "currency": "USD", "version": 1}],
            {},
            cdc_frontier=cdc_frontier,
        )

        payload = report.to_dict()
        self.assertEqual(payload["observation_window"]["cdc_frontier"]["status"], "bounded")
        self.assertEqual(
            payload["kubernetes_status"]["observationWindow"]["cdcFrontier"]["source"]["lsn"],
            "0/16B6C50",
        )

    def test_report_includes_operator_status_payload(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            }
        ]

        report = compare_paid_orders_to_index(source, {})
        status = report.kubernetes_status(report_ref="reports/latest.json")

        self.assertFalse(status["healthy"])
        self.assertEqual(status["phase"], "DriftDetected")
        self.assertEqual(status["guarantee"], "existence+fieldEquality")
        self.assertEqual(status["driftCount"], 1)
        self.assertEqual(status["counterexampleCount"], 1)
        self.assertEqual(status["reportRef"], "reports/latest.json")
        self.assertIn("checkedAt", status["observationWindow"])
        self.assertIn("maxLagSeconds", status["observationWindow"])

    def test_report_artifacts_include_compact_report_and_apply_retention(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            }
        ]
        report = compare_paid_orders_to_index(source, {})

        with TemporaryDirectory() as tmpdir:
            report_dir = Path(tmpdir)
            old_json = report_dir / "drift-20000101T000000Z.json"
            old_markdown = report_dir / "drift-20000101T000000Z.md"
            old_compact = report_dir / "drift-20000101T000000Z.compact.json"
            old_json.write_text("{}", encoding="utf-8")
            old_markdown.write_text("# old", encoding="utf-8")
            old_compact.write_text("{}", encoding="utf-8")
            for path in [old_json, old_markdown, old_compact]:
                os.utime(path, (946684800, 946684800))
            settings = SimpleNamespace(
                report_dir=report_dir,
                report_store="local",
                report_retention_count=1,
                report_retention_days=30,
            )

            artifacts = write_report_artifacts(settings, report)
            compact = json.loads(artifacts.compact_path.read_text(encoding="utf-8"))

            self.assertTrue(artifacts.compact_path.exists())
            self.assertEqual(compact["status"], "DriftDetected")
            self.assertFalse(old_json.exists())
            self.assertFalse(old_markdown.exists())
            self.assertFalse(old_compact.exists())

    def test_stale_when_indexed_document_differs(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 2,
            }
        ]
        indexed = {
            "order-1": {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 900,
                "currency": "USD",
                "version": 1,
            }
        }

        report = compare_paid_orders_to_index(source, indexed)

        self.assertFalse(report.healthy)
        fields = {mismatch["field"] for mismatch in report.stale[0]["mismatches"]}
        self.assertEqual(fields, {"amount_cents", "version"})
        self.assertEqual(report.counterexamples[0]["type"], "stale")

    def test_summarizes_paid_orders_for_aggregate_checks(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
                "updated_at": "2026-06-12T00:00:00+00:00",
            },
            {
                "id": "order-2",
                "status": "paid",
                "amount_cents": 800,
                "currency": "USD",
                "version": 1,
                "updated_at": "2026-06-12T00:01:00+00:00",
            },
        ]

        summary = summarize_paid_orders(source)

        self.assertEqual(summary["count"], 2)
        self.assertEqual(summary["total_amount_cents"], 2000)
        self.assertEqual(summary["source_watermark"], "2026-06-12T00:01:00+00:00")

    def test_aggregate_mismatch_reports_counterexample(self):
        report = compare_paid_order_aggregates(
            {
                "count": 2,
                "total_amount_cents": 2000,
                "source_watermark": "2026-06-12T00:01:00+00:00",
            },
            {
                "count": 1,
                "total_amount_cents": 1200,
            },
        )

        self.assertFalse(report.healthy)
        self.assertEqual(report.invariant, "paid-orders-aggregate")
        self.assertEqual(report.guarantee, "aggregate")
        self.assertEqual(report.drift_count, 2)
        fields = {item["field"] for item in report.aggregate_mismatches}
        self.assertEqual(fields, {"count", "total_amount_cents"})
        self.assertEqual(report.counterexamples[0]["type"], "aggregate_mismatch")

    def test_freshness_detects_missing_and_stale_derived_updates(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
                "updated_at": "2026-06-12T12:00:00+00:00",
            },
            {
                "id": "order-2",
                "status": "paid",
                "amount_cents": 800,
                "currency": "USD",
                "version": 1,
                "updated_at": "2026-06-12T12:00:10+00:00",
            },
        ]
        indexed = {
            "order-2": {
                **source[1],
                "indexed_at": "2026-06-12T12:00:05Z",
            }
        }

        report = compare_paid_order_freshness(
            source,
            indexed,
            max_lag_seconds=60,
            checked_at=datetime.fromisoformat("2026-06-12T12:03:00+00:00"),
            stream_topic="orders.events",
            stream_offset_start=10,
            stream_offset_end=20,
            source_lsn="0/16B6C50",
        )

        self.assertFalse(report.healthy)
        self.assertEqual(report.invariant, "paid-orders-freshness")
        self.assertEqual(report.guarantee, "boundedFreshness")
        self.assertEqual(report.drift_count, 2)
        self.assertEqual(len(report.missing), 0)
        self.assertEqual(len(report.freshness_violations), 2)
        self.assertEqual(report.freshness_violations[0]["observed_lag_seconds"], 180)
        self.assertEqual(report.freshness_violations[1]["observed_lag_seconds"], -5)
        self.assertEqual(report.counterexamples[-1]["type"], "freshness_lag")

        window = report.to_dict()["observation_window"]
        self.assertEqual(window["source_lsn"], "0/16B6C50")
        self.assertEqual(window["stream_offset_start"], 10)
        self.assertEqual(window["stream_offset_end"], 20)

    def test_freshness_records_late_observation_as_slo_breach_not_current_drift(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
                "updated_at": "2026-06-12T12:00:00+00:00",
            }
        ]
        indexed = {
            "order-1": {
                **source[0],
                "indexed_at": "2026-06-12T12:02:00Z",
            }
        }

        report = compare_paid_order_freshness(source, indexed, max_lag_seconds=60)
        payload = report.to_dict()

        self.assertTrue(report.healthy)
        self.assertEqual(report.drift_count, 0)
        self.assertEqual(len(report.freshness_violations), 0)
        self.assertEqual(len(report.freshness_breaches), 1)
        self.assertEqual(payload["slo_breach_count"], 1)
        self.assertEqual(payload["kubernetes_status"]["sloBreachCount"], 1)

    def test_freshness_is_healthy_when_indexed_inside_lag_window(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
                "updated_at": "2026-06-12T12:00:00+00:00",
            }
        ]
        indexed = {
            "order-1": {
                **source[0],
                "indexed_at": "2026-06-12T12:00:30Z",
            }
        }

        report = compare_paid_order_freshness(source, indexed, max_lag_seconds=60)

        self.assertTrue(report.healthy)
        self.assertEqual(report.drift_count, 0)

    def test_query_results_detect_missing_and_field_mismatch(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            },
            {
                "id": "order-2",
                "status": "paid",
                "amount_cents": 800,
                "currency": "USD",
                "version": 2,
            },
        ]
        target = [
            {
                "id": "order-2",
                "status": "paid",
                "amount_cents": 700,
                "currency": "USD",
                "version": 1,
            }
        ]

        report = compare_query_results(
            invariant_name="paid-orders-query-check",
            source_rows=source,
            target_rows=target,
            key_field="id",
            compare_fields=["status", "amount_cents", "currency", "version"],
            guarantee="existence+fieldEquality",
            source_scan={
                "mode": "keyset",
                "key_field": "id",
                "page_size": 1,
                "pages": 2,
                "rows": 2,
                "first_key": "order-1",
                "last_key": "order-2",
                "completed": True,
            },
        )

        self.assertFalse(report.healthy)
        self.assertEqual(report.invariant, "paid-orders-query-check")
        self.assertEqual(report.drift_count, 2)
        self.assertEqual(report.missing[0]["order_id"], "order-1")
        mismatch_fields = {item["field"] for item in report.stale[0]["mismatches"]}
        self.assertEqual(mismatch_fields, {"amount_cents", "version"})
        self.assertEqual(
            report.to_dict()["observation_window"]["source_scan"]["pages"],
            2,
        )
        self.assertEqual(
            report.kubernetes_status()["observationWindow"]["sourceScan"]["last_key"],
            "order-2",
        )

    def test_query_results_can_mark_resumed_scan_partial(self):
        report = compare_query_results(
            invariant_name="paid-orders-query-check",
            source_rows=[{"id": "order-2", "status": "paid"}],
            target_rows=[{"id": "order-2", "status": "paid"}],
            key_field="id",
            compare_fields=["status"],
            guarantee="existence+fieldEquality",
            source_scan={
                "mode": "keyset",
                "key_field": "id",
                "page_size": 1,
                "pages": 1,
                "rows": 1,
                "resume_after_key": "order-1",
                "completed": True,
            },
            check_status="partial",
        )

        payload = report.to_dict()
        self.assertFalse(report.healthy)
        self.assertEqual(payload["status"], "Unknown")
        self.assertEqual(payload["check_status"], "partial")
        self.assertEqual(payload["observation_window"]["completeness"], "partial")

    def test_cdc_frontier_is_bounded_when_outbox_and_kafka_cover_same_events(self):
        frontier = build_cdc_frontier(
            source_lsn="0/16B6C50",
            source_watermark="2026-06-12T12:00:00+00:00",
            outbox={
                "mode": "postgres-outbox",
                "event_count": 50,
                "published_count": 50,
                "unpublished_count": 0,
                "max_id": 50,
                "offset_recorded_count": 50,
                "published_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 50,
                        "max_published_offset": 49,
                    }
                ],
            },
            offsets={
                "stream_offset_start": 0,
                "stream_offset_end": 50,
                "stream_partitions": 1,
                "stream_event_count": 50,
                "stream_offsets": [{"partition": 0, "start": 0, "end": 50, "event_count": 50}],
            },
            stream_topic="orders.events",
            target_read_at=datetime.fromisoformat("2026-06-12T12:00:05+00:00"),
            target_frontier={
                "system": "opensearch",
                "index": "orders",
                "offset_recorded_count": 50,
                "applied_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 50,
                        "max_applied_offset": 49,
                    }
                ],
            },
        )

        self.assertEqual(frontier["status"], "bounded")
        self.assertEqual(frontier["stream"]["event_count"], 50)
        self.assertEqual(frontier["outbox"]["published_count"], 50)
        self.assertEqual(frontier["target"]["frontier"]["offset_recorded_count"], 50)

    def test_cdc_frontier_is_partial_when_outbox_has_unpublished_events(self):
        frontier = build_cdc_frontier(
            source_lsn="0/16B6C50",
            source_watermark="2026-06-12T12:00:00+00:00",
            outbox={
                "mode": "postgres-outbox",
                "event_count": 50,
                "published_count": 48,
                "unpublished_count": 2,
                "offset_recorded_count": 48,
                "published_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 48,
                        "max_published_offset": 47,
                    }
                ],
            },
            offsets={
                "stream_offset_start": 0,
                "stream_offset_end": 48,
                "stream_partitions": 1,
                "stream_event_count": 48,
                "stream_offsets": [],
            },
            stream_topic="orders.events",
            target_read_at=datetime.fromisoformat("2026-06-12T12:00:05+00:00"),
            target_frontier={
                "system": "opensearch",
                "index": "orders",
                "offset_recorded_count": 48,
                "applied_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 48,
                        "max_applied_offset": 47,
                    }
                ],
            },
        )

        self.assertEqual(frontier["status"], "partial")
        self.assertIn("unpublished", frontier["reason"])

    def test_cdc_frontier_is_partial_when_kafka_offsets_lag_outbox(self):
        frontier = build_cdc_frontier(
            source_lsn="0/16B6C50",
            source_watermark="2026-06-12T12:00:00+00:00",
            outbox={
                "mode": "postgres-outbox",
                "event_count": 50,
                "published_count": 50,
                "unpublished_count": 0,
                "offset_recorded_count": 50,
                "published_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 50,
                        "max_published_offset": 49,
                    }
                ],
            },
            offsets={
                "stream_offset_start": 0,
                "stream_offset_end": 40,
                "stream_partitions": 1,
                "stream_event_count": 40,
                "stream_offsets": [],
            },
            stream_topic="orders.events",
            target_read_at=datetime.fromisoformat("2026-06-12T12:00:05+00:00"),
            target_frontier={
                "system": "opensearch",
                "index": "orders",
                "offset_recorded_count": 40,
                "applied_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 40,
                        "max_applied_offset": 39,
                    }
                ],
            },
        )

        self.assertEqual(frontier["status"], "partial")
        self.assertIn("Kafka topic exposes 40 events", frontier["reason"])

    def test_cdc_frontier_is_partial_when_published_events_lack_offsets(self):
        frontier = build_cdc_frontier(
            source_lsn="0/16B6C50",
            source_watermark="2026-06-12T12:00:00+00:00",
            outbox={
                "mode": "postgres-outbox",
                "event_count": 50,
                "published_count": 50,
                "unpublished_count": 0,
                "offset_recorded_count": 48,
                "published_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 48,
                        "max_published_offset": 47,
                    }
                ],
            },
            offsets={
                "stream_offset_start": 0,
                "stream_offset_end": 50,
                "stream_partitions": 1,
                "stream_event_count": 50,
                "stream_offsets": [{"partition": 0, "start": 0, "end": 50, "event_count": 50}],
            },
            stream_topic="orders.events",
            target_read_at=datetime.fromisoformat("2026-06-12T12:00:05+00:00"),
            target_frontier={
                "system": "opensearch",
                "index": "orders",
                "offset_recorded_count": 48,
                "applied_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 48,
                        "max_applied_offset": 47,
                    }
                ],
            },
        )

        self.assertEqual(frontier["status"], "partial")
        self.assertIn("no Kafka offset evidence", frontier["reason"])

    def test_cdc_frontier_is_partial_when_target_offsets_lag_outbox(self):
        frontier = build_cdc_frontier(
            source_lsn="0/16B6C50",
            source_watermark="2026-06-12T12:00:00+00:00",
            outbox={
                "mode": "postgres-outbox",
                "event_count": 50,
                "published_count": 50,
                "unpublished_count": 0,
                "offset_recorded_count": 50,
                "published_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 50,
                        "max_published_offset": 49,
                    }
                ],
            },
            offsets={
                "stream_offset_start": 0,
                "stream_offset_end": 50,
                "stream_partitions": 1,
                "stream_event_count": 50,
                "stream_offsets": [{"partition": 0, "start": 0, "end": 50, "event_count": 50}],
            },
            stream_topic="orders.events",
            target_read_at=datetime.fromisoformat("2026-06-12T12:00:05+00:00"),
            target_frontier={
                "system": "opensearch",
                "index": "orders",
                "offset_recorded_count": 40,
                "applied_offsets": [
                    {
                        "topic": "orders.events",
                        "partition": 0,
                        "event_count": 40,
                        "max_applied_offset": 39,
                    }
                ],
            },
        )

        self.assertEqual(frontier["status"], "partial")
        self.assertIn("target partition 0 applied offset 39", frontier["reason"])

    def test_local_proof_detects_repairs_and_verifies(self):
        result = run_local_proof(count=10, skip_every_paid=5)
        payload = result.to_dict()

        self.assertTrue(result.passed)
        self.assertEqual(payload["report_type"], "kubedataguard-local-proof")
        self.assertEqual(payload["before"]["existence"]["status"], "DriftDetected")
        self.assertEqual(payload["after"]["existence"]["status"], "Healthy")
        self.assertEqual(payload["after"]["aggregate"]["status"], "Healthy")
        self.assertGreaterEqual(payload["repair"]["repaired"], 1)

    def test_repair_from_payload_reindexes_missing_and_stale_orders(self):
        settings = object()
        source_orders = [
            {"id": "order-1", "status": "paid"},
            {"id": "order-2", "status": "paid"},
        ]
        payload = {
            "missing": [{"order_id": "order-1"}],
            "stale": [{"order_id": "order-2"}],
        }

        fake_db = SimpleNamespace(fetch_orders_by_ids=Mock(return_value=source_orders))
        fake_search = SimpleNamespace(index_order=Mock())

        with patch.dict(
            "sys.modules",
            {
                "dataguard.db": fake_db,
                "dataguard.search": fake_search,
            },
        ):
            result = repair_from_payload(settings, payload, verbose=False)

        fake_db.fetch_orders_by_ids.assert_called_once_with(settings, ["order-1", "order-2"])
        self.assertEqual(fake_search.index_order.call_count, 2)
        self.assertEqual(result["candidate_order_ids"], ["order-1", "order-2"])
        self.assertEqual(result["repaired"], 2)

    def test_repair_from_payload_reindexes_freshness_violations(self):
        settings = object()
        source_orders = [{"id": "order-3", "status": "paid"}]
        payload = {
            "freshness_violations": [
                {
                    "order_id": "order-3",
                    "reason": "derived view has not observed the source update",
                }
            ],
            "freshness_breaches": [
                {
                    "order_id": "order-4",
                    "reason": "derived view observed the update late",
                }
            ],
        }

        fake_db = SimpleNamespace(fetch_orders_by_ids=Mock(return_value=source_orders))
        fake_search = SimpleNamespace(index_order=Mock())

        with patch.dict(
            "sys.modules",
            {
                "dataguard.db": fake_db,
                "dataguard.search": fake_search,
            },
        ):
            result = repair_from_payload(settings, payload, verbose=False)

        fake_db.fetch_orders_by_ids.assert_called_once_with(settings, ["order-3"])
        fake_search.index_order.assert_called_once()
        self.assertEqual(result["candidate_order_ids"], ["order-3"])
        self.assertEqual(result["repaired"], 1)

    def test_repair_from_payload_can_emit_reconcile_events_without_direct_writes(self):
        payload = {
            "missing": [{"order_id": "order-1"}],
            "stale": [{"order_id": "order-2"}],
        }

        with TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(report_dir=Path(tmpdir))
            result = repair_from_payload(
                settings,
                payload,
                mode="emit-reconcile-events",
                verbose=False,
            )

            event_path = Path(result["eventRef"].removeprefix("file://"))
            events = event_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(result["candidate_order_ids"], ["order-1", "order-2"])
        self.assertEqual(result["repair_mode"], "emit-reconcile-events")
        self.assertEqual(result["emitted"], 2)
        self.assertEqual(len(events), 2)
        self.assertIn('"action": "reconcile"', events[0])

    def test_repair_from_payload_can_emit_redis_invalidation_requests(self):
        payload = {"missing": [{"order_id": "order-1"}]}

        with TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(report_dir=Path(tmpdir))
            with patch.dict("os.environ", {"REDIS_KEY_TEMPLATE": "orders:{id}"}):
                result = repair_from_payload(
                    settings,
                    payload,
                    mode="invalidate-cache",
                    verbose=False,
                )

            event_path = Path(result["eventRef"].removeprefix("file://"))
            event = event_path.read_text(encoding="utf-8")

        self.assertEqual(result["repair_mode"], "invalidate-cache")
        self.assertIn('"action": "invalidate-cache"', event)
        self.assertIn('"key": "orders:order-1"', event)

    def test_repair_from_payload_can_emit_clickhouse_backfill_request(self):
        payload = {"missing": [{"order_id": "order-1"}]}

        with TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(report_dir=Path(tmpdir))
            with patch.dict("os.environ", {"CLICKHOUSE_BACKFILL_TABLE": "orders_rollup"}):
                result = repair_from_payload(
                    settings,
                    payload,
                    mode="clickhouse-backfill",
                    verbose=False,
                )

            request_path = Path(result["backfillRef"].removeprefix("file://"))
            request_payload = json.loads(request_path.read_text(encoding="utf-8"))

        self.assertEqual(result["repair_mode"], "clickhouse-backfill")
        self.assertEqual(result["table"], "orders_rollup")
        self.assertEqual(request_payload["ids"], ["order-1"])

    def test_repair_from_payload_can_publish_kafka_replay_requests(self):
        payload = {"missing": [{"order_id": "order-1"}]}
        settings = SimpleNamespace(order_events_topic="orders.events")
        fake_events = SimpleNamespace(
            publish_json=Mock(
                return_value={
                    "topic": "orders.reconcile",
                    "partition": 0,
                    "offset": 12,
                }
            )
        )

        with patch.dict("sys.modules", {"dataguard.events": fake_events}):
            with patch.dict("os.environ", {"REPAIR_KAFKA_TOPIC": "orders.reconcile"}):
                result = repair_from_payload(
                    settings,
                    payload,
                    mode="replay-kafka",
                    verbose=False,
                )

        fake_events.publish_json.assert_called_once()
        self.assertEqual(result["repair_mode"], "replay-kafka")
        self.assertEqual(result["published"], 1)
        self.assertEqual(result["topic"], "orders.reconcile")

    def test_repair_job_result_marks_failed_when_verification_still_drifts(self):
        verification = compare_paid_orders_to_index(
            [
                {
                    "id": "order-1",
                    "status": "paid",
                    "amount_cents": 1200,
                    "currency": "USD",
                    "version": 1,
                }
            ],
            {},
        )

        payload, status = build_repair_job_result(
            invariant_name="paid-orders-indexed",
            source_report_ref="configmap://default/source/report.json",
            repair_report_ref="configmap://default/repair/report.json",
            repair_result={"repaired": 0, "candidate_order_ids": ["order-1"]},
            repair_action="direct-reindex",
            verification=verification,
            observed_generation=9,
        )

        self.assertFalse(status["healthy"])
        self.assertEqual(status["phase"], "RepairFailed")
        self.assertEqual(status["reason"], "repair verification still found drift")
        self.assertEqual(status["driftCount"], 1)
        self.assertEqual(status["observedGeneration"], 9)
        self.assertEqual(payload["status"], "RepairFailed")
        self.assertEqual(payload["verification"]["status"], "DriftDetected")

    def test_prometheus_metrics_include_drift_and_cdc_frontier_status(self):
        payload = {
            "invariant": "paid-orders-indexed",
            "status": "DriftDetected",
            "guarantee": "existence+fieldEquality",
            "healthy": False,
            "drift_count": 8,
            "checked_records": 41,
            "counterexamples": [{"type": "missing"}],
            "observation_window": {
                "cdc_frontier": {"status": "bounded"},
            },
        }

        metrics = prometheus_metrics(payload)

        self.assertIn("kubedataguard_drift_count", metrics)
        self.assertIn('invariant="paid-orders-indexed"', metrics)
        self.assertIn('frontier_status="bounded"', metrics)

    def test_freshness_report_can_use_cache_specific_guarantee(self):
        report = compare_paid_order_freshness(
            [],
            {},
            invariant_name="paid-orders-redis-cache-freshness",
            guarantee="cacheFreshness",
        )

        self.assertEqual(report.invariant, "paid-orders-redis-cache-freshness")
        self.assertEqual(report.guarantee, "cacheFreshness")

    def test_execute_target_query_paginates_past_requested_size(self):
        settings = SimpleNamespace(orders_index="orders")

        class FakeOpenSearch:
            def __init__(self):
                self.requests = []

            def search(self, *, index, body):
                self.requests.append((index, body))
                if len(self.requests) == 1:
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_id": "order-1",
                                    "_source": {"status": "paid"},
                                    "sort": ["order-1", "order-1"],
                                }
                            ]
                        }
                    }
                if len(self.requests) == 2:
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_id": "order-2",
                                    "_source": {"id": "order-2", "status": "paid"},
                                    "sort": ["order-2", "order-2"],
                                }
                            ]
                        }
                    }
                return {"hits": {"hits": []}}

        fake_client = FakeOpenSearch()
        with patch("dataguard.search.client", return_value=fake_client):
            rows = execute_target_query(
                settings,
                '{"query":{"match_all":{}},"size":1}',
                key_field="id",
            )

        self.assertEqual([row["id"] for row in rows], ["order-1", "order-2"])
        self.assertEqual(len(fake_client.requests), 3)
        self.assertEqual(fake_client.requests[0][0], "orders")
        self.assertNotIn("search_after", fake_client.requests[0][1])
        self.assertEqual(fake_client.requests[1][1]["search_after"], ["order-1", "order-1"])
        self.assertIn("sort", fake_client.requests[0][1])

    def test_execute_source_query_keyset_scans_in_pages(self):
        settings = object()

        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.page = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params):
                self.conn.queries.append((query, params))
                self.page = self.conn.pages.pop(0)

            def fetchall(self):
                return self.page

        class FakeConnection:
            def __init__(self):
                self.pages = [
                    [{"id": "order-1", "status": "paid"}],
                    [{"id": "order-2", "status": "paid"}],
                    [],
                ]
                self.queries = []

            def cursor(self):
                return FakeCursor(self)

        fake_conn = FakeConnection()

        @contextmanager
        def fake_connect(_settings):
            yield fake_conn

        with patch("dataguard.db.connect", fake_connect):
            result = execute_source_query_keyset(
                settings,
                "select id, status from orders where status = 'paid'",
                key_field="id",
                page_size=1,
            )

        self.assertEqual([row["id"] for row in result.rows], ["order-1", "order-2"])
        self.assertEqual(result.evidence["mode"], "keyset")
        self.assertEqual(result.evidence["pages"], 2)
        self.assertEqual(result.evidence["rows"], 2)
        self.assertEqual(result.evidence["first_key"], "order-1")
        self.assertEqual(result.evidence["last_key"], "order-2")
        self.assertEqual(fake_conn.queries[0][1], (1,))
        self.assertEqual(fake_conn.queries[1][1], ("order-1", 1))
        self.assertIn("where \"id\" > %s", fake_conn.queries[1][0])

    def test_execute_source_query_keyset_can_stop_at_max_pages(self):
        settings = object()

        class FakeCursor:
            def __init__(self, conn):
                self.conn = conn
                self.page = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params):
                self.conn.queries.append((query, params))
                self.page = self.conn.pages.pop(0)

            def fetchall(self):
                return self.page

        class FakeConnection:
            def __init__(self):
                self.pages = [
                    [{"id": "order-1", "updated_at": datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)}],
                    [{"id": "order-2", "updated_at": datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc)}],
                    [{"id": "order-3", "updated_at": datetime(2026, 1, 1, 0, 0, 3, tzinfo=timezone.utc)}],
                ]
                self.queries = []

            def cursor(self):
                return FakeCursor(self)

        fake_conn = FakeConnection()
        callbacks = []

        @contextmanager
        def fake_connect(_settings):
            yield fake_conn

        with patch("dataguard.db.connect", fake_connect):
            result = execute_source_query_keyset(
                settings,
                "select id, updated_at from orders",
                key_field="id",
                page_size=1,
                max_pages=2,
                on_page=callbacks.append,
            )

        self.assertEqual([row["id"] for row in result.rows], ["order-1", "order-2"])
        self.assertEqual(len(fake_conn.queries), 2)
        self.assertEqual(len(callbacks), 2)
        self.assertFalse(result.evidence["completed"])
        self.assertEqual(result.evidence["stop_reason"], "max_pages")
        self.assertEqual(result.evidence["last_key"], "order-2")
        self.assertEqual(result.evidence["source_watermark"], "2026-01-01T00:00:02+00:00")
        self.assertFalse(callbacks[-1]["completed"])
        self.assertEqual(callbacks[-1]["stop_reason"], "page")

    def test_scan_checkpoint_round_trips_in_local_report_store(self):
        with TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(report_dir=Path(tmpdir), report_store="local")
            ref = save_scan_checkpoint(
                settings,
                "orders/query checkpoint",
                {
                    "status": "inProgress",
                    "query_hash": "abc123",
                    "last_key": "order-10",
                    "pages": 3,
                    "rows": 3000,
                },
            )

            loaded = load_scan_checkpoint(settings, "orders/query checkpoint")
            update_ref = update_checkpoint_report_ref(
                settings,
                "orders/query checkpoint",
                "file:///tmp/report.json",
            )
            updated = load_scan_checkpoint(settings, "orders/query checkpoint")

        self.assertTrue(ref.startswith("file://"))
        self.assertEqual(update_ref, ref)
        self.assertEqual(loaded["kind"], "KubeDataGuardScanCheckpoint")
        self.assertEqual(loaded["checkpoint_id"], "orders/query checkpoint")
        self.assertEqual(loaded["last_key"], "order-10")
        self.assertEqual(updated["report_ref"], "file:///tmp/report.json")

    def test_write_report_artifacts_defaults_to_local_file_refs(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            }
        ]
        report = compare_paid_orders_to_index(source, {"order-1": dict(source[0])})

        with TemporaryDirectory() as tmpdir:
            settings = SimpleNamespace(report_dir=Path(tmpdir), report_store="local")
            artifacts = write_report_artifacts(settings, report)

            self.assertTrue(artifacts.json_path.exists())
            self.assertTrue(artifacts.markdown_path.exists())
            self.assertTrue(artifacts.latest_path.exists())
            self.assertTrue(artifacts.report_ref.startswith("file://"))
            self.assertTrue(artifacts.latest_ref.endswith("/latest.json"))

    def test_write_report_artifacts_can_publish_to_s3_compatible_store(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            }
        ]
        report = compare_paid_orders_to_index(source, {})
        fake_s3 = SimpleNamespace(upload_file=Mock())
        fake_boto3 = SimpleNamespace(client=Mock(return_value=fake_s3))

        with TemporaryDirectory() as tmpdir, patch.dict("sys.modules", {"boto3": fake_boto3}):
            settings = SimpleNamespace(
                report_dir=Path(tmpdir),
                report_store="s3",
                report_bucket="kubedataguard-reports",
                report_prefix="test-run",
                report_s3_endpoint_url="http://minio:9000",
                aws_region="us-east-1",
            )
            artifacts = write_report_artifacts(settings, report)

        fake_boto3.client.assert_called_once_with(
            "s3",
            region_name="us-east-1",
            endpoint_url="http://minio:9000",
        )
        self.assertEqual(fake_s3.upload_file.call_count, 4)
        self.assertTrue(artifacts.report_ref.startswith("s3://kubedataguard-reports/test-run/drift-"))
        self.assertTrue(artifacts.compact_ref.startswith("s3://kubedataguard-reports/test-run/drift-"))
        self.assertEqual(artifacts.latest_ref, "s3://kubedataguard-reports/test-run/latest.json")

    def test_repair_job_result_stays_healthy_when_verification_passes(self):
        source = [
            {
                "id": "order-1",
                "status": "paid",
                "amount_cents": 1200,
                "currency": "USD",
                "version": 1,
            }
        ]
        verification = compare_paid_orders_to_index(source, {"order-1": dict(source[0])})

        payload, status = build_repair_job_result(
            invariant_name="paid-orders-indexed",
            source_report_ref="configmap://default/source/report.json",
            repair_report_ref="configmap://default/repair/report.json",
            repair_result={"repaired": 1, "candidate_order_ids": ["order-1"]},
            repair_action="direct-reindex",
            verification=verification,
            observed_generation=10,
        )

        self.assertTrue(status["healthy"])
        self.assertEqual(status["phase"], "Healthy")
        self.assertNotIn("reason", status)
        self.assertEqual(payload["status"], "Healthy")


if __name__ == "__main__":
    unittest.main()
