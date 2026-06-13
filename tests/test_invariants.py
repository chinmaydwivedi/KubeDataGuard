import unittest
from datetime import datetime
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
from dataguard.local_demo import run_local_proof
from dataguard.repair import repair_from_payload


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
        )

        self.assertFalse(report.healthy)
        self.assertEqual(report.invariant, "paid-orders-query-check")
        self.assertEqual(report.drift_count, 2)
        self.assertEqual(report.missing[0]["order_id"], "order-1")
        mismatch_fields = {item["field"] for item in report.stale[0]["mismatches"]}
        self.assertEqual(mismatch_fields, {"amount_cents", "version"})

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
            verification=verification,
            observed_generation=10,
        )

        self.assertTrue(status["healthy"])
        self.assertEqual(status["phase"], "Healthy")
        self.assertNotIn("reason", status)
        self.assertEqual(payload["status"], "Healthy")


if __name__ == "__main__":
    unittest.main()
