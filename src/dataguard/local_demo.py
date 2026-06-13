from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .invariants import (
    DriftReport,
    compare_paid_order_aggregates,
    compare_paid_orders_to_index,
    iso,
    summarize_paid_orders,
)


@dataclass(frozen=True)
class LocalProofResult:
    source_orders: list[dict[str, Any]]
    drifted_index: dict[str, dict[str, Any]]
    repaired_index: dict[str, dict[str, Any]]
    before_existence: DriftReport
    before_aggregate: DriftReport
    repair: dict[str, Any]
    after_existence: DriftReport
    after_aggregate: DriftReport

    @property
    def passed(self) -> bool:
        return (
            not self.before_existence.healthy
            and self.after_existence.healthy
            and self.after_aggregate.healthy
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_type": "kubedataguard-local-proof",
            "passed": self.passed,
            "demo_story": [
                "source Postgres orders are represented as local source rows",
                "derived OpenSearch documents are represented as a local index map",
                "some paid orders are missing and one document is stale",
                "existence+fieldEquality and aggregate invariants detect drift",
                "repair reindexes candidate orders from the source of truth",
                "post-repair verification is healthy",
            ],
            "source": {
                "system": "postgres",
                "paid_order_count": len(self.source_orders),
                "paid_order_ids": [order["id"] for order in self.source_orders],
                "summary": summarize_paid_orders(self.source_orders),
            },
            "derived_before": {
                "system": "opensearch",
                "document_count": len(self.drifted_index),
                "order_ids": sorted(self.drifted_index),
                "summary": summarize_paid_orders(list(self.drifted_index.values())),
            },
            "derived_after": {
                "system": "opensearch",
                "document_count": len(self.repaired_index),
                "order_ids": sorted(self.repaired_index),
                "summary": summarize_paid_orders(list(self.repaired_index.values())),
            },
            "before": {
                "existence": self.before_existence.to_dict(),
                "aggregate": self.before_aggregate.to_dict(),
            },
            "repair": self.repair,
            "after": {
                "existence": self.after_existence.to_dict(),
                "aggregate": self.after_aggregate.to_dict(),
            },
        }


def run_local_proof(
    *,
    count: int = 20,
    skip_every_paid: int = 5,
) -> LocalProofResult:
    source_orders = make_source_orders(count)
    drifted_index = make_drifted_index(
        source_orders,
        skip_every_paid=skip_every_paid,
    )
    checked_at = datetime(2026, 6, 12, 12, 0, tzinfo=timezone.utc)
    topic = "orders.events"
    offset_start = 1
    offset_end = len(source_orders)

    before_existence = compare_paid_orders_to_index(
        source_orders,
        drifted_index,
        max_lag_seconds=0,
        checked_at=checked_at,
        target_read_at=checked_at,
        stream_topic=topic,
        stream_offset_start=offset_start,
        stream_offset_end=offset_end,
    )
    before_aggregate = compare_paid_order_aggregates(
        summarize_paid_orders(source_orders),
        summarize_paid_orders(list(drifted_index.values())),
        max_lag_seconds=0,
        checked_at=checked_at,
        target_read_at=checked_at,
        stream_topic=topic,
        stream_offset_start=offset_start,
        stream_offset_end=offset_end,
    )

    repaired_index, repair_payload = repair_index_from_report(
        source_orders,
        drifted_index,
        before_existence,
    )

    after_checked_at = checked_at + timedelta(seconds=10)
    after_existence = compare_paid_orders_to_index(
        source_orders,
        repaired_index,
        max_lag_seconds=0,
        checked_at=after_checked_at,
        target_read_at=after_checked_at,
        stream_topic=topic,
        stream_offset_start=offset_start,
        stream_offset_end=offset_end,
    )
    after_aggregate = compare_paid_order_aggregates(
        summarize_paid_orders(source_orders),
        summarize_paid_orders(list(repaired_index.values())),
        max_lag_seconds=0,
        checked_at=after_checked_at,
        target_read_at=after_checked_at,
        stream_topic=topic,
        stream_offset_start=offset_start,
        stream_offset_end=offset_end,
    )

    return LocalProofResult(
        source_orders=source_orders,
        drifted_index=drifted_index,
        repaired_index=repaired_index,
        before_existence=before_existence,
        before_aggregate=before_aggregate,
        repair=repair_payload,
        after_existence=after_existence,
        after_aggregate=after_aggregate,
    )


def make_source_orders(count: int) -> list[dict[str, Any]]:
    start = datetime(2026, 6, 12, 11, 55, tzinfo=timezone.utc)
    orders: list[dict[str, Any]] = []
    for index in range(1, count + 1):
        timestamp = start + timedelta(seconds=index * 3)
        orders.append(
            {
                "id": f"order-{index:04d}",
                "customer_id": f"customer-{(index % 7) + 1:03d}",
                "status": "paid",
                "amount_cents": 1000 + index * 137,
                "currency": "USD",
                "version": 1,
                "created_at": iso(timestamp),
                "updated_at": iso(timestamp),
            }
        )
    return orders


def make_drifted_index(
    source_orders: list[dict[str, Any]],
    *,
    skip_every_paid: int,
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for position, order in enumerate(source_orders, start=1):
        if skip_every_paid and position % skip_every_paid == 0:
            continue
        document = dict(order)
        document["indexed_at"] = order["updated_at"]
        indexed[order["id"]] = document

    if source_orders:
        stale_order = source_orders[min(2, len(source_orders) - 1)]
        stale_document = dict(stale_order)
        stale_document["amount_cents"] = int(stale_document["amount_cents"]) - 250
        stale_document["version"] = 0
        stale_document["indexed_at"] = stale_order["updated_at"]
        indexed[stale_order["id"]] = stale_document

    return indexed


def repair_index_from_report(
    source_orders: list[dict[str, Any]],
    drifted_index: dict[str, dict[str, Any]],
    report: DriftReport,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    source_by_id = {order["id"]: order for order in source_orders}
    candidate_order_ids = sorted(
        {
            item["order_id"]
            for item in [*report.missing, *report.stale]
        }
    )
    repaired_index = {
        order_id: dict(document)
        for order_id, document in drifted_index.items()
    }
    repaired_at = "2026-06-12T12:00:10+00:00"
    repaired_order_ids: list[str] = []

    for order_id in candidate_order_ids:
        source_order = source_by_id[order_id]
        document = dict(source_order)
        document["indexed_at"] = repaired_at
        repaired_index[order_id] = document
        repaired_order_ids.append(order_id)

    return repaired_index, {
        "action": "reindex-records-from-source",
        "source_of_truth": "postgres.orders",
        "target": "opensearch.orders",
        "candidate_order_ids": candidate_order_ids,
        "repaired_order_ids": repaired_order_ids,
        "repaired": len(repaired_order_ids),
        "idempotency_model": "upsert by order id",
        "repair_scope": {
            "invariant": report.invariant,
            "guarantee": report.guarantee,
            "drift_count_before": report.drift_count,
        },
    }
