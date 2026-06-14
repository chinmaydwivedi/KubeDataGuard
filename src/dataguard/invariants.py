from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


CHECK_COMPLETE = "complete"
STATUS_HEALTHY = "Healthy"
STATUS_DRIFT_DETECTED = "DriftDetected"
STATUS_CHECK_FAILED = "CheckFailed"
STATUS_UNKNOWN = "Unknown"


@dataclass(frozen=True)
class ObservationWindow:
    checked_at: str
    target_read_at: str
    max_lag_seconds: int
    eligible_records_before: str
    source_watermark: str | None = None
    source_lsn: str | None = None
    stream_topic: str | None = None
    stream_offset_start: int | None = None
    stream_offset_end: int | None = None
    cdc_frontier: dict[str, Any] | None = None
    source_scan: dict[str, Any] | None = None
    checksum_tree: dict[str, Any] | None = None
    completeness: str = CHECK_COMPLETE

    def to_dict(self) -> dict[str, Any]:
        return {
            "checked_at": self.checked_at,
            "target_read_at": self.target_read_at,
            "max_lag_seconds": self.max_lag_seconds,
            "eligible_records_before": self.eligible_records_before,
            "source_watermark": self.source_watermark,
            "source_lsn": self.source_lsn,
            "stream_topic": self.stream_topic,
            "stream_offset_start": self.stream_offset_start,
            "stream_offset_end": self.stream_offset_end,
            "cdc_frontier": self.cdc_frontier,
            "source_scan": self.source_scan,
            "checksum_tree": self.checksum_tree,
            "completeness": self.completeness,
        }


@dataclass(frozen=True)
class DriftReport:
    invariant: str
    checked_records: int
    missing: list[dict[str, Any]] = field(default_factory=list)
    stale: list[dict[str, Any]] = field(default_factory=list)
    aggregate_mismatches: list[dict[str, Any]] = field(default_factory=list)
    freshness_violations: list[dict[str, Any]] = field(default_factory=list)
    freshness_breaches: list[dict[str, Any]] = field(default_factory=list)
    guarantee: str = "existence"
    observation_window: ObservationWindow | None = None
    check_status: str = CHECK_COMPLETE

    @property
    def drift_count(self) -> int:
        return (
            len(self.missing)
            + len(self.stale)
            + len(self.aggregate_mismatches)
            + len(self.freshness_violations)
        )

    @property
    def status(self) -> str:
        if self.check_status == CHECK_COMPLETE:
            if self.drift_count == 0:
                return STATUS_HEALTHY
            return STATUS_DRIFT_DETECTED
        if self.check_status == "failed":
            return STATUS_CHECK_FAILED
        return STATUS_UNKNOWN

    @property
    def healthy(self) -> bool:
        return self.status == STATUS_HEALTHY

    @property
    def phase(self) -> str:
        return self.status

    @property
    def counterexamples(self) -> list[dict[str, Any]]:
        examples: list[dict[str, Any]] = []

        for item in self.missing:
            source = item.get("source", {})
            examples.append(
                {
                    "type": "missing",
                    "order_id": item.get("order_id"),
                    "reason": item.get("reason"),
                    "source_version": source.get("version"),
                    "source_updated_at": source.get("updated_at"),
                    "expected_target": "OpenSearch orders document",
                }
            )

        for item in self.stale:
            source = item.get("source", {})
            target = item.get("target", {})
            examples.append(
                {
                    "type": "stale",
                    "order_id": item.get("order_id"),
                    "reason": item.get("reason"),
                    "source_version": source.get("version"),
                    "target_version": target.get("version"),
                    "source_updated_at": source.get("updated_at"),
                    "target_indexed_at": target.get("indexed_at"),
                    "mismatches": item.get("mismatches", []),
                }
            )

        for item in self.aggregate_mismatches:
            examples.append(
                {
                    "type": "aggregate_mismatch",
                    "field": item.get("field"),
                    "source": item.get("source"),
                    "target": item.get("target"),
                    "reason": item.get("reason"),
                }
            )

        for item in self.freshness_violations:
            examples.append(
                {
                    "type": "freshness_lag",
                    "order_id": item.get("order_id"),
                    "reason": item.get("reason"),
                    "source_updated_at": item.get("source_updated_at"),
                    "target_indexed_at": item.get("target_indexed_at"),
                    "observed_lag_seconds": item.get("observed_lag_seconds"),
                    "max_lag_seconds": item.get("max_lag_seconds"),
                }
            )

        return examples

    def to_dict(self) -> dict[str, Any]:
        return {
            "invariant": self.invariant,
            "guarantee": self.guarantee,
            "status": self.status,
            "phase": self.phase,
            "healthy": self.healthy,
            "check_status": self.check_status,
            "checked_records": self.checked_records,
            "drift_count": self.drift_count,
            "missing": self.missing,
            "stale": self.stale,
            "aggregate_mismatches": self.aggregate_mismatches,
            "freshness_violations": self.freshness_violations,
            "freshness_breaches": self.freshness_breaches,
            "slo_breach_count": len(self.freshness_breaches),
            "counterexamples": self.counterexamples,
            "observation_window": (
                self.observation_window.to_dict()
                if self.observation_window is not None
                else None
            ),
            "kubernetes_status": self.kubernetes_status(),
        }

    def kubernetes_status(self, *, report_ref: str | None = None) -> dict[str, Any]:
        evidence_window = (
            self.observation_window.to_dict()
            if self.observation_window is not None
            else None
        )
        status_window = kubernetes_observation_window(evidence_window)
        return {
            "healthy": self.healthy,
            "phase": self.phase,
            "guarantee": self.guarantee,
            "checkStatus": self.check_status,
            "driftCount": self.drift_count,
            "counterexampleCount": len(self.counterexamples),
            "sloBreachCount": len(self.freshness_breaches),
            "checkedRecords": self.checked_records,
            "lastCheckedAt": (
                status_window.get("checkedAt")
                if status_window is not None
                else None
            ),
            "observationWindow": status_window,
            "reportRef": report_ref,
        }


def compare_paid_orders_to_index(
    source_orders: list[dict[str, Any]],
    indexed_orders: dict[str, dict[str, Any]],
    *,
    max_lag_seconds: int = 60,
    checked_at: datetime | None = None,
    target_read_at: datetime | None = None,
    stream_topic: str | None = None,
    stream_offset_start: int | None = None,
    stream_offset_end: int | None = None,
    source_lsn: str | None = None,
    cdc_frontier: dict[str, Any] | None = None,
) -> DriftReport:
    missing: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []

    for source in source_orders:
        order_id = source["id"]
        target = indexed_orders.get(order_id)
        if target is None:
            missing.append(
                {
                    "order_id": order_id,
                    "reason": "paid order is absent from the search index",
                    "source": source,
                }
            )
            continue

        mismatches = compare_order_fields(source, target)
        if mismatches:
            stale.append(
                {
                    "order_id": order_id,
                    "reason": "search index document differs from source order",
                    "mismatches": mismatches,
                    "source": source,
                    "target": target,
                }
            )

    return DriftReport(
        invariant="paid-orders-indexed",
        checked_records=len(source_orders),
        missing=missing,
        stale=stale,
        guarantee="existence+fieldEquality",
        observation_window=build_observation_window(
            source_orders,
            max_lag_seconds=max_lag_seconds,
            checked_at=checked_at,
            target_read_at=target_read_at,
            stream_topic=stream_topic,
            stream_offset_start=stream_offset_start,
            stream_offset_end=stream_offset_end,
            source_lsn=source_lsn,
            cdc_frontier=cdc_frontier,
        ),
    )


def compare_order_fields(
    source: dict[str, Any],
    target: dict[str, Any],
) -> list[dict[str, Any]]:
    fields = ["status", "amount_cents", "currency", "version"]
    mismatches: list[dict[str, Any]] = []
    for field_name in fields:
        if source.get(field_name) != target.get(field_name):
            mismatches.append(
                {
                    "field": field_name,
                    "source": source.get(field_name),
                    "target": target.get(field_name),
                }
            )
    return mismatches


def compare_query_results(
    *,
    invariant_name: str,
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    key_field: str,
    compare_fields: list[str],
    guarantee: str,
    max_lag_seconds: int = 60,
    checked_at: datetime | None = None,
    target_read_at: datetime | None = None,
    stream_topic: str | None = None,
    stream_offset_start: int | None = None,
    stream_offset_end: int | None = None,
    source_lsn: str | None = None,
    source_scan: dict[str, Any] | None = None,
    cdc_frontier: dict[str, Any] | None = None,
    check_status: str = CHECK_COMPLETE,
) -> DriftReport:
    target_by_key = {
        str(row[key_field]): row
        for row in target_rows
        if row.get(key_field) is not None
    }
    missing: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []

    for source in source_rows:
        if source.get(key_field) is None:
            stale.append(
                {
                    "order_id": None,
                    "reason": f"source row is missing key field {key_field}",
                    "mismatches": [
                        {
                            "field": key_field,
                            "source": None,
                            "target": None,
                        }
                    ],
                    "source": source,
                    "target": {},
                }
            )
            continue

        row_key = str(source[key_field])
        target = target_by_key.get(row_key)
        if target is None:
            missing.append(
                {
                    "order_id": row_key,
                    "reason": "source row is absent from the target query result",
                    "source": source,
                }
            )
            continue

        mismatches = compare_named_fields(source, target, compare_fields)
        if mismatches:
            stale.append(
                {
                    "order_id": row_key,
                    "reason": "target row differs from source row",
                    "mismatches": mismatches,
                    "source": source,
                    "target": target,
                }
            )

    return DriftReport(
        invariant=invariant_name,
        checked_records=len(source_rows),
        missing=missing,
        stale=stale,
        guarantee=guarantee,
        check_status=check_status,
        observation_window=build_observation_window(
            source_rows,
            max_lag_seconds=max_lag_seconds,
            checked_at=checked_at,
            target_read_at=target_read_at,
            stream_topic=stream_topic,
            stream_offset_start=stream_offset_start,
            stream_offset_end=stream_offset_end,
            source_watermark=latest_source_watermark(source_rows),
            source_lsn=source_lsn,
            cdc_frontier=cdc_frontier,
            source_scan=source_scan,
            completeness=check_status,
        ),
    )


def compare_named_fields(
    source: dict[str, Any],
    target: dict[str, Any],
    fields: list[str],
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    for field_name in fields:
        source_value = comparable_value(source.get(field_name))
        target_value = comparable_value(target.get(field_name))
        if source_value != target_value:
            mismatches.append(
                {
                    "field": field_name,
                    "source": source.get(field_name),
                    "target": target.get(field_name),
                }
            )
    return mismatches


def comparable_value(value: Any) -> Any:
    if value is None:
        return None
    return str(value)


def summarize_paid_orders(orders: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "count": len(orders),
        "total_amount_cents": sum(int(order["amount_cents"]) for order in orders),
        "source_watermark": latest_source_watermark(orders),
    }


def compare_paid_order_aggregates(
    source_summary: dict[str, Any],
    target_summary: dict[str, Any],
    *,
    max_lag_seconds: int = 60,
    checked_at: datetime | None = None,
    target_read_at: datetime | None = None,
    stream_topic: str | None = None,
    stream_offset_start: int | None = None,
    stream_offset_end: int | None = None,
    source_lsn: str | None = None,
    cdc_frontier: dict[str, Any] | None = None,
) -> DriftReport:
    mismatches: list[dict[str, Any]] = []

    for field_name in ["count", "total_amount_cents"]:
        source_value = int(source_summary.get(field_name) or 0)
        target_value = int(target_summary.get(field_name) or 0)
        if source_value != target_value:
            mismatches.append(
                {
                    "field": field_name,
                    "source": source_value,
                    "target": target_value,
                    "reason": f"source aggregate {field_name} does not match derived view",
                }
            )

    source_watermark = source_summary.get("source_watermark")
    return DriftReport(
        invariant="paid-orders-aggregate",
        checked_records=int(source_summary.get("count") or 0),
        aggregate_mismatches=mismatches,
        guarantee="aggregate",
        observation_window=build_observation_window(
            [],
            max_lag_seconds=max_lag_seconds,
            checked_at=checked_at,
            target_read_at=target_read_at,
            stream_topic=stream_topic,
            stream_offset_start=stream_offset_start,
            stream_offset_end=stream_offset_end,
            source_lsn=source_lsn,
            cdc_frontier=cdc_frontier,
            source_watermark=source_watermark,
        ),
    )


def compare_paid_order_checksum_buckets(
    *,
    source_buckets: list[dict[str, Any]],
    target_buckets: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    prefix_length: int,
    max_lag_seconds: int = 60,
    checked_at: datetime | None = None,
    target_read_at: datetime | None = None,
    stream_topic: str | None = None,
    stream_offset_start: int | None = None,
    stream_offset_end: int | None = None,
    source_lsn: str | None = None,
    cdc_frontier: dict[str, Any] | None = None,
) -> DriftReport:
    from . import merkle

    source_summary, target_summary, bucket_mismatches = merkle.compare_bucket_summaries(
        source_buckets,
        target_buckets,
    )
    target_by_id = {
        str(row["id"]): row
        for row in target_rows
        if row.get("id") is not None
    }
    missing: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []

    for source in source_rows:
        order_id = str(source["id"])
        target = target_by_id.get(order_id)
        bucket = order_id[:prefix_length]
        if target is None:
            missing.append(
                {
                    "order_id": order_id,
                    "reason": "source row is absent from target inside mismatched checksum bucket",
                    "bucket": bucket,
                    "source": source,
                }
            )
            continue

        mismatches = compare_order_fields(source, target)
        if mismatches:
            stale.append(
                {
                    "order_id": order_id,
                    "reason": "target row differs from source inside mismatched checksum bucket",
                    "bucket": bucket,
                    "mismatches": mismatches,
                    "source": source,
                    "target": target,
                }
            )

    aggregate_mismatches = [
        {
            "field": f"checksum_bucket:{item['bucket']}",
            "source": item["source"],
            "target": item["target"],
            "reason": item["reason"],
        }
        for item in bucket_mismatches
    ]
    checksum_tree = merkle.checksum_tree_evidence(
        prefix_length=prefix_length,
        source_bucket_count=len(source_summary),
        target_bucket_count=len(target_summary),
        mismatches=bucket_mismatches,
        drilled_source_rows=len(source_rows),
        drilled_target_rows=len(target_rows),
    )
    checked_records = sum(int(item.get("count") or 0) for item in source_summary.values())

    return DriftReport(
        invariant="paid-orders-checksum",
        checked_records=checked_records,
        missing=missing,
        stale=stale,
        aggregate_mismatches=aggregate_mismatches,
        guarantee="checksumMerkle",
        observation_window=build_observation_window(
            source_rows,
            max_lag_seconds=max_lag_seconds,
            checked_at=checked_at,
            target_read_at=target_read_at,
            stream_topic=stream_topic,
            stream_offset_start=stream_offset_start,
            stream_offset_end=stream_offset_end,
            source_watermark=latest_bucket_watermark(source_summary.values()),
            source_lsn=source_lsn,
            cdc_frontier=cdc_frontier,
            checksum_tree=checksum_tree,
        ),
    )


def compare_paid_order_freshness(
    source_orders: list[dict[str, Any]],
    indexed_orders: dict[str, dict[str, Any]],
    *,
    invariant_name: str = "paid-orders-freshness",
    guarantee: str = "boundedFreshness",
    max_lag_seconds: int = 60,
    checked_at: datetime | None = None,
    target_read_at: datetime | None = None,
    stream_topic: str | None = None,
    stream_offset_start: int | None = None,
    stream_offset_end: int | None = None,
    source_lsn: str | None = None,
    cdc_frontier: dict[str, Any] | None = None,
) -> DriftReport:
    checked_at = ensure_utc(checked_at or datetime.now(timezone.utc))
    freshness_violations: list[dict[str, Any]] = []
    freshness_breaches: list[dict[str, Any]] = []

    for source in source_orders:
        order_id = source["id"]
        source_updated_at = parse_time(source["updated_at"])
        target = indexed_orders.get(order_id)
        if target is None:
            observed_lag_seconds = max(0, int((checked_at - source_updated_at).total_seconds()))
            freshness_violations.append(
                {
                    "order_id": order_id,
                    "reason": "derived view has not observed the source update",
                    "source_updated_at": source["updated_at"],
                    "target_indexed_at": None,
                    "observed_lag_seconds": observed_lag_seconds,
                    "max_lag_seconds": max_lag_seconds,
                    "source_lsn": source_lsn,
                }
            )
            continue

        target_indexed_at_raw = target.get("indexed_at")
        if not target_indexed_at_raw:
            freshness_violations.append(
                {
                    "order_id": order_id,
                    "reason": "target document does not expose indexed_at freshness evidence",
                    "source_updated_at": source["updated_at"],
                    "target_indexed_at": None,
                    "observed_lag_seconds": None,
                    "max_lag_seconds": max_lag_seconds,
                    "source_lsn": source_lsn,
                }
            )
            continue

        target_indexed_at = parse_time(str(target_indexed_at_raw))
        observed_lag_seconds = int((target_indexed_at - source_updated_at).total_seconds())
        if observed_lag_seconds < 0:
            freshness_violations.append(
                {
                    "order_id": order_id,
                    "reason": "derived view has not observed the latest source update",
                    "source_updated_at": source["updated_at"],
                    "target_indexed_at": str(target_indexed_at_raw),
                    "observed_lag_seconds": observed_lag_seconds,
                    "max_lag_seconds": max_lag_seconds,
                    "source_lsn": source_lsn,
                }
            )
            continue

        if observed_lag_seconds > max_lag_seconds:
            freshness_breaches.append(
                {
                    "order_id": order_id,
                    "reason": "derived view observed the update after the bounded freshness SLO",
                    "source_updated_at": source["updated_at"],
                    "target_indexed_at": str(target_indexed_at_raw),
                    "observed_lag_seconds": observed_lag_seconds,
                    "max_lag_seconds": max_lag_seconds,
                    "source_lsn": source_lsn,
                }
            )

    return DriftReport(
        invariant=invariant_name,
        checked_records=len(source_orders),
        freshness_violations=freshness_violations,
        freshness_breaches=freshness_breaches,
        guarantee=guarantee,
        observation_window=build_observation_window(
            source_orders,
            max_lag_seconds=max_lag_seconds,
            checked_at=checked_at,
            target_read_at=target_read_at,
            stream_topic=stream_topic,
            stream_offset_start=stream_offset_start,
            stream_offset_end=stream_offset_end,
            source_watermark=latest_source_watermark(source_orders),
            source_lsn=source_lsn,
            cdc_frontier=cdc_frontier,
        ),
    )


def build_observation_window(
    source_orders: list[dict[str, Any]],
    *,
    max_lag_seconds: int,
    checked_at: datetime | None,
    target_read_at: datetime | None,
    stream_topic: str | None,
    stream_offset_start: int | None = None,
    stream_offset_end: int | None = None,
    source_watermark: str | None = None,
    source_lsn: str | None = None,
    cdc_frontier: dict[str, Any] | None = None,
    source_scan: dict[str, Any] | None = None,
    checksum_tree: dict[str, Any] | None = None,
    completeness: str = CHECK_COMPLETE,
) -> ObservationWindow:
    checked_at = checked_at or datetime.now(timezone.utc)
    target_read_at = target_read_at or datetime.now(timezone.utc)
    checked_at = ensure_utc(checked_at)
    target_read_at = ensure_utc(target_read_at)
    eligible_before = checked_at - timedelta(seconds=max_lag_seconds)

    return ObservationWindow(
        checked_at=iso(checked_at),
        target_read_at=iso(target_read_at),
        max_lag_seconds=max_lag_seconds,
        eligible_records_before=iso(eligible_before),
        source_watermark=source_watermark or latest_source_watermark(source_orders),
        source_lsn=source_lsn,
        stream_topic=stream_topic,
        stream_offset_start=stream_offset_start,
        stream_offset_end=stream_offset_end,
        cdc_frontier=cdc_frontier,
        source_scan=source_scan,
        checksum_tree=checksum_tree,
        completeness=completeness,
    )


def latest_source_watermark(orders: list[dict[str, Any]]) -> str | None:
    values = [str(order["updated_at"]) for order in orders if order.get("updated_at")]
    if not values:
        return None
    return max(values)


def latest_bucket_watermark(summaries) -> str | None:
    values = [
        str(summary["source_watermark"])
        for summary in summaries
        if summary.get("source_watermark")
    ]
    if not values:
        return None
    return max(values)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_time(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return ensure_utc(datetime.fromisoformat(normalized))


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def kubernetes_observation_window(window: dict[str, Any] | None) -> dict[str, Any] | None:
    if window is None:
        return None
    payload = {
        "checkedAt": window.get("checked_at"),
        "targetReadAt": window.get("target_read_at"),
        "maxLagSeconds": window.get("max_lag_seconds"),
        "eligibleRecordsBefore": window.get("eligible_records_before"),
        "sourceWatermark": window.get("source_watermark"),
        "sourceLSN": window.get("source_lsn"),
        "streamTopic": window.get("stream_topic"),
        "streamOffsetStart": window.get("stream_offset_start"),
        "streamOffsetEnd": window.get("stream_offset_end"),
        "cdcFrontier": window.get("cdc_frontier"),
        "sourceScan": window.get("source_scan"),
        "checksumTree": window.get("checksum_tree"),
        "completeness": window.get("completeness"),
    }
    return {key: value for key, value in payload.items() if value is not None}
