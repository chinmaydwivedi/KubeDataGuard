from __future__ import annotations

import signal
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any

from . import checkpoints, db, search
from .config import Settings
from .invariants import (
    CHECK_COMPLETE,
    DriftReport,
    compare_paid_order_aggregates,
    compare_paid_order_freshness,
    compare_paid_orders_to_index,
    compare_query_results,
    iso,
    latest_source_watermark,
)


def check_paid_orders_indexed(
    settings: Settings,
    *,
    max_lag_seconds: int,
) -> DriftReport:
    checked_at = datetime.now(timezone.utc)
    offsets = stream_offsets(settings)
    source_orders = db.fetch_paid_orders(settings, max_lag_seconds=max_lag_seconds)
    source_lsn = source_lsn_or_none(settings)
    indexed_orders = search.mget_orders(settings, [order["id"] for order in source_orders])
    target_read_at = datetime.now(timezone.utc)
    cdc_frontier = build_check_cdc_frontier(
        settings,
        max_lag_seconds=max_lag_seconds,
        source_lsn=source_lsn,
        source_watermark=latest_source_watermark(source_orders),
        offsets=offsets,
        target_read_at=target_read_at,
    )
    return compare_paid_orders_to_index(
        source_orders,
        indexed_orders,
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=target_read_at,
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
        source_lsn=source_lsn,
        cdc_frontier=cdc_frontier,
    )


def check_paid_orders_aggregate(
    settings: Settings,
    *,
    max_lag_seconds: int,
) -> DriftReport:
    checked_at = datetime.now(timezone.utc)
    offsets = stream_offsets(settings)
    eligible_before = checked_at - timedelta(seconds=max_lag_seconds)
    source_summary = db.fetch_paid_order_summary(
        settings,
        max_lag_seconds=max_lag_seconds,
    )
    source_lsn = source_lsn_or_none(settings)
    target_summary = search.paid_order_summary(
        settings,
        updated_before=iso(eligible_before),
    )
    target_read_at = datetime.now(timezone.utc)
    cdc_frontier = build_check_cdc_frontier(
        settings,
        max_lag_seconds=max_lag_seconds,
        source_lsn=source_lsn,
        source_watermark=source_summary.get("source_watermark"),
        offsets=offsets,
        target_read_at=target_read_at,
    )
    return compare_paid_order_aggregates(
        source_summary,
        target_summary,
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=target_read_at,
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
        source_lsn=source_lsn,
        cdc_frontier=cdc_frontier,
    )


def check_paid_orders_freshness(
    settings: Settings,
    *,
    max_lag_seconds: int,
) -> DriftReport:
    checked_at = datetime.now(timezone.utc)
    offsets = stream_offsets(settings)
    source_orders = db.fetch_paid_orders(settings, max_lag_seconds=max_lag_seconds)
    source_lsn = source_lsn_or_none(settings)
    indexed_orders = search.mget_orders(settings, [order["id"] for order in source_orders])
    target_read_at = datetime.now(timezone.utc)
    cdc_frontier = build_check_cdc_frontier(
        settings,
        max_lag_seconds=max_lag_seconds,
        source_lsn=source_lsn,
        source_watermark=latest_source_watermark(source_orders),
        offsets=offsets,
        target_read_at=target_read_at,
    )
    return compare_paid_order_freshness(
        source_orders,
        indexed_orders,
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=target_read_at,
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
        source_lsn=source_lsn,
        cdc_frontier=cdc_frontier,
    )


def check_paid_orders_redis_freshness(
    settings: Settings,
    *,
    max_lag_seconds: int,
) -> DriftReport:
    from . import cache

    checked_at = datetime.now(timezone.utc)
    offsets = stream_offsets(settings)
    source_orders = db.fetch_paid_orders(settings, max_lag_seconds=max_lag_seconds)
    source_lsn = source_lsn_or_none(settings)
    cached_orders = cache.mget_orders(settings, [order["id"] for order in source_orders])
    target_read_at = datetime.now(timezone.utc)
    cdc_frontier = build_check_cdc_frontier(
        settings,
        max_lag_seconds=max_lag_seconds,
        source_lsn=source_lsn,
        source_watermark=latest_source_watermark(source_orders),
        offsets=offsets,
        target_read_at=target_read_at,
        target_frontier=cache.target_offset_frontier(settings),
    )
    report = compare_paid_order_freshness(
        source_orders,
        cached_orders,
        invariant_name="paid-orders-redis-cache-freshness",
        guarantee="cacheFreshness",
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=target_read_at,
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
        source_lsn=source_lsn,
        cdc_frontier=cdc_frontier,
    )
    return report


def check_query_invariant(
    settings: Settings,
    *,
    invariant_name: str,
    source_query: str,
    target_query: str,
    key_field: str,
    compare_fields: list[str],
    max_lag_seconds: int,
    source_scan_page_size: int = db.DEFAULT_SOURCE_SCAN_PAGE_SIZE,
    source_resume_after_key: str | None = None,
    source_checkpoint_id: str = "",
    source_reset_checkpoint: bool = False,
    source_max_pages: int = 0,
    check_id: str = "",
) -> DriftReport:
    checked_at = datetime.now(timezone.utc)
    offsets = stream_offsets(settings)
    source_lsn = source_lsn_or_none(settings)
    checkpoint_ref: str | None = None
    loaded_checkpoint = None
    query_hash = scan_query_hash(source_query=source_query, target_query=target_query, key_field=key_field)

    if source_checkpoint_id and not source_reset_checkpoint:
        loaded_checkpoint = checkpoints.load_scan_checkpoint(settings, source_checkpoint_id)
        if loaded_checkpoint:
            loaded_query_hash = loaded_checkpoint.get("query_hash")
            if loaded_query_hash and loaded_query_hash != query_hash:
                raise ValueError(
                    f"checkpoint {source_checkpoint_id!r} was created for a different query"
                )
            if not source_resume_after_key:
                source_resume_after_key = loaded_checkpoint.get("last_key") or None

    def save_checkpoint(scan_evidence: dict[str, object]) -> None:
        nonlocal checkpoint_ref
        if not source_checkpoint_id:
            return
        checkpoint_ref = checkpoints.save_scan_checkpoint(
            settings,
            source_checkpoint_id,
            {
                "status": "complete" if scan_evidence.get("completed") else "inProgress",
                "query_hash": query_hash,
                "check_id": check_id,
                "key_field": key_field,
                "last_key": scan_evidence.get("last_key"),
                "first_key": scan_evidence.get("first_key"),
                "resume_after_key": scan_evidence.get("resume_after_key"),
                "source_watermark": scan_evidence.get("source_watermark"),
                "source_lsn": source_lsn,
                "pages": scan_evidence.get("pages"),
                "rows": scan_evidence.get("rows"),
                "stop_reason": scan_evidence.get("stop_reason"),
            },
        )

    source_scan = db.execute_source_query_keyset(
        settings,
        source_query,
        key_field=key_field,
        page_size=source_scan_page_size,
        resume_after_key=source_resume_after_key,
        max_pages=source_max_pages,
        on_page=save_checkpoint,
    )
    save_checkpoint(source_scan.evidence)
    target_rows = search.execute_target_query(
        settings,
        target_query,
        key_field=key_field,
    )
    target_read_at = datetime.now(timezone.utc)
    guarantee = "existence"
    if compare_fields:
        guarantee = "existence+fieldEquality"
    scan_evidence = dict(source_scan.evidence)
    scan_evidence["query_hash"] = query_hash
    if source_checkpoint_id:
        scan_evidence["checkpoint_id"] = source_checkpoint_id
    if checkpoint_ref:
        scan_evidence["checkpoint_ref"] = checkpoint_ref
    if loaded_checkpoint:
        scan_evidence["loaded_checkpoint"] = {
            "checkpoint_id": loaded_checkpoint.get("checkpoint_id"),
            "last_key": loaded_checkpoint.get("last_key"),
            "updated_at": loaded_checkpoint.get("updated_at"),
            "status": loaded_checkpoint.get("status"),
        }
    check_status = CHECK_COMPLETE
    if source_resume_after_key or not source_scan.evidence.get("completed", True):
        check_status = "partial"
    cdc_frontier = build_check_cdc_frontier(
        settings,
        max_lag_seconds=max_lag_seconds,
        source_lsn=source_lsn,
        source_watermark=scan_evidence.get("source_watermark"),
        offsets=offsets,
        target_read_at=target_read_at,
    )
    return compare_query_results(
        invariant_name=invariant_name,
        source_rows=source_scan.rows,
        target_rows=target_rows,
        key_field=key_field,
        compare_fields=compare_fields,
        guarantee=guarantee,
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=target_read_at,
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
        source_lsn=source_lsn,
        cdc_frontier=cdc_frontier,
        source_scan=scan_evidence,
        check_status=check_status,
    )


def scan_query_hash(*, source_query: str, target_query: str, key_field: str) -> str:
    payload = "\n".join([source_query.strip(), target_query.strip(), key_field.strip()])
    return sha256(payload.encode("utf-8")).hexdigest()


def stream_offsets(settings: Settings) -> dict[str, Any]:
    from . import events

    class OffsetTimeout(Exception):
        pass

    def raise_timeout(_signum, _frame):
        raise OffsetTimeout()

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, raise_timeout)
    signal.alarm(3)
    try:
        return events.topic_offset_range(settings)
    except Exception:
        return {
            "stream_offset_start": None,
            "stream_offset_end": None,
            "stream_partitions": 0,
            "stream_event_count": None,
            "stream_offsets": [],
        }
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def source_lsn_or_none(settings: Settings) -> str | None:
    try:
        return db.current_wal_lsn(settings)
    except Exception:
        return None


def build_check_cdc_frontier(
    settings: Settings,
    *,
    max_lag_seconds: int,
    source_lsn: str | None,
    source_watermark: str | None,
    offsets: dict[str, Any],
    target_read_at: datetime,
    target_frontier: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        outbox = db.outbox_frontier(settings, max_lag_seconds=max_lag_seconds)
    except Exception as exc:
        outbox = {
            "mode": "postgres-outbox",
            "available": False,
            "error": str(exc),
        }
    if target_frontier is None:
        try:
            target_frontier = search.target_offset_frontier(settings)
        except Exception as exc:
            target_frontier = {
                "system": "opensearch",
                "available": False,
                "error": str(exc),
            }
    return build_cdc_frontier(
        source_lsn=source_lsn,
        source_watermark=source_watermark,
        outbox=outbox,
        offsets=offsets,
        stream_topic=settings.order_events_topic,
        target_read_at=target_read_at,
        target_frontier=target_frontier,
    )


def build_cdc_frontier(
    *,
    source_lsn: str | None,
    source_watermark: str | None,
    outbox: dict[str, Any] | None,
    offsets: dict[str, Any],
    stream_topic: str,
    target_read_at: datetime,
    target_frontier: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reasons: list[str] = []
    status = "bounded"

    if not source_lsn:
        status = "partial"
        reasons.append("source WAL LSN unavailable")

    outbox_available = bool(outbox and outbox.get("available", True))
    if not outbox_available:
        status = "partial"
        reasons.append("source outbox frontier unavailable")

    published_count = int((outbox or {}).get("published_count") or 0)
    unpublished_count = int((outbox or {}).get("unpublished_count") or 0)
    offset_recorded_count = int((outbox or {}).get("offset_recorded_count") or 0)
    if outbox_available and unpublished_count > 0:
        status = "partial"
        reasons.append(f"{unpublished_count} eligible outbox events are unpublished")
    if outbox_available and offset_recorded_count < published_count:
        status = "partial"
        missing_offsets = published_count - offset_recorded_count
        reasons.append(f"{missing_offsets} published outbox events have no Kafka offset evidence")

    stream_event_count = offsets.get("stream_event_count")
    stream_offset_start = offsets.get("stream_offset_start")
    stream_offset_end = offsets.get("stream_offset_end")
    if stream_event_count is None or stream_offset_start is None or stream_offset_end is None:
        status = "partial"
        reasons.append("Kafka topic offset frontier unavailable")
    elif outbox_available and int(stream_event_count) < published_count:
        status = "partial"
        reasons.append(
            f"Kafka topic exposes {stream_event_count} events but outbox has {published_count} published events"
        )

    partition_ends = {
        (stream_topic, item.get("partition")): item.get("end")
        for item in offsets.get("stream_offsets") or []
    }
    target_available = bool(target_frontier and target_frontier.get("available", True))
    if not target_available:
        status = "partial"
        reasons.append("target applied-offset frontier unavailable")
    target_offsets = {
        (item.get("topic"), item.get("partition")): item.get("max_applied_offset")
        for item in (target_frontier or {}).get("applied_offsets") or []
    }
    for published_offset in (outbox or {}).get("published_offsets") or []:
        topic = published_offset.get("topic")
        partition = published_offset.get("partition")
        max_published_offset = published_offset.get("max_published_offset")
        if topic != stream_topic:
            status = "partial"
            reasons.append(
                f"outbox event frontier is for topic {topic!r}, not configured topic {stream_topic!r}"
            )
            continue
        end_offset = partition_ends.get((topic, partition))
        if end_offset is None or max_published_offset is None:
            status = "partial"
            reasons.append(f"Kafka partition {partition} frontier unavailable")
            continue
        if int(end_offset) <= int(max_published_offset):
            status = "partial"
            reasons.append(
                "Kafka partition "
                f"{partition} end offset {end_offset} does not cover "
                f"published outbox offset {max_published_offset}"
            )
        if target_available:
            target_max_offset = target_offsets.get((topic, partition))
            if target_max_offset is None:
                status = "partial"
                reasons.append(f"target partition {partition} applied-offset frontier unavailable")
            elif max_published_offset is not None and int(target_max_offset) < int(max_published_offset):
                status = "partial"
                reasons.append(
                    "target partition "
                    f"{partition} applied offset {target_max_offset} does not cover "
                    f"published outbox offset {max_published_offset}"
                )

    if not source_lsn and not outbox_available and stream_event_count is None:
        status = "unavailable"

    return {
        "mode": "postgres-wal+outbox+kafka",
        "status": status,
        "reason": "; ".join(reasons)
        or "source WAL, outbox, Kafka topic, and target applied-offset frontiers bound this check",
        "source": {
            "system": "postgres",
            "lsn": source_lsn,
            "watermark": source_watermark,
        },
        "outbox": outbox,
        "stream": {
            "system": "kafka",
            "topic": stream_topic,
            "offset_start": stream_offset_start,
            "offset_end": stream_offset_end,
            "partitions": offsets.get("stream_partitions"),
            "event_count": stream_event_count,
            "partition_offsets": offsets.get("stream_offsets") or [],
        },
        "target": {
            "read_at": iso(target_read_at),
            "frontier": target_frontier,
            "offset_gap_proof": "max-offset-watermark-only",
        },
    }
