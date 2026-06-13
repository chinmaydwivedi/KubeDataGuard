from __future__ import annotations

import signal
from datetime import datetime, timedelta, timezone
from hashlib import sha256

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
)


def check_paid_orders_indexed(
    settings: Settings,
    *,
    max_lag_seconds: int,
) -> DriftReport:
    checked_at = datetime.now(timezone.utc)
    offsets = stream_offsets(settings)
    source_orders = db.fetch_paid_orders(settings, max_lag_seconds=max_lag_seconds)
    indexed_orders = search.mget_orders(settings, [order["id"] for order in source_orders])
    return compare_paid_orders_to_index(
        source_orders,
        indexed_orders,
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=datetime.now(timezone.utc),
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
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
    target_summary = search.paid_order_summary(
        settings,
        updated_before=iso(eligible_before),
    )
    return compare_paid_order_aggregates(
        source_summary,
        target_summary,
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=datetime.now(timezone.utc),
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
    )


def check_paid_orders_freshness(
    settings: Settings,
    *,
    max_lag_seconds: int,
) -> DriftReport:
    checked_at = datetime.now(timezone.utc)
    offsets = stream_offsets(settings)
    source_lsn = db.current_wal_lsn(settings)
    source_orders = db.fetch_paid_orders(settings, max_lag_seconds=max_lag_seconds)
    indexed_orders = search.mget_orders(settings, [order["id"] for order in source_orders])
    return compare_paid_order_freshness(
        source_orders,
        indexed_orders,
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=datetime.now(timezone.utc),
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
        source_lsn=source_lsn,
    )


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
    source_lsn = db.current_wal_lsn(settings)
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
    return compare_query_results(
        invariant_name=invariant_name,
        source_rows=source_scan.rows,
        target_rows=target_rows,
        key_field=key_field,
        compare_fields=compare_fields,
        guarantee=guarantee,
        max_lag_seconds=max_lag_seconds,
        checked_at=checked_at,
        target_read_at=datetime.now(timezone.utc),
        stream_topic=settings.order_events_topic,
        stream_offset_start=offsets["stream_offset_start"],
        stream_offset_end=offsets["stream_offset_end"],
        source_lsn=source_lsn,
        source_scan=scan_evidence,
        check_status=check_status,
    )


def scan_query_hash(*, source_query: str, target_query: str, key_field: str) -> str:
    payload = "\n".join([source_query.strip(), target_query.strip(), key_field.strip()])
    return sha256(payload.encode("utf-8")).hexdigest()


def stream_offsets(settings: Settings) -> dict[str, int | None]:
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
        return {"stream_offset_start": None, "stream_offset_end": None}
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)
