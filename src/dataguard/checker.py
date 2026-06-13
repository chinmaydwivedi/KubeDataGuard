from __future__ import annotations

import signal
from datetime import datetime, timedelta, timezone

from . import db, search
from .config import Settings
from .invariants import (
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
) -> DriftReport:
    checked_at = datetime.now(timezone.utc)
    offsets = stream_offsets(settings)
    source_lsn = db.current_wal_lsn(settings)
    source_rows = db.execute_source_query(settings, source_query)
    target_rows = search.execute_target_query(
        settings,
        target_query,
        key_field=key_field,
    )
    guarantee = "existence"
    if compare_fields:
        guarantee = "existence+fieldEquality"
    return compare_query_results(
        invariant_name=invariant_name,
        source_rows=source_rows,
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
    )


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
