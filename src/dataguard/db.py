from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

import psycopg
from psycopg.rows import dict_row

from .config import Settings


SCHEMA_SQL = """
create extension if not exists pgcrypto;

create table if not exists orders (
    id uuid primary key default gen_random_uuid(),
    customer_id text not null,
    status text not null check (status in ('created', 'paid', 'cancelled')),
    amount_cents integer not null check (amount_cents >= 0),
    currency text not null default 'USD',
    version integer not null default 1,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists order_events_outbox (
    id bigserial primary key,
    event_id uuid not null default gen_random_uuid(),
    event_type text not null,
    order_id uuid not null references orders(id),
    payload jsonb not null,
    created_at timestamptz not null default now(),
    published_at timestamptz
);

create index if not exists idx_orders_status_updated_at
    on orders(status, updated_at);

create index if not exists idx_order_events_outbox_order_id
    on order_events_outbox(order_id);
"""


@contextmanager
def connect(settings: Settings):
    with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as conn:
        yield conn


def wait_for_postgres(settings: Settings, timeout_seconds: int = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with connect(settings) as conn:
                with conn.cursor() as cur:
                    cur.execute("select 1")
                    return
        except Exception as exc:  # pragma: no cover - depends on external service
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"Postgres did not become ready: {last_error}")


def init_schema(settings: Settings) -> None:
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()


def reset_demo_data(settings: Settings) -> None:
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("truncate table order_events_outbox restart identity cascade")
            cur.execute("delete from orders")
        conn.commit()


def insert_order_with_event(
    settings: Settings,
    *,
    customer_id: str,
    status: str,
    amount_cents: int,
    currency: str = "USD",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into orders (
                    customer_id,
                    status,
                    amount_cents,
                    currency,
                    created_at,
                    updated_at
                )
                values (%s, %s, %s, %s, %s, %s)
                returning *
                """,
                (customer_id, status, amount_cents, currency, now, now),
            )
            order = dict(cur.fetchone())
            payload = order_event_payload(order)
            cur.execute(
                """
                insert into order_events_outbox (event_type, order_id, payload)
                values (%s, %s, %s)
                returning event_id
                """,
                (payload["event_type"], order["id"], json.dumps(payload)),
            )
            event = cur.fetchone()
        conn.commit()

    payload["event_id"] = str(event["event_id"])
    return payload


def mark_event_published(settings: Settings, event_id: str) -> None:
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update order_events_outbox
                set published_at = now()
                where event_id = %s
                """,
                (event_id,),
            )
        conn.commit()


def order_event_payload(order: dict[str, Any]) -> dict[str, Any]:
    status = order["status"]
    event_type = {
        "created": "OrderCreated",
        "paid": "OrderPaid",
        "cancelled": "OrderCancelled",
    }[status]
    return {
        "event_type": event_type,
        "order_id": str(order["id"]),
        "version": order["version"],
        "occurred_at": _iso(order["updated_at"]),
        "order": serialize_order(order),
    }


def serialize_order(order: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(order["id"]),
        "customer_id": order["customer_id"],
        "status": order["status"],
        "amount_cents": int(order["amount_cents"]),
        "currency": order["currency"],
        "version": int(order["version"]),
        "created_at": _iso(order["created_at"]),
        "updated_at": _iso(order["updated_at"]),
    }


def fetch_paid_orders(settings: Settings, max_lag_seconds: int) -> list[dict[str, Any]]:
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select *
                from orders
                where status = 'paid'
                  and updated_at <= now() - make_interval(secs => %s)
                order by updated_at asc, id asc
                """,
                (max_lag_seconds,),
            )
            return [serialize_order(row) for row in cur.fetchall()]


def fetch_paid_order_summary(settings: Settings, max_lag_seconds: int) -> dict[str, Any]:
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                  count(*)::integer as count,
                  coalesce(sum(amount_cents), 0)::bigint as total_amount_cents,
                  max(updated_at) as source_watermark
                from orders
                where status = 'paid'
                  and updated_at <= now() - make_interval(secs => %s)
                """,
                (max_lag_seconds,),
            )
            row = cur.fetchone()

    return {
        "count": int(row["count"]),
        "total_amount_cents": int(row["total_amount_cents"]),
        "source_watermark": (
            _iso(row["source_watermark"])
            if row["source_watermark"] is not None
            else None
        ),
    }


def current_wal_lsn(settings: Settings) -> str:
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("select pg_current_wal_lsn()::text as lsn")
            row = cur.fetchone()
    return str(row["lsn"])


def fetch_orders_by_ids(settings: Settings, order_ids: Iterable[str]) -> list[dict[str, Any]]:
    ids = list(order_ids)
    if not ids:
        return []
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select *
                from orders
                where id = any(%s::uuid[])
                order by updated_at asc, id asc
                """,
                (ids,),
            )
            return [serialize_order(row) for row in cur.fetchall()]


def execute_source_query(settings: Settings, query: str) -> list[dict[str, Any]]:
    if not query.strip():
        raise ValueError("source query is required")
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    return [serialize_query_row(row) for row in rows]


def set_orders_updated_at(
    settings: Settings,
    order_ids: Iterable[str],
    updated_at: datetime,
) -> int:
    ids = list(order_ids)
    if not ids:
        return 0
    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update orders
                set updated_at = %s
                where id = any(%s::uuid[])
                """,
                (updated_at, ids),
            )
            updated = cur.rowcount
        conn.commit()
    return int(updated)


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def serialize_query_row(row: dict[str, Any]) -> dict[str, Any]:
    return {str(key): json_safe_value(value) for key, value in row.items()}


def json_safe_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value
