from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib import parse, request

from .config import Settings


def wait_for_clickhouse(settings: Settings, timeout_seconds: int = 90) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            execute(settings, "select 1")
            return
        except Exception as exc:  # pragma: no cover - depends on external service
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"ClickHouse did not become ready: {last_error}")


def init_schema(settings: Settings) -> None:
    database = quote_identifier(settings.clickhouse_database)
    table = table_ref(settings)
    execute(settings, f"create database if not exists {database}")
    execute(
        settings,
        f"""
        create table if not exists {table} (
            order_id String,
            status LowCardinality(String),
            amount_cents Int64,
            currency LowCardinality(String),
            version UInt32,
            source_updated_at DateTime64(3, 'UTC'),
            ingested_at DateTime64(3, 'UTC'),
            dataguard_topic String,
            dataguard_partition Int32,
            dataguard_offset Int64
        )
        engine = ReplacingMergeTree(version)
        order by order_id
        """,
    )


def reset_table(settings: Settings) -> None:
    execute(settings, f"truncate table if exists {table_ref(settings)}")


def backfill_orders_analytics(
    settings: Settings,
    order_ids: Iterable[str] | None = None,
    *,
    skip_every_paid: int | None = None,
) -> dict[str, Any]:
    from . import db

    if order_ids:
        orders = db.fetch_orders_by_ids(settings, order_ids)
    else:
        orders = db.fetch_paid_orders(settings, max_lag_seconds=0)

    rows = []
    skipped: list[str] = []
    paid_seen = 0
    ingested_at = clickhouse_datetime(datetime.now(timezone.utc).isoformat())
    for order in orders:
        if order.get("status") == "paid":
            paid_seen += 1
            if skip_every_paid and paid_seen % skip_every_paid == 0:
                skipped.append(str(order["id"]))
                continue
        rows.append(
            {
                "order_id": str(order["id"]),
                "status": str(order["status"]),
                "amount_cents": int(order["amount_cents"]),
                "currency": str(order["currency"]),
                "version": int(order["version"]),
                "source_updated_at": clickhouse_datetime(order["updated_at"]),
                "ingested_at": ingested_at,
                "dataguard_topic": "",
                "dataguard_partition": -1,
                "dataguard_offset": -1,
            }
        )

    if rows:
        insert_json_each_row(settings, f"insert into {table_ref(settings)} format JSONEachRow", rows)

    return {
        "table": f"{settings.clickhouse_database}.{settings.clickhouse_table}",
        "selected": len(orders),
        "inserted": len(rows),
        "skipped": len(skipped),
        "skipped_order_ids": skipped,
    }


def paid_order_summary(settings: Settings, max_lag_seconds: int) -> dict[str, Any]:
    rows = query_json_each_row(
        settings,
        f"""
        select
            count() as count,
            coalesce(sum(amount_cents), 0) as total_amount_cents,
            max(source_updated_at) as source_watermark
        from {table_ref(settings)}
        where status = 'paid'
          and source_updated_at <= now('UTC') - interval {int(max_lag_seconds)} second
        format JSONEachRow
        """,
    )
    row = rows[0] if rows else {}
    watermark = row.get("source_watermark")
    return {
        "count": int(row.get("count") or 0),
        "total_amount_cents": int(row.get("total_amount_cents") or 0),
        "source_watermark": str(watermark) if watermark else None,
    }


def execute(settings: Settings, sql: str, body: bytes | None = None) -> str:
    query = {"query": sql.strip()}
    if settings.clickhouse_user:
        query["user"] = settings.clickhouse_user
    if settings.clickhouse_password:
        query["password"] = settings.clickhouse_password
    url = f"{settings.clickhouse_url.rstrip('/')}/?{parse.urlencode(query)}"
    req = request.Request(url, data=body or b"", method="POST")
    with request.urlopen(req, timeout=30) as response:  # noqa: S310 - endpoint is operator-configured
        return response.read().decode("utf-8")


def query_json_each_row(settings: Settings, sql: str) -> list[dict[str, Any]]:
    raw = execute(settings, sql)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def insert_json_each_row(settings: Settings, sql: str, rows: list[dict[str, Any]]) -> None:
    body = "\n".join(json.dumps(row, sort_keys=True) for row in rows).encode("utf-8")
    execute(settings, sql, body=body)


def table_ref(settings: Settings) -> str:
    return f"{quote_identifier(settings.clickhouse_database)}.{quote_identifier(settings.clickhouse_table)}"


def quote_identifier(value: str) -> str:
    if not value:
        raise ValueError("ClickHouse identifier cannot be empty")
    if not all(char == "_" or char.isalnum() for char in value):
        raise ValueError(f"unsupported ClickHouse identifier: {value!r}")
    return f"`{value}`"


def clickhouse_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")
