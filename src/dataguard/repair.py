from __future__ import annotations

import json
import os
import time
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib import request

from .config import Settings
from .reporting import load_report

DIRECT_REINDEX = "direct-reindex"
EMIT_RECONCILE_EVENTS = "emit-reconcile-events"
REPLAY_KAFKA = "replay-kafka"
CALL_WEBHOOK = "call-webhook"
INVALIDATE_CACHE = "invalidate-cache"
CLICKHOUSE_BACKFILL = "clickhouse-backfill"

REPAIR_MODES = [
    DIRECT_REINDEX,
    EMIT_RECONCILE_EVENTS,
    REPLAY_KAFKA,
    CALL_WEBHOOK,
    INVALIDATE_CACHE,
    CLICKHOUSE_BACKFILL,
]


def repair_from_report(settings: Settings, report_path: Path, *, mode: str = DIRECT_REINDEX) -> dict[str, Any]:
    payload = load_report(report_path)
    result = repair_from_payload(settings, payload, mode=mode)
    result["report"] = str(report_path)
    return result


def repair_from_payload(
    settings: Settings,
    payload: dict[str, Any],
    *,
    mode: str = DIRECT_REINDEX,
    verbose: bool = True,
) -> dict[str, Any]:
    order_ids = repair_order_ids(payload)

    if mode == EMIT_RECONCILE_EVENTS:
        return emit_reconcile_events(settings, order_ids)
    if mode == REPLAY_KAFKA:
        return publish_kafka_replay_requests(settings, order_ids)
    if mode == CALL_WEBHOOK:
        return call_reconciliation_webhook(order_ids)
    if mode == INVALIDATE_CACHE:
        return emit_redis_invalidation_requests(settings, order_ids)
    if mode == CLICKHOUSE_BACKFILL:
        return emit_clickhouse_backfill_request(settings, order_ids)
    if mode != DIRECT_REINDEX:
        raise ValueError(f"unsupported repair mode: {mode}")

    db = import_module("dataguard.db")
    search = import_module("dataguard.search")

    orders = db.fetch_orders_by_ids(settings, order_ids)

    repaired = 0
    for order in orders:
        search.index_order(settings, order, refresh=True)
        repaired += 1
        if verbose:
            print(f"repaired order_id={order['id']}")

    return {
        "candidate_order_ids": order_ids,
        "repair_mode": mode,
        "repaired": repaired,
    }


def repair_order_ids(payload: dict[str, Any]) -> list[str]:
    return sorted(
        {
            item["order_id"]
            for item in [
                *payload.get("missing", []),
                *payload.get("stale", []),
                *payload.get("freshness_violations", []),
            ]
        }
    )


def emit_reconcile_events(settings: Settings, order_ids: list[str]) -> dict[str, Any]:
    return emit_request_file(
        settings,
        order_ids,
        mode=EMIT_RECONCILE_EVENTS,
        filename_prefix="reconcile-events",
        payload_factory=lambda order_id: {
            "action": "reconcile",
            "entity": "order",
            "id": order_id,
        },
    )


def emit_redis_invalidation_requests(settings: Settings, order_ids: list[str]) -> dict[str, Any]:
    key_template = os.getenv("REDIS_KEY_TEMPLATE", "order:{id}")
    return emit_request_file(
        settings,
        order_ids,
        mode=INVALIDATE_CACHE,
        filename_prefix="redis-invalidation",
        payload_factory=lambda order_id: {
            "action": "invalidate-cache",
            "cache": "redis",
            "entity": "order",
            "id": order_id,
            "key": key_template.format(id=order_id),
        },
    )


def emit_clickhouse_backfill_request(settings: Settings, order_ids: list[str]) -> dict[str, Any]:
    table = os.getenv("CLICKHOUSE_BACKFILL_TABLE", "orders_analytics")
    partition_hint = os.getenv("CLICKHOUSE_BACKFILL_PARTITION", "")
    report_dir = Path(getattr(settings, "report_dir", "."))
    report_dir.mkdir(parents=True, exist_ok=True)
    request_path = report_dir / f"clickhouse-backfill-{int(time.time())}.json"
    payload = {
        "action": "clickhouse-backfill",
        "entity": "order",
        "table": table,
        "partition_hint": partition_hint or None,
        "ids": order_ids,
        "reason": "kubedataguard-drift",
    }
    request_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "candidate_order_ids": order_ids,
        "repair_mode": CLICKHOUSE_BACKFILL,
        "requested": len(order_ids),
        "backfillRef": f"file://{request_path}",
        "table": table,
    }


def emit_request_file(
    settings: Settings,
    order_ids: list[str],
    *,
    mode: str,
    filename_prefix: str,
    payload_factory,
) -> dict[str, Any]:
    report_dir = Path(getattr(settings, "report_dir", "."))
    report_dir.mkdir(parents=True, exist_ok=True)
    event_path = report_dir / f"{filename_prefix}-{int(time.time())}.jsonl"
    with event_path.open("w", encoding="utf-8") as handle:
        for order_id in order_ids:
            handle.write(json.dumps(payload_factory(order_id), sort_keys=True) + "\n")

    return {
        "candidate_order_ids": order_ids,
        "repair_mode": mode,
        "emitted": len(order_ids),
        "eventRef": f"file://{event_path}",
    }


def publish_kafka_replay_requests(settings: Settings, order_ids: list[str]) -> dict[str, Any]:
    events = import_module("dataguard.events")
    topic = os.getenv("REPAIR_KAFKA_TOPIC", f"{settings.order_events_topic}.reconcile")
    published: list[dict[str, Any]] = []
    for order_id in order_ids:
        payload = {
            "action": "replay",
            "entity": "order",
            "id": order_id,
            "reason": "kubedataguard-drift",
        }
        metadata = events.publish_json(settings, topic=topic, key=order_id, value=payload)
        published.append({"id": order_id, **metadata})
    return {
        "candidate_order_ids": order_ids,
        "repair_mode": REPLAY_KAFKA,
        "published": len(published),
        "topic": topic,
        "events": published,
    }


def call_reconciliation_webhook(order_ids: list[str]) -> dict[str, Any]:
    endpoint = os.getenv("REPAIR_WEBHOOK_URL", "")
    if not endpoint:
        raise ValueError("REPAIR_WEBHOOK_URL is required for call-webhook repair mode")

    batch_size = int(os.getenv("REPAIR_WEBHOOK_BATCH_SIZE", "500"))
    responses: list[dict[str, Any]] = []
    for batch in batches(order_ids, batch_size):
        body = json.dumps(
            {
                "action": "reconcile",
                "entity": "order",
                "ids": batch,
                "reason": "kubedataguard-drift",
            },
            sort_keys=True,
        ).encode("utf-8")
        req = request.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=30) as response:  # noqa: S310 - endpoint is operator-configured
            responses.append(
                {
                    "status": response.status,
                    "reason": response.reason,
                    "count": len(batch),
                }
            )
    return {
        "candidate_order_ids": order_ids,
        "repair_mode": CALL_WEBHOOK,
        "endpoint": endpoint,
        "dispatched": len(order_ids),
        "responses": responses,
    }


def batches(values: list[str], size: int) -> list[list[str]]:
    if size < 1:
        raise ValueError("batch size must be at least 1")
    return [values[index : index + size] for index in range(0, len(values), size)]
