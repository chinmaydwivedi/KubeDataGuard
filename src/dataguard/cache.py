from __future__ import annotations

import json
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from .config import Settings

if TYPE_CHECKING:
    from redis import Redis


def client(settings: Settings) -> "Redis":
    import redis

    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


def wait_for_redis(settings: Settings, timeout_seconds: int = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    redis_client = client(settings)
    while time.monotonic() < deadline:
        try:
            if redis_client.ping():
                return
        except Exception as exc:  # pragma: no cover - depends on external service
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"Redis did not become ready: {last_error}")


def order_key(settings: Settings, order_id: str) -> str:
    return f"{settings.order_cache_prefix}:{order_id}"


def cache_order(
    settings: Settings,
    order: dict[str, Any],
    *,
    event_metadata: dict[str, Any] | None = None,
) -> None:
    document = dict(order)
    document["cached_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    document["dataguard_cached_from"] = "kafka" if event_metadata else "source"
    if event_metadata:
        document["dataguard_topic"] = event_metadata.get("topic")
        document["dataguard_partition"] = event_metadata.get("partition")
        document["dataguard_offset"] = event_metadata.get("offset")
    client(settings).set(order_key(settings, order["id"]), json.dumps(document, sort_keys=True))


def mget_orders(settings: Settings, order_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    ids = list(order_ids)
    if not ids:
        return {}
    redis_client = client(settings)
    values = redis_client.mget([order_key(settings, order_id) for order_id in ids])
    found: dict[str, dict[str, Any]] = {}
    for order_id, raw in zip(ids, values, strict=True):
        if raw:
            document = json.loads(raw)
            if "cached_at" in document and "indexed_at" not in document:
                document["indexed_at"] = document["cached_at"]
            found[str(order_id)] = document
    return found


def reset_order_cache(settings: Settings) -> int:
    redis_client = client(settings)
    pattern = f"{settings.order_cache_prefix}:*"
    deleted = 0
    for key in redis_client.scan_iter(pattern):
        deleted += int(redis_client.delete(key) or 0)
    return deleted


def target_offset_frontier(settings: Settings) -> dict[str, Any]:
    redis_client = client(settings)
    offsets: dict[tuple[str, int], dict[str, Any]] = {}
    count = 0
    for key in redis_client.scan_iter(f"{settings.order_cache_prefix}:*"):
        raw = redis_client.get(key)
        if not raw:
            continue
        document = json.loads(raw)
        topic = document.get("dataguard_topic")
        partition = document.get("dataguard_partition")
        offset = document.get("dataguard_offset")
        if topic is None or partition is None or offset is None:
            continue
        count += 1
        frontier_key = (str(topic), int(partition))
        current = offsets.setdefault(
            frontier_key,
            {
                "topic": str(topic),
                "partition": int(partition),
                "event_count": 0,
                "max_applied_offset": None,
            },
        )
        current["event_count"] += 1
        if current["max_applied_offset"] is None or int(offset) > int(current["max_applied_offset"]):
            current["max_applied_offset"] = int(offset)

    return {
        "system": "redis",
        "key_prefix": settings.order_cache_prefix,
        "offset_recorded_count": count,
        "applied_offsets": list(offsets.values()),
    }
