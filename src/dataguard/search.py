from __future__ import annotations

import json
import time
from collections.abc import Iterable
from typing import Any

from opensearchpy import OpenSearch

from .config import Settings


INDEX_BODY = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s",
        }
    },
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "customer_id": {"type": "keyword"},
            "status": {"type": "keyword"},
            "amount_cents": {"type": "integer"},
            "currency": {"type": "keyword"},
            "version": {"type": "integer"},
            "created_at": {"type": "date"},
            "updated_at": {"type": "date"},
            "indexed_at": {"type": "date"},
        }
    },
}


def client(settings: Settings) -> OpenSearch:
    return OpenSearch(hosts=[settings.opensearch_url])


def wait_for_opensearch(settings: Settings, timeout_seconds: int = 90) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    os_client = client(settings)
    while time.monotonic() < deadline:
        try:
            if os_client.ping():
                return
        except Exception as exc:  # pragma: no cover - depends on external service
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"OpenSearch did not become ready: {last_error}")


def ensure_index(settings: Settings) -> None:
    os_client = client(settings)
    if not os_client.indices.exists(settings.orders_index):
        os_client.indices.create(index=settings.orders_index, body=INDEX_BODY)


def reset_index(settings: Settings) -> None:
    os_client = client(settings)
    if os_client.indices.exists(settings.orders_index):
        os_client.indices.delete(index=settings.orders_index)
    os_client.indices.create(index=settings.orders_index, body=INDEX_BODY)


def index_order(settings: Settings, order: dict[str, Any], *, refresh: bool = False) -> None:
    document = dict(order)
    document["indexed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    os_client = client(settings)
    os_client.index(
        index=settings.orders_index,
        id=order["id"],
        body=document,
        refresh=refresh,
    )


def update_indexed_at(
    settings: Settings,
    *,
    order_id: str,
    indexed_at: str,
    refresh: bool = False,
) -> None:
    os_client = client(settings)
    os_client.update(
        index=settings.orders_index,
        id=order_id,
        body={"doc": {"indexed_at": indexed_at}},
        refresh=refresh,
    )


def mget_orders(settings: Settings, order_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    ids = list(order_ids)
    if not ids:
        return {}
    os_client = client(settings)
    response = os_client.mget(index=settings.orders_index, body={"ids": ids})
    found: dict[str, dict[str, Any]] = {}
    for doc in response.get("docs", []):
        if doc.get("found"):
            found[str(doc["_id"])] = doc["_source"]
    return found


def paid_order_summary(
    settings: Settings,
    *,
    updated_before: str | None = None,
) -> dict[str, int]:
    filters: list[dict[str, Any]] = [{"term": {"status": "paid"}}]
    if updated_before:
        filters.append({"range": {"updated_at": {"lte": updated_before}}})

    os_client = client(settings)
    response = os_client.search(
        index=settings.orders_index,
        body={
            "size": 0,
            "query": {"bool": {"filter": filters}},
            "aggs": {
                "total_amount_cents": {
                    "sum": {
                        "field": "amount_cents",
                    }
                }
            },
        },
    )

    total = response["hits"]["total"]
    count = total["value"] if isinstance(total, dict) else total
    total_amount = response["aggregations"]["total_amount_cents"]["value"] or 0
    return {
        "count": int(count),
        "total_amount_cents": int(total_amount),
    }


def execute_target_query(
    settings: Settings,
    query: str,
    *,
    key_field: str,
) -> list[dict[str, Any]]:
    if not query.strip():
        raise ValueError("target query is required")
    body = json.loads(query)
    if not isinstance(body, dict):
        raise ValueError("target query must be a JSON object")
    body.setdefault("size", 10000)

    os_client = client(settings)
    response = os_client.search(index=settings.orders_index, body=body)
    rows: list[dict[str, Any]] = []
    for hit in response.get("hits", {}).get("hits", []):
        source = dict(hit.get("_source") or {})
        source.setdefault(key_field, str(hit.get("_id")))
        rows.append(source)
    return rows
