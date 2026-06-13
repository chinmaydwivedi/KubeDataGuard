from __future__ import annotations

import json
import time
from collections.abc import Iterable
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from .config import Settings

if TYPE_CHECKING:
    from opensearchpy import OpenSearch


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

DEFAULT_TARGET_QUERY_PAGE_SIZE = 1000
MAX_TARGET_QUERY_PAGE_SIZE = 5000


def client(settings: Settings) -> "OpenSearch":
    from opensearchpy import OpenSearch

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
    page_size: int = DEFAULT_TARGET_QUERY_PAGE_SIZE,
) -> list[dict[str, Any]]:
    if not query.strip():
        raise ValueError("target query is required")
    body = json.loads(query)
    if not isinstance(body, dict):
        raise ValueError("target query must be a JSON object")
    if "from" in body:
        raise ValueError("target query must not use 'from'; KubeDataGuard paginates with search_after")

    page_size = bounded_page_size(body.pop("size", page_size))
    ensure_search_after_sort(body, key_field=key_field)

    os_client = client(settings)
    rows: list[dict[str, Any]] = []
    search_after: list[Any] | None = None
    while True:
        page_body = deepcopy(body)
        page_body["size"] = page_size
        if search_after is not None:
            page_body["search_after"] = search_after

        response = os_client.search(index=settings.orders_index, body=page_body)
        hits = response.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            source = dict(hit.get("_source") or {})
            source.setdefault(key_field, str(hit.get("_id")))
            rows.append(source)

        if len(hits) < page_size:
            break
        search_after = hits[-1].get("sort")
        if not search_after:
            raise ValueError("OpenSearch response is missing sort values required for search_after pagination")
    return rows


def bounded_page_size(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("target query size must be an integer page size") from exc
    if parsed < 1:
        raise ValueError("target query page size must be at least 1")
    return min(parsed, MAX_TARGET_QUERY_PAGE_SIZE)


def ensure_search_after_sort(body: dict[str, Any], *, key_field: str) -> None:
    if "sort" not in body:
        body["sort"] = [
            {key_field: {"order": "asc", "unmapped_type": "keyword"}},
            {"_id": {"order": "asc"}},
        ]
        return
    if not isinstance(body["sort"], list):
        raise ValueError("target query sort must be a list when paginating with search_after")
