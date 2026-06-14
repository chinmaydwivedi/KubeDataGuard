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
            "dataguard_event_id": {"type": "keyword"},
            "dataguard_event_type": {"type": "keyword"},
            "dataguard_indexed_from": {"type": "keyword"},
            "dataguard_topic": {"type": "keyword"},
            "dataguard_partition": {"type": "integer"},
            "dataguard_offset": {"type": "long"},
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


def index_order(
    settings: Settings,
    order: dict[str, Any],
    *,
    refresh: bool = False,
    event_metadata: dict[str, Any] | None = None,
) -> None:
    document = dict(order)
    document["indexed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    document["dataguard_indexed_from"] = "kafka" if event_metadata else "source"
    if event_metadata:
        document["dataguard_event_id"] = event_metadata.get("event_id")
        document["dataguard_event_type"] = event_metadata.get("event_type")
        document["dataguard_topic"] = event_metadata.get("topic")
        document["dataguard_partition"] = event_metadata.get("partition")
        document["dataguard_offset"] = event_metadata.get("offset")
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


def paid_order_bucket_summaries(
    settings: Settings,
    *,
    updated_before: str | None = None,
    prefix_length: int = 1,
    max_buckets: int = 65536,
) -> list[dict[str, Any]]:
    prefix_length = bounded_prefix_length(prefix_length)
    filters: list[dict[str, Any]] = [{"term": {"status": "paid"}}]
    if updated_before:
        filters.append({"range": {"updated_at": {"lte": updated_before}}})

    response = client(settings).search(
        index=settings.orders_index,
        body={
            "size": 0,
            "query": {"bool": {"filter": filters}},
            "aggs": {
                "id_prefixes": {
                    "terms": {
                        "script": id_prefix_script(prefix_length),
                        "size": max_buckets,
                    },
                    "aggs": {
                        "total_amount_cents": {"sum": {"field": "amount_cents"}},
                        "version_sum": {"sum": {"field": "version"}},
                    },
                }
            },
        },
    )

    buckets = response.get("aggregations", {}).get("id_prefixes", {}).get("buckets", [])
    summaries: list[dict[str, Any]] = []
    for bucket in buckets:
        summaries.append(
            {
                "bucket": str(bucket["key"]),
                "count": int(bucket.get("doc_count") or 0),
                "total_amount_cents": int(bucket.get("total_amount_cents", {}).get("value") or 0),
                "version_sum": int(bucket.get("version_sum", {}).get("value") or 0),
                "source_watermark": None,
            }
        )
    return sorted(summaries, key=lambda item: item["bucket"])


def fetch_paid_orders_by_id_prefix(
    settings: Settings,
    prefixes: Iterable[str],
    *,
    updated_before: str | None = None,
    prefix_length: int = 1,
    page_size: int = DEFAULT_TARGET_QUERY_PAGE_SIZE,
) -> list[dict[str, Any]]:
    prefix_list = sorted({str(prefix) for prefix in prefixes if str(prefix)})
    if not prefix_list:
        return []
    prefix_length = bounded_prefix_length(prefix_length)
    filters: list[dict[str, Any]] = [
        {"term": {"status": "paid"}},
        {
            "script": {
                "script": {
                    "lang": "painless",
                    "source": (
                        "if (doc['id'].size() == 0) { return false; } "
                        "int prefixLength = (int) params.prefix_length; "
                        "String value = doc['id'].value; "
                        "String bucket = value.length() <= prefixLength ? value : value.substring(0, prefixLength); "
                        "for (String prefix : params.prefixes) { "
                        "  if (bucket == prefix) { return true; } "
                        "} "
                        "return false;"
                    ),
                    "params": {"prefixes": prefix_list, "prefix_length": prefix_length},
                }
            }
        },
    ]
    if updated_before:
        filters.append({"range": {"updated_at": {"lte": updated_before}}})

    body = {
        "query": {"bool": {"filter": filters}},
        "sort": [
            {"id": {"order": "asc", "unmapped_type": "keyword"}},
            {"_id": {"order": "asc"}},
        ],
    }
    return paginated_search(settings, body, page_size=page_size)


def target_offset_frontier(settings: Settings) -> dict[str, Any]:
    os_client = client(settings)
    response = os_client.search(
        index=settings.orders_index,
        body={
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"exists": {"field": "dataguard_offset"}},
                        {"exists": {"field": "dataguard_topic"}},
                        {"exists": {"field": "dataguard_partition"}},
                    ]
                }
            },
            "aggs": {
                "topics": {
                    "terms": {"field": "dataguard_topic", "size": 50},
                    "aggs": {
                        "partitions": {
                            "terms": {"field": "dataguard_partition", "size": 200},
                            "aggs": {
                                "max_offset": {"max": {"field": "dataguard_offset"}},
                                "event_count": {"value_count": {"field": "dataguard_offset"}},
                            },
                        }
                    },
                }
            },
        },
    )

    offsets: list[dict[str, Any]] = []
    for topic_bucket in response.get("aggregations", {}).get("topics", {}).get("buckets", []):
        topic = topic_bucket.get("key")
        for partition_bucket in topic_bucket.get("partitions", {}).get("buckets", []):
            max_offset = partition_bucket.get("max_offset", {}).get("value")
            offsets.append(
                {
                    "topic": topic,
                    "partition": int(partition_bucket["key"]),
                    "event_count": int(partition_bucket.get("event_count", {}).get("value") or 0),
                    "max_applied_offset": int(max_offset) if max_offset is not None else None,
                }
            )

    total = response.get("hits", {}).get("total", 0)
    count = total.get("value", 0) if isinstance(total, dict) else total
    return {
        "system": "opensearch",
        "index": settings.orders_index,
        "offset_recorded_count": int(count),
        "applied_offsets": offsets,
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

    return paginated_search(settings, body, page_size=page_size, key_field=key_field)


def paginated_search(
    settings: Settings,
    body: dict[str, Any],
    *,
    page_size: int,
    key_field: str = "id",
) -> list[dict[str, Any]]:
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


def bounded_prefix_length(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("checksum prefix length must be an integer") from exc
    if parsed < 1:
        raise ValueError("checksum prefix length must be at least 1")
    return min(parsed, 12)


def id_prefix_script(prefix_length: int) -> dict[str, Any]:
    return {
        "lang": "painless",
        "source": (
            "if (doc['id'].size() == 0) { return ''; } "
            "int prefixLength = (int) params.prefix_length; "
            "String value = doc['id'].value; "
            "int size = value.length() <= prefixLength ? value.length() : prefixLength; "
            "return value.substring(0, size);"
        ),
        "params": {"prefix_length": prefix_length},
    }


def ensure_search_after_sort(body: dict[str, Any], *, key_field: str) -> None:
    if "sort" not in body:
        body["sort"] = [
            {key_field: {"order": "asc", "unmapped_type": "keyword"}},
            {"_id": {"order": "asc"}},
        ]
        return
    if not isinstance(body["sort"], list):
        raise ValueError("target query sort must be a list when paginating with search_after")
