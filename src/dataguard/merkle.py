from __future__ import annotations

from hashlib import sha256
from typing import Any


def normalize_bucket_summaries(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for row in rows:
        bucket = str(row.get("bucket") or "")
        count = int(row.get("count") or 0)
        total_amount_cents = int(row.get("total_amount_cents") or 0)
        version_sum = int(row.get("version_sum") or 0)
        summary = {
            "bucket": bucket,
            "count": count,
            "total_amount_cents": total_amount_cents,
            "version_sum": version_sum,
            "source_watermark": row.get("source_watermark"),
        }
        summary["fingerprint"] = bucket_fingerprint(summary)
        summaries[bucket] = summary
    return summaries


def bucket_fingerprint(summary: dict[str, Any]) -> str:
    fields = [
        str(summary.get("count") or 0),
        str(summary.get("total_amount_cents") or 0),
        str(summary.get("version_sum") or 0),
    ]
    return sha256("|".join(fields).encode("utf-8")).hexdigest()


def compare_bucket_summaries(
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    source = normalize_bucket_summaries(source_rows)
    target = normalize_bucket_summaries(target_rows)
    mismatches: list[dict[str, Any]] = []

    for bucket in sorted(set(source) | set(target)):
        source_summary = source.get(bucket) or empty_bucket(bucket)
        target_summary = target.get(bucket) or empty_bucket(bucket)
        if source_summary["fingerprint"] == target_summary["fingerprint"]:
            continue
        mismatches.append(
            {
                "bucket": bucket,
                "reason": mismatch_reason(source_summary, target_summary),
                "source": source_summary,
                "target": target_summary,
            }
        )

    return source, target, mismatches


def empty_bucket(bucket: str) -> dict[str, Any]:
    summary = {
        "bucket": bucket,
        "count": 0,
        "total_amount_cents": 0,
        "version_sum": 0,
        "source_watermark": None,
    }
    summary["fingerprint"] = bucket_fingerprint(summary)
    return summary


def mismatch_reason(source: dict[str, Any], target: dict[str, Any]) -> str:
    differences = []
    for field in ["count", "total_amount_cents", "version_sum"]:
        if int(source.get(field) or 0) != int(target.get(field) or 0):
            differences.append(field)
    if not differences:
        return "bucket fingerprint differs"
    return "bucket differs on " + ", ".join(differences)


def mismatched_bucket_keys(mismatches: list[dict[str, Any]]) -> list[str]:
    return sorted({str(item["bucket"]) for item in mismatches})


def summarize_rows_by_prefix(
    rows: list[dict[str, Any]],
    *,
    prefix_length: int,
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        bucket = str(row["id"])[:prefix_length]
        summary = buckets.setdefault(
            bucket,
            {
                "bucket": bucket,
                "count": 0,
                "total_amount_cents": 0,
                "version_sum": 0,
                "source_watermark": None,
            },
        )
        summary["count"] += 1
        summary["total_amount_cents"] += int(row.get("amount_cents") or 0)
        summary["version_sum"] += int(row.get("version") or 0)
        updated_at = row.get("updated_at")
        if updated_at and (
            summary["source_watermark"] is None
            or str(updated_at) > str(summary["source_watermark"])
        ):
            summary["source_watermark"] = str(updated_at)
    return [buckets[key] for key in sorted(buckets)]


def checksum_tree_evidence(
    *,
    prefix_length: int,
    source_bucket_count: int,
    target_bucket_count: int,
    mismatches: list[dict[str, Any]],
    drilled_source_rows: int,
    drilled_target_rows: int,
) -> dict[str, Any]:
    return {
        "mode": "prefix-bucket-checksum",
        "bucket_key": "id_prefix",
        "prefix_length": prefix_length,
        "source_bucket_count": source_bucket_count,
        "target_bucket_count": target_bucket_count,
        "mismatched_bucket_count": len(mismatches),
        "mismatched_buckets": mismatches[:25],
        "drilldown": {
            "strategy": "fetch-only-mismatched-buckets",
            "source_rows": drilled_source_rows,
            "target_rows": drilled_target_rows,
        },
        "fingerprint_fields": ["count", "total_amount_cents", "version_sum"],
    }
