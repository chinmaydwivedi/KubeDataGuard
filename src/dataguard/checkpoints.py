from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings


CHECKPOINT_KIND = "KubeDataGuardScanCheckpoint"


def load_scan_checkpoint(settings: Settings, checkpoint_id: str) -> dict[str, Any] | None:
    if not checkpoint_id:
        return None
    store = checkpoint_store(settings)
    if store == "local":
        path = local_checkpoint_path(settings, checkpoint_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    if store == "s3":
        return load_s3_checkpoint(settings, checkpoint_id)
    raise ValueError(f"unsupported REPORT_STORE for checkpoints: {settings.report_store}")


def save_scan_checkpoint(
    settings: Settings,
    checkpoint_id: str,
    payload: dict[str, Any],
) -> str:
    if not checkpoint_id:
        raise ValueError("checkpoint_id is required")
    normalized = normalize_checkpoint_payload(checkpoint_id, payload)
    store = checkpoint_store(settings)
    if store == "local":
        path = local_checkpoint_path(settings, checkpoint_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(normalized, indent=2, sort_keys=True), encoding="utf-8")
        return f"file://{path}"
    if store == "s3":
        return save_s3_checkpoint(settings, checkpoint_id, normalized)
    raise ValueError(f"unsupported REPORT_STORE for checkpoints: {settings.report_store}")


def update_checkpoint_report_ref(
    settings: Settings,
    checkpoint_id: str,
    report_ref: str,
) -> str | None:
    if not checkpoint_id:
        return None
    payload = load_scan_checkpoint(settings, checkpoint_id)
    if not payload:
        return None
    payload["report_ref"] = report_ref
    return save_scan_checkpoint(settings, checkpoint_id, payload)


def normalize_checkpoint_payload(checkpoint_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized["kind"] = CHECKPOINT_KIND
    normalized["checkpoint_id"] = checkpoint_id
    normalized["updated_at"] = datetime.now(timezone.utc).isoformat()
    return normalized


def checkpoint_store(settings: Settings) -> str:
    return (getattr(settings, "report_store", "local") or "local").strip().lower()


def local_checkpoint_path(settings: Settings, checkpoint_id: str) -> Path:
    return Path(settings.report_dir) / "scan-checkpoints" / f"{safe_checkpoint_id(checkpoint_id)}.json"


def checkpoint_s3_key(settings: Settings, checkpoint_id: str) -> str:
    prefix = getattr(settings, "report_prefix", "kubedataguard/reports").strip("/")
    key = f"scan-checkpoints/{safe_checkpoint_id(checkpoint_id)}.json"
    if not prefix:
        return key
    return f"{prefix}/{key}"


def load_s3_checkpoint(settings: Settings, checkpoint_id: str) -> dict[str, Any] | None:
    if not settings.report_bucket:
        raise ValueError("REPORT_BUCKET is required when REPORT_STORE=s3")
    client = s3_client(settings)
    key = checkpoint_s3_key(settings, checkpoint_id)
    try:
        response = client.get_object(Bucket=settings.report_bucket, Key=key)
    except Exception as exc:
        if is_missing_s3_object(exc):
            return None
        raise
    return json.loads(response["Body"].read().decode("utf-8"))


def save_s3_checkpoint(
    settings: Settings,
    checkpoint_id: str,
    payload: dict[str, Any],
) -> str:
    if not settings.report_bucket:
        raise ValueError("REPORT_BUCKET is required when REPORT_STORE=s3")
    client = s3_client(settings)
    key = checkpoint_s3_key(settings, checkpoint_id)
    client.put_object(
        Bucket=settings.report_bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{settings.report_bucket}/{key}"


def s3_client(settings: Settings):
    import boto3

    kwargs: dict[str, str] = {"region_name": settings.aws_region}
    if settings.report_s3_endpoint_url:
        kwargs["endpoint_url"] = settings.report_s3_endpoint_url
    return boto3.client("s3", **kwargs)


def is_missing_s3_object(exc: Exception) -> bool:
    code = getattr(exc, "response", {}).get("Error", {}).get("Code")
    return str(code) in {"NoSuchKey", "404", "NotFound"}


def safe_checkpoint_id(checkpoint_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", checkpoint_id.strip())
    safe = safe.strip(".-")
    if not safe:
        raise ValueError("checkpoint_id must contain at least one safe character")
    return safe[:180]
