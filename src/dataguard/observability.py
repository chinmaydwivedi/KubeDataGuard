from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .config import Settings


def write_observability_artifacts(
    settings: Settings,
    payload: dict[str, Any],
    *,
    artifact_name: str = "latest",
) -> dict[str, str]:
    report_dir = Path(getattr(settings, "report_dir", "."))
    report_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = report_dir / f"{artifact_name}.prom"
    trace_path = report_dir / f"{artifact_name}.otel.jsonl"
    metrics_path.write_text(prometheus_metrics(payload), encoding="utf-8")
    append_trace_span(trace_path, payload)
    return {
        "metrics": str(metrics_path),
        "trace": str(trace_path),
    }


def prometheus_metrics(payload: dict[str, Any]) -> str:
    invariant = str(payload.get("invariant") or "unknown")
    status = str(payload.get("status") or "Unknown")
    guarantee = str(payload.get("guarantee") or "unknown")
    labels = {
        "invariant": invariant,
        "status": status,
        "guarantee": guarantee,
    }
    lines = [
        "# HELP kubedataguard_drift_count Number of current drift counterexamples.",
        "# TYPE kubedataguard_drift_count gauge",
        metric_line("kubedataguard_drift_count", payload.get("drift_count", 0), labels),
        "# HELP kubedataguard_checked_records Number of records checked in the last run.",
        "# TYPE kubedataguard_checked_records gauge",
        metric_line("kubedataguard_checked_records", payload.get("checked_records", 0), labels),
        "# HELP kubedataguard_counterexample_count Number of compact counterexamples emitted.",
        "# TYPE kubedataguard_counterexample_count gauge",
        metric_line("kubedataguard_counterexample_count", len(payload.get("counterexamples") or []), labels),
        "# HELP kubedataguard_slo_breach_count Number of historical SLO breaches observed.",
        "# TYPE kubedataguard_slo_breach_count gauge",
        metric_line("kubedataguard_slo_breach_count", len(payload.get("freshness_breaches") or []), labels),
        "# HELP kubedataguard_check_healthy Whether the last check was healthy.",
        "# TYPE kubedataguard_check_healthy gauge",
        metric_line("kubedataguard_check_healthy", 1 if payload.get("healthy") else 0, labels),
    ]

    frontier = ((payload.get("observation_window") or {}).get("cdc_frontier") or {})
    frontier_status = frontier.get("status")
    if frontier_status:
        frontier_labels = dict(labels)
        frontier_labels["frontier_status"] = str(frontier_status)
        lines.extend(
            [
                "# HELP kubedataguard_cdc_frontier_status Last CDC frontier state as a one-hot gauge.",
                "# TYPE kubedataguard_cdc_frontier_status gauge",
                metric_line("kubedataguard_cdc_frontier_status", 1, frontier_labels),
            ]
        )
    return "\n".join(lines) + "\n"


def append_trace_span(path: Path, payload: dict[str, Any]) -> None:
    window = payload.get("observation_window") or {}
    span = {
        "name": "kubedataguard.check",
        "kind": "INTERNAL",
        "timeUnixNano": int(time.time() * 1_000_000_000),
        "attributes": {
            "dataguard.invariant": payload.get("invariant"),
            "dataguard.status": payload.get("status"),
            "dataguard.guarantee": payload.get("guarantee"),
            "dataguard.healthy": payload.get("healthy"),
            "dataguard.drift_count": payload.get("drift_count"),
            "dataguard.checked_records": payload.get("checked_records"),
            "dataguard.check_status": payload.get("check_status"),
            "dataguard.check_id": payload.get("checkID"),
            "dataguard.source_lsn": window.get("source_lsn"),
            "dataguard.cdc_frontier_status": (window.get("cdc_frontier") or {}).get("status"),
        },
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(span, sort_keys=True) + "\n")


def metric_line(name: str, value: Any, labels: dict[str, str]) -> str:
    return f"{name}{format_labels(labels)} {numeric(value)}"


def format_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    encoded = ",".join(f'{key}="{escape_label(value)}"' for key, value in sorted(labels.items()))
    return "{" + encoded + "}"


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def numeric(value: Any) -> int | float:
    if isinstance(value, bool):
        return 1 if value else 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0
