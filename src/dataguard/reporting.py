from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .invariants import DriftReport


def write_reports(report_dir: Path, report: DriftReport) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = report_dir / f"drift-{stamp}.json"
    markdown_path = report_dir / f"drift-{stamp}.md"
    payload = report.to_dict()
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(markdown_report(payload), encoding="utf-8")
    latest_path = report_dir / "latest.json"
    latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return json_path, markdown_path


def markdown_report(payload: dict[str, Any]) -> str:
    observation_window = payload.get("observation_window") or {}
    lines = [
        f"# Drift Report: {payload['invariant']}",
        "",
        f"- Status: `{payload.get('status', 'Unknown')}`",
        f"- Guarantee: `{payload.get('guarantee', 'unknown')}`",
        f"- Healthy: `{payload['healthy']}`",
        f"- Check status: `{payload.get('check_status', 'unknown')}`",
        f"- Checked records: `{payload['checked_records']}`",
        f"- Drift count: `{payload['drift_count']}`",
        f"- Missing: `{len(payload['missing'])}`",
        f"- Stale: `{len(payload['stale'])}`",
        f"- Aggregate mismatches: `{len(payload.get('aggregate_mismatches', []))}`",
        f"- Freshness violations: `{len(payload.get('freshness_violations', []))}`",
        f"- Freshness SLO breaches: `{len(payload.get('freshness_breaches', []))}`",
        "",
    ]

    if observation_window:
        lines.extend(
            [
                "## Observation Window",
                "",
                f"- Checked at: `{observation_window.get('checked_at')}`",
                f"- Target read at: `{observation_window.get('target_read_at')}`",
                f"- Max lag seconds: `{observation_window.get('max_lag_seconds')}`",
                f"- Eligible records before: `{observation_window.get('eligible_records_before')}`",
                f"- Source watermark: `{observation_window.get('source_watermark')}`",
                f"- Source LSN: `{observation_window.get('source_lsn')}`",
                f"- Stream topic: `{observation_window.get('stream_topic')}`",
                f"- Stream offset start: `{observation_window.get('stream_offset_start')}`",
                f"- Stream offset end: `{observation_window.get('stream_offset_end')}`",
                f"- Completeness: `{observation_window.get('completeness')}`",
                "",
            ]
        )

    if payload.get("kubernetes_status"):
        lines.extend(["## Kubernetes Status Payload", "", "```json"])
        lines.append(json.dumps(payload["kubernetes_status"], indent=2, sort_keys=True))
        lines.extend(["```", ""])

    if payload["missing"]:
        lines.extend(["## Missing Orders", ""])
        for item in payload["missing"]:
            source = item["source"]
            lines.append(
                f"- `{item['order_id']}` status=`{source['status']}` "
                f"amount_cents=`{source['amount_cents']}`"
            )
        lines.append("")

    if payload["stale"]:
        lines.extend(["## Stale Orders", ""])
        for item in payload["stale"]:
            mismatch_text = ", ".join(
                f"{m['field']}: source={m['source']} target={m['target']}"
                for m in item["mismatches"]
            )
            lines.append(f"- `{item['order_id']}` {mismatch_text}")
        lines.append("")

    if payload.get("aggregate_mismatches"):
        lines.extend(["## Aggregate Mismatches", ""])
        for item in payload["aggregate_mismatches"]:
            lines.append(
                f"- `{item['field']}` source=`{item['source']}` "
                f"target=`{item['target']}`"
            )
        lines.append("")

    if payload.get("freshness_violations"):
        lines.extend(["## Freshness Violations", ""])
        for item in payload["freshness_violations"]:
            lines.append(
                f"- `{item['order_id']}` lag_seconds=`{item.get('observed_lag_seconds')}` "
                f"max_lag_seconds=`{item.get('max_lag_seconds')}` "
                f"target_indexed_at=`{item.get('target_indexed_at')}`"
            )
        lines.append("")

    if payload.get("freshness_breaches"):
        lines.extend(["## Freshness SLO Breaches", ""])
        for item in payload["freshness_breaches"]:
            lines.append(
                f"- `{item['order_id']}` lag_seconds=`{item.get('observed_lag_seconds')}` "
                f"max_lag_seconds=`{item.get('max_lag_seconds')}` "
                f"target_indexed_at=`{item.get('target_indexed_at')}`"
            )
        lines.append("")

    if payload.get("counterexamples"):
        lines.extend(["## Counterexamples", ""])
        for item in payload["counterexamples"]:
            if item["type"] == "aggregate_mismatch":
                lines.append(
                    f"- aggregate `{item['field']}` source=`{item['source']}` "
                    f"target=`{item['target']}`"
                )
            else:
                lines.append(
                    f"- {item['type']} order_id=`{item['order_id']}` "
                    f"source_version=`{item.get('source_version')}`"
                )
        lines.append("")

    if payload["healthy"]:
        lines.append("No drift found. The derived search view satisfies the invariant.")
    else:
        lines.append("Drift found. Run the repair command or inspect the pipeline before trusting the derived view.")

    lines.append("")
    return "\n".join(lines)


def load_report(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
