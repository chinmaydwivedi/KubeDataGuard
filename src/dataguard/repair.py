from __future__ import annotations

import json
import time
from importlib import import_module
from pathlib import Path
from typing import Any

from .config import Settings
from .reporting import load_report

DIRECT_REINDEX = "direct-reindex"
EMIT_RECONCILE_EVENTS = "emit-reconcile-events"


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
    order_ids = sorted(
        {
            item["order_id"]
            for item in [
                *payload.get("missing", []),
                *payload.get("stale", []),
                *payload.get("freshness_violations", []),
            ]
        }
    )

    if mode == EMIT_RECONCILE_EVENTS:
        return emit_reconcile_events(settings, order_ids)
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


def emit_reconcile_events(settings: Settings, order_ids: list[str]) -> dict[str, Any]:
    report_dir = Path(getattr(settings, "report_dir", "."))
    report_dir.mkdir(parents=True, exist_ok=True)
    event_path = report_dir / f"reconcile-events-{int(time.time())}.jsonl"
    with event_path.open("w", encoding="utf-8") as handle:
        for order_id in order_ids:
            handle.write(
                json.dumps(
                    {
                        "action": "reconcile",
                        "entity": "order",
                        "id": order_id,
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    return {
        "candidate_order_ids": order_ids,
        "repair_mode": EMIT_RECONCILE_EVENTS,
        "emitted": len(order_ids),
        "eventRef": f"file://{event_path}",
    }
