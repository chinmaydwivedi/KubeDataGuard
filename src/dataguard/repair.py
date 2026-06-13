from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import Settings
from .reporting import load_report


def repair_from_report(settings: Settings, report_path: Path) -> dict[str, Any]:
    payload = load_report(report_path)
    result = repair_from_payload(settings, payload)
    result["report"] = str(report_path)
    return result


def repair_from_payload(
    settings: Settings,
    payload: dict[str, Any],
    *,
    verbose: bool = True,
) -> dict[str, Any]:
    from . import db, search

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
    orders = db.fetch_orders_by_ids(settings, order_ids)

    repaired = 0
    for order in orders:
        search.index_order(settings, order, refresh=True)
        repaired += 1
        if verbose:
            print(f"repaired order_id={order['id']}")

    return {
        "candidate_order_ids": order_ids,
        "repaired": repaired,
    }
