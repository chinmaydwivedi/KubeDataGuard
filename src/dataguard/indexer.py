from __future__ import annotations

from . import events, search
from .config import Settings


def run_indexer(
    settings: Settings,
    *,
    group_id: str,
    max_messages: int | None,
    skip_every_paid: int | None,
) -> dict[str, int]:
    processed = 0
    indexed = 0
    skipped = 0
    paid_seen = 0

    for item in events.consume_events(
        settings,
        group_id=group_id,
        max_messages=max_messages,
    ):
        event = item["event"]
        processed += 1

        if event["event_type"] == "OrderPaid":
            paid_seen += 1
            if skip_every_paid and paid_seen % skip_every_paid == 0:
                skipped += 1
                item["commit"]()
                print(
                    "simulated consumer bug: committed without indexing "
                    f"order_id={event['order_id']}"
                )
                continue

        search.index_order(
            settings,
            event["order"],
            refresh=False,
            event_metadata={
                "event_id": event.get("event_id"),
                "event_type": event.get("event_type"),
                "topic": item["topic"],
                "partition": item["partition"],
                "offset": item["offset"],
            },
        )
        indexed += 1
        item["commit"]()
        print(
            f"indexed event_type={event['event_type']} "
            f"order_id={event['order_id']}"
        )

    return {
        "processed": processed,
        "indexed": indexed,
        "skipped": skipped,
    }
