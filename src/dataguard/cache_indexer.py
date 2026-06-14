from __future__ import annotations

from . import cache, events
from .config import Settings


def run_cache_indexer(
    settings: Settings,
    *,
    group_id: str,
    max_messages: int | None,
    skip_every_paid: int | None,
) -> dict[str, int]:
    processed = 0
    cached = 0
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
                    "simulated cache consumer bug: committed without caching "
                    f"order_id={event['order_id']}"
                )
                continue

        cache.cache_order(
            settings,
            event["order"],
            event_metadata={
                "topic": item["topic"],
                "partition": item["partition"],
                "offset": item["offset"],
            },
        )
        cached += 1
        item["commit"]()
        print(
            f"cached event_type={event['event_type']} "
            f"order_id={event['order_id']}"
        )

    return {
        "processed": processed,
        "cached": cached,
        "skipped": skipped,
    }
