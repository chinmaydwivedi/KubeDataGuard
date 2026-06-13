from __future__ import annotations

import random
from typing import Any

from . import db, events
from .config import Settings


def generate_orders(
    settings: Settings,
    *,
    count: int,
    paid_ratio: float,
    seed: int,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    generated: list[dict[str, Any]] = []

    for index in range(count):
        status = "paid" if rng.random() < paid_ratio else rng.choice(["created", "cancelled"])
        event = db.insert_order_with_event(
            settings,
            customer_id=f"customer-{rng.randint(1, 20):03d}",
            status=status,
            amount_cents=rng.randint(1000, 25000),
        )
        events.publish_event(settings, event)
        db.mark_event_published(settings, event["event_id"])
        generated.append(event)
        print(
            f"generated {index + 1:03d}/{count}: "
            f"{event['event_type']} order_id={event['order_id']}"
        )

    return generated

