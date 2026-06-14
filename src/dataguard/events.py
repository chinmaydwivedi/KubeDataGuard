from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from .config import Settings


def wait_for_kafka(settings: Settings, timeout_seconds: int = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id="dataguard-init",
            )
            admin.list_topics()
            admin.close()
            return
        except Exception as exc:  # pragma: no cover - depends on external service
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"Kafka API did not become ready: {last_error}")


def ensure_topic(settings: Settings, partitions: int = 1) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="dataguard-admin",
    )
    try:
        topic = NewTopic(
            name=settings.order_events_topic,
            num_partitions=partitions,
            replication_factor=1,
        )
        admin.create_topics([topic], validate_only=False)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def producer(settings: Settings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )


def publish_event(settings: Settings, event: dict[str, Any]) -> dict[str, Any]:
    client = producer(settings)
    try:
        future = client.send(
            settings.order_events_topic,
            key=event["order_id"],
            value=event,
        )
        metadata = future.get(timeout=30)
        client.flush(timeout=30)
        return {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "timestamp": getattr(metadata, "timestamp", None),
        }
    finally:
        client.close()


def consumer(
    settings: Settings,
    *,
    group_id: str,
    from_beginning: bool = True,
) -> KafkaConsumer:
    return KafkaConsumer(
        settings.order_events_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest" if from_beginning else "latest",
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


def consume_events(
    settings: Settings,
    *,
    group_id: str,
    max_messages: int | None,
) -> Iterator[dict[str, Any]]:
    client = consumer(settings, group_id=group_id)
    seen = 0
    try:
        for message in client:
            yield {
                "message": message,
                "event": message.value,
                "offset": message.offset,
                "partition": message.partition,
                "topic": message.topic,
                "commit": client.commit,
            }
            seen += 1
            if max_messages is not None and seen >= max_messages:
                break
    finally:
        client.close()


def topic_offset_range(settings: Settings) -> dict[str, Any]:
    client = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        api_version=(2, 8, 0),
        request_timeout_ms=3000,
        api_version_auto_timeout_ms=3000,
        metadata_max_age_ms=3000,
    )
    try:
        partitions = client.partitions_for_topic(settings.order_events_topic) or set()
        if not partitions:
            return {
                "stream_offset_start": None,
                "stream_offset_end": None,
                "stream_partitions": 0,
                "stream_event_count": None,
                "stream_offsets": [],
            }

        topic_partitions = [
            TopicPartition(settings.order_events_topic, partition)
            for partition in sorted(partitions)
        ]
        starts = client.beginning_offsets(topic_partitions)
        ends = client.end_offsets(topic_partitions)
        start_values = [starts[item] for item in topic_partitions if item in starts]
        end_values = [ends[item] for item in topic_partitions if item in ends]
        partition_offsets = []
        event_count = 0
        for item in topic_partitions:
            start = starts.get(item)
            end = ends.get(item)
            if start is None or end is None:
                continue
            event_count += max(0, end - start)
            partition_offsets.append(
                {
                    "partition": item.partition,
                    "start": start,
                    "end": end,
                    "event_count": max(0, end - start),
                }
            )
        return {
            "stream_offset_start": min(start_values) if start_values else None,
            "stream_offset_end": max(end_values) if end_values else None,
            "stream_partitions": len(partition_offsets),
            "stream_event_count": event_count,
            "stream_offsets": partition_offsets,
        }
    finally:
        client.close()
