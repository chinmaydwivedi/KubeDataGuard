from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    postgres_dsn: str
    kafka_bootstrap_servers: str
    order_events_topic: str
    opensearch_url: str
    orders_index: str
    report_dir: Path
    report_store: str
    report_bucket: str
    report_prefix: str
    report_s3_endpoint_url: str
    aws_region: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            postgres_dsn=os.getenv(
                "POSTGRES_DSN",
                "postgresql://dataguard:dataguard@localhost:5432/dataguard",
            ),
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:19092",
            ),
            order_events_topic=os.getenv("ORDER_EVENTS_TOPIC", "orders.events"),
            opensearch_url=os.getenv("OPENSEARCH_URL", "http://localhost:9200"),
            orders_index=os.getenv("ORDERS_INDEX", "orders"),
            report_dir=Path(os.getenv("REPORT_DIR", "reports")),
            report_store=os.getenv("REPORT_STORE", "local"),
            report_bucket=os.getenv("REPORT_BUCKET", ""),
            report_prefix=os.getenv("REPORT_PREFIX", "kubedataguard/reports"),
            report_s3_endpoint_url=os.getenv(
                "REPORT_S3_ENDPOINT_URL",
                os.getenv("AWS_ENDPOINT_URL", ""),
            ),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
        )
