from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Settings

REPAIR_MODE_CHOICES = [
    "direct-reindex",
    "emit-reconcile-events",
    "replay-kafka",
    "call-webhook",
    "invalidate-cache",
    "clickhouse-backfill",
]


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dataguard",
        description="KubeDataGuard local MVP CLI",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    init_parser = subcommands.add_parser("init", help="initialize external systems")
    init_parser.add_argument("--reset", action="store_true", help="delete demo data and recreate the search index")

    generate_parser = subcommands.add_parser("generate", help="generate demo orders and Kafka events")
    generate_parser.add_argument("--count", type=int, default=25)
    generate_parser.add_argument("--paid-ratio", type=float, default=0.7)
    generate_parser.add_argument("--seed", type=int, default=42)

    index_parser = subcommands.add_parser("index", help="consume order events into OpenSearch")
    index_parser.add_argument("--group-id", default="orders-search-indexer")
    index_parser.add_argument("--max-messages", type=int)
    index_parser.add_argument(
        "--skip-every-paid",
        type=int,
        help="simulate a bug by committing every Nth paid event without indexing it",
    )

    cache_index_parser = subcommands.add_parser("cache-index", help="consume order events into Redis cache")
    cache_index_parser.add_argument("--group-id", default="orders-redis-cache-indexer")
    cache_index_parser.add_argument("--max-messages", type=int)
    cache_index_parser.add_argument(
        "--skip-every-paid",
        type=int,
        help="simulate a bug by committing every Nth paid event without caching it",
    )

    analytics_init_parser = subcommands.add_parser("analytics-init", help="initialize ClickHouse analytics table")
    analytics_init_parser.add_argument("--reset", action="store_true", help="truncate the analytics table")

    analytics_backfill_parser = subcommands.add_parser("analytics-backfill", help="backfill paid orders into ClickHouse analytics")
    analytics_backfill_parser.add_argument(
        "--skip-every-paid",
        type=int,
        help="simulate an analytics backfill bug by skipping every Nth paid order",
    )

    freshness_drift_parser = subcommands.add_parser(
        "inject-freshness-drift",
        help="simulate a stale derived view that violates bounded freshness",
    )
    freshness_drift_parser.add_argument("--count", type=int, default=5)
    freshness_drift_parser.add_argument("--source-age-seconds", type=int, default=120)
    freshness_drift_parser.add_argument("--target-staleness-seconds", type=int, default=30)

    check_parser = subcommands.add_parser("check", help="check a data consistency invariant")
    check_parser.add_argument("--max-lag-seconds", type=int, default=60)
    check_parser.add_argument(
        "--invariant",
        choices=["existence", "aggregate", "checksum", "freshness", "redis-freshness", "clickhouse-aggregate", "query"],
        default="existence",
        help="which invariant to check",
    )
    add_query_args(check_parser)
    add_checksum_args(check_parser)
    check_parser.add_argument("--write-report", action="store_true")

    check_job_parser = subcommands.add_parser(
        "check-job",
        help="run an invariant check from a Kubernetes Job and publish a compact ConfigMap result",
    )
    check_job_parser.add_argument("--max-lag-seconds", type=int, default=60)
    check_job_parser.add_argument(
        "--invariant",
        choices=["existence", "aggregate", "checksum", "freshness", "redis-freshness", "clickhouse-aggregate", "query"],
        default="existence",
        help="which invariant to check",
    )
    add_query_args(check_job_parser)
    add_checksum_args(check_job_parser)
    check_job_parser.add_argument("--namespace")
    check_job_parser.add_argument("--config-map", required=True)
    check_job_parser.add_argument("--invariant-name", required=True)
    check_job_parser.add_argument("--observed-generation", type=int, default=0)
    check_job_parser.add_argument("--check-id", default="")

    repair_parser = subcommands.add_parser("repair", help="repair drift from a report")
    repair_parser.add_argument("--report", type=Path)
    repair_parser.add_argument(
        "--repair-mode",
        choices=REPAIR_MODE_CHOICES,
        default="direct-reindex",
    )
    repair_parser.add_argument("--verify", action="store_true")
    repair_parser.add_argument("--max-lag-seconds", type=int, default=60)
    repair_parser.add_argument(
        "--verify-invariant",
        choices=["existence", "aggregate", "checksum", "freshness", "redis-freshness", "clickhouse-aggregate", "query"],
        default="existence",
    )
    add_query_args(repair_parser)
    add_checksum_args(repair_parser)

    repair_job_parser = subcommands.add_parser(
        "repair-job",
        help="run a repair from a Kubernetes Job and publish a ConfigMap result",
    )
    repair_job_parser.add_argument("--namespace")
    repair_job_parser.add_argument("--report-config-map", required=True)
    repair_job_parser.add_argument("--config-map", required=True)
    repair_job_parser.add_argument("--invariant-name", required=True)
    repair_job_parser.add_argument("--observed-generation", type=int, default=0)
    repair_job_parser.add_argument("--check-id", default="")
    repair_job_parser.add_argument(
        "--repair-mode",
        choices=REPAIR_MODE_CHOICES,
        default="direct-reindex",
    )
    repair_job_parser.add_argument("--max-lag-seconds", type=int, default=60)
    repair_job_parser.add_argument(
        "--verify-invariant",
        choices=["existence", "aggregate", "checksum", "freshness", "redis-freshness", "clickhouse-aggregate", "query"],
        default="existence",
    )
    add_query_args(repair_job_parser)
    add_checksum_args(repair_job_parser)

    local_demo_parser = subcommands.add_parser(
        "demo-local",
        help="run a no-Docker local proof of drift detection and repair",
    )
    local_demo_parser.add_argument("--count", type=int, default=20)
    local_demo_parser.add_argument("--skip-every-paid", type=int, default=5)

    args = parser.parse_args()
    settings = Settings.from_env()

    if args.command == "init":
        run_init(settings, reset=args.reset)
    elif args.command == "generate":
        run_generate(settings, args)
    elif args.command == "index":
        run_index(settings, args)
    elif args.command == "cache-index":
        run_cache_index(settings, args)
    elif args.command == "analytics-init":
        run_analytics_init(settings, args)
    elif args.command == "analytics-backfill":
        run_analytics_backfill(settings, args)
    elif args.command == "inject-freshness-drift":
        run_inject_freshness_drift(settings, args)
    elif args.command == "check":
        run_check(settings, args)
    elif args.command == "check-job":
        run_check_job(settings, args)
    elif args.command == "repair":
        run_repair(settings, args)
    elif args.command == "repair-job":
        run_repair_job(settings, args)
    elif args.command == "demo-local":
        run_demo_local(args)
    else:  # pragma: no cover - argparse prevents this
        raise SystemExit(f"unknown command: {args.command}")


def run_init(settings: Settings, *, reset: bool) -> None:
    from . import db, events, search

    print("waiting for Postgres, Kafka API, and OpenSearch")
    db.wait_for_postgres(settings)
    events.wait_for_kafka(settings)
    search.wait_for_opensearch(settings)

    print("initializing Postgres schema")
    db.init_schema(settings)
    print("ensuring Kafka topic")
    events.ensure_topic(settings)
    if reset:
        print("resetting demo data and OpenSearch index")
        db.reset_demo_data(settings)
        search.reset_index(settings)
        try:
            from . import cache

            deleted = cache.reset_order_cache(settings)
            print(f"reset Redis order cache keys: {deleted}")
        except Exception as exc:
            print(f"skipped Redis cache reset: {exc}")
    else:
        print("ensuring OpenSearch index")
        search.ensure_index(settings)
    print("init complete")


def run_generate(settings: Settings, args: argparse.Namespace) -> None:
    from . import generator

    generated = generator.generate_orders(
        settings,
        count=args.count,
        paid_ratio=args.paid_ratio,
        seed=args.seed,
    )
    print(json.dumps({"generated": len(generated)}, indent=2))


def run_index(settings: Settings, args: argparse.Namespace) -> None:
    from . import indexer

    result = indexer.run_indexer(
        settings,
        group_id=args.group_id,
        max_messages=args.max_messages,
        skip_every_paid=args.skip_every_paid,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


def run_cache_index(settings: Settings, args: argparse.Namespace) -> None:
    from . import cache_indexer

    result = cache_indexer.run_cache_indexer(
        settings,
        group_id=args.group_id,
        max_messages=args.max_messages,
        skip_every_paid=args.skip_every_paid,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


def run_analytics_init(settings: Settings, args: argparse.Namespace) -> None:
    from . import analytics

    print("waiting for ClickHouse")
    analytics.wait_for_clickhouse(settings)
    print("initializing ClickHouse analytics schema")
    analytics.init_schema(settings)
    if args.reset:
        print("resetting ClickHouse analytics table")
        analytics.reset_table(settings)
    print("analytics init complete")


def run_analytics_backfill(settings: Settings, args: argparse.Namespace) -> None:
    from . import analytics

    result = analytics.backfill_orders_analytics(
        settings,
        skip_every_paid=args.skip_every_paid,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


def run_inject_freshness_drift(settings: Settings, args: argparse.Namespace) -> None:
    from . import db, search
    from .invariants import iso

    if args.count < 1:
        raise SystemExit("--count must be at least 1")
    if args.source_age_seconds < 1:
        raise SystemExit("--source-age-seconds must be at least 1")
    if args.target_staleness_seconds < 1:
        raise SystemExit("--target-staleness-seconds must be at least 1")

    source_orders = db.fetch_paid_orders(settings, max_lag_seconds=0)
    indexed_orders = search.mget_orders(settings, [order["id"] for order in source_orders])
    candidate_ids = [order["id"] for order in source_orders if order["id"] in indexed_orders]
    selected_ids = candidate_ids[: args.count]
    if not selected_ids:
        raise SystemExit("no indexed paid orders found; run generate and index first")

    now = datetime.now(timezone.utc)
    source_updated_at = now - timedelta(seconds=args.source_age_seconds)
    target_indexed_at = source_updated_at - timedelta(seconds=args.target_staleness_seconds)

    updated = db.set_orders_updated_at(settings, selected_ids, source_updated_at)
    target_indexed_at_text = iso(target_indexed_at)
    for order_id in selected_ids:
        search.update_indexed_at(
            settings,
            order_id=order_id,
            indexed_at=target_indexed_at_text,
            refresh=True,
        )

    result = {
        "mutated": updated,
        "candidate_order_ids": selected_ids,
        "source_updated_at": iso(source_updated_at),
        "target_indexed_at": target_indexed_at_text,
        "expected_violation": "target indexed_at is older than source updated_at",
    }
    print(json.dumps(result, indent=2, sort_keys=True))


def run_check(settings: Settings, args: argparse.Namespace):
    from . import checker
    from .observability import write_observability_artifacts
    from .reporting import write_report_artifacts

    report = check_invariant(
        checker,
        settings,
        invariant=args.invariant,
        max_lag_seconds=args.max_lag_seconds,
        invariant_name=getattr(args, "invariant_name", None),
        source_query=getattr(args, "source_query", None),
        target_query=getattr(args, "target_query", None),
        key_field=getattr(args, "key_field", "id"),
        compare_fields=parse_compare_fields(getattr(args, "compare_fields", "")),
        source_scan_page_size=getattr(args, "source_scan_page_size", 1000),
        source_resume_after_key=getattr(args, "source_resume_after_key", ""),
        source_checkpoint_id=getattr(args, "source_checkpoint_id", ""),
        source_reset_checkpoint=getattr(args, "source_reset_checkpoint", False),
        source_max_pages=getattr(args, "source_max_pages", 0),
        checksum_prefix_length=getattr(args, "checksum_prefix_length", 1),
        check_id=getattr(args, "check_id", ""),
    )

    payload = report.to_dict()
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.write_report:
        artifacts = write_report_artifacts(settings, report)
        update_scan_checkpoint_report_ref(settings, payload, artifacts.report_ref)
        write_observability_artifacts(settings, payload)
        print(f"wrote {artifacts.json_path}")
        print(f"wrote {artifacts.markdown_path}")
        print(f"report ref {artifacts.report_ref}")


def run_check_job(settings: Settings, args: argparse.Namespace) -> None:
    from . import checker
    from .k8s_report import namespace_from_service_account, publish_report_configmap
    from .observability import write_observability_artifacts
    from .reporting import write_report_artifacts

    report = check_invariant(
        checker,
        settings,
        invariant=args.invariant,
        max_lag_seconds=args.max_lag_seconds,
        invariant_name=args.invariant_name,
        source_query=args.source_query,
        target_query=args.target_query,
        key_field=args.key_field,
        compare_fields=parse_compare_fields(args.compare_fields),
        source_scan_page_size=args.source_scan_page_size,
        source_resume_after_key=args.source_resume_after_key,
        source_checkpoint_id=args.source_checkpoint_id,
        source_reset_checkpoint=args.source_reset_checkpoint,
        source_max_pages=args.source_max_pages,
        checksum_prefix_length=args.checksum_prefix_length,
        check_id=args.check_id,
    )

    payload = report.to_dict()
    artifacts = write_report_artifacts(settings, report)
    update_scan_checkpoint_report_ref(settings, payload, artifacts.report_ref)
    status = report.kubernetes_status(report_ref=artifacts.report_ref)
    status["observedGeneration"] = args.observed_generation
    if args.check_id:
        status["checkID"] = args.check_id
    payload["kubernetes_status"] = status
    if args.check_id:
        payload["checkID"] = args.check_id
    write_observability_artifacts(settings, payload, artifact_name=args.check_id or "latest")

    namespace = args.namespace or namespace_from_service_account()
    publish_report_configmap(
        namespace=namespace,
        name=args.config_map,
        report=payload,
        status=status,
    )
    print(json.dumps({"configMap": args.config_map, "status": status}, indent=2, sort_keys=True))


def run_repair(settings: Settings, args: argparse.Namespace) -> None:
    from . import checker, repair
    from .observability import write_observability_artifacts

    report_path = args.report or settings.report_dir / "latest.json"
    result = repair.repair_from_report(settings, report_path, mode=args.repair_mode)
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.verify:
        report = check_invariant(
            checker,
            settings,
            invariant=args.verify_invariant,
            max_lag_seconds=args.max_lag_seconds,
            invariant_name=getattr(args, "invariant_name", None),
            source_query=args.source_query,
            target_query=args.target_query,
            key_field=args.key_field,
            compare_fields=parse_compare_fields(args.compare_fields),
            source_scan_page_size=args.source_scan_page_size,
            source_resume_after_key=args.source_resume_after_key,
            source_checkpoint_id=args.source_checkpoint_id,
            source_reset_checkpoint=args.source_reset_checkpoint,
            source_max_pages=args.source_max_pages,
            checksum_prefix_length=args.checksum_prefix_length,
        )
        print("post-repair verification:")
        payload = report.to_dict()
        write_observability_artifacts(settings, payload, artifact_name="post-repair")
        print(json.dumps(payload, indent=2, sort_keys=True))


def run_repair_job(settings: Settings, args: argparse.Namespace) -> None:
    from . import checker, repair
    from .k8s_report import namespace_from_service_account, publish_report_configmap, read_configmap_data
    from .observability import write_observability_artifacts
    from .reporting import write_report_artifacts

    namespace = args.namespace or namespace_from_service_account()
    source_report_ref = f"configmap://{namespace}/{args.report_config_map}/repair-input.json"
    source_data = read_configmap_data(namespace=namespace, name=args.report_config_map)
    source_report = json.loads(source_data.get("repair-input.json") or source_data["report.json"])

    repair_result = repair.repair_from_payload(settings, source_report, mode=args.repair_mode)
    repair_result["sourceReportRef"] = source_report_ref

    verification = check_invariant(
        checker,
        settings,
        invariant=args.verify_invariant,
        max_lag_seconds=args.max_lag_seconds,
        invariant_name=args.invariant_name,
        source_query=args.source_query,
        target_query=args.target_query,
        key_field=args.key_field,
        compare_fields=parse_compare_fields(args.compare_fields),
        source_scan_page_size=args.source_scan_page_size,
        source_resume_after_key=args.source_resume_after_key,
        source_checkpoint_id=args.source_checkpoint_id,
        source_reset_checkpoint=args.source_reset_checkpoint,
        source_max_pages=args.source_max_pages,
        checksum_prefix_length=args.checksum_prefix_length,
        check_id=args.check_id,
    )

    verification_payload = verification.to_dict()
    artifacts = write_report_artifacts(settings, verification)
    update_scan_checkpoint_report_ref(settings, verification_payload, artifacts.report_ref)
    payload, status = build_repair_job_result(
        invariant_name=args.invariant_name,
        source_report_ref=source_report_ref,
        repair_report_ref=artifacts.report_ref,
        repair_result=repair_result,
        repair_action=args.repair_mode,
        verification=verification,
        verification_payload=verification_payload,
        observed_generation=args.observed_generation,
        check_id=args.check_id,
    )
    write_observability_artifacts(settings, verification_payload, artifact_name=f"{args.check_id or 'repair'}-verification")
    publish_report_configmap(
        namespace=namespace,
        name=args.config_map,
        report=payload,
        status=status,
    )
    print(json.dumps({"configMap": args.config_map, "status": status}, indent=2, sort_keys=True))


def add_query_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-query", default="")
    parser.add_argument("--target-query", default="")
    parser.add_argument("--key-field", default="id")
    parser.add_argument("--compare-fields", default="")
    parser.add_argument("--source-scan-page-size", type=int, default=1000)
    parser.add_argument("--source-resume-after-key", default="")
    parser.add_argument("--source-checkpoint-id", default="")
    parser.add_argument("--source-reset-checkpoint", action="store_true")
    parser.add_argument("--source-max-pages", type=int, default=0)


def add_checksum_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--checksum-prefix-length", type=int, default=1)


def parse_compare_fields(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def check_invariant(
    checker,
    settings: Settings,
    *,
    invariant: str,
    max_lag_seconds: int,
    invariant_name: str | None = None,
    source_query: str | None = None,
    target_query: str | None = None,
    key_field: str = "id",
    compare_fields: list[str] | None = None,
    source_scan_page_size: int = 1000,
    source_resume_after_key: str = "",
    source_checkpoint_id: str = "",
    source_reset_checkpoint: bool = False,
    source_max_pages: int = 0,
    checksum_prefix_length: int = 1,
    check_id: str = "",
):
    if invariant == "existence":
        return checker.check_paid_orders_indexed(
            settings,
            max_lag_seconds=max_lag_seconds,
        )
    if invariant == "aggregate":
        return checker.check_paid_orders_aggregate(
            settings,
            max_lag_seconds=max_lag_seconds,
        )
    if invariant == "checksum":
        return checker.check_paid_orders_checksum(
            settings,
            max_lag_seconds=max_lag_seconds,
            prefix_length=checksum_prefix_length,
        )
    if invariant == "freshness":
        return checker.check_paid_orders_freshness(
            settings,
            max_lag_seconds=max_lag_seconds,
        )
    if invariant == "redis-freshness":
        return checker.check_paid_orders_redis_freshness(
            settings,
            max_lag_seconds=max_lag_seconds,
        )
    if invariant == "clickhouse-aggregate":
        return checker.check_paid_orders_clickhouse_aggregate(
            settings,
            max_lag_seconds=max_lag_seconds,
        )
    if invariant == "query":
        if not source_query:
            raise SystemExit("--source-query is required for query invariants")
        if not target_query:
            raise SystemExit("--target-query is required for query invariants")
        return checker.check_query_invariant(
            settings,
            invariant_name=invariant_name or "query-invariant",
            source_query=source_query,
            target_query=target_query,
            key_field=key_field,
            compare_fields=compare_fields or [],
            max_lag_seconds=max_lag_seconds,
            source_scan_page_size=source_scan_page_size,
            source_resume_after_key=source_resume_after_key or None,
            source_checkpoint_id=source_checkpoint_id,
            source_reset_checkpoint=source_reset_checkpoint,
            source_max_pages=source_max_pages,
            check_id=check_id,
        )
    raise SystemExit(f"unknown invariant: {invariant}")


def update_scan_checkpoint_report_ref(settings: Settings, payload: dict, report_ref: str) -> None:
    source_scan = (payload.get("observation_window") or {}).get("source_scan") or {}
    checkpoint_id = source_scan.get("checkpoint_id")
    if not checkpoint_id:
        return
    from . import checkpoints

    checkpoints.update_checkpoint_report_ref(settings, str(checkpoint_id), report_ref)


def build_repair_job_result(
    *,
    invariant_name: str,
    source_report_ref: str,
    repair_report_ref: str,
    repair_result: dict,
    repair_action: str,
    verification,
    verification_payload: dict | None = None,
    observed_generation: int,
    check_id: str = "",
) -> tuple[dict, dict]:
    verification_payload = verification_payload or verification.to_dict()
    status = verification.kubernetes_status(report_ref=repair_report_ref)
    status["observedGeneration"] = observed_generation
    if check_id:
        status["checkID"] = check_id
    status["repairRef"] = repair_report_ref
    status["repairAction"] = repair_action
    if not verification.healthy:
        status["phase"] = "RepairFailed"
        status["healthy"] = False
        status["reason"] = "repair verification still found drift"

    payload = {
        "kind": "KubeDataGuardRepairReport",
        "invariant": invariant_name,
        "sourceReportRef": source_report_ref,
        "repair": repair_result,
        "verification": verification_payload,
        "status": status["phase"],
        "healthy": status["healthy"],
    }
    return payload, status


def run_demo_local(args: argparse.Namespace) -> None:
    from . import local_demo

    result = local_demo.run_local_proof(
        count=args.count,
        skip_every_paid=args.skip_every_paid,
    )
    print(json.dumps(result.to_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
