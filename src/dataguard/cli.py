from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Settings


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
        choices=["existence", "aggregate", "freshness", "query"],
        default="existence",
        help="which invariant to check",
    )
    add_query_args(check_parser)
    check_parser.add_argument("--write-report", action="store_true")

    check_job_parser = subcommands.add_parser(
        "check-job",
        help="run an invariant check from a Kubernetes Job and publish a compact ConfigMap result",
    )
    check_job_parser.add_argument("--max-lag-seconds", type=int, default=60)
    check_job_parser.add_argument(
        "--invariant",
        choices=["existence", "aggregate", "freshness", "query"],
        default="existence",
        help="which invariant to check",
    )
    add_query_args(check_job_parser)
    check_job_parser.add_argument("--namespace")
    check_job_parser.add_argument("--config-map", required=True)
    check_job_parser.add_argument("--invariant-name", required=True)
    check_job_parser.add_argument("--observed-generation", type=int, default=0)
    check_job_parser.add_argument("--check-id", default="")

    repair_parser = subcommands.add_parser("repair", help="repair drift from a report")
    repair_parser.add_argument("--report", type=Path)
    repair_parser.add_argument("--verify", action="store_true")
    repair_parser.add_argument("--max-lag-seconds", type=int, default=60)
    repair_parser.add_argument(
        "--verify-invariant",
        choices=["existence", "aggregate", "freshness", "query"],
        default="existence",
    )
    add_query_args(repair_parser)

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
    repair_job_parser.add_argument("--max-lag-seconds", type=int, default=60)
    repair_job_parser.add_argument(
        "--verify-invariant",
        choices=["existence", "aggregate", "freshness", "query"],
        default="existence",
    )
    add_query_args(repair_job_parser)

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
    from .reporting import write_reports

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
    )

    payload = report.to_dict()
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.write_report:
        json_path, markdown_path = write_reports(settings.report_dir, report)
        print(f"wrote {json_path}")
        print(f"wrote {markdown_path}")


def run_check_job(settings: Settings, args: argparse.Namespace) -> None:
    from . import checker
    from .k8s_report import namespace_from_service_account, publish_report_configmap
    from .reporting import write_reports

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
    )

    payload = report.to_dict()
    json_path, _markdown_path = write_reports(settings.report_dir, report)
    report_ref = f"file://{json_path}"
    status = report.kubernetes_status(report_ref=report_ref)
    status["observedGeneration"] = args.observed_generation
    if args.check_id:
        status["checkID"] = args.check_id
    payload["kubernetes_status"] = status

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

    report_path = args.report or settings.report_dir / "latest.json"
    result = repair.repair_from_report(settings, report_path)
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
        )
        print("post-repair verification:")
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))


def run_repair_job(settings: Settings, args: argparse.Namespace) -> None:
    from . import checker, repair
    from .k8s_report import namespace_from_service_account, publish_report_configmap, read_configmap_data
    from .reporting import write_reports

    namespace = args.namespace or namespace_from_service_account()
    source_report_ref = f"configmap://{namespace}/{args.report_config_map}/repair-input.json"
    source_data = read_configmap_data(namespace=namespace, name=args.report_config_map)
    source_report = json.loads(source_data.get("repair-input.json") or source_data["report.json"])

    repair_result = repair.repair_from_payload(settings, source_report)
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
    )

    verification_payload = verification.to_dict()
    json_path, _markdown_path = write_reports(settings.report_dir, verification)
    repair_report_ref = f"file://{json_path}"
    payload, status = build_repair_job_result(
        invariant_name=args.invariant_name,
        source_report_ref=source_report_ref,
        repair_report_ref=repair_report_ref,
        repair_result=repair_result,
        verification=verification,
        verification_payload=verification_payload,
        observed_generation=args.observed_generation,
        check_id=args.check_id,
    )
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
    if invariant == "freshness":
        return checker.check_paid_orders_freshness(
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
        )
    raise SystemExit(f"unknown invariant: {invariant}")


def build_repair_job_result(
    *,
    invariant_name: str,
    source_report_ref: str,
    repair_report_ref: str,
    repair_result: dict,
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
    status["repairAction"] = "reindex-records"
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
