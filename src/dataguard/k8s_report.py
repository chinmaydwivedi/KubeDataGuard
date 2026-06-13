from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


SERVICE_ACCOUNT_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")


def namespace_from_service_account() -> str:
    return (SERVICE_ACCOUNT_DIR / "namespace").read_text(encoding="utf-8").strip()


def publish_report_configmap(
    *,
    namespace: str,
    name: str,
    report: dict[str, Any],
    status: dict[str, Any],
) -> None:
    host = os.environ["KUBERNETES_SERVICE_HOST"]
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    url = f"https://{host}:{port}/api/v1/namespaces/{namespace}/configmaps"
    token = (SERVICE_ACCOUNT_DIR / "token").read_text(encoding="utf-8").strip()
    context = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT_DIR / "ca.crt"))

    payload = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "kubedataguard",
                "dataguard.io/report": "true",
            },
        },
        "data": {
            "repair-input.json": json.dumps(repair_input(report), indent=2, sort_keys=True),
            "status.json": json.dumps(status, indent=2, sort_keys=True),
        },
    }

    if configmap_exists(url=f"{url}/{name}", token=token, context=context):
        request_json(
            url=f"{url}/{name}",
            method="PATCH",
            token=token,
            context=context,
            payload={
                "metadata": payload["metadata"],
                "data": payload["data"],
            },
            content_type="application/merge-patch+json",
        )
    else:
        request_json(
            url=url,
            method="POST",
            token=token,
            context=context,
            payload=payload,
            content_type="application/json",
        )


def configmap_exists(*, url: str, token: str, context: ssl.SSLContext) -> bool:
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(request, context=context, timeout=10):
            return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def request_json(
    *,
    url: str,
    method: str,
    token: str,
    context: ssl.SSLContext,
    payload: dict[str, Any],
    content_type: str,
) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": content_type,
        },
    )
    with urllib.request.urlopen(request, context=context, timeout=10):
        return


def read_configmap_data(*, namespace: str, name: str) -> dict[str, str]:
    host = os.environ["KUBERNETES_SERVICE_HOST"]
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    url = f"https://{host}:{port}/api/v1/namespaces/{namespace}/configmaps/{name}"
    token = (SERVICE_ACCOUNT_DIR / "token").read_text(encoding="utf-8").strip()
    context = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT_DIR / "ca.crt"))

    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, context=context, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError(f"ConfigMap {namespace}/{name} has no data object")
    return {str(key): str(value) for key, value in data.items()}


def repair_input(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": report.get("kind", "KubeDataGuardRepairInput"),
        "invariant": report.get("invariant"),
        "status": report.get("status"),
        "healthy": report.get("healthy"),
        "missing": candidate_items(report.get("missing", [])),
        "stale": candidate_items(report.get("stale", [])),
        "freshness_violations": candidate_items(report.get("freshness_violations", [])),
        "aggregate_mismatches": report.get("aggregate_mismatches", []),
        "sourceReportRef": report.get("sourceReportRef"),
    }


def candidate_items(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []
    compact: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        compact.append(
            {
                key: value
                for key, value in item.items()
                if key in {"order_id", "reason", "mismatches"}
            }
        )
    return compact
