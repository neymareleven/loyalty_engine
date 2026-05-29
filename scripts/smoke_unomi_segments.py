#!/usr/bin/env python3
"""End-to-end smoke test for Unomi segmentation flow.

Validates, for a single X-Brand:
- segmentation mode = UNOMI
- create static segment
- create dynamic segment (with explicit unomi_condition)
- list/get segments from backend
- static members add/list/remove + sync-unomi
- update dynamic unomi_condition
- delete segments
- direct verification in Unomi CDP (scope + ids)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from dotenv import load_dotenv


def _b64_basic(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | list[Any] | None = None,
    timeout_sec: int = 30,
) -> tuple[int, Any]:
    data = None
    req_headers = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req_headers.setdefault("Accept", "application/json")
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8")
            return int(resp.status), (json.loads(raw) if raw.strip() else None)
    except (TimeoutError, socket.timeout):
        return 599, {"error": "timeout", "message": f"request timed out after {timeout_sec}s", "url": url}
    except urllib.error.HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode("utf-8")
        except Exception:
            pass
        parsed: Any = raw
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except Exception:
            pass
        return int(e.code), parsed
    except urllib.error.URLError as e:
        reason = str(getattr(e, "reason", e))
        if "timed out" in reason.lower():
            return 599, {"error": "timeout", "message": reason, "url": url}
        return 598, {"error": "connection", "message": reason, "url": url}


def _request_json_retry(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | list[Any] | None = None,
    timeout_sec: int = 30,
    retries: int = 2,
) -> tuple[int, Any]:
    attempts = max(1, retries + 1)
    last_status: int = 0
    last_payload: Any = None
    for attempt in range(1, attempts + 1):
        status, payload = _request_json(
            method,
            url,
            headers=headers,
            body=body,
            timeout_sec=timeout_sec,
        )
        last_status, last_payload = status, payload
        if status != 599:
            return status, payload
        if attempt < attempts:
            print(f"  WARN timeout ({attempt}/{attempts}) on {method} {url}; retrying...")
            time.sleep(1.5)
    return last_status, last_payload


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    load_dotenv(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Smoke test Unomi segmentation end-to-end")
    parser.add_argument("--backend-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--x-brand", required=True, help="Active brand (X-Brand) to test")
    parser.add_argument("--api-username", default=os.getenv("API_BASIC_AUTH_USERNAME", "karaf"))
    parser.add_argument("--api-password", default=os.getenv("API_BASIC_AUTH_PASSWORD", "karaf"))
    parser.add_argument("--unomi-base-url", default=os.getenv("UNOMI_BASE_URL", ""))
    parser.add_argument("--unomi-username", default=os.getenv("UNOMI_USERNAME", "karaf"))
    parser.add_argument("--unomi-password", default=os.getenv("UNOMI_PASSWORD", ""))
    parser.add_argument("--http-timeout-sec", type=int, default=60)
    parser.add_argument("--http-retries", type=int, default=2)
    args = parser.parse_args()

    if not args.unomi_base_url.strip():
        raise SystemExit("UNOMI_BASE_URL is required (arg or env)")
    if not args.unomi_password.strip():
        raise SystemExit("UNOMI_PASSWORD is required (arg or env)")

    backend = args.backend_base_url.rstrip("/")
    unomi = args.unomi_base_url.rstrip("/")
    brand = args.x_brand.strip()
    now = int(time.time())

    api_headers = {
        "Authorization": _b64_basic(args.api_username, args.api_password),
        "X-Brand": brand,
    }
    unomi_headers = {
        "Authorization": _b64_basic(args.unomi_username, args.unomi_password),
    }

    static_name = f"smoke-static-{brand}-{now}"
    dynamic_name = f"smoke-dynamic-{brand}-{now}"
    profile_id = f"smoke-profile-{brand}-{now}"

    static_seg_id: str | None = None
    dynamic_seg_id: str | None = None
    static_unomi_id: str | None = None
    dynamic_unomi_id: str | None = None
    customer_id: str | None = None

    print(f"[1/10] Check segmentation mode for X-Brand={brand}")
    status, mode = _request_json(
        "GET",
        f"{backend}/admin/segments/segmentation-mode",
        headers=api_headers,
        timeout_sec=args.http_timeout_sec,
    )
    _assert(status == 200, f"segmentation-mode failed: {status} {mode}")
    _assert(bool(mode.get("currentBrandUsesUnomi")), f"Brand {brand} is not in UNOMI mode: {mode}")
    _assert(str(mode.get("activeBrand")) == brand, f"activeBrand mismatch: {mode.get('activeBrand')} != {brand}")
    print(f"  OK mode={mode.get('segmentationMode')} scope={mode.get('unomiScope')}")

    print("[2/10] Create synthetic customer for static membership")
    customer_payload = {
        "brand": brand,
        "profileId": profile_id,
        "properties": {"gender": "UNKNOWN"},
    }
    status, customer = _request_json(
        "POST",
        f"{backend}/customers/upsert",
        headers=api_headers,
        body=customer_payload,
        timeout_sec=args.http_timeout_sec,
    )
    _assert(status == 200, f"customers/upsert failed: {status} {customer}")
    customer_id = str(customer.get("id") or "").strip()
    _assert(customer_id, "customers/upsert returned no customer.id")
    print(f"  OK customer_id={customer_id} profile_id={profile_id}")

    try:
        print("[3/10] Create static Unomi segment")
        status, created_static = _request_json(
            "POST",
            f"{backend}/admin/segments",
            headers=api_headers,
            body={"name": static_name, "is_dynamic": False, "active": True},
        )
        _assert(status == 200, f"create static segment failed: {status} {created_static}")
        static_seg_id = str(created_static.get("id"))
        static_unomi_id = str(created_static.get("unomi_segment_id") or "")
        _assert(created_static.get("provider") == "UNOMI", f"provider mismatch: {created_static}")
        print(f"  OK id={static_seg_id} unomi_segment_id={static_unomi_id}")

        print("[4/10] Create dynamic Unomi segment (explicit unomi_condition)")
        dyn_condition = {
            "type": "profilePropertyCondition",
            "parameterValues": {
                "propertyName": "itemId",
                "comparisonOperator": "equals",
                "propertyValue": profile_id,
            },
        }
        status, created_dynamic = _request_json(
            "POST",
            f"{backend}/admin/segments",
            headers=api_headers,
            body={
                "name": dynamic_name,
                "is_dynamic": True,
                "active": True,
                "conditions": {"and": [{"field": "customer.status", "operator": "eq", "value": "ACTIVE"}]},
                "unomi_condition": dyn_condition,
            },
        )
        _assert(status == 200, f"create dynamic segment failed: {status} {created_dynamic}")
        dynamic_seg_id = str(created_dynamic.get("id"))
        dynamic_unomi_id = str(created_dynamic.get("unomi_segment_id") or "")
        _assert(created_dynamic.get("provider") == "UNOMI", f"provider mismatch: {created_dynamic}")
        print(f"  OK id={dynamic_seg_id} unomi_segment_id={dynamic_unomi_id}")

        print("[5/10] Backend list/get should return brand-scoped Unomi segments")
        status, listed = _request_json_retry(
            "GET",
            f"{backend}/admin/segments",
            headers=api_headers,
            timeout_sec=args.http_timeout_sec,
            retries=args.http_retries,
        )
        _assert(status == 200 and isinstance(listed, list), f"segments list failed: {status} {listed}")
        by_id = {str(item.get("id")): item for item in listed if isinstance(item, dict)}
        _assert(static_seg_id in by_id, "static segment missing from backend list")
        _assert(dynamic_seg_id in by_id, "dynamic segment missing from backend list")
        _assert(by_id[static_seg_id].get("unomi_scope") == brand, f"static scope mismatch: {by_id[static_seg_id]}")
        _assert(by_id[dynamic_seg_id].get("unomi_scope") == brand, f"dynamic scope mismatch: {by_id[dynamic_seg_id]}")
        for seg_id in (static_seg_id, dynamic_seg_id):
            status, one = _request_json("GET", f"{backend}/admin/segments/{seg_id}", headers=api_headers)
            _assert(status == 200, f"segment get failed for {seg_id}: {status} {one}")
        print("  OK list/get")

        print("[6/10] Static members add/list/remove")
        status, added = _request_json(
            "POST",
            f"{backend}/admin/segments/{static_seg_id}/members",
            headers=api_headers,
            body={"customer_id": customer_id},
        )
        _assert(status == 200, f"add member failed: {status} {added}")

        status, members = _request_json(
            "GET", f"{backend}/admin/segments/{static_seg_id}/members?limit=100&offset=0", headers=api_headers
        )
        _assert(status == 200, f"list members failed: {status} {members}")
        _assert(int(members.get("total") or 0) >= 1, f"unexpected static members total: {members}")

        status, removed = _request_json(
            "DELETE", f"{backend}/admin/segments/{static_seg_id}/members/{customer_id}", headers=api_headers
        )
        _assert(status == 200 and bool(removed.get("deleted")), f"remove member failed: {status} {removed}")
        print("  OK members add/list/remove")

        print("[7/10] Manual sync endpoint for static segment")
        status, synced = _request_json(
            "POST", f"{backend}/admin/segments/{static_seg_id}/sync-unomi", headers=api_headers
        )
        _assert(status == 200 and bool(synced.get("synced")), f"sync-unomi failed: {status} {synced}")
        print("  OK sync-unomi")

        print("[8/10] Dynamic update should push condition changes to Unomi")
        next_dyn_condition = {
            "type": "profilePropertyCondition",
            "parameterValues": {
                "propertyName": "itemId",
                "comparisonOperator": "equals",
                "propertyValue": f"{profile_id}-updated",
            },
        }
        status, patched = _request_json(
            "PATCH",
            f"{backend}/admin/segments/{dynamic_seg_id}",
            headers=api_headers,
            body={"unomi_condition": next_dyn_condition},
        )
        _assert(status == 200, f"dynamic patch failed: {status} {patched}")
        print("  OK dynamic patch")

        print("[9/10] Direct Unomi checks (scope and segment existence)")
        status, remote_list = _request_json(
            "GET",
            f"{unomi}/cxs/segments/?offset=0&size=1000",
            headers=unomi_headers,
        )
        _assert(status == 200 and isinstance(remote_list, list), f"Unomi list failed: {status} {remote_list}")
        ids_in_scope: set[str] = set()
        for item in remote_list:
            if not isinstance(item, dict):
                continue
            md = item.get("metadata") if isinstance(item.get("metadata"), dict) else item
            if not isinstance(md, dict):
                continue
            if str(md.get("scope") or "").strip() != brand:
                continue
            sid = str(md.get("id") or "").strip()
            if sid:
                ids_in_scope.add(sid)
        _assert(static_unomi_id in ids_in_scope, "static segment missing from Unomi list/scope")
        _assert(dynamic_unomi_id in ids_in_scope, "dynamic segment missing from Unomi list/scope")
        print("  OK Unomi list scope contains created segments")

    finally:
        print("[10/10] Cleanup (delete created segments)")
        for seg_id in [dynamic_seg_id, static_seg_id]:
            if not seg_id:
                continue
            status, payload = _request_json("DELETE", f"{backend}/admin/segments/{seg_id}", headers=api_headers)
            if status != 200:
                print(f"  WARN delete failed for {seg_id}: {status} {payload}")
            else:
                print(f"  OK deleted {seg_id}")

        for sid in [dynamic_unomi_id, static_unomi_id]:
            if not sid:
                continue
            status, _payload = _request_json(
                "GET", f"{unomi}/cxs/segments/{urllib.parse.quote(sid, safe='')}", headers=unomi_headers
            )
            _assert(
                status in (404, 204),
                f"Unomi segment still exists after delete: {sid} (status={status})",
            )
        print("  OK cleanup validated on Unomi")

    print("SUCCESS: Unomi segmentation smoke test passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as e:
        print(f"FAILED: {e}")
        raise SystemExit(1)
    except Exception as e:
        print(f"FAILED (unexpected error): {e}")
        raise SystemExit(1)
