"""Smoke test: Loyalty → Unomi profile sync via /cxs/eventcollector (contactInfoSubmitted).

Usage:
  python scripts/smoke_unomi_profile_sync.py --x-brand batira
  python scripts/smoke_unomi_profile_sync.py --x-brand batira --via-upsert
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from datetime import datetime

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import app.db  # loads .env
from app.db import SessionLocal
from app.services.contact_service import get_or_create_customer
from app.services.unomi_client import UnomiClient, UnomiClientError
from app.services.unomi_profile_service import (
    _push_profile_via_eventcollector,
    build_unomi_eventcollector_payload,
    sync_customer_profile_to_unomi,
)
from app.services.unomi_settings_service import (
    resolve_unomi_profile_connection,
    unomi_env_status,
    unomi_profile_sync_event_type,
    unomi_profile_sync_transport,
)


def _wait_for_profile(client: UnomiClient, profile_id: str, *, attempts: int = 8, delay_sec: float = 1.5):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            profile = client.get_profile(profile_id)
            if profile:
                return profile, attempt
        except UnomiClientError as e:
            last_error = e
        time.sleep(delay_sec)
    raise RuntimeError(f"Profile {profile_id!r} not found in Unomi after {attempts} attempts") from last_error


def _print_props(profile: dict, keys: list[str]) -> None:
    props = profile.get("properties") if isinstance(profile.get("properties"), dict) else {}
    for key in keys:
        print(f"  properties.{key} = {props.get(key)!r}")


def smoke_eventcollector(*, brand: str, profile_id: str) -> int:
    status = unomi_env_status(brand=brand)
    print("profile-sync config:", {
        "profileSyncEnabled": status.get("profileSyncEnabled"),
        "profileSyncTransport": unomi_profile_sync_transport(),
        "profileSyncEventType": unomi_profile_sync_event_type(),
        "unomiScope": status.get("unomiScope"),
        "unomiBaseUrl": status.get("unomiBaseUrl"),
    })

    cfg = resolve_unomi_profile_connection(brand=brand)
    if not cfg:
        print("ERROR: profile sync not configured for brand", brand)
        return 1

    client = UnomiClient(cfg, timeout_sec=45.0)
    properties = {
        "brand": brand,
        "email": f"{profile_id}@loyalty-engine.local",
        "firstName": "Smoke",
        "lastName": "Loyalty",
        "gender": "F",
        "loyaltyStatus": "UNCONFIGURED",
        "statusPoints": 0,
        "loyaltyEngineSyncedAt": datetime.utcnow().isoformat(),
    }

    payload = build_unomi_eventcollector_payload(
        profile_id=profile_id,
        scope=cfg.scope,
        properties=properties,
        event_type="contactInfoSubmitted",
    )
    print("eventcollector payload eventType:", payload["events"][0]["eventType"])
    print("profileId:", profile_id, "scope:", cfg.scope)

    profile_body = {
        "itemId": profile_id,
        "itemType": "profile",
        "properties": properties,
        "systemProperties": {"scope": cfg.scope},
        "segments": [],
        "scores": {},
        "consents": {},
    }
    try:
        client.save_profile(profile_body)
        print("save_profile OK (stable itemId)")
    except UnomiClientError as e:
        print(f"ERROR save_profile: {e}")
        if e.body:
            print(e.body[:800])
        return 1

    try:
        _push_profile_via_eventcollector(
            client,
            profile_id=profile_id,
            scope=cfg.scope,
            properties=properties,
            brand=brand,
        )
        print("eventcollector POST OK (contactInfoSubmitted)")
    except UnomiClientError as e:
        print(f"ERROR eventcollector: {e}")
        if e.body:
            print(e.body[:800])
        return 1

    try:
        profile, attempt = _wait_for_profile(client, profile_id)
        print(f"GET /cxs/profiles/{profile_id} OK (attempt {attempt})")
        _print_props(profile, ["email", "firstName", "lastName", "brand", "loyaltyStatus", "statusPoints"])
    except Exception as e:
        print(f"ERROR: profile not found: {e}")
        return 1

    print("SMOKE OK (save_profile + contactInfoSubmitted)")
    return 0


def smoke_via_upsert(*, brand: str, profile_id: str) -> int:
    db = SessionLocal()
    try:
        customer = get_or_create_customer(
            db,
            brand,
            profile_id,
            {"gender": "M", "birthdate": "1990-06-15"},
            contact_properties={
                "email": f"upsert-{profile_id}@loyalty-engine.local",
                "firstName": "Upsert",
                "lastName": "Smoke",
            },
            push_to_unomi=False,
        )
        db.commit()
        db.refresh(customer)

        result = sync_customer_profile_to_unomi(
            db,
            customer=customer,
            reason="smoke_test",
            extra_properties={
                "email": f"upsert-{profile_id}@loyalty-engine.local",
                "firstName": "Upsert",
                "lastName": "Smoke",
            },
        )
        print("sync_customer_profile_to_unomi:", result)
        if not result or not result.get("synced"):
            print("ERROR: sync did not succeed", result)
            return 1

        cfg = resolve_unomi_profile_connection(brand=brand)
        if not cfg:
            return 1
        client = UnomiClient(cfg, timeout_sec=45.0)
        profile, attempt = _wait_for_profile(client, profile_id)
        print(f"GET /cxs/profiles/{profile_id} OK after upsert sync (attempt {attempt})")
        _print_props(profile, ["email", "firstName", "loyaltyStatus", "brand"])
        print("SMOKE OK (upsert path)")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test Unomi profile sync (contactInfoSubmitted)")
    parser.add_argument("--x-brand", default=os.getenv("SMOKE_BRAND", "batira"))
    parser.add_argument("--profile-id", default="")
    parser.add_argument("--via-upsert", action="store_true", help="Also test DB upsert + sync_customer_profile_to_unomi")
    args = parser.parse_args()

    brand = str(args.x_brand).strip()
    profile_id = (args.profile_id or "").strip() or f"smoke-loyalty-{uuid.uuid4().hex[:12]}"

    code = smoke_eventcollector(brand=brand, profile_id=profile_id)
    if code != 0:
        return code
    if args.via_upsert:
        upsert_id = f"smoke-upsert-{uuid.uuid4().hex[:10]}"
        return smoke_via_upsert(brand=brand, profile_id=upsert_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
