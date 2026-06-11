"""Smoke test: Unomi CDP connectivity + segment save/delete.

Usage:
  python scripts/smoke_unomi_segments.py --x-brand batira
"""

from __future__ import annotations

import argparse
import os
import sys

# Allow running from repo root without installing the package.
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import app.db  # loads .env
from app.services.unomi_settings_service import resolve_unomi_connection, unomi_env_status
from app.services.unomi_client import UnomiClient, UnomiClientError
from app.services.unomi_segment_service import build_unomi_segment_definition, _slug_segment_id


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test Unomi segments API")
    parser.add_argument("--x-brand", default=os.getenv("SMOKE_BRAND", "batira"), help="Brand scope (X-Brand)")
    args = parser.parse_args()
    brand = str(args.x_brand).strip()

    status = unomi_env_status(brand=brand)
    print("segmentation-mode:", status)

    cfg = resolve_unomi_connection(brand=brand)
    if not cfg:
        print("ERROR: Unomi not configured for brand", brand)
        return 1

    client = UnomiClient(cfg, timeout_sec=45.0)
    seg_id = _slug_segment_id(brand, "smoke-connectivity-test")
    condition = {
        "type": "profilePropertyCondition",
        "parameterValues": {
            "propertyName": "itemId",
            "comparisonOperator": "equals",
            "propertyValue": "__no_profiles__",
        },
    }
    definition = build_unomi_segment_definition(
        segment_id=seg_id,
        name="Smoke connectivity test",
        scope=cfg.scope,
        description="Auto smoke test — safe to delete",
        condition=condition,
    )

    try:
        count = len(client.list_segment_metadata(size=3))
        print(f"list segments OK ({count} sample)")
        client.save_segment(definition)
        print(f"save segment OK ({seg_id})")
        client.delete_segment(seg_id)
        print("delete segment OK")
    except UnomiClientError as e:
        print(f"ERROR: {e}")
        if e.body:
            print(e.body[:500])
        return 1

    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
