"""Reproduce POST /customers/upsert locally or on prod (same DB as the API).

Usage (from repo root, with .env present):
  python scripts/debug_upsert.py --email test15@gmail.com --profile-id 83543e47-5088-4d8d-b478-a02abd94c5e8
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import inspect

from app.db import SessionLocal, engine
from app.models.customer import Customer
from app.models.customer_unomi_profile_alias import CustomerUnomiProfileAlias
from app.services.contact_service import resolve_customer_for_upsert
from app.services.customer_upsert_service import customer_identity_payload


def _table_exists(name: str) -> bool:
    return inspect(engine).has_table(name)


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug customer upsert resolution")
    parser.add_argument("--brand", default="batira")
    parser.add_argument("--email", required=True)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--gender", default="F")
    parser.add_argument("--commit", action="store_true", help="Commit changes (default: rollback)")
    args = parser.parse_args()

    print("Tables:")
    print(f"  customers: { _table_exists('customers') }")
    print(f"  customer_unomi_profile_aliases: { _table_exists('customer_unomi_profile_aliases') }")

    parsed = {
        "brand": args.brand,
        "email": args.email,
        "gender": args.gender,
        "birthdate": None,
        "extra_properties": {},
    }
    identity = customer_identity_payload(parsed)

    with SessionLocal() as db:
        try:
            customer, is_new = resolve_customer_for_upsert(
                db,
                brand=args.brand,
                profile_id=args.profile_id,
                identity_payload=identity,
            )
            db.flush()

            alias_count = (
                db.query(CustomerUnomiProfileAlias)
                .filter(CustomerUnomiProfileAlias.customer_id == customer.id)
                .count()
            )

            print("\nResult:")
            print(f"  customer_id: {customer.id}")
            print(f"  master profile_id: {customer.profile_id}")
            print(f"  email: {customer.email}")
            print(f"  is_new_registration: {is_new}")
            print(f"  alias_count: {alias_count}")

            if args.commit:
                db.commit()
                print("\nCommitted.")
            else:
                db.rollback()
                print("\nRolled back (pass --commit to persist).")
            return 0
        except Exception as exc:
            db.rollback()
            print("\nERROR:", type(exc).__name__, str(exc))
            traceback.print_exc()
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
