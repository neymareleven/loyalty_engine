"""Diagnose a sale transaction: customer resolution, rule executions, points.

Usage (from repo root):
  python scripts/debug_sale_points.py --brand batira --order-number 7026
  python scripts/debug_sale_points.py --brand batira --email test17@gmail.com
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import func, or_

from app.db import SessionLocal
from app.models.customer import Customer
from app.models.customer_unomi_profile_alias import CustomerUnomiProfileAlias
from app.models.point_movement import PointMovement
from app.models.product import Product
from app.models.transaction import Transaction
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.services.contact_service import get_customer, list_customer_unomi_profile_ids
from app.services.rule_engine import _normalize_match_key


def _print_json(label: str, data) -> None:
    print(f"\n=== {label} ===")
    print(json.dumps(data, indent=2, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug sale points for an order or email")
    parser.add_argument("--brand", default="batira")
    parser.add_argument("--order-number", dest="order_number")
    parser.add_argument("--email")
    parser.add_argument("--profile-id", dest="profile_id")
    args = parser.parse_args()

    if not args.order_number and not args.email and not args.profile_id:
        parser.error("Provide --order-number and/or --email and/or --profile-id")

    with SessionLocal() as db:
        txs: list[Transaction] = []
        if args.order_number:
            rows = (
                db.query(Transaction)
                .filter(Transaction.brand == args.brand)
                .filter(Transaction.transaction_type.ilike("sale"))
                .filter(
                    or_(
                        Transaction.payload["orderNumber"].as_string() == args.order_number,
                        Transaction.payload["order_number"].as_string() == args.order_number,
                    )
                )
                .order_by(Transaction.created_at.desc())
                .all()
            )
            txs.extend(rows)

        if args.email:
            email = args.email.strip().lower()
            cust = (
                db.query(Customer)
                .filter(Customer.brand == args.brand)
                .filter(func.lower(Customer.email) == email)
                .all()
            )
            _print_json(
                "Customers with email",
                [
                    {
                        "id": str(c.id),
                        "profile_id": c.profile_id,
                        "status_points": c.status_points,
                        "created_at": c.created_at,
                    }
                    for c in cust
                ],
            )
            for c in cust:
                aliases = (
                    db.query(CustomerUnomiProfileAlias.profile_id)
                    .filter(CustomerUnomiProfileAlias.customer_id == c.id)
                    .all()
                )
                print(
                    f"  customer {c.profile_id}: aliases="
                    + ", ".join(a[0] for a in aliases if a[0])
                    or "(none)"
                )

        for tx in txs:
            payload = tx.payload if isinstance(tx.payload, dict) else {}
            profile_id = tx.profile_id
            billing = (
                payload.get("billing_email")
                or payload.get("billingEmail")
                or payload.get("email")
            )
            by_profile = get_customer(db, args.brand, profile_id)
            by_email = None
            if billing:
                by_email = (
                    db.query(Customer)
                    .filter(Customer.brand == args.brand)
                    .filter(func.lower(Customer.email) == str(billing).strip().lower())
                    .first()
                )

            _print_json(
                f"Transaction {tx.transaction_id}",
                {
                    "status": tx.status,
                    "error_code": tx.error_code,
                    "error_message": tx.error_message,
                    "profile_id": profile_id,
                    "billing_email": billing,
                    "orderNumber": payload.get("orderNumber") or payload.get("order_number"),
                    "orderTotal": payload.get("orderTotal"),
                    "productNames": payload.get("productNames"),
                    "paymentMethod": payload.get("paymentMethod"),
                    "resolved_by_profile": (
                        {"id": str(by_profile.id), "master": by_profile.profile_id}
                        if by_profile
                        else None
                    ),
                    "resolved_by_email": (
                        {"id": str(by_email.id), "master": by_email.profile_id}
                        if by_email
                        else None
                    ),
                    "same_customer": (
                        by_profile.id == by_email.id
                        if by_profile and by_email
                        else None
                    ),
                },
            )

            execs = (
                db.query(TransactionRuleExecution)
                .filter(TransactionRuleExecution.transaction_id == tx.id)
                .all()
            )
            _print_json(
                "Rule executions",
                [
                    {
                        "rule_id": str(e.rule_id),
                        "result": e.result,
                        "details": e.details,
                    }
                    for e in execs
                ],
            )

            if by_email:
                pms = (
                    db.query(PointMovement)
                    .filter(PointMovement.customer_id == by_email.id)
                    .filter(PointMovement.source_transaction_id == tx.id)
                    .all()
                )
                _print_json(
                    f"Point movements on email customer ({by_email.profile_id})",
                    [{"points": p.points, "type": p.type} for p in pms],
                )

            names = payload.get("productNames") or []
            if isinstance(names, list):
                print("\n=== Product catalog match ===")
                for name in names:
                    mk = _normalize_match_key(name)
                    prod = (
                        db.query(Product)
                        .filter(Product.brand == args.brand)
                        .filter(Product.match_key == mk)
                        .first()
                        if mk
                        else None
                    )
                    print(
                        f"  {name!r} -> match_key={mk!r} "
                        f"catalog={'YES points=' + str(prod.points_value) if prod else 'MISSING'}"
                    )

            if tx.error_code == "NO_POINTS_EARNED":
                print(
                    "\n>>> NO_POINTS_EARNED: a sale rule matched but earn_points resolved to 0. "
                    "Check product catalog (sum_product_points_unomi) or earn_points expression."
                )
            elif tx.error_code == "SALE_RULES_SKIPPED":
                print(
                    "\n>>> SALE_RULES_SKIPPED: no sale rule matched (segment/conditions)."
                )
            elif tx.error_code == "CUSTOMER_NOT_FOUND":
                print("\n>>> CUSTOMER_NOT_FOUND: no loyalty customer at ingest time.")
            elif tx.status == "PROCESSED_ERRORS":
                failed = [e for e in execs if e.result == "FAILED"]
                if failed:
                    err = (failed[0].details or {}).get("error", "")
                    if "StringDataRightTruncation" in err and "loyalty_status" in err:
                        print(
                            "\n>>> PROCESSED_ERRORS: loyalty_status tier key too long for "
                            "customers.loyalty_status column (varchar 20). "
                            "Run: alembic upgrade head  (migration e2f3a4b5c6d7)"
                        )
                    else:
                        print(f"\n>>> PROCESSED_ERRORS: {err[:500]}")

        if args.profile_id:
            c = get_customer(db, args.brand, args.profile_id)
            if c:
                ids = list_customer_unomi_profile_ids(db, c)
                _print_json(
                    f"Profile {args.profile_id} resolves to",
                    {"master": c.profile_id, "email": c.email, "all_ids": ids},
                )
            else:
                print(f"No customer for profile_id={args.profile_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
