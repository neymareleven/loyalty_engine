"""Re-run rule engine on a failed sale (e.g. after loyalty_status column fix).

Usage:
  python scripts/reprocess_sale_transaction.py --brand batira --order-number 7026
  python scripts/reprocess_sale_transaction.py --brand batira --transaction-id <uuid>
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from sqlalchemy import or_

from app.db import SessionLocal
from app.models.transaction import Transaction
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.services.rule_engine import process_transaction_rules


def main() -> int:
    parser = argparse.ArgumentParser(description="Reprocess a sale transaction")
    parser.add_argument("--brand", default="batira")
    parser.add_argument("--order-number", dest="order_number")
    parser.add_argument("--transaction-id", dest="transaction_id")
    parser.add_argument("--commit", action="store_true", help="Persist changes (default: dry-run)")
    args = parser.parse_args()

    if not args.order_number and not args.transaction_id:
        parser.error("Provide --order-number or --transaction-id")

    with SessionLocal() as db:
        tx = None
        if args.transaction_id:
            tx = (
                db.query(Transaction)
                .filter(Transaction.brand == args.brand)
                .filter(Transaction.transaction_id == args.transaction_id)
                .first()
            )
        else:
            tx = (
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
                .first()
            )

        if not tx:
            print("Transaction not found")
            return 1

        print(f"Found: {tx.transaction_id} status={tx.status} error_code={tx.error_code}")

        if tx.status not in {"PROCESSED_ERRORS", "FAILED", "BLOCKED"}:
            print(f"Refusing to reprocess status={tx.status} (expected PROCESSED_ERRORS/FAILED/BLOCKED)")
            return 1

        deleted = (
            db.query(TransactionRuleExecution)
            .filter(TransactionRuleExecution.transaction_id == tx.id)
            .delete(synchronize_session=False)
        )
        print(f"Deleted {deleted} rule execution row(s)")

        tx.status = "PENDING"
        tx.error_code = None
        tx.error_message = None
        tx.processed_at = None
        db.flush()

        process_transaction_rules(db, tx)
        tx.processed_at = datetime.utcnow()
        db.flush()

        print(f"After reprocess: status={tx.status} error_code={tx.error_code}")
        if tx.error_message:
            print(f"error_message: {tx.error_message}")

        if args.commit:
            db.commit()
            print("Committed.")
        else:
            db.rollback()
            print("Rolled back (pass --commit to persist).")

    return 0 if tx.status == "PROCESSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
