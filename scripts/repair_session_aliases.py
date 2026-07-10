"""Audit and repair session alias fallout (mis-linked profiles, mis-attributed points).

Usage:
  # Audit (default): three categories — suspicious aliases, reprocess candidates, point transfers
  python scripts/repair_session_aliases.py --brand batira

  # Transfer points for a mis-attributed PROCESSED sale (dry-run)
  python scripts/repair_session_aliases.py --brand batira --fix-points --order-number 7026

  # Apply point transfer + Unomi sync for both customers
  python scripts/repair_session_aliases.py --brand batira --fix-points --order-number 7026 --commit

  # Delete suspicious session aliases listed in audit category A
  python scripts/repair_session_aliases.py --brand batira --delete-aliases --commit
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from uuid import UUID

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models.customer import Customer
from app.models.customer_unomi_profile_alias import CustomerUnomiProfileAlias
from app.models.point_movement import PointMovement
from app.models.transaction import Transaction
from app.services.contact_service import _extract_email_from_payload, get_customer
from app.services.loyalty_status_service import update_customer_status
from app.services.unomi_profile_service import sync_customer_profile_to_unomi
from app.services.wallet_service import get_status_points_balance

REPROCESS_STATUSES = frozenset({"PROCESSED_ERRORS", "FAILED", "BLOCKED"})
CORRECTION_TX_TYPE = "ADMIN_ALIAS_POINT_CORRECTION"


@dataclass
class SuspiciousAlias:
    alias_id: UUID
    profile_id: str
    source: str
    owner_customer_id: UUID
    owner_email: str | None
    owner_master_profile_id: str
    conflicting_emails: list[str] = field(default_factory=list)
    sample_order_numbers: list[str] = field(default_factory=list)


@dataclass
class ReprocessCandidate:
    transaction_id: str
    transaction_uuid: UUID
    order_number: str | None
    status: str
    error_code: str | None
    profile_id: str
    billing_email: str | None


@dataclass
class MisattributedSale:
    transaction_id: str
    transaction_uuid: UUID
    order_number: str | None
    status: str
    ingest_profile_id: str
    billing_email: str | None
    wrong_customer_id: UUID
    wrong_customer_email: str | None
    wrong_customer_master: str
    target_customer_id: UUID | None
    target_customer_email: str | None
    target_customer_master: str | None
    points_on_wrong_customer: int
    earn_expires_at: date | None
    already_corrected: bool
    correction_note: str | None = None


def _order_number(payload: dict | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw = payload.get("orderNumber") or payload.get("order_number")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _customer_by_email(db: Session, *, brand: str, email: str | None) -> Customer | None:
    if not email:
        return None
    return (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(func.lower(Customer.email) == email.strip().lower())
        .first()
    )


def _net_points_for_transaction(db: Session, *, customer_id: UUID, transaction_uuid: UUID) -> int:
    total = (
        db.query(func.coalesce(func.sum(PointMovement.points), 0))
        .filter(PointMovement.customer_id == customer_id)
        .filter(PointMovement.source_transaction_id == transaction_uuid)
        .scalar()
    )
    return int(total or 0)


def _earn_expires_at_for_transaction(
    db: Session, *, customer_id: UUID, transaction_uuid: UUID
) -> date | None:
    row = (
        db.query(PointMovement.expires_at)
        .filter(PointMovement.customer_id == customer_id)
        .filter(PointMovement.source_transaction_id == transaction_uuid)
        .filter(PointMovement.points > 0)
        .order_by(PointMovement.created_at.desc())
        .first()
    )
    return row[0] if row else None


def _correction_already_applied(db: Session, *, brand: str, order_number: str | None) -> bool:
    if not order_number:
        return False
    existing = (
        db.query(Transaction.id)
        .filter(Transaction.brand == brand)
        .filter(Transaction.transaction_type == CORRECTION_TX_TYPE)
        .filter(
            or_(
                Transaction.payload["orderNumber"].as_string() == order_number,
                Transaction.payload["order_number"].as_string() == order_number,
            )
        )
        .first()
    )
    return existing is not None


def _correction_note(*, order_number: str | None, audit_date: date | None = None) -> str:
    when = (audit_date or date.today()).isoformat()
    order = order_number or "?"
    return f"correction alias erroné - order {order} - voir audit du {when}"


def audit_suspicious_session_aliases(db: Session, *, brand: str) -> list[SuspiciousAlias]:
    """Category A: session aliases whose profile_id appears on sales for a different email customer."""
    aliases = (
        db.query(CustomerUnomiProfileAlias)
        .filter(CustomerUnomiProfileAlias.brand == brand)
        .filter(CustomerUnomiProfileAlias.source == "session")
        .all()
    )
    suspicious: list[SuspiciousAlias] = []

    for alias in aliases:
        owner = db.query(Customer).filter(Customer.id == alias.customer_id).first()
        if not owner:
            continue

        sales = (
            db.query(Transaction)
            .filter(Transaction.brand == brand)
            .filter(Transaction.transaction_type.ilike("sale"))
            .filter(Transaction.profile_id == alias.profile_id)
            .order_by(Transaction.created_at.desc())
            .all()
        )
        if not sales:
            continue

        conflicting: set[str] = set()
        sample_orders: list[str] = []
        for tx in sales:
            payload = tx.payload if isinstance(tx.payload, dict) else {}
            billing = _extract_email_from_payload(payload, brand=brand)
            if not billing:
                continue
            email_customer = _customer_by_email(db, brand=brand, email=billing)
            if email_customer and email_customer.id != owner.id:
                conflicting.add(billing)
                order = _order_number(payload)
                if order and order not in sample_orders:
                    sample_orders.append(order)

        if not conflicting:
            continue

        suspicious.append(
            SuspiciousAlias(
                alias_id=alias.id,
                profile_id=alias.profile_id,
                source=alias.source,
                owner_customer_id=owner.id,
                owner_email=(owner.email or "").strip().lower() or None,
                owner_master_profile_id=owner.profile_id,
                conflicting_emails=sorted(conflicting),
                sample_order_numbers=sample_orders[:5],
            )
        )

    return suspicious


def audit_sale_transactions(
    db: Session,
    *,
    brand: str,
    order_number: str | None = None,
) -> tuple[list[ReprocessCandidate], list[MisattributedSale]]:
    """Categories B (reprocess) and C (PROCESSED mis-attribution needing ADJUST)."""
    q = (
        db.query(Transaction)
        .filter(Transaction.brand == brand)
        .filter(Transaction.transaction_type.ilike("sale"))
    )
    if order_number:
        q = q.filter(
            or_(
                Transaction.payload["orderNumber"].as_string() == order_number,
                Transaction.payload["order_number"].as_string() == order_number,
            )
        )

    reprocess: list[ReprocessCandidate] = []
    misattributed: list[MisattributedSale] = []
    audit_day = date.today()

    for tx in q.order_by(Transaction.created_at.desc()).all():
        payload = tx.payload if isinstance(tx.payload, dict) else {}
        billing = _extract_email_from_payload(payload, brand=brand)
        order = _order_number(payload)
        by_profile = get_customer(db, brand, tx.profile_id)
        by_email = _customer_by_email(db, brand=brand, email=billing)

        if tx.status in REPROCESS_STATUSES:
            reprocess.append(
                ReprocessCandidate(
                    transaction_id=tx.transaction_id,
                    transaction_uuid=tx.id,
                    order_number=order,
                    status=tx.status,
                    error_code=tx.error_code,
                    profile_id=tx.profile_id,
                    billing_email=billing,
                )
            )
            continue

        if tx.status != "PROCESSED":
            continue

        if not by_profile or not billing:
            continue

        if by_email and by_email.id == by_profile.id:
            continue

        points = _net_points_for_transaction(db, customer_id=by_profile.id, transaction_uuid=tx.id)
        if points <= 0:
            continue

        already = _correction_already_applied(db, brand=brand, order_number=order)
        misattributed.append(
            MisattributedSale(
                transaction_id=tx.transaction_id,
                transaction_uuid=tx.id,
                order_number=order,
                status=tx.status,
                ingest_profile_id=tx.profile_id,
                billing_email=billing,
                wrong_customer_id=by_profile.id,
                wrong_customer_email=(by_profile.email or "").strip().lower() or None,
                wrong_customer_master=by_profile.profile_id,
                target_customer_id=by_email.id if by_email else None,
                target_customer_email=(by_email.email or "").strip().lower() if by_email else billing,
                target_customer_master=by_email.profile_id if by_email else None,
                points_on_wrong_customer=points,
                earn_expires_at=_earn_expires_at_for_transaction(
                    db, customer_id=by_profile.id, transaction_uuid=tx.id
                ),
                already_corrected=already,
                correction_note=_correction_note(order_number=order, audit_date=audit_day),
            )
        )

    return reprocess, misattributed


def _print_audit_report(
    *,
    brand: str,
    suspicious: list[SuspiciousAlias],
    reprocess: list[ReprocessCandidate],
    misattributed: list[MisattributedSale],
) -> None:
    print(f"\n{'=' * 72}")
    print(f"AUDIT session aliases — brand={brand} — {datetime.utcnow().isoformat()}Z")
    print(f"{'=' * 72}")

    print("\n[A] Alias session suspects à supprimer")
    print("    (profile_id lié à un customer dont l'email ≠ billing_email des ventes ingestées)")
    if not suspicious:
        print("    (aucun)")
    for item in suspicious:
        print(f"    - alias {item.profile_id} → customer {item.owner_customer_id}")
        print(f"      master={item.owner_master_profile_id} owner_email={item.owner_email!r}")
        print(f"      emails conflictuels: {', '.join(item.conflicting_emails)}")
        if item.sample_order_numbers:
            print(f"      ventes exemples: {', '.join(item.sample_order_numbers)}")

    print("\n[B] Ventes PROCESSED_ERRORS / FAILED / BLOCKED → reprocesser")
    print("    python scripts/reprocess_sale_transaction.py --brand ... --order-number ... --commit")
    if not reprocess:
        print("    (aucune)")
    for item in reprocess:
        print(
            f"    - order={item.order_number!r} tx={item.transaction_id} "
            f"status={item.status} error={item.error_code!r} "
            f"profile={item.profile_id} billing={item.billing_email!r}"
        )

    print("\n[C] Ventes PROCESSED mais points sur le mauvais customer → correction ADJUST")
    print("    python scripts/repair_session_aliases.py --brand ... --fix-points --order-number ... [--commit]")
    if not misattributed:
        print("    (aucune)")
    for item in misattributed:
        target = item.target_customer_id or "(customer cible absent — upsert requis)"
        flag = " [DÉJÀ CORRIGÉ]" if item.already_corrected else ""
        print(
            f"    - order={item.order_number!r} tx={item.transaction_id}{flag}\n"
            f"      ingest_profile={item.ingest_profile_id} billing={item.billing_email!r}\n"
            f"      mauvais customer={item.wrong_customer_id} ({item.wrong_customer_email}) "
            f"master={item.wrong_customer_master}\n"
            f"      bon customer={target} ({item.target_customer_email}) "
            f"master={item.target_customer_master}\n"
            f"      points à transférer={item.points_on_wrong_customer} "
            f"note={item.correction_note!r}"
        )


def apply_point_transfer(
    db: Session,
    *,
    brand: str,
    item: MisattributedSale,
    commit: bool,
) -> bool:
    if item.already_corrected:
        print(f"Skip order={item.order_number!r}: correction déjà enregistrée.")
        return True

    if not item.target_customer_id:
        print(
            f"Refusing order={item.order_number!r}: customer cible absent pour "
            f"{item.target_customer_email!r}. Créez-le via upsert avant --fix-points."
        )
        return False

    if item.points_on_wrong_customer <= 0:
        print(f"Skip order={item.order_number!r}: aucun point net sur le mauvais customer.")
        return True

    note = item.correction_note or _correction_note(order_number=item.order_number)
    amount = int(item.points_on_wrong_customer)
    now = datetime.utcnow()
    corr_tx_id = f"alias_point_correction_{brand}_{item.order_number or item.transaction_id}"

    wrong = (
        db.query(Customer)
        .filter(Customer.id == item.wrong_customer_id)
        .with_for_update()
        .first()
    )
    target = (
        db.query(Customer)
        .filter(Customer.id == item.target_customer_id)
        .with_for_update()
        .first()
    )
    if not wrong or not target:
        print(f"Customer introuvable pour order={item.order_number!r}")
        return False

    corr_tx = Transaction(
        transaction_id=corr_tx_id,
        brand=brand,
        profile_id=target.profile_id,
        transaction_type=CORRECTION_TX_TYPE,
        source="ADMIN_SCRIPT",
        payload={
            "orderNumber": item.order_number,
            "note": note,
            "sourceSaleTransactionId": str(item.transaction_uuid),
            "sourceSaleEventId": item.transaction_id,
            "fromCustomerId": str(wrong.id),
            "toCustomerId": str(target.id),
            "pointsTransferred": amount,
            "ingestProfileId": item.ingest_profile_id,
            "billingEmail": item.billing_email,
        },
        status="PROCESSED",
        processed_at=now,
    )
    db.add(corr_tx)
    db.flush()

    db.add(
        PointMovement(
            customer_id=wrong.id,
            points=-amount,
            type="ADJUST",
            source_transaction_id=corr_tx.id,
            expires_at=None,
        )
    )
    db.add(
        PointMovement(
            customer_id=target.id,
            points=amount,
            type="ADJUST",
            source_transaction_id=corr_tx.id,
            expires_at=item.earn_expires_at,
        )
    )
    db.flush()

    wrong.status_points = int(get_status_points_balance(db, wrong.id) or 0)
    target.status_points = int(get_status_points_balance(db, target.id) or 0)

    update_customer_status(
        db,
        wrong,
        reason="ALIAS_POINT_CORRECTION",
        source_transaction_id=corr_tx.id,
        sync_unomi=False,
    )
    update_customer_status(
        db,
        target,
        reason="ALIAS_POINT_CORRECTION",
        source_transaction_id=corr_tx.id,
        sync_unomi=False,
    )
    db.flush()

    print(
        f"{'Applied' if commit else 'Would apply'} transfer order={item.order_number!r}: "
        f"-{amount} on {wrong.profile_id} ({wrong.email}) → "
        f"+{amount} on {target.profile_id} ({target.email})\n"
        f"  note: {note}\n"
        f"  wrong balance after: {wrong.status_points}\n"
        f"  target balance after: {target.status_points}"
    )

    if commit:
        sync_wrong = sync_customer_profile_to_unomi(
            db, customer=wrong, reason="alias_point_correction", transport_override="profiles"
        )
        sync_target = sync_customer_profile_to_unomi(
            db, customer=target, reason="alias_point_correction", transport_override="profiles"
        )
        print(f"  Unomi sync wrong: {json.dumps(sync_wrong, default=str)}")
        print(f"  Unomi sync target: {json.dumps(sync_target, default=str)}")

    return True


def delete_suspicious_aliases(
    db: Session,
    *,
    brand: str,
    suspicious: list[SuspiciousAlias],
    commit: bool,
) -> int:
    deleted = 0
    for item in suspicious:
        row = (
            db.query(CustomerUnomiProfileAlias)
            .filter(CustomerUnomiProfileAlias.id == item.alias_id)
            .first()
        )
        if not row:
            continue
        print(
            f"{'Deleting' if commit else 'Would delete'} session alias "
            f"{item.profile_id} (owner {item.owner_master_profile_id})"
        )
        if commit:
            db.delete(row)
        deleted += 1
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit/repair session alias mis-attributions")
    parser.add_argument("--brand", default="batira")
    parser.add_argument("--order-number", dest="order_number", help="Scope to one sale order number")
    parser.add_argument(
        "--audit",
        action="store_true",
        default=True,
        help="Print audit report (default)",
    )
    parser.add_argument(
        "--fix-points",
        action="store_true",
        help="Apply ADJUST point transfers for category [C] sales",
    )
    parser.add_argument(
        "--delete-aliases",
        action="store_true",
        help="Delete category [A] suspicious session aliases",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Persist changes (default: dry-run / rollback)",
    )
    args = parser.parse_args()

    exit_code = 0
    with SessionLocal() as db:
        suspicious = audit_suspicious_session_aliases(db, brand=args.brand)
        reprocess, misattributed = audit_sale_transactions(
            db, brand=args.brand, order_number=args.order_number
        )

        if not args.fix_points and not args.delete_aliases:
            _print_audit_report(
                brand=args.brand,
                suspicious=suspicious,
                reprocess=reprocess,
                misattributed=misattributed,
            )

        if args.delete_aliases:
            count = delete_suspicious_aliases(
                db, brand=args.brand, suspicious=suspicious, commit=args.commit
            )
            print(f"\nAliases {'deleted' if args.commit else 'to delete'}: {count}")

        if args.fix_points:
            if not misattributed:
                print("\nNo mis-attributed PROCESSED sales with transferable points.")
            ok = True
            for item in misattributed:
                if not apply_point_transfer(db, brand=args.brand, item=item, commit=args.commit):
                    ok = False
                    exit_code = 1

        if args.commit:
            if args.fix_points or args.delete_aliases:
                db.commit()
                print("\nCommitted.")
        else:
            if args.fix_points or args.delete_aliases:
                db.rollback()
                print("\nRolled back (pass --commit to persist).")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
