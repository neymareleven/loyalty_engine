"""Audit and repair session alias fallout (mis-linked profiles, mis-attributed points).

Usage:
  # Audit (default): three categories — suspicious aliases, reprocess candidates, point transfers
  python scripts/repair_session_aliases.py --brand batira

  # Transfer points for a mis-attributed PROCESSED sale (dry-run)
  python scripts/repair_session_aliases.py --brand batira --fix-points --order-number 7026

  # Explicit transfer after manual cleanup (when --fix-points no longer detects mismatch)
  python scripts/repair_session_aliases.py --brand batira --force-transfer \
    --from-customer 6a5aa537-e2c5-447d-b1df-9f28528a87b8 \
    --to-customer b6153492-0832-45c8-8d78-889970809cbc \
    --order-number 7026
  python scripts/repair_session_aliases.py --brand batira --force-transfer ... --commit

  # Apply point transfer + Unomi sync for both customers
  python scripts/repair_session_aliases.py --brand batira --fix-points --order-number 7026 --commit

  # Delete suspicious session aliases listed in audit category A
  python scripts/repair_session_aliases.py --brand batira --delete-aliases --commit

  # Deep analysis: domain triage [C], root-cause buckets [B], masked-email cases (read-only)
  python scripts/repair_session_aliases.py --brand batira --analyze
  python scripts/repair_session_aliases.py --brand batira --analyze --json-out audit_analysis.json
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

# Domains flagged as disposable / abuse — extend via --extra-disposable-domain.
DEFAULT_DISPOSABLE_DOMAINS = frozenset(
    {
        "hacknapp.com",
        "poisonword.com",
        "soppat.com",
        "bmoar.com",
        "4heats.com",
        "canvect.com",
        "mypethealh.com",
        "tempmail.com",
        "guerrillamail.com",
        "mailinator.com",
        "yopmail.com",
    }
)

TEST_EMAIL_LOCAL_PREFIXES = ("test", "demo", "fake", "spam", "abuse")


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
    detection: str = "profile_email_mismatch"
    ingest_is_alias: bool = False
    email_overwrite_masked: bool = False


@dataclass
class CategoryBInvestigation:
    transaction_id: str
    order_number: str | None
    status: str
    error_code: str | None
    profile_id: str
    billing_email: str | None
    root_cause: str
    detail: str
    recoverable_now: bool


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


def _correction_already_applied_for_sale(
    db: Session,
    *,
    brand: str,
    order_number: str | None,
    sale_transaction_uuid: UUID,
) -> bool:
    if _correction_already_applied(db, brand=brand, order_number=order_number):
        return True
    existing = (
        db.query(Transaction.id)
        .filter(Transaction.brand == brand)
        .filter(Transaction.transaction_type == CORRECTION_TX_TYPE)
        .filter(Transaction.payload["sourceSaleTransactionId"].as_string() == str(sale_transaction_uuid))
        .first()
    )
    return existing is not None


def _find_sale_transaction(
    db: Session,
    *,
    brand: str,
    order_number: str | None,
    sale_transaction_id: str | None = None,
) -> Transaction | None:
    if sale_transaction_id:
        tx = (
            db.query(Transaction)
            .filter(Transaction.brand == brand)
            .filter(Transaction.id == sale_transaction_id)
            .first()
        )
        if tx:
            return tx
        tx = (
            db.query(Transaction)
            .filter(Transaction.brand == brand)
            .filter(Transaction.transaction_id == sale_transaction_id)
            .first()
        )
        if tx:
            return tx
    if not order_number:
        return None
    return (
        db.query(Transaction)
        .filter(Transaction.brand == brand)
        .filter(Transaction.transaction_type.ilike("sale"))
        .filter(
            or_(
                Transaction.payload["orderNumber"].as_string() == order_number,
                Transaction.payload["order_number"].as_string() == order_number,
            )
        )
        .order_by(Transaction.created_at.desc())
        .first()
    )


def _point_movements_for_sale_on_customer(
    db: Session,
    *,
    customer_id: UUID,
    sale_transaction_uuid: UUID,
) -> list[PointMovement]:
    return (
        db.query(PointMovement)
        .filter(PointMovement.customer_id == customer_id)
        .filter(PointMovement.source_transaction_id == sale_transaction_uuid)
        .order_by(PointMovement.created_at.asc())
        .all()
    )


def _execute_point_transfer(
    db: Session,
    *,
    brand: str,
    wrong: Customer,
    target: Customer,
    amount: int,
    order_number: str | None,
    sale_tx: Transaction,
    earn_expires_at: date | None,
    note: str,
    ingest_profile_id: str | None,
    billing_email: str | None,
    commit: bool,
    mode: str,
) -> bool:
    if amount <= 0:
        print(f"Refusing transfer: amount must be positive (got {amount}).")
        return False
    if wrong.id == target.id:
        print("Refusing transfer: source and target customer are the same.")
        return False

    wrong_before = int(get_status_points_balance(db, wrong.id) or 0)
    target_before = int(get_status_points_balance(db, target.id) or 0)

    now = datetime.utcnow()
    corr_tx_id = f"alias_point_correction_{brand}_{order_number or sale_tx.transaction_id}"

    wrong = db.query(Customer).filter(Customer.id == wrong.id).with_for_update().first()
    target = db.query(Customer).filter(Customer.id == target.id).with_for_update().first()
    if not wrong or not target:
        print("Customer introuvable.")
        return False

    corr_tx = Transaction(
        transaction_id=corr_tx_id,
        brand=brand,
        profile_id=target.profile_id,
        transaction_type=CORRECTION_TX_TYPE,
        source="ADMIN_SCRIPT",
        payload={
            "orderNumber": order_number,
            "note": note,
            "mode": mode,
            "sourceSaleTransactionId": str(sale_tx.id),
            "sourceSaleEventId": sale_tx.transaction_id,
            "fromCustomerId": str(wrong.id),
            "toCustomerId": str(target.id),
            "pointsTransferred": amount,
            "ingestProfileId": ingest_profile_id,
            "billingEmail": billing_email,
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
            expires_at=earn_expires_at,
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

    wrong_after = wrong.status_points
    target_after = target.status_points

    print(
        f"{'Applied' if commit else 'Would apply'} {mode} transfer order={order_number!r}:\n"
        f"  sale_tx={sale_tx.transaction_id} ({sale_tx.id})\n"
        f"  source {wrong.profile_id} ({wrong.email}) id={wrong.id}\n"
        f"    balance: {wrong_before} → {wrong_after} ({-amount})\n"
        f"  target {target.profile_id} ({target.email}) id={target.id}\n"
        f"    balance: {target_before} → {target_after} (+{amount})\n"
        f"  note: {note}"
    )

    if commit:
        sync_wrong = sync_customer_profile_to_unomi(
            db, customer=wrong, reason="alias_point_correction", transport_override="profiles"
        )
        sync_target = sync_customer_profile_to_unomi(
            db, customer=target, reason="alias_point_correction", transport_override="profiles"
        )
        print(f"  Unomi sync source ({wrong.profile_id}): {json.dumps(sync_wrong, default=str)}")
        print(f"  Unomi sync target ({target.profile_id}): {json.dumps(sync_target, default=str)}")

    return True


def force_transfer_points(
    db: Session,
    *,
    brand: str,
    from_customer_id: UUID,
    to_customer_id: UUID,
    order_number: str | None,
    sale_transaction_id: str | None = None,
    commit: bool,
) -> bool:
    """Transfer points tied to a sale transaction, ignoring current profile/email resolution."""
    sale_tx = _find_sale_transaction(
        db,
        brand=brand,
        order_number=order_number,
        sale_transaction_id=sale_transaction_id,
    )
    if not sale_tx:
        print(f"Sale transaction not found for order={order_number!r}")
        return False

    order = _order_number(sale_tx.payload if isinstance(sale_tx.payload, dict) else {}) or order_number
    if _correction_already_applied_for_sale(
        db,
        brand=brand,
        order_number=order,
        sale_transaction_uuid=sale_tx.id,
    ):
        print(
            f"Refusing: correction already exists for order={order!r} "
            f"or sale_tx={sale_tx.id}"
        )
        return False

    wrong = db.query(Customer).filter(Customer.id == from_customer_id).first()
    target = db.query(Customer).filter(Customer.id == to_customer_id).first()
    if not wrong or not target:
        print("from-customer or to-customer not found.")
        return False
    if wrong.brand != brand or target.brand != brand:
        print(f"Both customers must belong to brand={brand!r}.")
        return False

    movements = _point_movements_for_sale_on_customer(
        db, customer_id=from_customer_id, sale_transaction_uuid=sale_tx.id
    )
    net = sum(int(m.points or 0) for m in movements)
    if net <= 0:
        print(
            f"No positive net points on source customer for sale {sale_tx.transaction_id} "
            f"(net={net}, movements={len(movements)})."
        )
        return False

    earn_expires_at = None
    for m in reversed(movements):
        if int(m.points or 0) > 0 and m.expires_at:
            earn_expires_at = m.expires_at
            break

    payload = sale_tx.payload if isinstance(sale_tx.payload, dict) else {}
    billing = _extract_email_from_payload(payload, brand=brand)
    note = _correction_note(order_number=order)

    print(f"\n=== FORCE TRANSFER {'(commit)' if commit else '(dry-run)'} ===")
    print(f"Sale: {sale_tx.transaction_id} status={sale_tx.status} profile={sale_tx.profile_id}")
    for m in movements:
        print(
            f"  point_movement {m.id}: type={m.type} points={m.points} "
            f"expires_at={m.expires_at}"
        )
    print(f"Net transferable on source: {net}")

    return _execute_point_transfer(
        db,
        brand=brand,
        wrong=wrong,
        target=target,
        amount=net,
        order_number=order,
        sale_tx=sale_tx,
        earn_expires_at=earn_expires_at,
        note=note,
        ingest_profile_id=sale_tx.profile_id,
        billing_email=billing,
        commit=commit,
        mode="force_transfer",
    )


def _email_domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    return email.strip().lower().split("@", 1)[1]


def _classify_billing_email(
    email: str | None,
    *,
    disposable_domains: frozenset[str],
) -> str:
    """Return: disposable | test_pattern | likely_real | missing."""
    if not email:
        return "missing"
    domain = _email_domain(email)
    local = email.strip().lower().split("@", 1)[0]
    if domain and domain in disposable_domains:
        return "disposable"
    if local.startswith(TEST_EMAIL_LOCAL_PREFIXES):
        return "test_pattern"
    if domain in {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "live.com", "icloud.com"}:
        return "likely_real"
    if domain and "." in domain:
        return "likely_real"
    return "missing"


def _is_alias_profile_for_customer(
    db: Session, *, brand: str, customer_id: UUID, profile_id: str
) -> bool:
    profile_id = (profile_id or "").strip()
    if not profile_id:
        return False
    row = (
        db.query(CustomerUnomiProfileAlias.id)
        .filter(CustomerUnomiProfileAlias.brand == brand)
        .filter(CustomerUnomiProfileAlias.customer_id == customer_id)
        .filter(CustomerUnomiProfileAlias.profile_id == profile_id)
        .first()
    )
    return row is not None


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


def _build_misattributed_sale(
    db: Session,
    *,
    tx: Transaction,
    by_profile: Customer,
    by_email: Customer | None,
    billing: str | None,
    order: str | None,
    audit_day: date,
    detection: str,
    ingest_is_alias: bool,
    email_overwrite_masked: bool,
) -> MisattributedSale | None:
    points = _net_points_for_transaction(db, customer_id=by_profile.id, transaction_uuid=tx.id)
    if points <= 0:
        return None
    already = _correction_already_applied_for_sale(
        db, brand=tx.brand, order_number=order, sale_transaction_uuid=tx.id
    )
    return MisattributedSale(
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
        detection=detection,
        ingest_is_alias=ingest_is_alias,
        email_overwrite_masked=email_overwrite_masked,
    )


def audit_sale_transactions(
    db: Session,
    *,
    brand: str,
    order_number: str | None = None,
    include_masked: bool = False,
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

        ingest_is_alias = _is_alias_profile_for_customer(
            db, brand=brand, customer_id=by_profile.id, profile_id=tx.profile_id
        )
        same_customer = bool(by_email and by_email.id == by_profile.id)

        if same_customer and not include_masked:
            continue

        if same_customer and include_masked:
            # Email overwrite masks mismatch: billing matches corrupted customers.email.
            if not ingest_is_alias:
                continue
            item = _build_misattributed_sale(
                db,
                tx=tx,
                by_profile=by_profile,
                by_email=by_email,
                billing=billing,
                order=order,
                audit_day=audit_day,
                detection="masked_alias_ingest",
                ingest_is_alias=True,
                email_overwrite_masked=True,
            )
            if item:
                misattributed.append(item)
            continue

        item = _build_misattributed_sale(
            db,
            tx=tx,
            by_profile=by_profile,
            by_email=by_email,
            billing=billing,
            order=order,
            audit_day=audit_day,
            detection="profile_email_mismatch",
            ingest_is_alias=ingest_is_alias,
            email_overwrite_masked=False,
        )
        if item:
            misattributed.append(item)

    return reprocess, misattributed


def investigate_category_b(
    db: Session,
    *,
    brand: str,
) -> list[CategoryBInvestigation]:
    """Classify BLOCKED/FAILED/PROCESSED_ERRORS sales (category B) by likely root cause."""
    rows = (
        db.query(Transaction)
        .filter(Transaction.brand == brand)
        .filter(Transaction.transaction_type.ilike("sale"))
        .filter(Transaction.status.in_(REPROCESS_STATUSES))
        .order_by(Transaction.created_at.desc())
        .all()
    )
    out: list[CategoryBInvestigation] = []

    for tx in rows:
        payload = tx.payload if isinstance(tx.payload, dict) else {}
        billing = _extract_email_from_payload(payload, brand=brand)
        order = _order_number(payload)
        by_profile = get_customer(db, brand, tx.profile_id)
        by_email = _customer_by_email(db, brand=brand, email=billing)

        if tx.error_code == "WRONG_INGESTION_ROUTE":
            root, detail = "wrong_route", "Not a sale-ingest issue (profile event on /transactions)."
            recoverable = False
        elif not billing:
            root, detail = "no_billing_email", "Sale payload has no billing_email/email — cannot auto-create customer."
            recoverable = False
        elif by_profile and by_email and by_profile.id != by_email.id:
            root, detail = (
                "profile_email_split",
                "Profile resolves to one customer, billing email to another (alias/session bug).",
            )
            recoverable = True
        elif by_profile and billing:
            profile_email = (by_profile.email or "").strip().lower()
            if profile_email and profile_email != billing:
                root, detail = (
                    "profile_email_mismatch_rejected",
                    "resolve_customer_for_transaction rejects when owner email != billing (post-fix behaviour).",
                )
                recoverable = bool(by_email)
            else:
                root, detail = "other_blocked", tx.error_message or "See error_message in DB."
                recoverable = tx.error_code == "CUSTOMER_NOT_FOUND"
        elif not by_profile and not by_email:
            root, detail = (
                "customer_absent",
                "No customer for profileId or billing email at ingest time.",
            )
            recoverable = True
        elif not by_profile and by_email:
            root, detail = (
                "profile_unknown_email_known",
                "Billing email matches a customer but ingest profileId is unknown.",
            )
            recoverable = True
        else:
            root, detail = "other", tx.error_message or "Unclassified."
            recoverable = tx.error_code in {"CUSTOMER_NOT_FOUND", None}

        if tx.error_code == "CUSTOMER_NOT_FOUND" and root == "other":
            if not billing:
                root, detail = "no_billing_email", detail
            elif by_profile and (by_profile.email or "").strip().lower() != billing:
                root, detail = "profile_email_mismatch_rejected", detail

        out.append(
            CategoryBInvestigation(
                transaction_id=tx.transaction_id,
                order_number=order,
                status=tx.status,
                error_code=tx.error_code,
                profile_id=tx.profile_id,
                billing_email=billing,
                root_cause=root,
                detail=detail,
                recoverable_now=recoverable,
            )
        )

    return out


def _summarize_category_c_by_domain(
    items: list[MisattributedSale],
    *,
    disposable_domains: frozenset[str],
) -> dict:
    by_domain: dict[str, dict] = {}
    by_class: dict[str, dict] = {
        "disposable": {"orders": 0, "points": 0, "emails": set()},
        "test_pattern": {"orders": 0, "points": 0, "emails": set()},
        "likely_real": {"orders": 0, "points": 0, "emails": set()},
        "missing": {"orders": 0, "points": 0, "emails": set()},
    }

    for item in items:
        email = item.billing_email or item.target_customer_email or ""
        cls = _classify_billing_email(email, disposable_domains=disposable_domains)
        domain = _email_domain(email) or "(no-domain)"
        pts = int(item.points_on_wrong_customer or 0)

        bucket = by_domain.setdefault(
            domain,
            {
                "domain": domain,
                "classification": cls,
                "orders": 0,
                "points": 0,
                "top_emails": {},
                "sample_orders": [],
            },
        )
        bucket["orders"] += 1
        bucket["points"] += pts
        bucket["top_emails"][email] = bucket["top_emails"].get(email, 0) + pts
        if item.order_number and len(bucket["sample_orders"]) < 5:
            bucket["sample_orders"].append(item.order_number)

        by_class[cls]["orders"] += 1
        by_class[cls]["points"] += pts
        if email:
            by_class[cls]["emails"].add(email)

    domain_rows = sorted(by_domain.values(), key=lambda r: r["points"], reverse=True)
    for row in domain_rows:
        row["top_emails"] = sorted(
            [{"email": e, "points": p} for e, p in row["top_emails"].items()],
            key=lambda x: x["points"],
            reverse=True,
        )[:5]

    return {
        "by_classification": {
            k: {
                "orders": v["orders"],
                "points": v["points"],
                "unique_emails": len(v["emails"]),
            }
            for k, v in by_class.items()
        },
        "by_domain": domain_rows,
        "totals": {
            "orders": len(items),
            "points": sum(int(i.points_on_wrong_customer or 0) for i in items),
        },
        "likely_real_totals": {
            "orders": by_class["likely_real"]["orders"],
            "points": by_class["likely_real"]["points"],
        },
        "exclude_disposable_and_test": {
            "orders": by_class["likely_real"]["orders"] + by_class["missing"]["orders"],
            "points": by_class["likely_real"]["points"] + by_class["missing"]["points"],
        },
    }


def _print_analyze_report(
    *,
    brand: str,
    naive_c: list[MisattributedSale],
    robust_c: list[MisattributedSale],
    masked_only: list[MisattributedSale],
    category_b: list[CategoryBInvestigation],
    domain_summary: dict,
    order_probe: str | None,
) -> None:
    print(f"\n{'=' * 72}")
    print(f"ANALYSE APPROFONDIE — brand={brand} — {datetime.utcnow().isoformat()}Z")
    print(f"{'=' * 72}")

    print("\n[C] Résumé par classification email (billing_email du payload — source de vérité)")
    for cls, stats in domain_summary["by_classification"].items():
        print(f"  {cls:14} orders={stats['orders']:4}  points={stats['points']:8}  emails={stats['unique_emails']}")

    print(
        f"\n  TOTAL [C] naive (script actuel):     "
        f"orders={len(naive_c)}  points={sum(i.points_on_wrong_customer for i in naive_c)}"
    )
    print(
        f"  TOTAL [C] robust (+ alias masqués):  "
        f"orders={len(robust_c)}  points={sum(i.points_on_wrong_customer for i in robust_c)}"
    )
    print(
        f"  Cas masqués par email écrasé:        "
        f"orders={len(masked_only)}  points={sum(i.points_on_wrong_customer for i in masked_only)}"
    )
    print(
        f"\n  Points « vrais clients » (hors jetable + test_pattern): "
        f"orders={domain_summary['exclude_disposable_and_test']['orders']}  "
        f"points={domain_summary['exclude_disposable_and_test']['points']}"
    )

    print("\n[C] Top domaines par volume de points (robust)")
    for row in domain_summary["by_domain"][:20]:
        print(
            f"  {row['domain']:28} [{row['classification']:12}] "
            f"orders={row['orders']:3} points={row['points']:7}  "
            f"samples={', '.join(row['sample_orders'][:3])}"
        )
        for te in row["top_emails"][:2]:
            print(f"      {te['email']}: {te['points']} pts")

    print("\n[B] CUSTOMER_NOT_FOUND / erreurs — causes racines")
    buckets: dict[str, list[CategoryBInvestigation]] = {}
    for item in category_b:
        buckets.setdefault(item.root_cause, []).append(item)

    for cause, rows in sorted(buckets.items(), key=lambda kv: len(kv[1]), reverse=True):
        print(f"  {cause}: {len(rows)}")
        for sample in rows[:3]:
            print(
                f"    order={sample.order_number!r} status={sample.status} "
                f"error={sample.error_code!r} billing={sample.billing_email!r}"
            )

    if order_probe:
        print(f"\n[PROBE] Commande {order_probe}")
        in_naive = [i for i in naive_c if i.order_number == order_probe]
        in_robust = [i for i in robust_c if i.order_number == order_probe]
        print(f"  Dans [C] naive: {bool(in_naive)}")
        print(f"  Dans [C] robust: {bool(in_robust)}")
        if in_robust:
            r = in_robust[0]
            print(
                f"  detection={r.detection} masked={r.email_overwrite_masked} "
                f"alias_ingest={r.ingest_is_alias}\n"
                f"  wrong={r.wrong_customer_email} target={r.target_customer_email} "
                f"points={r.points_on_wrong_customer}"
            )
        if in_naive and not in_robust:
            print("  → présent en naive seulement")
        if in_robust and not in_naive:
            print("  → DÉTECTÉ UNIQUEMENT en mode robust (email écrasé masque le mismatch)")
        if not in_naive and not in_robust:
            print("  → absent des deux: pas PROCESSED+points, ou pas de mismatch détectable")


def run_analyze(
    db: Session,
    *,
    brand: str,
    disposable_domains: frozenset[str],
    order_probe: str | None,
    json_out: str | None,
) -> dict:
    _, naive_c = audit_sale_transactions(db, brand=brand, include_masked=False)
    _, robust_c = audit_sale_transactions(db, brand=brand, include_masked=True)
    naive_ids = {i.transaction_uuid for i in naive_c}
    masked_only = [i for i in robust_c if i.transaction_uuid not in naive_ids]
    category_b = investigate_category_b(db, brand=brand)
    domain_summary = _summarize_category_c_by_domain(robust_c, disposable_domains=disposable_domains)

    _print_analyze_report(
        brand=brand,
        naive_c=naive_c,
        robust_c=robust_c,
        masked_only=masked_only,
        category_b=category_b,
        domain_summary=domain_summary,
        order_probe=order_probe,
    )

    payload = {
        "brand": brand,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "category_c_naive": len(naive_c),
        "category_c_robust": len(robust_c),
        "category_c_masked_only": len(masked_only),
        "domain_summary": domain_summary,
        "category_b_buckets": {
            cause: len([x for x in category_b if x.root_cause == cause])
            for cause in sorted({x.root_cause for x in category_b})
        },
        "masked_orders": [
            {"order": i.order_number, "billing": i.billing_email, "points": i.points_on_wrong_customer}
            for i in masked_only
        ],
    }

    if json_out:
        with open(json_out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        print(f"\nJSON écrit: {json_out}")

    return payload


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
    print("    Si nettoyage alias/email déjà fait: --force-transfer --from-customer ... --to-customer ...")
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

    if item.target_customer_id == item.wrong_customer_id:
        print(
            f"Refusing order={item.order_number!r}: mauvais et bon customer identiques "
            f"({item.wrong_customer_id}). Probable masquage par email écrasé — "
            f"usez --force-transfer ou restaurez customers.email avant --fix-points."
        )
        return False

    if item.points_on_wrong_customer <= 0:
        print(f"Skip order={item.order_number!r}: aucun point net sur le mauvais customer.")
        return True

    wrong = db.query(Customer).filter(Customer.id == item.wrong_customer_id).first()
    target = db.query(Customer).filter(Customer.id == item.target_customer_id).first()
    if not wrong or not target:
        print(f"Customer introuvable pour order={item.order_number!r}")
        return False

    sale_tx = (
        db.query(Transaction)
        .filter(Transaction.id == item.transaction_uuid)
        .first()
    )
    if not sale_tx:
        print(f"Sale transaction {item.transaction_uuid} introuvable.")
        return False

    note = item.correction_note or _correction_note(order_number=item.order_number)
    return _execute_point_transfer(
        db,
        brand=brand,
        wrong=wrong,
        target=target,
        amount=int(item.points_on_wrong_customer),
        order_number=item.order_number,
        sale_tx=sale_tx,
        earn_expires_at=item.earn_expires_at,
        note=note,
        ingest_profile_id=item.ingest_profile_id,
        billing_email=item.billing_email,
        commit=commit,
        mode="fix_points",
    )


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
        "--force-transfer",
        action="store_true",
        help="Explicit point transfer by customer UUID + order (ignores mismatch detection)",
    )
    parser.add_argument(
        "--from-customer",
        dest="from_customer",
        help="Source customer UUID (with --force-transfer)",
    )
    parser.add_argument(
        "--to-customer",
        dest="to_customer",
        help="Target customer UUID (with --force-transfer)",
    )
    parser.add_argument(
        "--sale-transaction-id",
        dest="sale_transaction_id",
        help="Sale transaction UUID or event_id (optional with --force-transfer)",
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
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Read-only deep analysis: domain triage [C], root-cause [B], masked cases",
    )
    parser.add_argument(
        "--json-out",
        help="Write --analyze results to JSON file",
    )
    parser.add_argument(
        "--probe-order",
        default="7026",
        help="Order number to probe in --analyze (default: 7026)",
    )
    parser.add_argument(
        "--extra-disposable-domain",
        action="append",
        default=[],
        help="Additional disposable email domain(s) for --analyze triage",
    )
    args = parser.parse_args()

    if args.force_transfer:
        if not args.from_customer or not args.to_customer:
            parser.error("--force-transfer requires --from-customer and --to-customer")
        if not args.order_number and not args.sale_transaction_id:
            parser.error("--force-transfer requires --order-number or --sale-transaction-id")

    disposable_domains = DEFAULT_DISPOSABLE_DOMAINS | frozenset(
        (d or "").strip().lower() for d in (args.extra_disposable_domain or []) if d
    )

    exit_code = 0
    with SessionLocal() as db:
        if args.analyze:
            run_analyze(
                db,
                brand=args.brand,
                disposable_domains=disposable_domains,
                order_probe=args.probe_order,
                json_out=args.json_out,
            )
            return 0

        if args.force_transfer:
            try:
                from_id = UUID(args.from_customer)
                to_id = UUID(args.to_customer)
            except ValueError:
                print("Invalid UUID for --from-customer or --to-customer")
                return 1

            ok = force_transfer_points(
                db,
                brand=args.brand,
                from_customer_id=from_id,
                to_customer_id=to_id,
                order_number=args.order_number,
                sale_transaction_id=args.sale_transaction_id,
                commit=args.commit,
            )
            if args.commit:
                if ok:
                    db.commit()
                    print("\nCommitted.")
                else:
                    db.rollback()
                    print("\nRolled back (transfer refused or failed).")
                    exit_code = 1
            else:
                db.rollback()
                print("\nRolled back (pass --commit to persist).")
                if not ok:
                    exit_code = 1
            return exit_code

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
