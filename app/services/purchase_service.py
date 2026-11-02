"""Purchase-count + purchase-amount qualification (mirror of points, with expiry per sale)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.purchase_movement import PurchaseMovement
from app.services.loyalty_settings_service import get_loyalty_settings

QUALIFICATION_POINTS = "POINTS"
QUALIFICATION_PURCHASES = "PURCHASES"

_PURCHASE_TRANSACTION_TYPES = frozenset({"sale"})
_AMOUNT_FIELD_CANDIDATES = ("orderTotal", "total", "order_total", "subtotal")


def normalize_qualification_mode(value: Any) -> str:
    mode = str(value or QUALIFICATION_POINTS).strip().upper()
    if mode in {QUALIFICATION_PURCHASES, "PURCHASE", "SALES", "ORDERS"}:
        return QUALIFICATION_PURCHASES
    return QUALIFICATION_POINTS


def get_qualification_mode(db: Session, *, brand: str) -> str:
    settings = get_loyalty_settings(db, brand=brand)
    return normalize_qualification_mode(getattr(settings, "qualification_mode", None) if settings else None)


def extract_purchase_amount(payload: dict | None) -> int:
    if not isinstance(payload, dict):
        return 0
    for key in _AMOUNT_FIELD_CANDIDATES:
        raw = payload.get(key)
        if raw is None or raw == "":
            continue
        if isinstance(raw, bool):
            continue
        if isinstance(raw, int):
            return max(0, raw)
        if isinstance(raw, float):
            return max(0, int(raw))
        if isinstance(raw, str):
            from app.services.rule_engine import _as_number_from_text

            parsed = _as_number_from_text(raw)
            if parsed is not None:
                return max(0, int(parsed))
    return 0


def _active_purchase_filters(*, today: date | None = None):
    today = today or date.today()
    return (
        PurchaseMovement.type != "EXPIRE",
        (PurchaseMovement.expires_at.is_(None)) | (PurchaseMovement.expires_at >= today),
    )


def get_purchase_balances(db: Session, customer_id) -> tuple[int, int]:
    today = date.today()
    row = (
        db.query(
            func.coalesce(func.sum(PurchaseMovement.quantity), 0),
            func.coalesce(func.sum(PurchaseMovement.amount), 0),
        )
        .filter(PurchaseMovement.customer_id == customer_id, *_active_purchase_filters(today=today))
        .first()
    )
    count = max(0, int((row[0] if row else 0) or 0))
    amount = max(0, int((row[1] if row else 0) or 0))
    return count, amount


def get_next_purchase_expiry_date(db: Session, customer_id) -> date | None:
    count, amount = get_purchase_balances(db, customer_id)
    if count <= 0 and amount <= 0:
        return None
    today = date.today()
    return (
        db.query(func.min(PurchaseMovement.expires_at))
        .filter(
            PurchaseMovement.customer_id == customer_id,
            PurchaseMovement.type != "EXPIRE",
            PurchaseMovement.expires_at.isnot(None),
            PurchaseMovement.expires_at >= today,
            (PurchaseMovement.quantity > 0) | (PurchaseMovement.amount > 0),
        )
        .scalar()
    )


def resolve_purchases_expires_at(db: Session, customer_id) -> datetime | None:
    expiry_date = get_next_purchase_expiry_date(db, customer_id)
    if not isinstance(expiry_date, date):
        return None
    return datetime.combine(expiry_date, time(23, 59, 59))


def sync_customer_purchase_counters(db: Session, customer) -> None:
    count, amount = get_purchase_balances(db, customer.id)
    customer.purchase_count = count
    customer.purchase_amount_total = amount
    customer.purchases_expires_at = resolve_purchases_expires_at(db, customer.id)


def is_purchase_transaction_type(transaction_type: str | None) -> bool:
    return str(transaction_type or "").strip().lower() in _PURCHASE_TRANSACTION_TYPES


def first_purchase_bypass_eligible(
    *,
    enabled: bool,
    min_amount: int | None,
    purchase_amount: int,
    is_first_purchase: bool,
    tier_key: str | None,
) -> bool:
    if not enabled or not is_first_purchase:
        return False
    if not (tier_key or "").strip():
        return False
    try:
        threshold = int(min_amount) if min_amount is not None else None
    except (TypeError, ValueError):
        return False
    if threshold is None or threshold < 1:
        return False
    return int(purchase_amount or 0) >= threshold


def record_purchase_from_transaction(db: Session, *, customer, transaction) -> PurchaseMovement | None:
    """Record one qualifying sale as +1 purchase and +amount. Idempotent per transaction."""
    if customer is None or transaction is None:
        return None
    if not is_purchase_transaction_type(getattr(transaction, "transaction_type", None)):
        return None
    status = str(getattr(transaction, "status", "") or "").upper()
    if not status.startswith("PROCESSED"):
        return None

    existing = (
        db.query(PurchaseMovement.id)
        .filter(PurchaseMovement.source_transaction_id == transaction.id)
        .filter(PurchaseMovement.type == "EARN")
        .first()
    )
    if existing:
        return None

    prior_earn = (
        db.query(func.count(PurchaseMovement.id))
        .filter(PurchaseMovement.customer_id == customer.id)
        .filter(PurchaseMovement.type == "EARN")
        .scalar()
    )
    is_first_purchase = int(prior_earn or 0) == 0

    payload = transaction.payload if isinstance(transaction.payload, dict) else {}
    amount = extract_purchase_amount(payload)
    settings = get_loyalty_settings(db, brand=customer.brand)
    validity_days = getattr(settings, "purchase_validity_days", None) if settings else None
    expires_at = (date.today() + timedelta(days=int(validity_days))) if validity_days is not None else None

    bypass_tier_key = None
    if settings and first_purchase_bypass_eligible(
        enabled=bool(getattr(settings, "first_purchase_bypass_enabled", False)),
        min_amount=getattr(settings, "first_purchase_bypass_min_amount", None),
        purchase_amount=amount,
        is_first_purchase=is_first_purchase,
        tier_key=getattr(settings, "first_purchase_bypass_tier_key", None),
    ):
        bypass_tier_key = (getattr(settings, "first_purchase_bypass_tier_key", None) or "").strip() or None

    movement = PurchaseMovement(
        customer_id=customer.id,
        quantity=1,
        amount=amount,
        type="EARN",
        source_transaction_id=transaction.id,
        expires_at=expires_at,
    )
    db.add(movement)
    db.flush()
    sync_customer_purchase_counters(db, customer)

    from app.services.loyalty_status_service import update_customer_status

    update_customer_status(
        db,
        customer,
        reason="PURCHASE_RECORDED",
        source_transaction_id=transaction.id,
        depth=0,
        refresh_window=False,
        emit_events=True,
        sync_unomi=False,
        min_tier_key=bypass_tier_key,
    )
    return movement


def apply_purchase_delta(
    db: Session,
    *,
    customer,
    quantity_delta: int,
    amount_delta: int,
    source_transaction_id,
    movement_type: str,
) -> PurchaseMovement | None:
    if quantity_delta == 0 and amount_delta == 0:
        return None
    settings = get_loyalty_settings(db, brand=customer.brand)
    validity_days = getattr(settings, "purchase_validity_days", None) if settings else None
    expires_at = None
    if movement_type == "EARN" and (quantity_delta > 0 or amount_delta > 0) and validity_days is not None:
        expires_at = date.today() + timedelta(days=int(validity_days))

    movement = PurchaseMovement(
        customer_id=customer.id,
        quantity=int(quantity_delta),
        amount=int(amount_delta),
        type=movement_type,
        source_transaction_id=source_transaction_id,
        expires_at=expires_at,
    )
    db.add(movement)
    db.flush()
    sync_customer_purchase_counters(db, customer)
    return movement


def serialize_purchase_movement_out(movement: PurchaseMovement, *, today: date | None = None) -> dict:
    today = today or date.today()
    expired = bool(
        movement.type != "EXPIRE"
        and movement.expires_at is not None
        and movement.expires_at < today
        and (int(movement.quantity or 0) > 0 or int(movement.amount or 0) > 0)
    )
    if movement.type == "EXPIRE" or expired:
        status = "expired"
    elif movement.expires_at is not None:
        status = "active_with_expiry"
    else:
        status = "active"
    return {
        "id": movement.id,
        "customer_id": movement.customer_id,
        "quantity": int(movement.quantity or 0),
        "amount": int(movement.amount or 0),
        "type": movement.type,
        "source_transaction_id": movement.source_transaction_id,
        "created_at": movement.created_at,
        "expires_at": movement.expires_at,
        "isExpired": expired or movement.type == "EXPIRE",
        "status": status,
    }
