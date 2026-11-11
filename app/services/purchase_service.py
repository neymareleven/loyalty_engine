"""Purchase-count + purchase-amount qualification (mirror of points, with expiry per sale)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.models.purchase_movement import PurchaseMovement
from app.models.transaction import Transaction
from app.services.loyalty_settings_service import get_loyalty_settings

QUALIFICATION_POINTS = "POINTS"
QUALIFICATION_PURCHASES = "PURCHASES"

_PURCHASE_TRANSACTION_TYPES = frozenset({"sale"})
_AMOUNT_FIELD_CANDIDATES = (
    "orderTotal",
    "total",
    "order_total",
    "subtotal",
    "amount",
    "orderAmount",
    "grandTotal",
    "ttc",
    "paidAmount",
    "value",
)
_NESTED_PAYLOAD_KEYS = ("properties", "attributes", "order", "data", "payload")


def normalize_qualification_mode(value: Any) -> str:
    mode = str(value or QUALIFICATION_POINTS).strip().upper()
    if mode in {QUALIFICATION_PURCHASES, "PURCHASE", "SALES", "ORDERS"}:
        return QUALIFICATION_PURCHASES
    return QUALIFICATION_POINTS


def get_qualification_mode(db: Session, *, brand: str) -> str:
    settings = get_loyalty_settings(db, brand=brand)
    return normalize_qualification_mode(getattr(settings, "qualification_mode", None) if settings else None)


def _parse_amount_value(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return max(0, raw)
    if isinstance(raw, float):
        return max(0, int(raw))
    if isinstance(raw, str):
        from app.services.rule_engine import _as_number_from_text

        parsed = _as_number_from_text(raw)
        if parsed is not None:
            return max(0, int(parsed))
    return None


def extract_purchase_amount(payload: dict | None) -> int:
    if not isinstance(payload, dict):
        return 0
    from app.services.sale_payload_service import normalize_sale_payload

    payload = normalize_sale_payload(payload) or payload
    bags: list[dict] = [payload]
    for nest in _NESTED_PAYLOAD_KEYS:
        inner = payload.get(nest)
        if isinstance(inner, dict):
            bags.append(inner)
    for bag in bags:
        for key in _AMOUNT_FIELD_CANDIDATES:
            parsed = _parse_amount_value(bag.get(key))
            if parsed is not None:
                return parsed
        for key, raw in bag.items():
            kl = str(key or "").strip().lower().replace("-", "_")
            if kl.endswith("raw"):
                continue
            if not (kl.endswith("total") or kl.endswith("amount")):
                continue
            parsed = _parse_amount_value(raw)
            if parsed is not None:
                return parsed
    return 0


def transaction_occurred_at(transaction) -> datetime:
    for attr in ("processed_at", "created_at"):
        value = getattr(transaction, attr, None)
        if isinstance(value, datetime):
            return value.replace(tzinfo=None) if value.tzinfo else value
    return datetime.utcnow()


def purchase_expires_on(occurred_at: datetime | date | None, validity_days: int | None) -> date | None:
    if validity_days is None:
        return None
    try:
        days = int(validity_days)
    except (TypeError, ValueError):
        return None
    if isinstance(occurred_at, datetime):
        start = occurred_at.date()
    elif isinstance(occurred_at, date):
        start = occurred_at
    else:
        start = date.today()
    return start + timedelta(days=days)


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


def _processed_sale_query(db: Session, customer):
    """Sales shown on the customer file (profile + aliases + email), PROCESSED*."""
    from app.services.contact_service import customer_transaction_filters

    return (
        db.query(Transaction)
        .filter(Transaction.brand == customer.brand)
        .filter(customer_transaction_filters(db, brand=customer.brand, customer=customer))
        .filter(func.lower(Transaction.transaction_type) == "sale")
        .filter(func.upper(Transaction.status).like("PROCESSED%"))
    )


def _customer_for_sale_transaction(db: Session, tx) -> Any:
    from app.services.contact_service import _extract_email_from_payload, get_customer

    brand = getattr(tx, "brand", None)
    customer = get_customer(db, brand, getattr(tx, "profile_id", None) or "")
    if customer:
        return customer
    payload = tx.payload if isinstance(tx.payload, dict) else {}
    email = _extract_email_from_payload(payload, brand=brand)
    if not email:
        return None
    from app.models.customer import Customer

    return (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(func.lower(Customer.email) == email)
        .first()
    )


def customer_has_prior_processed_sale(
    db: Session,
    customer,
    *,
    excluding_transaction_id=None,
) -> bool:
    """True if the customer already has another PROCESSED sale (any profile alias / email)."""
    q = _processed_sale_query(db, customer)
    if excluding_transaction_id is not None:
        q = q.filter(Transaction.id != excluding_transaction_id)
    return q.first() is not None


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
    is_first_purchase = int(prior_earn or 0) == 0 and not customer_has_prior_processed_sale(
        db, customer, excluding_transaction_id=getattr(transaction, "id", None)
    )

    payload = transaction.payload if isinstance(transaction.payload, dict) else {}
    amount = extract_purchase_amount(payload)
    settings = get_loyalty_settings(db, brand=customer.brand)
    validity_days = getattr(settings, "purchase_validity_days", None) if settings else None
    occurred_at = transaction_occurred_at(transaction)
    expires_at = purchase_expires_on(occurred_at, validity_days)

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
        created_at=occurred_at,
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
        refresh_window=bool(bypass_tier_key),
        emit_events=True,
        sync_unomi=False,
        min_tier_key=bypass_tier_key,
        allow_downgrade_before_expiry=bool(bypass_tier_key),
    )
    return movement


def backfill_purchase_movements_from_sales(
    db: Session,
    *,
    brand: str,
    after_id: UUID | None = None,
    batch_size: int = 500,
) -> dict:
    """Create missing EARN purchase lots from historical PROCESSED sale transactions.

    Idempotent. Does not apply first-purchase bypass and does not emit palier events.
    Expiry is computed from the sale date + purchase_validity_days.
    """
    batch_size = max(1, min(int(batch_size or 500), 5000))
    settings = get_loyalty_settings(db, brand=brand)
    validity_days = getattr(settings, "purchase_validity_days", None) if settings else None

    q = (
        db.query(Transaction)
        .outerjoin(
            PurchaseMovement,
            and_(
                PurchaseMovement.source_transaction_id == Transaction.id,
                PurchaseMovement.type == "EARN",
            ),
        )
        .filter(Transaction.brand == brand)
        .filter(func.lower(Transaction.transaction_type) == "sale")
        .filter(func.upper(Transaction.status).like("PROCESSED%"))
        .filter(PurchaseMovement.id.is_(None))
        .order_by(Transaction.id.asc())
    )
    if after_id:
        q = q.filter(Transaction.id > after_id)

    sales = q.limit(batch_size).all()
    created = 0
    skipped = 0
    touched_ids: set = set()
    last_id = None
    customer_cache: dict[str, Any] = {}

    for tx in sales:
        last_id = tx.id
        cache_key = str(tx.profile_id or "")
        customer = customer_cache.get(cache_key) if cache_key in customer_cache else None
        if cache_key not in customer_cache:
            customer = _customer_for_sale_transaction(db, tx)
            customer_cache[cache_key] = customer
        if customer is None:
            skipped += 1
            continue

        payload = tx.payload if isinstance(tx.payload, dict) else {}
        amount = extract_purchase_amount(payload)
        occurred_at = transaction_occurred_at(tx)
        movement = PurchaseMovement(
            customer_id=customer.id,
            quantity=1,
            amount=amount,
            type="EARN",
            source_transaction_id=tx.id,
            created_at=occurred_at,
            expires_at=purchase_expires_on(occurred_at, validity_days),
        )
        db.add(movement)
        created += 1
        touched_ids.add(customer.id)

    if touched_ids:
        db.flush()
        from app.models.customer import Customer

        customers = db.query(Customer).filter(Customer.id.in_(list(touched_ids))).all()
        for customer in customers:
            sync_customer_purchase_counters(db, customer)
        db.flush()

    finished = len(sales) < batch_size
    return {
        "scanned": len(sales),
        "created": created,
        "skipped": skipped,
        "finished": finished,
        "after_id": last_id,
    }


def backfill_customer_purchase_movements_from_sales(db: Session, customer) -> int:
    """Backfill missing sale lots for one customer (same scope as the purchase history UI)."""
    settings = get_loyalty_settings(db, brand=customer.brand)
    validity_days = getattr(settings, "purchase_validity_days", None) if settings else None

    sales = (
        _processed_sale_query(db, customer)
        .outerjoin(
            PurchaseMovement,
            and_(
                PurchaseMovement.source_transaction_id == Transaction.id,
                PurchaseMovement.type == "EARN",
            ),
        )
        .filter(PurchaseMovement.id.is_(None))
        .all()
    )
    created = 0
    for tx in sales:
        payload = tx.payload if isinstance(tx.payload, dict) else {}
        occurred_at = transaction_occurred_at(tx)
        db.add(
            PurchaseMovement(
                customer_id=customer.id,
                quantity=1,
                amount=extract_purchase_amount(payload),
                type="EARN",
                source_transaction_id=tx.id,
                created_at=occurred_at,
                expires_at=purchase_expires_on(occurred_at, validity_days),
            )
        )
        created += 1
    if created:
        db.flush()
        sync_customer_purchase_counters(db, customer)
    else:
        sync_customer_purchase_counters(db, customer)
    return created


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
        expires_at = purchase_expires_on(datetime.utcnow(), validity_days)

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
