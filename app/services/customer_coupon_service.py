from __future__ import annotations

from datetime import datetime

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.transaction import Transaction
from app.services.catalog_invalidation_service import coupon_admin_allowed_transitions

ALLOWED_COUPON_STATUSES = frozenset({"ISSUED", "USED", "EXPIRED", "INVALIDATED"})

ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "ISSUED": frozenset({"USED", "EXPIRED"}),
    "USED": frozenset({"ISSUED", "EXPIRED"}),
    "EXPIRED": frozenset(),
    "INVALIDATED": frozenset(),
}

SKIP_REWARD_SYNC_STATUSES = frozenset({"INVALIDATED", "CANCELLED"})


def _utcnow() -> datetime:
    return datetime.utcnow()


def _assert_transition(*, from_status: str, to_status: str) -> None:
    if to_status not in {"ISSUED", "USED", "EXPIRED"}:
        raise HTTPException(status_code=400, detail=f"Invalid status: {to_status}")
    allowed = ALLOWED_TRANSITIONS.get(from_status, frozenset())
    if to_status not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot transition coupon from {from_status} to {to_status}",
        )


def _get_customer_coupon_for_update(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    customer_coupon_id: str,
) -> tuple[Customer, CustomerCoupon]:
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    coupon = (
        db.query(CustomerCoupon)
        .filter(CustomerCoupon.id == customer_coupon_id)
        .filter(CustomerCoupon.customer_id == customer.id)
        .with_for_update()
        .first()
    )
    if not coupon:
        raise HTTPException(status_code=404, detail="Customer coupon not found")
    return customer, coupon


def _admin_audit_transaction_id(*, customer_coupon_id: str, transaction_type: str, now: datetime) -> str:
    """event_id is varchar(100) — keep audit ids short."""
    coupon_key = str(customer_coupon_id).replace("-", "")[:12]
    type_key = "".join(ch for ch in transaction_type if ch.isalnum())[:8].lower()
    ts = now.strftime("%Y%m%d%H%M%S%f")[:20]
    return f"admcp_{type_key}_{coupon_key}_{ts}"[:100]


def _create_admin_audit_transaction(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    customer_coupon_id: str,
    transaction_type: str,
    from_status: str,
    to_status: str,
) -> Transaction:
    now = _utcnow()
    tx = Transaction(
        transaction_id=_admin_audit_transaction_id(
            customer_coupon_id=customer_coupon_id,
            transaction_type=transaction_type,
            now=now,
        ),
        brand=brand,
        profile_id=profile_id,
        transaction_type=transaction_type,
        source="ADMIN_UI",
        payload={
            "couponId": str(customer_coupon_id),
            "fromStatus": from_status,
            "toStatus": to_status,
        },
        status="PROCESSED",
        processed_at=now,
    )
    db.add(tx)
    db.flush()
    return tx


def sync_rewards_on_coupon_status(
    db: Session,
    *,
    customer: Customer,
    coupon: CustomerCoupon,
    to_status: str,
    tx: Transaction,
    now: datetime,
) -> None:
    rewards = (
        db.query(CustomerReward)
        .filter(CustomerReward.customer_id == customer.id)
        .filter(CustomerReward.customer_coupon_id == coupon.id)
        .all()
    )

    if to_status == "USED":
        for cr in rewards:
            if cr.status in SKIP_REWARD_SYNC_STATUSES:
                continue
            if cr.status != "ISSUED":
                continue
            if cr.expires_at is not None and cr.expires_at < now:
                cr.status = "EXPIRED"
            else:
                cr.status = "USED"
                cr.used_at = now
            cr.source_transaction_id = tx.id
        return

    if to_status == "ISSUED":
        for cr in rewards:
            if cr.status in SKIP_REWARD_SYNC_STATUSES:
                continue
            if cr.status != "USED":
                continue
            if cr.expires_at is not None and cr.expires_at < now:
                cr.status = "EXPIRED"
            else:
                cr.status = "ISSUED"
                cr.used_at = None
            cr.source_transaction_id = tx.id
        return

    if to_status == "EXPIRED":
        for cr in rewards:
            if cr.status in SKIP_REWARD_SYNC_STATUSES:
                continue
            if cr.status == "ISSUED":
                cr.status = "EXPIRED"
                cr.source_transaction_id = tx.id


def set_customer_coupon_status(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    customer_coupon_id: str,
    status: str,
) -> CustomerCoupon:
    """Admin lifecycle: ISSUED <-> USED, ISSUED/USED -> EXPIRED."""
    target = str(status or "").strip().upper()
    customer, coupon = _get_customer_coupon_for_update(
        db,
        brand=brand,
        profile_id=profile_id,
        customer_coupon_id=customer_coupon_id,
    )

    allowed = coupon_admin_allowed_transitions(coupon)
    if not allowed:
        raise HTTPException(
            status_code=409,
            detail=(
                "Ce coupon ne peut plus être modifié "
                "(invalidé, expiré ou modèle retiré du catalogue)."
            ),
        )
    if target not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Transition non autorisée depuis le statut {coupon.status}",
        )

    now = _utcnow()
    from_status = coupon.status

    if coupon.expires_at is not None and coupon.expires_at < now and from_status == "ISSUED":
        coupon.status = "EXPIRED"
        db.flush()
        raise HTTPException(status_code=400, detail="Coupon is expired")

    if from_status == target:
        return coupon

    _assert_transition(from_status=from_status, to_status=target)

    tx_type_by_target = {
        "USED": "ADMIN_USE_COUPON",
        "ISSUED": "ADMIN_REOPEN_COUPON",
        "EXPIRED": "ADMIN_EXPIRE_COUPON",
    }
    tx = _create_admin_audit_transaction(
        db,
        brand=brand,
        profile_id=profile_id,
        customer_coupon_id=customer_coupon_id,
        transaction_type=tx_type_by_target[target],
        from_status=from_status,
        to_status=target,
    )

    coupon.status = target
    coupon.source_transaction_id = tx.id
    if target == "USED":
        coupon.used_at = now
    elif target == "ISSUED":
        coupon.used_at = None

    sync_rewards_on_coupon_status(
        db,
        customer=customer,
        coupon=coupon,
        to_status=target,
        tx=tx,
        now=now,
    )
    db.flush()
    return coupon
