from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.models.coupon_type import CouponType
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.services.coupon_rewards_service import resolve_rewards_catalog, resolve_rewards_to_issue
from app.services.reward_service import issue_reward


IssueCouponFrequency = Literal[
    "ALWAYS",
    "ONCE_PER_CALENDAR_YEAR",
    "ONCE_PER_CUSTOMER",
]


def _since_one_calendar_year(now: datetime) -> datetime:
    """Return a calendar-aware cutoff equivalent to (now - 1 year).

    This avoids approximations like 365 days and handles leap years.
    For leap day, we clamp to Feb 28 (e.g. 2024-02-29 -> 2023-02-28).
    """

    try:
        return now.replace(year=now.year - 1)
    except ValueError:
        # Likely Feb 29 on a leap year.
        return now.replace(year=now.year - 1, month=2, day=28)


def _coerce_uuid(value):
    # Keep same conventions as reward_service.
    import uuid

    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def _is_expired(*, expires_at) -> bool:
    if expires_at is None:
        return False
    try:
        return expires_at < datetime.utcnow()
    except Exception:
        return False


def issue_coupon(
    db: Session,
    *,
    customer,
    transaction,
    coupon_type_id: str,
    frequency: IssueCouponFrequency = "ONCE_PER_CALENDAR_YEAR",
    reward_ids: list[str] | None = None,
    rule_id: str | None = None,
    rule_execution_id: str | None = None,
    idempotency_key: str | None = None,
):
    if not coupon_type_id:
        raise HTTPException(status_code=400, detail="coupon_type_id is required")

    if frequency not in ("ALWAYS", "ONCE_PER_CALENDAR_YEAR", "ONCE_PER_CUSTOMER"):
        raise HTTPException(status_code=400, detail="Invalid frequency")

    ct = (
        db.query(CouponType)
        .filter(CouponType.id == coupon_type_id)
        .filter(CouponType.brand == customer.brand)
        .filter(CouponType.active.is_(True))
        .first()
    )
    if not ct:
        raise HTTPException(status_code=404, detail="Coupon type not found")

    now = datetime.utcnow()

    def _mark_issued_reward_ids(coupon_obj, ids: list[str]):
        coupon_obj._issued_reward_ids = ids  # type: ignore[attr-defined]

    if frequency == "ONCE_PER_CUSTOMER":
        existing = (
            db.query(CustomerCoupon)
            .filter(CustomerCoupon.customer_id == customer.id)
            .filter(CustomerCoupon.coupon_type_id == ct.id)
            .order_by(CustomerCoupon.created_at.asc())
            .first()
        )
        if existing:
            _mark_issued_reward_ids(existing, [])
            return existing

    if frequency == "ONCE_PER_CALENDAR_YEAR":
        since = _since_one_calendar_year(now)
        existing = (
            db.query(CustomerCoupon)
            .filter(CustomerCoupon.customer_id == customer.id)
            .filter(CustomerCoupon.coupon_type_id == ct.id)
            .filter(CustomerCoupon.issued_at >= since)
            .order_by(CustomerCoupon.issued_at.desc())
            .first()
        )
        if existing:
            _mark_issued_reward_ids(existing, [])
            return existing

    validity_days = getattr(ct, "validity_days", None)
    if validity_days is not None:
        try:
            validity_days = int(validity_days)
        except Exception:
            validity_days = None
    if validity_days is not None and validity_days < 0:
        validity_days = None
    expires_at = None
    if validity_days is not None:
        expires_at = now + timedelta(days=int(validity_days))

    coupon = CustomerCoupon(
        customer_id=customer.id,
        coupon_type_id=ct.id,
        status="ISSUED",
        expires_at=expires_at,
        source_transaction_id=transaction.id if transaction is not None else None,
        rule_id=_coerce_uuid(rule_id),
        rule_execution_id=_coerce_uuid(rule_execution_id),
        idempotency_key=idempotency_key,
        payload={
            "couponType": {
                "id": str(ct.id),
                "name": ct.name,
            },
            "couponTypeSnapshot": {
                "id": str(ct.id),
                "name": ct.name,
                "description": ct.description,
            },
        },
    )

    try:
        with db.begin_nested():
            db.add(coupon)
            db.flush()

            rewards = resolve_rewards_to_issue(
                db,
                coupon_type=ct,
                reward_ids_override=reward_ids,
            )
            issued_reward_ids: list[str] = []

            for r in rewards:
                # deterministically idempotent per coupon+reward.
                cr_idem = None
                if coupon.id and r.id:
                    cr_idem = f"coupon_issue:{coupon.id}:{r.id}"

                issue_reward(
                    db,
                    customer,
                    transaction,
                    reward_id=str(r.id),
                    customer_coupon_id=str(coupon.id),
                    rule_id=str(rule_id) if rule_id is not None else None,
                    rule_execution_id=str(rule_execution_id) if rule_execution_id is not None else None,
                    idempotency_key=cr_idem,
                    expires_at_override=coupon.expires_at,
                    coupon_type=ct,
                )
                issued_reward_ids.append(str(r.id))

            _mark_issued_reward_ids(coupon, issued_reward_ids)
            db.flush()

    except IntegrityError:
        # If caller uses idempotency_key, we can safely return existing.
        if idempotency_key:
            existing = db.query(CustomerCoupon).filter(CustomerCoupon.idempotency_key == idempotency_key).first()
            if existing:
                _mark_issued_reward_ids(existing, [])
                return existing

        if frequency == "ONCE_PER_CUSTOMER":
            existing = (
                db.query(CustomerCoupon)
                .filter(CustomerCoupon.customer_id == customer.id)
                .filter(CustomerCoupon.coupon_type_id == ct.id)
                .order_by(CustomerCoupon.created_at.asc())
                .first()
            )
            if existing:
                _mark_issued_reward_ids(existing, [])
                return existing

        # Concurrency safe fallback for rolling-year uniqueness.
        if frequency == "ONCE_PER_CALENDAR_YEAR":
            since = _since_one_calendar_year(now)
            existing = (
                db.query(CustomerCoupon)
                .filter(CustomerCoupon.customer_id == customer.id)
                .filter(CustomerCoupon.coupon_type_id == ct.id)
                .filter(CustomerCoupon.issued_at >= since)
                .order_by(CustomerCoupon.issued_at.desc())
                .first()
            )
            if existing:
                _mark_issued_reward_ids(existing, [])
                return existing
        raise

    _mark_issued_reward_ids(coupon, getattr(coupon, "_issued_reward_ids", []))
    return coupon


def use_coupon(
    db: Session,
    *,
    customer,
    transaction,
    coupon_type_id: str,
    rule_id: str | None = None,
    rule_execution_id: str | None = None,
):
    """Consume the oldest ISSUED customer coupon for a coupon type (rule engine / legacy)."""
    if not coupon_type_id:
        raise HTTPException(status_code=400, detail="coupon_type_id is required")

    ct = (
        db.query(CouponType)
        .filter(CouponType.id == coupon_type_id)
        .filter(CouponType.brand == customer.brand)
        .first()
    )
    if not ct:
        raise HTTPException(status_code=404, detail="Coupon type not found")

    now = datetime.utcnow()

    while True:
        coupon = (
            db.query(CustomerCoupon)
            .filter(CustomerCoupon.customer_id == customer.id)
            .filter(CustomerCoupon.coupon_type_id == ct.id)
            .filter(CustomerCoupon.status.in_(("ISSUED",)))
            .order_by(CustomerCoupon.issued_at.asc(), CustomerCoupon.created_at.asc())
            .first()
        )
        if not coupon:
            raise HTTPException(status_code=400, detail="No usable coupon")

        with db.begin_nested():
            locked = (
                db.query(CustomerCoupon)
                .filter(CustomerCoupon.id == coupon.id)
                .with_for_update()
                .one()
            )
            if locked.status != "ISSUED":
                db.flush()
                continue
            if locked.expires_at is not None and locked.expires_at < now:
                locked.status = "EXPIRED"
                db.flush()
                continue

            if rule_id is not None:
                locked.rule_id = _coerce_uuid(rule_id)
            if rule_execution_id is not None:
                locked.rule_execution_id = _coerce_uuid(rule_execution_id)
            if transaction is not None and transaction.id is not None:
                locked.source_transaction_id = transaction.id

            from app.services.customer_coupon_service import sync_rewards_on_coupon_status

            locked.status = "USED"
            locked.used_at = now
            sync_rewards_on_coupon_status(
                db,
                customer=customer,
                coupon=locked,
                to_status="USED",
                tx=transaction,
                now=now,
            )
            db.flush()
            return locked



def expire_coupons(db: Session, *, brand: str):
    now = datetime.utcnow()

    expiring = (
        db.query(CustomerCoupon)
        .join(CouponType, CouponType.id == CustomerCoupon.coupon_type_id)
        .filter(CouponType.brand == brand)
        .filter(CustomerCoupon.status.in_(("ISSUED",)))
        .filter(CustomerCoupon.expires_at.isnot(None))
        .filter(CustomerCoupon.expires_at < now)
        .all()
    )

    for cc in expiring:
        cc.status = "EXPIRED"

        rewards = (
            db.query(CustomerReward)
            .filter(CustomerReward.customer_coupon_id == cc.id)
            .filter(CustomerReward.status.in_(("ISSUED",)))
            .all()
        )
        for cr in rewards:
            cr.status = "EXPIRED"

    db.flush()
    return len(expiring)
