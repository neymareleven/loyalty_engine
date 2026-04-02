from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.models.brand_loyalty_settings import BrandLoyaltySettings
from app.models.coupon_type import CouponType
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.reward import Reward
from app.models.reward_category import RewardCategory
from app.services.reward_service import issue_reward


IssueCouponFrequency = Literal[
    "ALWAYS",
    "ONCE_PER_CALENDAR_YEAR",
]


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


def _get_coupon_validity_days(db: Session, *, brand: str) -> int | None:
    s = db.query(BrandLoyaltySettings).filter(BrandLoyaltySettings.brand == brand).first()
    if not s:
        return None
    v = getattr(s, "coupon_validity_days", None)
    if v is None:
        return None
    try:
        v = int(v)
    except Exception:
        return None
    if v < 0:
        return None
    if v == 0:
        return 0
    return v


def _calendar_year(now: datetime) -> int:
    return int(now.year)


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
    rule_id: str | None = None,
    rule_execution_id: str | None = None,
    idempotency_key: str | None = None,
):
    if not coupon_type_id:
        raise HTTPException(status_code=400, detail="coupon_type_id is required")

    if frequency not in ("ALWAYS", "ONCE_PER_CALENDAR_YEAR"):
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

    rc = (
        db.query(RewardCategory)
        .filter(RewardCategory.coupon_type_id == ct.id)
        .filter(RewardCategory.brand == customer.brand)
        .first()
    )
    if not rc:
        raise HTTPException(status_code=400, detail="Reward category not linked to coupon type")

    now = datetime.utcnow()
    year = _calendar_year(now)

    if frequency == "ONCE_PER_CALENDAR_YEAR":
        existing = (
            db.query(CustomerCoupon)
            .filter(CustomerCoupon.customer_id == customer.id)
            .filter(CustomerCoupon.coupon_type_id == ct.id)
            .filter(CustomerCoupon.calendar_year == year)
            .first()
        )
        if existing:
            return existing

    validity_days = _get_coupon_validity_days(db, brand=customer.brand)
    expires_at = None
    if validity_days is not None:
        expires_at = now + timedelta(days=int(validity_days))

    coupon = CustomerCoupon(
        customer_id=customer.id,
        coupon_type_id=ct.id,
        calendar_year=year,
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
            "rewardCategoryId": str(rc.id),
        },
    )

    try:
        with db.begin_nested():
            db.add(coupon)
            db.flush()

            # Snapshot: all active rewards in category at issue time.
            rewards = (
                db.query(Reward)
                .filter(Reward.brand == customer.brand)
                .filter(Reward.active.is_(True))
                .filter(Reward.reward_category_id == rc.id)
                .order_by(Reward.created_at.asc())
                .all()
            )

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
                )

            db.flush()

    except IntegrityError:
        # If caller uses idempotency_key, we can safely return existing.
        if idempotency_key:
            existing = db.query(CustomerCoupon).filter(CustomerCoupon.idempotency_key == idempotency_key).first()
            if existing:
                return existing

        # Concurrency safe fallback for yearly uniqueness.
        if frequency == "ONCE_PER_CALENDAR_YEAR":
            existing = (
                db.query(CustomerCoupon)
                .filter(CustomerCoupon.customer_id == customer.id)
                .filter(CustomerCoupon.coupon_type_id == ct.id)
                .filter(CustomerCoupon.calendar_year == year)
                .first()
            )
            if existing:
                return existing
        raise

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

    # Find the first actually usable coupon. If the oldest ISSUED one is already expired,
    # expire it and retry (so we can consume a newer one if it exists).
    while True:
        coupon = (
            db.query(CustomerCoupon)
            .filter(CustomerCoupon.customer_id == customer.id)
            .filter(CustomerCoupon.coupon_type_id == ct.id)
            .filter(CustomerCoupon.status == "ISSUED")
            .order_by(CustomerCoupon.issued_at.asc(), CustomerCoupon.created_at.asc())
            .first()
        )
        if not coupon:
            raise HTTPException(status_code=400, detail="No usable coupon")

        with db.begin_nested():
            locked = db.query(CustomerCoupon).filter(CustomerCoupon.id == coupon.id).with_for_update().one()
            if locked.status != "ISSUED":
                # Concurrency: another worker consumed it. Retry selection.
                db.flush()
                continue
            if locked.expires_at is not None and locked.expires_at < now:
                locked.status = "EXPIRED"
                db.flush()
                continue

            locked.status = "USED"
            locked.used_at = now
            locked.source_transaction_id = transaction.id if transaction is not None else locked.source_transaction_id
            locked.rule_id = _coerce_uuid(rule_id) if rule_id is not None else locked.rule_id
            locked.rule_execution_id = _coerce_uuid(rule_execution_id) if rule_execution_id is not None else locked.rule_execution_id

            rewards = (
                db.query(CustomerReward)
                .filter(CustomerReward.customer_coupon_id == locked.id)
                .filter(CustomerReward.status == "ISSUED")
                .all()
            )
            for cr in rewards:
                if cr.expires_at is not None and cr.expires_at < now:
                    cr.status = "EXPIRED"
                else:
                    cr.status = "USED"
                    cr.used_at = now

            db.flush()

            return locked



def expire_coupons(db: Session, *, brand: str):
    now = datetime.utcnow()

    expiring = (
        db.query(CustomerCoupon)
        .join(CouponType, CouponType.id == CustomerCoupon.coupon_type_id)
        .filter(CouponType.brand == brand)
        .filter(CustomerCoupon.status == "ISSUED")
        .filter(CustomerCoupon.expires_at.isnot(None))
        .filter(CustomerCoupon.expires_at < now)
        .all()
    )

    for cc in expiring:
        cc.status = "EXPIRED"

        rewards = (
            db.query(CustomerReward)
            .filter(CustomerReward.customer_coupon_id == cc.id)
            .filter(CustomerReward.status == "ISSUED")
            .all()
        )
        for cr in rewards:
            cr.status = "EXPIRED"

    db.flush()
    return len(expiring)
