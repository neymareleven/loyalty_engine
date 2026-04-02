from datetime import datetime
import uuid
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException

from app.models.reward import Reward
from app.models.customer_reward import CustomerReward


def _coerce_uuid(value):
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


# ============================================================
# REDEEM REWARD (cas principal appelé par le rule engine)
# ============================================================
def redeem_reward(
    db: Session,
    customer,
    transaction,
    *,
    reward_id: str,
    rule_id: str | None = None,
    rule_execution_id: str | None = None,
    idempotency_key: str | None = None,
):
    raise HTTPException(
        status_code=400,
        detail="Reward redemption by points is removed. Use coupon issuance/usage instead.",
    )


def issue_reward(
    db: Session,
    customer,
    transaction,
    *,
    reward_id: str,
    customer_coupon_id: str | None = None,
    rule_id: str | None = None,
    rule_execution_id: str | None = None,
    idempotency_key: str | None = None,
    expires_at_override: datetime | None = None,
):
    if not reward_id:
        raise HTTPException(status_code=400, detail="reward_id is required")

    reward = (
        db.query(Reward)
        .filter(
            Reward.id == reward_id,
            Reward.brand == customer.brand,
            Reward.active.is_(True),
        )
        .first()
    )

    if not reward:
        raise HTTPException(status_code=404, detail="Reward not found")

    expires_at = expires_at_override

    if idempotency_key:
        existing = (
            db.query(CustomerReward)
            .filter(CustomerReward.idempotency_key == idempotency_key)
            .first()
        )
        if existing:
            db.flush()
            return existing

    payload = {
        "name": reward.name,
        "description": reward.description,
        "rewardCategoryId": str(reward.reward_category_id) if reward.reward_category_id is not None else None,
    }

    customer_reward = CustomerReward(
        customer_id=customer.id,
        reward_id=reward.id,
        customer_coupon_id=_coerce_uuid(customer_coupon_id),
        status="ISSUED",
        expires_at=expires_at,
        source_transaction_id=transaction.id,
        rule_id=_coerce_uuid(rule_id),
        rule_execution_id=_coerce_uuid(rule_execution_id),
        idempotency_key=idempotency_key,
        payload=payload,
    )

    try:
        with db.begin_nested():
            db.add(customer_reward)
            db.flush()
    except IntegrityError:
        if not idempotency_key:
            raise
        existing = (
            db.query(CustomerReward)
            .filter(CustomerReward.idempotency_key == idempotency_key)
            .first()
        )
        if existing:
            db.flush()
            return existing
        raise

    return customer_reward


# ============================================================
# USE REWARD
# ============================================================
def use_reward(db: Session, customer_reward: CustomerReward):
    if customer_reward.expires_at is not None and customer_reward.expires_at < datetime.utcnow():
        customer_reward.status = "EXPIRED"
        db.flush()
        raise HTTPException(status_code=400, detail="Reward not usable")

    if customer_reward.status != "ISSUED":
        raise HTTPException(status_code=400, detail="Reward not usable")

    customer_reward.status = "USED"
    customer_reward.used_at = datetime.utcnow()

    db.flush()
    return customer_reward


def reopen_reward(db: Session, customer_reward: CustomerReward):
    now = datetime.utcnow()
    if customer_reward.expires_at is not None and customer_reward.expires_at < now:
        customer_reward.status = "EXPIRED"
        db.flush()
        raise HTTPException(status_code=400, detail="Reward not usable")

    if customer_reward.status != "USED":
        raise HTTPException(status_code=400, detail="Reward not usable")

    customer_reward.status = "ISSUED"
    customer_reward.used_at = None

    db.flush()
    return customer_reward


# ============================================================
# EXPIRE REWARDS (job batch / cron)
# ============================================================
def expire_rewards(db: Session, *, brand: str):
    now = datetime.utcnow()

    expired = (
        db.query(CustomerReward)
        .join(Reward, Reward.id == CustomerReward.reward_id)
        .filter(Reward.brand == brand)
        .filter(CustomerReward.status == "ISSUED")
        .filter(CustomerReward.expires_at.isnot(None))
        .filter(CustomerReward.expires_at < now)
        .all()
    )

    for cr in expired:
        cr.status = "EXPIRED"

    db.flush()
    return len(expired)
