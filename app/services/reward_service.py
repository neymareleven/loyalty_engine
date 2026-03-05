from datetime import datetime, timedelta
import uuid
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException

from app.models.reward import Reward
from app.models.customer_reward import CustomerReward
from app.services.wallet_service import get_points_balance
from app.services.loyalty_service import burn_points


def _coerce_uuid(value):
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def _voucher_code_from_customer_reward_id(customer_reward_id) -> str:
    # Deterministic, unique (UUID-based) voucher code.
    # Example: V-1A2B3C4D5E6F
    hx = getattr(customer_reward_id, "hex", None)
    if callable(hx):
        hx = customer_reward_id.hex
    if not isinstance(hx, str):
        hx = str(customer_reward_id).replace("-", "")
    hx = (hx or "").upper()
    return f"V-{hx[:12]}"


def _ensure_voucher_code(customer_reward: CustomerReward):
    payload = customer_reward.payload
    if not isinstance(payload, dict):
        payload = {}

    if payload.get("type") != "VOUCHER":
        return

    if payload.get("voucher_code"):
        return

    payload["voucher_code"] = _voucher_code_from_customer_reward_id(customer_reward.id)
    customer_reward.payload = payload


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
    if not reward_id:
        raise HTTPException(status_code=400, detail="reward_id is required")

    # 🔎 récupération reward active pour la bonne brand
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

    # ============================================================
    # 1️⃣ Si reward payante → vérifier solde + burn
    # ============================================================
    if reward.cost_points and reward.cost_points > 0:
        balance = get_points_balance(db, customer.id)

        if balance < reward.cost_points:
            raise HTTPException(status_code=400, detail="Not enough points")

        burn_points(
            db,
            customer,
            points=reward.cost_points,
            source_transaction_id=transaction.id,
        )

    # ============================================================
    # 2️⃣ Création du CustomerReward (traçabilité réelle)
    # ============================================================
    expires_at = None
    if reward.validity_days:
        expires_at = datetime.utcnow() + timedelta(days=reward.validity_days)

    if idempotency_key:
        existing = (
            db.query(CustomerReward)
            .filter(CustomerReward.idempotency_key == idempotency_key)
            .first()
        )
        if existing:
            _ensure_voucher_code(existing)
            db.flush()
            return existing

    reward_type = (reward.type or "").upper()
    payload = {
        "type": reward_type,
        "currency": reward.currency,
        "value_amount": reward.value_amount,
        "value_percent": reward.value_percent,
        "params": reward.params,
    }

    customer_reward = CustomerReward(
        customer_id=customer.id,
        reward_id=reward.id,
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
            _ensure_voucher_code(customer_reward)
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
            _ensure_voucher_code(existing)
            db.flush()
            return existing
        raise

    return customer_reward


def issue_reward(
    db: Session,
    customer,
    transaction,
    *,
    reward_id: str,
    rule_id: str | None = None,
    rule_execution_id: str | None = None,
    idempotency_key: str | None = None,
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

    expires_at = None
    if reward.validity_days:
        expires_at = datetime.utcnow() + timedelta(days=reward.validity_days)

    if idempotency_key:
        existing = (
            db.query(CustomerReward)
            .filter(CustomerReward.idempotency_key == idempotency_key)
            .first()
        )
        if existing:
            _ensure_voucher_code(existing)
            db.flush()
            return existing

    reward_type = (reward.type or "").upper()
    payload = {
        "type": reward_type,
        "currency": reward.currency,
        "value_amount": reward.value_amount,
        "value_percent": reward.value_percent,
        "params": reward.params,
    }

    customer_reward = CustomerReward(
        customer_id=customer.id,
        reward_id=reward.id,
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
            _ensure_voucher_code(customer_reward)
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
            _ensure_voucher_code(existing)
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
