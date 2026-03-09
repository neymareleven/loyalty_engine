from datetime import datetime, timedelta
import uuid
import re
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException

from app.models.reward import Reward
from app.models.customer_reward import CustomerReward
from app.services.wallet_service import get_points_balance
from app.services.loyalty_service import burn_points, earn_points
from app.services.cash_wallet_service import credit_cash


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


def _extract_amount_and_currency(payload) -> tuple[int | None, str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None, None

    # Common external payload fields (CDP / ecommerce / PSP / POS)
    candidates = [
        "amount",
        "total",
        "orderTotal",
        "order_total",
        "orderAmount",
        "order_amount",
        "orderValue",
        "order_value",
        "grandTotal",
        "grand_total",
    ]

    for key in candidates:
        if key not in payload:
            continue
        raw = payload.get(key)
        if raw is None:
            continue

        if isinstance(raw, bool):
            continue

        if isinstance(raw, (int, float)):
            return int(raw), None, key

        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                continue

            # Currency: first 3-letter token found in the string (e.g. XAF, CFA, EUR)
            m_cur = re.search(r"\b([A-Za-z]{3})\b", s)
            cur = m_cur.group(1).upper() if m_cur else None

            # Amount: keep digits only (handles formats like "1,235CFA", "1 235 CFA")
            digits = re.sub(r"[^0-9]", "", s)
            if not digits:
                continue
            try:
                amt = int(digits)
            except Exception:
                continue
            return amt, cur, key

    cur = payload.get("currency") or payload.get("Currency") or payload.get("devise")
    if isinstance(cur, str) and len(cur.strip()) == 3:
        return None, cur.strip().upper(), "currency"

    return None, None, None


def _apply_reward_effect(db: Session, *, customer, transaction, reward: Reward, customer_reward: CustomerReward):
    reward_type = (reward.type or "").strip().upper()
    payload = customer_reward.payload
    if not isinstance(payload, dict):
        payload = {}

    if reward_type == "POINTS":
        if reward.value_amount is None:
            raise HTTPException(status_code=400, detail="POINTS reward requires value_amount")
        pts = int(reward.value_amount)
        if pts <= 0:
            raise HTTPException(status_code=400, detail="POINTS reward value_amount must be > 0")
        earn_points(db, customer, points=pts, source_transaction_id=transaction.id, depth=0)
        payload["applied"] = {"kind": "POINTS", "points": pts}
        customer_reward.payload = payload
        return

    if reward_type == "CASHBACK":
        amount: int | None = None
        currency: str | None = (reward.currency or None)

        if reward.value_percent is not None:
            base_amount, base_currency, base_key = _extract_amount_and_currency(transaction.payload or {})
            if base_amount is None:
                raise HTTPException(
                    status_code=400,
                    detail="CASHBACK percent requires a transaction amount in payload (e.g. orderTotal/total/amount)",
                )
            percent = int(reward.value_percent)
            amount = int((base_amount * percent) / 100)
            currency = base_currency or currency
            payload["computedFrom"] = {"field": base_key, "baseAmount": base_amount, "percent": percent}
        else:
            if reward.value_amount is None:
                raise HTTPException(status_code=400, detail="CASHBACK requires value_amount or value_percent")
            amount = int(reward.value_amount)

        if amount is None or amount <= 0:
            raise HTTPException(status_code=400, detail="CASHBACK computed amount must be > 0")

        if not currency or len(str(currency).strip()) != 3:
            raise HTTPException(status_code=400, detail="CASHBACK requires a 3-letter currency (in reward or payload)")

        credit_cash(
            db,
            customer.id,
            amount=amount,
            currency=str(currency).strip().upper(),
            source_transaction_id=transaction.id,
        )
        payload["applied"] = {"kind": "CASHBACK", "amount": amount, "currency": str(currency).strip().upper()}
        customer_reward.payload = payload
        return


def _reward_limits_window_start(*, now: datetime, reset_period: str) -> datetime | None:
    rp = (reset_period or "").strip().upper()
    if rp == "LIFETIME":
        return None
    if rp == "DAY":
        return datetime(now.year, now.month, now.day)
    if rp == "MONTH":
        return datetime(now.year, now.month, 1)
    if rp == "YEAR":
        return datetime(now.year, 1, 1)
    return None


def _enforce_reward_limits(db: Session, *, customer_id, reward: Reward):
    max_attr = getattr(reward, "max_attributions", None)
    reset_period = getattr(reward, "reset_period", None)

    if max_attr is None:
        return
    try:
        max_attr = int(max_attr)
    except Exception:
        return
    if max_attr <= 0:
        return

    if not reset_period:
        return

    now = datetime.utcnow()
    start = _reward_limits_window_start(now=now, reset_period=str(reset_period))

    q = (
        db.query(func.count(CustomerReward.id))
        .filter(CustomerReward.customer_id == customer_id)
        .filter(CustomerReward.reward_id == reward.id)
        .filter(CustomerReward.status != "CANCELLED")
    )
    if start is not None:
        q = q.filter(CustomerReward.issued_at >= start)

    used = int(q.scalar() or 0)
    if used >= max_attr:
        raise HTTPException(status_code=400, detail="Reward usage limit reached")


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

    expires_at = None
    if reward.validity_days:
        expires_at = datetime.utcnow() + timedelta(days=reward.validity_days)

    # Idempotency must be checked BEFORE any side-effect (limits, burn, credit).
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

    # ============================================================
    # 0️⃣ Usage limits
    # ============================================================
    _enforce_reward_limits(db, customer_id=customer.id, reward=reward)

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
            _apply_reward_effect(db, customer=customer, transaction=transaction, reward=reward, customer_reward=customer_reward)
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

    # ============================================================
    # 0️⃣ Usage limits
    # ============================================================
    _enforce_reward_limits(db, customer_id=customer.id, reward=reward)

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
            _apply_reward_effect(db, customer=customer, transaction=transaction, reward=reward, customer_reward=customer_reward)
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
