from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.models.reward import Reward
from app.models.customer_reward import CustomerReward
from app.services.wallet_service import get_points_balance
from app.services.loyalty_service import burn_points


# ============================================================
# REDEEM REWARD (cas principal appelé par le rule engine)
# ============================================================
def redeem_reward(db: Session, customer, transaction):
    payload = transaction.payload or {}
    reward_id = payload.get("rewardId")

    if not reward_id:
        raise HTTPException(status_code=400, detail="rewardId is required")

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

        # burn propre via FakeTx pour ne pas polluer le payload original
        burn_points(
            db,
            customer,
            type(
                "FakeTx",
                (),
                {
                    "id": transaction.id,
                    "payload": {"points": reward.cost_points},
                },
            ),
        )

    # ============================================================
    # 2️⃣ Création du CustomerReward (traçabilité réelle)
    # ============================================================
    expires_at = None
    if reward.validity_days:
        expires_at = datetime.utcnow() + timedelta(days=reward.validity_days)

    customer_reward = CustomerReward(
        customer_id=customer.id,
        reward_id=reward.id,
        status="ISSUED",
        expires_at=expires_at,
        source_transaction_id=transaction.id,
    )

    db.add(customer_reward)
    db.flush()

    return customer_reward


def issue_reward(db: Session, customer, transaction):
    payload = transaction.payload or {}
    reward_id = payload.get("rewardId")

    if not reward_id:
        raise HTTPException(status_code=400, detail="rewardId is required")

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

    customer_reward = CustomerReward(
        customer_id=customer.id,
        reward_id=reward.id,
        status="ISSUED",
        expires_at=expires_at,
        source_transaction_id=transaction.id,
    )

    db.add(customer_reward)
    db.flush()

    return customer_reward


# ============================================================
# USE REWARD
# ============================================================
def use_reward(db: Session, customer_reward: CustomerReward):
    if customer_reward.status != "ISSUED":
        raise HTTPException(status_code=400, detail="Reward not usable")

    customer_reward.status = "USED"
    customer_reward.used_at = datetime.utcnow()

    db.flush()
    return customer_reward


# ============================================================
# EXPIRE REWARDS (job batch / cron)
# ============================================================
def expire_rewards(db: Session):
    now = datetime.utcnow()

    expired = (
        db.query(CustomerReward)
        .filter(CustomerReward.status == "ISSUED")
        .filter(CustomerReward.expires_at.isnot(None))
        .filter(CustomerReward.expires_at < now)
        .all()
    )

    for cr in expired:
        cr.status = "EXPIRED"

    db.flush()
    return len(expired)
