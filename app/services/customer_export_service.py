"""Export loyalty customer data before anonymize/erase (admin)."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.cash_movement import CashMovement
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_loyalty_guarantee import CustomerLoyaltyGuarantee
from app.models.customer_reward import CustomerReward
from app.models.point_movement import PointMovement
from app.models.purchase_movement import PurchaseMovement
from app.models.redemption import Redemption
from app.models.transaction import Transaction
from app.services.contact_service import get_customer
from app.services.customer_identity_service import customer_is_anonymized
from app.services.customer_serialization import serialize_customer_out
from app.services.loyalty_history_service import serialize_loyalty_history_item


def export_customer_data(db: Session, *, brand: str, profile_id: str) -> dict | None:
    customer = get_customer(db, brand, profile_id)
    if not customer or customer_is_anonymized(customer):
        return None

    txs = (
        db.query(Transaction)
        .filter(Transaction.customer_id == customer.id)
        .order_by(Transaction.created_at.desc())
        .limit(2000)
        .all()
    )
    points = (
        db.query(PointMovement)
        .filter(PointMovement.customer_id == customer.id)
        .order_by(PointMovement.created_at.desc())
        .limit(2000)
        .all()
    )
    purchases = (
        db.query(PurchaseMovement)
        .filter(PurchaseMovement.customer_id == customer.id)
        .order_by(PurchaseMovement.created_at.desc())
        .limit(2000)
        .all()
    )
    cash = (
        db.query(CashMovement)
        .filter(CashMovement.customer_id == customer.id)
        .order_by(CashMovement.created_at.desc())
        .limit(2000)
        .all()
    )
    coupons = db.query(CustomerCoupon).filter(CustomerCoupon.customer_id == customer.id).all()
    rewards = db.query(CustomerReward).filter(CustomerReward.customer_id == customer.id).all()
    redemptions = db.query(Redemption).filter(Redemption.customer_id == customer.id).all()
    guarantees = (
        db.query(CustomerLoyaltyGuarantee)
        .filter(CustomerLoyaltyGuarantee.customer_id == customer.id)
        .all()
    )

    history_txs = (
        db.query(Transaction)
        .filter(Transaction.customer_id == customer.id)
        .filter(
            Transaction.transaction_type.in_(
                [
                    "TIER_CHANGED",
                    "FIRST_PURCHASE_BYPASS",
                    "ADMIN_SET_TIER",
                    "LOYALTY_STATUS_EXPIRED",
                    "CUSTOMER_REGISTRATION",
                ]
            )
        )
        .order_by(Transaction.created_at.desc())
        .limit(500)
        .all()
    )

    return {
        "brand": brand,
        "profileId": customer.profile_id,
        "customer": serialize_customer_out(db, customer=customer, brand=brand),
        "transactions": [
            {
                "id": str(t.id),
                "transactionType": t.transaction_type,
                "transactionId": t.transaction_id,
                "status": t.status,
                "payload": t.payload,
                "createdAt": t.created_at.isoformat() if t.created_at else None,
            }
            for t in txs
        ],
        "pointMovements": [
            {
                "id": str(m.id),
                "points": m.points,
                "type": m.type,
                "createdAt": m.created_at.isoformat() if m.created_at else None,
            }
            for m in points
        ],
        "purchaseMovements": [
            {
                "id": str(m.id),
                "quantity": m.quantity,
                "amount": m.amount,
                "type": m.type,
                "createdAt": m.created_at.isoformat() if m.created_at else None,
            }
            for m in purchases
        ],
        "cashMovements": [
            {
                "id": str(m.id),
                "amount": m.amount,
                "type": m.type,
                "createdAt": m.created_at.isoformat() if getattr(m, "created_at", None) else None,
            }
            for m in cash
        ],
        "coupons": [
            {
                "id": str(c.id),
                "code": c.code,
                "status": c.status,
                "issuedAt": c.issued_at.isoformat() if c.issued_at else None,
                "expiresAt": c.expires_at.isoformat() if c.expires_at else None,
            }
            for c in coupons
        ],
        "rewards": [
            {
                "id": str(r.id),
                "status": r.status,
                "rewardId": str(r.reward_id) if r.reward_id else None,
            }
            for r in rewards
        ],
        "redemptions": [
            {
                "id": str(r.id),
                "status": r.status,
                "code": getattr(r, "code", None),
            }
            for r in redemptions
        ],
        "guarantees": [
            {
                "id": str(g.id),
                "source": g.source,
                "floorTierKey": g.floor_tier_key,
                "expiresAt": g.expires_at.isoformat() if g.expires_at else None,
                "revokedAt": g.revoked_at.isoformat() if g.revoked_at else None,
            }
            for g in guarantees
        ],
        "loyaltyHistory": [
            serialize_loyalty_history_item(db, t, brand=brand) for t in history_txs
        ],
    }
