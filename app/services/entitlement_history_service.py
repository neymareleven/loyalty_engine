"""Unified entitlement history (per customer + global admin view)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.transaction import Transaction
from app.services.catalog_invalidation_service import ENTITLEMENT_HISTORY_TX_TYPES
from app.services.contact_service import customer_transaction_filters, get_customer


def _coupon_label(coupon: CustomerCoupon) -> str:
    payload = coupon.payload if isinstance(coupon.payload, dict) else {}
    snap = payload.get("couponTypeSnapshot") or payload.get("couponType")
    if isinstance(snap, dict) and snap.get("name"):
        return str(snap["name"])
    return f"Coupon {str(coupon.id)[:8]}"


def _reward_label(reward: CustomerReward) -> str:
    payload = reward.payload if isinstance(reward.payload, dict) else {}
    snap = payload.get("rewardSnapshot")
    if isinstance(snap, dict) and snap.get("name"):
        return str(snap["name"])
    return f"Avantage {str(reward.id)[:8]}"


def _tx_to_event(tx: Transaction) -> dict[str, Any]:
    payload = tx.payload if isinstance(tx.payload, dict) else {}
    return {
        "kind": "transaction",
        "id": str(tx.id),
        "occurred_at": tx.processed_at or tx.created_at,
        "event_type": tx.transaction_type,
        "source": tx.source,
        "profile_id": tx.profile_id,
        "brand": tx.brand,
        "summary": _summarize_transaction(tx.transaction_type, payload),
        "payload": payload,
    }


def _summarize_transaction(transaction_type: str, payload: dict) -> str:
    if transaction_type == "ADMIN_USE_COUPON":
        return "Coupon marqué comme utilisé (admin)"
    if transaction_type == "ADMIN_REOPEN_COUPON":
        return "Coupon rouvert (admin)"
    if transaction_type == "ADMIN_EXPIRE_COUPON":
        return "Coupon marqué comme expiré (admin)"
    if transaction_type == "CATALOG_COUPON_TYPE_DELETED":
        name = payload.get("entityName") or "type de coupon"
        inv = int(payload.get("coupons_invalidated") or 0)
        return f"Type de coupon « {name} » retiré du catalogue ({inv} coupon(s) invalidé(s))"
    if transaction_type == "CATALOG_REWARD_DELETED":
        name = payload.get("entityName") or "récompense"
        inv = int(payload.get("rewards_invalidated") or 0)
        return f"Récompense « {name} » retirée du catalogue ({inv} attribution(s) invalidée(s))"
    if transaction_type == "CATALOG_PRODUCT_DELETED":
        name = payload.get("entityName") or "produit"
        count = int(payload.get("productSnapshotsUpdated") or 0)
        return f"Produit « {name} » retiré du catalogue ({count} snapshot(s) mis à jour)"
    return transaction_type


def _coupon_issue_event(*, customer: Customer, coupon: CustomerCoupon) -> dict[str, Any]:
    return {
        "kind": "coupon_issued",
        "id": str(coupon.id),
        "occurred_at": coupon.issued_at or coupon.created_at,
        "event_type": "COUPON_ISSUED",
        "source": "LOYALTY_ENGINE",
        "profile_id": customer.profile_id,
        "brand": customer.brand,
        "summary": f"Coupon émis : {_coupon_label(coupon)}",
        "payload": {
            "customerCouponId": str(coupon.id),
            "status": coupon.status,
            "couponTypeId": str(coupon.coupon_type_id) if coupon.coupon_type_id else None,
        },
    }


def _reward_issue_event(*, customer: Customer, reward: CustomerReward) -> dict[str, Any]:
    return {
        "kind": "reward_issued",
        "id": str(reward.id),
        "occurred_at": reward.issued_at,
        "event_type": "REWARD_ISSUED",
        "source": "LOYALTY_ENGINE",
        "profile_id": customer.profile_id,
        "brand": customer.brand,
        "summary": f"Récompense émise : {_reward_label(reward)}",
        "payload": {
            "customerRewardId": str(reward.id),
            "customerCouponId": str(reward.customer_coupon_id) if reward.customer_coupon_id else None,
            "status": reward.status,
            "rewardId": str(reward.reward_id) if reward.reward_id else None,
        },
    }


def build_customer_entitlement_history(
    db: Session,
    *,
    brand: str,
    customer: Customer,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    tx_rows = (
        db.query(Transaction)
        .filter(Transaction.brand == brand)
        .filter(customer_transaction_filters(db, brand=brand, customer=customer))
        .filter(Transaction.transaction_type.in_(sorted(ENTITLEMENT_HISTORY_TX_TYPES)))
        .order_by(Transaction.created_at.desc())
        .all()
    )
    coupons = (
        db.query(CustomerCoupon)
        .filter(CustomerCoupon.customer_id == customer.id)
        .order_by(CustomerCoupon.issued_at.desc())
        .all()
    )
    rewards = (
        db.query(CustomerReward)
        .filter(CustomerReward.customer_id == customer.id)
        .order_by(CustomerReward.issued_at.desc())
        .all()
    )

    events: list[dict[str, Any]] = [_tx_to_event(tx) for tx in tx_rows]
    events.extend(_coupon_issue_event(customer=customer, coupon=c) for c in coupons)
    events.extend(_reward_issue_event(customer=customer, reward=r) for r in rewards)
    events.sort(key=lambda e: e.get("occurred_at") or "", reverse=True)

    total = len(events)
    page = events[offset : offset + limit]
    return {
        "brand": brand,
        "profileId": customer.profile_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": page,
    }


def build_global_entitlement_history(
    db: Session,
    *,
    brand: str,
    limit: int = 100,
    offset: int = 0,
    profile_id: str | None = None,
) -> dict[str, Any]:
    q = (
        db.query(Transaction)
        .filter(Transaction.brand == brand)
        .filter(Transaction.transaction_type.in_(sorted(ENTITLEMENT_HISTORY_TX_TYPES)))
    )
    if profile_id:
        q = q.filter(Transaction.profile_id == profile_id)

    total = q.count()
    rows = q.order_by(Transaction.created_at.desc()).offset(offset).limit(limit).all()
    return {
        "brand": brand,
        "profileId": profile_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [_tx_to_event(tx) for tx in rows],
    }
