"""Admin helpers: deletion policy metadata and catalog snapshots."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from app.models.coupon_type import CouponType
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.reward import Reward


def coupon_type_customer_coupon_count(db: Session, *, coupon_type_id) -> int:
    return (
        db.query(CustomerCoupon)
        .filter(CustomerCoupon.coupon_type_id == coupon_type_id)
        .count()
    )


def coupon_type_deletion_meta(db: Session, *, coupon_type_id) -> dict:
    count = coupon_type_customer_coupon_count(db, coupon_type_id=coupon_type_id)
    can_delete = count == 0
    return {
        "customer_coupon_count": count,
        "can_delete": can_delete,
        "recommended_action": None if can_delete else "deactivate",
    }


def reward_blocking_customer_reward_count(db: Session, *, reward_id) -> int:
    return (
        db.query(CustomerReward)
        .filter(CustomerReward.reward_id == reward_id)
        .filter(CustomerReward.status.in_(("USED", "EXPIRED")))
        .count()
    )


def build_customer_reward_snapshot_payload(
    *,
    reward: Reward,
    coupon_type: CouponType | None = None,
) -> dict:
    payload: dict = {
        "rewardSnapshot": {
            "id": str(reward.id),
            "name": reward.name,
            "description": reward.description,
        },
        # Legacy keys kept for existing consumers
        "name": reward.name,
        "description": reward.description,
        "rewardId": str(reward.id),
    }
    if coupon_type is not None:
        payload["couponTypeSnapshot"] = {
            "id": str(coupon_type.id),
            "name": coupon_type.name,
            "description": coupon_type.description,
        }
    return payload


def _payload_dict(raw: Any) -> dict:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return {}


def _snapshot_already_complete(payload: dict) -> bool:
    snap = payload.get("rewardSnapshot")
    return isinstance(snap, dict) and bool(snap.get("name"))


def _reward_snapshot_from_legacy(payload: dict) -> dict | None:
    legacy = payload.get("rewardSnapshot")
    if isinstance(legacy, dict) and legacy.get("name"):
        return legacy

    name = payload.get("name")
    if not name:
        return None

    rid = payload.get("rewardId") or payload.get("reward_id")
    return {
        "id": str(rid) if rid else None,
        "name": name,
        "description": payload.get("description"),
    }


def merge_customer_reward_snapshot_payload(
    existing: Any,
    *,
    reward: Reward | None = None,
    coupon_type: CouponType | None = None,
    legacy_payload: dict | None = None,
) -> dict:
    """Merge catalog snapshots into an existing customer reward payload."""
    base = _payload_dict(existing)
    legacy = legacy_payload or base

    if reward is not None:
        snap = build_customer_reward_snapshot_payload(reward=reward, coupon_type=coupon_type)
    else:
        reward_snap = _reward_snapshot_from_legacy(legacy)
        if not reward_snap:
            return base
        snap = {
            "rewardSnapshot": reward_snap,
            "name": reward_snap.get("name"),
            "description": reward_snap.get("description"),
            "rewardId": reward_snap.get("id"),
        }
        if coupon_type is not None:
            snap["couponTypeSnapshot"] = {
                "id": str(coupon_type.id),
                "name": coupon_type.name,
                "description": coupon_type.description,
            }
        elif isinstance(legacy.get("couponTypeSnapshot"), dict):
            snap["couponTypeSnapshot"] = legacy["couponTypeSnapshot"]
        elif isinstance(legacy.get("couponType"), dict):
            ct = legacy["couponType"]
            snap["couponTypeSnapshot"] = {
                "id": ct.get("id"),
                "name": ct.get("name"),
                "description": ct.get("description"),
            }

    preserved_keys = ("cancelledAt", "cancelReason", "cancelled_at", "cancel_reason")
    merged = dict(base)
    for key in preserved_keys:
        if key in base:
            merged[key] = base[key]

    for key, value in snap.items():
        merged[key] = value

    return merged


def backfill_customer_reward_snapshots(db: Session, *, batch_size: int = 500) -> dict[str, int]:
    """Populate rewardSnapshot / couponTypeSnapshot on historical customer_rewards rows."""
    stats = {"total": 0, "updated": 0, "skipped": 0, "unresolved": 0}

    query = db.query(CustomerReward).order_by(CustomerReward.issued_at.asc())
    offset = 0
    while True:
        rows = query.offset(offset).limit(batch_size).all()
        if not rows:
            break
        offset += len(rows)

        coupon_ids = {cr.customer_coupon_id for cr in rows if cr.customer_coupon_id}
        coupon_by_id: dict = {}
        coupon_type_by_id: dict = {}
        if coupon_ids:
            coupons = db.query(CustomerCoupon).filter(CustomerCoupon.id.in_(coupon_ids)).all()
            coupon_by_id = {c.id: c for c in coupons}
            ct_ids = {c.coupon_type_id for c in coupons if c.coupon_type_id}
            if ct_ids:
                coupon_types = db.query(CouponType).filter(CouponType.id.in_(ct_ids)).all()
                coupon_type_by_id = {ct.id: ct for ct in coupon_types}

        reward_ids = {cr.reward_id for cr in rows if cr.reward_id}
        reward_by_id: dict = {}
        if reward_ids:
            rewards = db.query(Reward).filter(Reward.id.in_(reward_ids)).all()
            reward_by_id = {r.id: r for r in rewards}

        for cr in rows:
            stats["total"] += 1
            current = _payload_dict(cr.payload)
            if _snapshot_already_complete(current):
                stats["skipped"] += 1
                continue

            reward = reward_by_id.get(cr.reward_id) if cr.reward_id else None
            coupon_type = None
            if cr.customer_coupon_id:
                coupon = coupon_by_id.get(cr.customer_coupon_id)
                if coupon and coupon.coupon_type_id:
                    coupon_type = coupon_type_by_id.get(coupon.coupon_type_id)

            if reward is None and not _reward_snapshot_from_legacy(current):
                stats["unresolved"] += 1
                continue

            cr.payload = merge_customer_reward_snapshot_payload(
                current,
                reward=reward,
                coupon_type=coupon_type,
                legacy_payload=current,
            )
            stats["updated"] += 1

        db.flush()

    return stats
