from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.coupon_type import CouponType
from app.models.coupon_type_reward import CouponTypeReward
from app.models.reward import Reward


def _normalize_id_list(values: list | None, *, field_name: str) -> list[str]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise HTTPException(status_code=400, detail=f"{field_name} must be a list")
    normalized: list[str] = []
    for value in values:
        if value is None:
            continue
        s = str(value).strip()
        if not s:
            continue
        if s not in normalized:
            normalized.append(s)
    return normalized


def list_coupon_type_reward_ids(db: Session, *, coupon_type_id) -> list[UUID]:
    rows = (
        db.query(CouponTypeReward.reward_id)
        .filter(CouponTypeReward.coupon_type_id == coupon_type_id)
        .order_by(CouponTypeReward.reward_id.asc())
        .all()
    )
    return [row[0] for row in rows]


def list_reward_coupon_type_ids(db: Session, *, reward_id) -> list[UUID]:
    rows = (
        db.query(CouponTypeReward.coupon_type_id)
        .filter(CouponTypeReward.reward_id == reward_id)
        .order_by(CouponTypeReward.coupon_type_id.asc())
        .all()
    )
    return [row[0] for row in rows]


def list_coupon_type_rewards(db: Session, *, coupon_type: CouponType, active_only: bool | None = True):
    q = (
        db.query(Reward)
        .join(CouponTypeReward, CouponTypeReward.reward_id == Reward.id)
        .filter(CouponTypeReward.coupon_type_id == coupon_type.id)
        .filter(Reward.brand == coupon_type.brand)
    )
    if active_only is True:
        q = q.filter(Reward.active.is_(True))
    elif active_only is False:
        q = q.filter(Reward.active.is_(False))
    return q.order_by(Reward.name.asc()).all()


def resolve_rewards_catalog(
    db: Session,
    *,
    coupon_type: CouponType,
    active_only: bool | None = True,
) -> list[Reward]:
    return list_coupon_type_rewards(db, coupon_type=coupon_type, active_only=active_only)


def resolve_rewards_to_issue(
    db: Session,
    *,
    coupon_type: CouponType,
    reward_ids_override: list[str] | None,
) -> list[Reward]:
    """Resolve rewards to emit when issuing a coupon.

    - reward_ids_override is None: all active rewards linked to the coupon type.
    - reward_ids_override is a list: strict mode — every id must be linked and active;
      only those rewards are emitted (rule subset guarantee).
    - reward_ids_override is []: coupon only, no customer rewards.
    """
    catalog = resolve_rewards_catalog(db, coupon_type=coupon_type, active_only=True)
    if reward_ids_override is None:
        if not catalog:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Coupon type '{coupon_type.name}' has no active rewards linked. "
                    "Link rewards via coupon_type_ids when creating rewards."
                ),
            )
        return catalog

    normalized = _normalize_id_list(reward_ids_override, field_name="reward_ids")
    if not normalized:
        return []

    catalog_by_id = {str(r.id): r for r in catalog}
    allowed_ids = sorted(catalog_by_id.keys())

    not_linked = [rid for rid in normalized if rid not in catalog_by_id]
    if not_linked:
        raise HTTPException(
            status_code=400,
            detail={
                "message": (
                    "reward_ids contains rewards not linked to this coupon type. "
                    "Only rewards attached via coupon_type_rewards can be issued."
                ),
                "invalidRewardIds": not_linked,
                "allowedRewardIds": allowed_ids,
                "couponTypeId": str(coupon_type.id),
            },
        )

    inactive = [rid for rid in normalized if not catalog_by_id[rid].active]
    if inactive:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "reward_ids contains inactive rewards",
                "inactiveRewardIds": inactive,
                "couponTypeId": str(coupon_type.id),
            },
        )

    return [catalog_by_id[rid] for rid in normalized]


def _load_rewards(db: Session, *, reward_ids: list, brand: str) -> dict[str, Reward]:
    rewards = db.query(Reward).filter(Reward.id.in_(reward_ids)).all()
    found = {str(r.id): r for r in rewards}
    missing = [rid for rid in reward_ids if rid not in found]
    if missing:
        raise HTTPException(status_code=400, detail="Unknown reward_id(s): " + ", ".join(missing))

    wrong_brand = [rid for rid, r in found.items() if r.brand != brand]
    if wrong_brand:
        raise HTTPException(status_code=400, detail="Reward(s) not in active brand: " + ", ".join(wrong_brand))
    return found


def _load_coupon_types(db: Session, *, coupon_type_ids: list, brand: str) -> dict[str, CouponType]:
    coupon_types = db.query(CouponType).filter(CouponType.id.in_(coupon_type_ids)).all()
    found = {str(ct.id): ct for ct in coupon_types}
    missing = [cid for cid in coupon_type_ids if cid not in found]
    if missing:
        raise HTTPException(status_code=400, detail="Unknown coupon_type_id(s): " + ", ".join(missing))

    wrong_brand = [cid for cid, ct in found.items() if ct.brand != brand]
    if wrong_brand:
        raise HTTPException(status_code=400, detail="Coupon type(s) not in active brand: " + ", ".join(wrong_brand))
    return found


def replace_coupon_type_rewards(
    db: Session,
    *,
    coupon_type: CouponType,
    reward_ids: list | None,
    brand: str,
) -> list[UUID]:
    normalized = _normalize_id_list(reward_ids, field_name="reward_ids")
    db.query(CouponTypeReward).filter(CouponTypeReward.coupon_type_id == coupon_type.id).delete(
        synchronize_session=False
    )
    if not normalized:
        db.flush()
        return []

    found = _load_rewards(db, reward_ids=normalized, brand=brand)
    for rid in normalized:
        db.add(CouponTypeReward(coupon_type_id=coupon_type.id, reward_id=found[rid].id))

    db.flush()
    return [found[rid].id for rid in normalized]


def link_reward_to_coupon_types(
    db: Session,
    *,
    reward: Reward,
    coupon_type_ids: list | None,
    brand: str,
    replace: bool = False,
) -> list[UUID]:
    """Attach a reward to one or more existing coupon types (coupon-first workflow)."""
    normalized = _normalize_id_list(coupon_type_ids, field_name="coupon_type_ids")
    if replace:
        db.query(CouponTypeReward).filter(CouponTypeReward.reward_id == reward.id).delete(
            synchronize_session=False
        )
    if not normalized:
        db.flush()
        return list_reward_coupon_type_ids(db, reward_id=reward.id)

    found = _load_coupon_types(db, coupon_type_ids=normalized, brand=brand)
    for cid in normalized:
        ct = found[cid]
        exists = (
            db.query(CouponTypeReward)
            .filter(CouponTypeReward.coupon_type_id == ct.id)
            .filter(CouponTypeReward.reward_id == reward.id)
            .first()
        )
        if not exists:
            db.add(CouponTypeReward(coupon_type_id=ct.id, reward_id=reward.id))

    db.flush()
    return list_reward_coupon_type_ids(db, reward_id=reward.id)
