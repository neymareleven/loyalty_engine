"""Human-readable fields for customer coupons / rewards API responses."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models.coupon_type import CouponType
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.schemas.customer_coupon import CustomerCouponOut
from app.schemas.customer_reward import CustomerRewardOut
from app.services.catalog_invalidation_service import (
    coupon_admin_allowed_transitions,
    reward_admin_allowed_transitions,
)

_STATUS_LABELS = {
    "ISSUED": "Actif",
    "USED": "Utilisé",
    "EXPIRED": "Expiré",
    "INVALIDATED": "Invalidé",
    "CANCELLED": "Invalidé",
}


def _payload_dict(payload: Any) -> dict:
    return payload if isinstance(payload, dict) else {}


def _snapshot_name(payload: dict | None, *keys: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        snap = payload.get(key)
        if isinstance(snap, dict):
            name = snap.get("name") or snap.get("title")
            if isinstance(name, str) and name.strip():
                return name.strip()
    return None


def _catalog_removed(payload: dict) -> bool:
    return bool(payload.get("catalogRemoved"))


def _status_label(status: str, *, catalog_removed: bool) -> str:
    base = _STATUS_LABELS.get(str(status or "").upper(), str(status or ""))
    if catalog_removed and str(status).upper() not in {"INVALIDATED", "CANCELLED"}:
        return f"{base} (modèle retiré du catalogue)"
    return base


def _serialize_product_snapshots(payload: dict) -> list[dict[str, Any]]:
    snapshots = payload.get("productSnapshots")
    if not isinstance(snapshots, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in snapshots:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "match_key": item.get("matchKey"),
                "quantity": item.get("quantity"),
                "catalog_removed": bool(item.get("catalogRemoved")),
                "invalidated_at": item.get("invalidatedAt"),
                "display_label": (
                    f"{item.get('name')} (retiré du catalogue)"
                    if item.get("catalogRemoved") and item.get("name")
                    else item.get("name")
                ),
            }
        )
    return rows


def serialize_customer_coupon_out(
    db: Session,
    *,
    coupon: CustomerCoupon,
    coupon_type: CouponType | None = None,
) -> dict[str, Any]:
    if coupon_type is None and coupon.coupon_type_id:
        coupon_type = db.query(CouponType).filter(CouponType.id == coupon.coupon_type_id).first()

    data = CustomerCouponOut.model_validate(coupon).model_dump()
    payload = _payload_dict(coupon.payload)

    type_name = (
        coupon_type.name.strip()
        if coupon_type and coupon_type.name
        else _snapshot_name(payload, "couponTypeSnapshot", "couponType")
    )
    type_description = (
        (coupon_type.description or "").strip() or None
        if coupon_type
        else None
    )
    if not type_description:
        snap = payload.get("couponTypeSnapshot")
        if isinstance(snap, dict) and snap.get("description"):
            type_description = str(snap["description"]).strip() or None

    catalog_removed = _catalog_removed(payload) or str(coupon.status).upper() == "INVALIDATED"

    data["coupon_type_name"] = type_name
    data["coupon_type_description"] = type_description
    data["display_label"] = type_name or f"Coupon {str(coupon.id)[:8]}"
    data["catalog_removed"] = catalog_removed
    data["status_label"] = _status_label(coupon.status, catalog_removed=catalog_removed)
    data["allowed_admin_transitions"] = coupon_admin_allowed_transitions(coupon)
    data["admin_actions_enabled"] = bool(data["allowed_admin_transitions"])
    return data


def serialize_customer_reward_out(db: Session, *, reward: CustomerReward) -> dict[str, Any]:
    data = CustomerRewardOut.model_validate(reward).model_dump()
    payload = _payload_dict(reward.payload)
    reward_name = _snapshot_name(payload, "rewardSnapshot")
    coupon_type_name = _snapshot_name(payload, "couponTypeSnapshot")
    catalog_removed = _catalog_removed(payload)

    data["reward_name"] = reward_name
    data["coupon_type_name"] = coupon_type_name
    data["display_label"] = reward_name or coupon_type_name or f"Avantage {str(reward.id)[:8]}"
    data["catalog_removed"] = catalog_removed
    data["status_label"] = _status_label(reward.status, catalog_removed=catalog_removed)
    data["allowed_admin_transitions"] = reward_admin_allowed_transitions(reward)
    data["admin_actions_enabled"] = bool(data["allowed_admin_transitions"])
    data["products"] = _serialize_product_snapshots(payload)
    return data
