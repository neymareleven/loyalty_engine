"""Human-readable fields for customer coupons / rewards API responses."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models.coupon_type import CouponType
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.reward import Reward
from app.schemas.customer_coupon import CustomerCouponOut
from app.schemas.customer_reward import CustomerRewardOut
from app.services.benefit_service import benefit_scope, describe_benefit
from app.services.catalog_invalidation_service import (
    coupon_admin_allowed_transitions,
    reward_admin_allowed_transitions,
)

BENEFIT_TYPE_LABELS = {
    "PERCENT_DISCOUNT": "Remise en %",
    "FIXED_DISCOUNT": "Remise fixe",
    "FREE_SHIPPING": "Livraison offerte",
    "SHIPPING_PERCENT_DISCOUNT": "Remise % sur la livraison",
    "SHIPPING_FIXED_DISCOUNT": "Remise fixe sur la livraison",
    "FREE_PRODUCT": "Produit offert",
    "CUSTOM": "Avantage personnalisé",
}

APPLIES_TO_LABELS = {
    "CART": "Tout le panier",
    "PRODUCTS": "Produits spécifiques",
    "CATEGORIES": "Catégories spécifiques",
}

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


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _benefit_details(db: Session, *, reward: CustomerReward, payload: dict) -> dict[str, Any]:
    benefit = payload.get("benefit") if isinstance(payload.get("benefit"), dict) else {}
    snap = payload.get("rewardSnapshot") if isinstance(payload.get("rewardSnapshot"), dict) else {}
    catalog = None
    if reward.reward_id:
        catalog = db.query(Reward).filter(Reward.id == reward.reward_id).first()

    def pick(camel: str, snake: str, attr: str):
        if benefit.get(camel) is not None:
            return benefit.get(camel)
        if benefit.get(snake) is not None:
            return benefit.get(snake)
        if catalog is not None:
            return getattr(catalog, attr, None)
        return None

    btype = str(pick("benefitType", "benefit_type", "benefit_type") or "CUSTOM").upper()
    applies = str(pick("appliesTo", "applies_to", "applies_to") or "CART").upper()
    value = _int_or_none(pick("benefitValue", "benefit_value", "benefit_value"))
    max_discount = _int_or_none(pick("maxDiscountAmount", "max_discount_amount", "max_discount_amount"))
    min_cart = _int_or_none(pick("minCartAmount", "min_cart_amount", "min_cart_amount"))
    free_qty = _int_or_none(pick("freeProductQuantity", "free_product_quantity", "free_product_quantity"))
    if free_qty is None:
        free_qty = 1
    stackable = pick("stackable", "stackable", "stackable")
    stackable_bool = bool(stackable) if stackable is not None else False
    name = (
        (snap.get("name") if isinstance(snap.get("name"), str) else None)
        or (payload.get("name") if isinstance(payload.get("name"), str) else None)
        or (catalog.name if catalog is not None else None)
    )
    description = snap.get("description") if snap.get("description") else payload.get("description")
    if not description and catalog is not None:
        description = catalog.description
    description = (str(description).strip() or None) if description else None
    summary = describe_benefit(
        benefit_type=btype,
        benefit_value=value,
        max_discount_amount=max_discount,
        min_cart_amount=min_cart,
        name=name,
        free_product_quantity=free_qty,
        applies_to=applies,
    )
    return {
        "description": description,
        "benefit_type": btype,
        "benefit_type_label": BENEFIT_TYPE_LABELS.get(btype, btype),
        "benefit_value": value,
        "max_discount_amount": max_discount,
        "min_cart_amount": min_cart,
        "applies_to": applies,
        "applies_to_label": APPLIES_TO_LABELS.get(applies, applies),
        "scope": benefit_scope(btype, applies),
        "free_product_quantity": free_qty,
        "stackable": stackable_bool,
        "stackable_label": "Cumulable" if stackable_bool else "Non cumulable",
        "benefit_description": summary,
        "usage_conditions": {
            "applies_to": applies,
            "applies_to_label": APPLIES_TO_LABELS.get(applies, applies),
            "min_cart_amount": min_cart,
            "max_discount_amount": max_discount,
            "stackable": stackable_bool,
            "stackable_label": "Cumulable" if stackable_bool else "Non cumulable",
            "free_product_quantity": free_qty if btype == "FREE_PRODUCT" else None,
            "summary": summary,
        },
    }


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
    data.update(_benefit_details(db, reward=reward, payload=payload))
    return data
