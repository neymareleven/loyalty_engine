"""Catalog deletion → client entitlement invalidation (granular, snapshot-safe)."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.models.coupon_type import CouponType
from app.models.customer import Customer
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.product import Product
from app.models.reward import Reward
from app.models.reward_product import RewardProduct
from app.models.transaction import Transaction

TERMINAL_COUPON_STATUSES = frozenset({"EXPIRED", "INVALIDATED"})
ACTIVE_COUPON_STATUSES = frozenset({"ISSUED"})
ACTIVE_REWARD_STATUSES = frozenset({"ISSUED"})

CATALOG_DELETE_TX_TYPES = frozenset(
    {
        "CATALOG_COUPON_TYPE_DELETED",
        "CATALOG_REWARD_DELETED",
        "CATALOG_PRODUCT_DELETED",
    }
)

ENTITLEMENT_HISTORY_TX_TYPES = frozenset(
    {
        "ADMIN_USE_COUPON",
        "ADMIN_REOPEN_COUPON",
        "ADMIN_EXPIRE_COUPON",
        "CATALOG_COUPON_TYPE_DELETED",
        "CATALOG_REWARD_DELETED",
        "CATALOG_PRODUCT_DELETED",
    }
)


def _utcnow() -> datetime:
    return datetime.utcnow()


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


def _catalog_audit_transaction_id(*, prefix: str, entity_id: str, profile_id: str, now: datetime) -> str:
    ent = str(entity_id).replace("-", "")[:10]
    prof = "".join(ch for ch in profile_id if ch.isalnum())[:12]
    ts = now.strftime("%Y%m%d%H%M%S%f")[:18]
    return f"{prefix}_{ent}_{prof}_{ts}"[:100]


def stamp_catalog_removed(
    payload: Any,
    *,
    reason: str,
    entity_type: str,
    entity_id: str | None = None,
    entity_name: str | None = None,
) -> dict:
    merged = _payload_dict(payload)
    now = _utcnow()
    merged["catalogRemoved"] = True
    merged["catalogRemovedAt"] = now.isoformat() + "Z"
    merged["catalogRemovedReason"] = reason
    merged["catalogRemovedEntityType"] = entity_type
    if entity_id:
        merged["catalogRemovedEntityId"] = str(entity_id)
    if entity_name:
        merged["catalogRemovedEntityName"] = entity_name
    return merged


def invalidate_product_in_snapshot(
    payload: Any,
    *,
    product_id: str,
    product_name: str | None = None,
) -> dict:
    merged = _payload_dict(payload)
    snapshots = merged.get("productSnapshots")
    if not isinstance(snapshots, list):
        return merged
    now = _utcnow().isoformat() + "Z"
    pid = str(product_id)
    updated: list[dict] = []
    for item in snapshots:
        if not isinstance(item, dict):
            updated.append(item)
            continue
        row = dict(item)
        if str(row.get("id") or "") == pid:
            row["catalogRemoved"] = True
            row["invalidatedAt"] = now
            if product_name:
                row["name"] = product_name
        updated.append(row)
    merged["productSnapshots"] = updated
    return merged


def coupon_admin_allowed_transitions(coupon: CustomerCoupon) -> list[str]:
    payload = _payload_dict(coupon.payload)
    if coupon.status in TERMINAL_COUPON_STATUSES:
        return []
    if payload.get("catalogRemoved"):
        return []
    if coupon.status == "ISSUED":
        return ["USED", "EXPIRED"]
    if coupon.status == "USED":
        return ["ISSUED", "EXPIRED"]
    return []


def reward_admin_allowed_transitions(reward: CustomerReward) -> list[str]:
    payload = _payload_dict(reward.payload)
    if reward.status in {"INVALIDATED", "EXPIRED", "CANCELLED"}:
        return []
    if payload.get("catalogRemoved"):
        return []
    return []


def _count_customer_coupons_by_status(db: Session, *, coupon_type_id) -> dict[str, int]:
    rows = (
        db.query(CustomerCoupon.status, CustomerCoupon.id)
        .filter(CustomerCoupon.coupon_type_id == coupon_type_id)
        .all()
    )
    counts: dict[str, int] = {}
    for status, _ in rows:
        key = str(status or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    counts["total"] = len(rows)
    return counts


def _count_customer_rewards_by_status(db: Session, *, reward_id) -> dict[str, int]:
    rows = (
        db.query(CustomerReward.status, CustomerReward.id)
        .filter(CustomerReward.reward_id == reward_id)
        .all()
    )
    counts: dict[str, int] = {}
    for status, _ in rows:
        key = str(status or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    counts["total"] = len(rows)
    return counts


def preview_coupon_type_delete(db: Session, *, coupon_type: CouponType) -> dict[str, Any]:
    counts = _count_customer_coupons_by_status(db, coupon_type_id=coupon_type.id)
    issued = counts.get("ISSUED", 0)
    used = counts.get("USED", 0)
    expired = counts.get("EXPIRED", 0)
    invalidated = counts.get("INVALIDATED", 0)
    total = counts.get("total", 0)

    reward_rows = (
        db.query(CustomerReward)
        .join(CustomerCoupon, CustomerCoupon.id == CustomerReward.customer_coupon_id)
        .filter(CustomerCoupon.coupon_type_id == coupon_type.id)
        .filter(CustomerReward.status == "ISSUED")
        .count()
    )

    message = (
        f"Le type de coupon « {coupon_type.name} » sera retiré du catalogue. "
        f"{total} coupon(s) client(s) existent "
        f"({issued} actif(s), {used} utilisé(s), {expired} expiré(s)"
        f"{f', {invalidated} invalidé(s)' if invalidated else ''}). "
    )
    if issued:
        message += (
            f"{issued} coupon(s) actif(s) et {reward_rows} récompense(s) active(s) seront invalidés immédiatement. "
        )
    if used or expired:
        message += (
            "Les coupons déjà utilisés ou expirés restent en historique (lecture seule, plus d'action admin). "
        )
    message += "Aucune nouvelle émission ne sera possible."

    return {
        "resource_type": "coupon_type",
        "resource_id": coupon_type.id,
        "resource_name": coupon_type.name,
        "can_delete": True,
        "recommended_action": None,
        "counts": {
            "customer_coupons_total": total,
            "customer_coupons_issued": issued,
            "customer_coupons_used": used,
            "customer_coupons_expired": expired,
            "customer_coupons_invalidated": invalidated,
            "customer_rewards_issued_to_invalidate": reward_rows,
        },
        "effects": [e for e in [
            "stop_new_issuance",
            "invalidate_active_customer_coupons" if issued else None,
            "invalidate_active_customer_rewards" if reward_rows else None,
            "mark_historical_coupons_read_only" if (used or expired) else None,
            "preserve_snapshots_and_history",
        ] if e],
        "message": message,
    }


def preview_reward_delete(db: Session, *, reward: Reward) -> dict[str, Any]:
    counts = _count_customer_rewards_by_status(db, reward_id=reward.id)
    issued = counts.get("ISSUED", 0)
    used = counts.get("USED", 0)
    expired = counts.get("EXPIRED", 0)
    cancelled = counts.get("CANCELLED", 0) + counts.get("INVALIDATED", 0)
    total = counts.get("total", 0)

    linked_products = (
        db.query(RewardProduct).filter(RewardProduct.reward_id == reward.id).count()
    )

    message = (
        f"La récompense « {reward.name} » sera retirée du catalogue. "
        f"{total} attribution(s) client(s) existent "
        f"({issued} active(s), {used} utilisée(s), {expired} expirée(s)"
        f"{f', {cancelled} invalidée(s)' if cancelled else ''}). "
    )
    if issued:
        message += f"{issued} attribution(s) active(s) seront invalidées (les autres rewards du même coupon ne sont pas affectées). "
    if used or expired:
        message += "Les attributions déjà utilisées ou expirées restent en historique (lecture seule). "
    message += "Aucune nouvelle émission de cette récompense ne sera possible."

    return {
        "resource_type": "reward",
        "resource_id": reward.id,
        "resource_name": reward.name,
        "can_delete": True,
        "recommended_action": None,
        "counts": {
            "customer_rewards_total": total,
            "customer_rewards_issued": issued,
            "customer_rewards_used": used,
            "customer_rewards_expired": expired,
            "customer_rewards_invalidated": cancelled,
            "catalog_product_links": linked_products,
        },
        "effects": [e for e in [
            "stop_new_issuance",
            "invalidate_active_customer_rewards_only" if issued else None,
            "mark_historical_rewards_read_only" if (used or expired) else None,
            "auto_invalidate_parent_coupon_if_no_active_rewards",
            "preserve_snapshots_and_history",
        ] if e],
        "message": message,
    }


def preview_product_delete(db: Session, *, product: Product) -> dict[str, Any]:
    reward_links = (
        db.query(RewardProduct).filter(RewardProduct.product_id == product.id).count()
    )

    affected_snapshots = 0
    if reward_links:
        pid = str(product.id)
        for cr in db.query(CustomerReward).filter(CustomerReward.payload.isnot(None)).all():
            payload = _payload_dict(cr.payload)
            snapshots = payload.get("productSnapshots")
            if not isinstance(snapshots, list):
                continue
            if any(str(s.get("id") or "") == pid for s in snapshots if isinstance(s, dict)):
                affected_snapshots += 1

    message = (
        f"Le produit « {product.name} » sera retiré du catalogue. "
        f"Il est lié à {reward_links} récompense(s) catalogue. "
        "Seul ce produit sera marqué comme retiré dans les snapshots client ; "
        "les coupons et récompenses client restent valides. "
        "Aucune nouvelle émission catalogue n'inclura ce produit."
    )

    return {
        "resource_type": "product",
        "resource_id": product.id,
        "resource_name": product.name,
        "can_delete": True,
        "recommended_action": None,
        "counts": {
            "reward_catalog_links": reward_links,
            "customer_rewards_with_product_snapshots": affected_snapshots,
        },
        "effects": [
            "detach_from_reward_catalog",
            "mark_product_invalidated_in_client_snapshots",
            "no_coupon_or_reward_status_change",
        ],
        "message": message,
    }


def _create_customer_catalog_audit(
    db: Session,
    *,
    customer: Customer,
    transaction_type: str,
    payload: dict,
) -> Transaction:
    now = _utcnow()
    entity_id = str(payload.get("entityId") or payload.get("couponTypeId") or payload.get("rewardId") or "x")
    tx = Transaction(
        transaction_id=_catalog_audit_transaction_id(
            prefix="catdel",
            entity_id=entity_id,
            profile_id=customer.profile_id,
            now=now,
        ),
        brand=customer.brand,
        profile_id=customer.profile_id,
        transaction_type=transaction_type,
        source="ADMIN_CATALOG",
        payload=payload,
        status="PROCESSED",
        processed_at=now,
    )
    db.add(tx)
    db.flush()
    return tx


def _invalidate_coupon_row(
    db: Session,
    *,
    coupon: CustomerCoupon,
    reason: str,
    entity_type: str,
    entity_id: str,
    entity_name: str,
) -> bool:
    if coupon.status not in ACTIVE_COUPON_STATUSES:
        return False
    coupon.status = "INVALIDATED"
    coupon.payload = stamp_catalog_removed(
        coupon.payload,
        reason=reason,
        entity_type=entity_type,
        entity_id=entity_id,
        entity_name=entity_name,
    )
    return True


def _mark_coupon_catalog_removed_read_only(
    coupon: CustomerCoupon,
    *,
    reason: str,
    entity_type: str,
    entity_id: str,
    entity_name: str,
) -> bool:
    payload = _payload_dict(coupon.payload)
    if payload.get("catalogRemoved"):
        return False
    coupon.payload = stamp_catalog_removed(
        coupon.payload,
        reason=reason,
        entity_type=entity_type,
        entity_id=entity_id,
        entity_name=entity_name,
    )
    return True


def _invalidate_reward_row(
    reward: CustomerReward,
    *,
    reason: str,
    entity_type: str,
    entity_id: str,
    entity_name: str,
) -> bool:
    if reward.status not in ACTIVE_REWARD_STATUSES:
        return False
    reward.status = "INVALIDATED"
    reward.payload = stamp_catalog_removed(
        reward.payload,
        reason=reason,
        entity_type=entity_type,
        entity_id=entity_id,
        entity_name=entity_name,
    )
    return True


def _mark_reward_catalog_removed_read_only(
    reward: CustomerReward,
    *,
    reason: str,
    entity_type: str,
    entity_id: str,
    entity_name: str,
) -> bool:
    payload = _payload_dict(reward.payload)
    if payload.get("catalogRemoved"):
        return False
    reward.payload = stamp_catalog_removed(
        reward.payload,
        reason=reason,
        entity_type=entity_type,
        entity_id=entity_id,
        entity_name=entity_name,
    )
    return True


def maybe_invalidate_coupon_without_active_rewards(db: Session, *, customer_coupon_id) -> bool:
    coupon = db.query(CustomerCoupon).filter(CustomerCoupon.id == customer_coupon_id).first()
    if not coupon or coupon.status != "ISSUED":
        return False
    active = (
        db.query(CustomerReward.id)
        .filter(CustomerReward.customer_coupon_id == coupon.id)
        .filter(CustomerReward.status == "ISSUED")
        .first()
    )
    if active:
        return False
    coupon.status = "INVALIDATED"
    payload = _payload_dict(coupon.payload)
    if not payload.get("catalogRemoved"):
        coupon.payload = stamp_catalog_removed(
            coupon.payload,
            reason="NO_ACTIVE_REWARDS",
            entity_type="customer_coupon",
            entity_id=str(coupon.id),
        )
    return True


def apply_coupon_type_catalog_delete(db: Session, *, coupon_type: CouponType) -> dict[str, int]:
    stats = {
        "coupons_invalidated": 0,
        "coupons_marked_read_only": 0,
        "rewards_invalidated": 0,
        "rewards_marked_read_only": 0,
        "audit_events": 0,
    }
    reason = "COUPON_TYPE_DELETED"
    entity_id = str(coupon_type.id)
    entity_name = coupon_type.name or ""

    coupons = (
        db.query(CustomerCoupon)
        .filter(CustomerCoupon.coupon_type_id == coupon_type.id)
        .all()
    )
    customer_ids = {c.customer_id for c in coupons}
    customers_by_id = {}
    if customer_ids:
        customers = db.query(Customer).filter(Customer.id.in_(customer_ids)).all()
        customers_by_id = {c.id: c for c in customers}

    per_customer: dict[Any, dict[str, int]] = {}

    for coupon in coupons:
        bucket = per_customer.setdefault(coupon.customer_id, {
            "coupons_invalidated": 0,
            "coupons_marked_read_only": 0,
            "rewards_invalidated": 0,
            "rewards_marked_read_only": 0,
        })

        if _invalidate_coupon_row(
            db,
            coupon=coupon,
            reason=reason,
            entity_type="coupon_type",
            entity_id=entity_id,
            entity_name=entity_name,
        ):
            stats["coupons_invalidated"] += 1
            bucket["coupons_invalidated"] += 1
        elif _mark_coupon_catalog_removed_read_only(
            coupon,
            reason=reason,
            entity_type="coupon_type",
            entity_id=entity_id,
            entity_name=entity_name,
        ):
            stats["coupons_marked_read_only"] += 1
            bucket["coupons_marked_read_only"] += 1

        rewards = (
            db.query(CustomerReward)
            .filter(CustomerReward.customer_coupon_id == coupon.id)
            .all()
        )
        for cr in rewards:
            if _invalidate_reward_row(
                cr,
                reason=reason,
                entity_type="coupon_type",
                entity_id=entity_id,
                entity_name=entity_name,
            ):
                stats["rewards_invalidated"] += 1
                bucket["rewards_invalidated"] += 1
            elif _mark_reward_catalog_removed_read_only(
                cr,
                reason=reason,
                entity_type="coupon_type",
                entity_id=entity_id,
                entity_name=entity_name,
            ):
                stats["rewards_marked_read_only"] += 1
                bucket["rewards_marked_read_only"] += 1

    for customer_id, bucket in per_customer.items():
        customer = customers_by_id.get(customer_id)
        if not customer:
            continue
        if not any(bucket.values()):
            continue
        _create_customer_catalog_audit(
            db,
            customer=customer,
            transaction_type="CATALOG_COUPON_TYPE_DELETED",
            payload={
                "entityType": "coupon_type",
                "entityId": entity_id,
                "entityName": entity_name,
                **bucket,
            },
        )
        stats["audit_events"] += 1

    db.flush()
    return stats


def apply_reward_catalog_delete(db: Session, *, reward: Reward) -> dict[str, int]:
    stats = {
        "rewards_invalidated": 0,
        "rewards_marked_read_only": 0,
        "coupons_auto_invalidated": 0,
        "audit_events": 0,
    }
    reason = "REWARD_DELETED"
    entity_id = str(reward.id)
    entity_name = reward.name or ""

    rows = (
        db.query(CustomerReward)
        .filter(CustomerReward.reward_id == reward.id)
        .all()
    )
    customer_ids = {r.customer_id for r in rows}
    customers_by_id = {}
    if customer_ids:
        customers = db.query(Customer).filter(Customer.id.in_(customer_ids)).all()
        customers_by_id = {c.id: c for c in customers}

    per_customer: dict[Any, dict[str, int]] = {}
    touched_coupon_ids: set[Any] = set()

    for cr in rows:
        bucket = per_customer.setdefault(cr.customer_id, {
            "rewards_invalidated": 0,
            "rewards_marked_read_only": 0,
            "coupons_auto_invalidated": 0,
        })
        if _invalidate_reward_row(
            cr,
            reason=reason,
            entity_type="reward",
            entity_id=entity_id,
            entity_name=entity_name,
        ):
            stats["rewards_invalidated"] += 1
            bucket["rewards_invalidated"] += 1
        elif _mark_reward_catalog_removed_read_only(
            cr,
            reason=reason,
            entity_type="reward",
            entity_id=entity_id,
            entity_name=entity_name,
        ):
            stats["rewards_marked_read_only"] += 1
            bucket["rewards_marked_read_only"] += 1
        if cr.customer_coupon_id:
            touched_coupon_ids.add(cr.customer_coupon_id)

    for coupon_id in touched_coupon_ids:
        if maybe_invalidate_coupon_without_active_rewards(db, customer_coupon_id=coupon_id):
            coupon = db.query(CustomerCoupon).filter(CustomerCoupon.id == coupon_id).first()
            if coupon:
                bucket = per_customer.setdefault(coupon.customer_id, {
                    "rewards_invalidated": 0,
                    "rewards_marked_read_only": 0,
                    "coupons_auto_invalidated": 0,
                })
                stats["coupons_auto_invalidated"] += 1
                bucket["coupons_auto_invalidated"] += 1

    for customer_id, bucket in per_customer.items():
        customer = customers_by_id.get(customer_id)
        if not customer:
            continue
        if not any(bucket.values()):
            continue
        _create_customer_catalog_audit(
            db,
            customer=customer,
            transaction_type="CATALOG_REWARD_DELETED",
            payload={
                "entityType": "reward",
                "entityId": entity_id,
                "entityName": entity_name,
                **bucket,
            },
        )
        stats["audit_events"] += 1

    db.flush()
    return stats


def apply_product_catalog_delete(db: Session, *, product: Product) -> dict[str, int]:
    stats = {"reward_links_removed": 0, "snapshots_updated": 0, "audit_events": 0}
    entity_id = str(product.id)
    entity_name = product.name or ""

    stats["reward_links_removed"] = (
        db.query(RewardProduct)
        .filter(RewardProduct.product_id == product.id)
        .delete(synchronize_session=False)
    )

    rows = db.query(CustomerReward).filter(CustomerReward.payload.isnot(None)).all()
    per_customer: dict[Any, int] = {}
    for cr in rows:
        payload = _payload_dict(cr.payload)
        snapshots = payload.get("productSnapshots")
        if not isinstance(snapshots, list):
            continue
        found = any(str(s.get("id") or "") == entity_id for s in snapshots if isinstance(s, dict))
        if not found:
            continue
        cr.payload = invalidate_product_in_snapshot(
            cr.payload,
            product_id=entity_id,
            product_name=entity_name,
        )
        stats["snapshots_updated"] += 1
        per_customer[cr.customer_id] = per_customer.get(cr.customer_id, 0) + 1

    if per_customer:
        customers = db.query(Customer).filter(Customer.id.in_(per_customer.keys())).all()
        for customer in customers:
            count = per_customer.get(customer.id, 0)
            if not count:
                continue
            _create_customer_catalog_audit(
                db,
                customer=customer,
                transaction_type="CATALOG_PRODUCT_DELETED",
                payload={
                    "entityType": "product",
                    "entityId": entity_id,
                    "entityName": entity_name,
                    "productSnapshotsUpdated": count,
                },
            )
            stats["audit_events"] += 1

    db.flush()
    return stats
