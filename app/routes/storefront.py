from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.api_client import ApiClient
from app.models.coupon_type import CouponType
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.loyalty_tier import LoyaltyTier
from app.models.redemption import Redemption
from app.models.transaction import Transaction
from sqlalchemy import and_, or_

from app.services.contact_service import list_customer_unomi_profile_ids
from app.services.loyalty_history_service import serialize_loyalty_history_item
from app.services.loyalty_status_service import compute_loyalty_status_from_tiers
from app.services.purchase_service import get_purchase_balances, get_qualification_mode
from app.services.redemption_service import (
    applied_benefits_for_cart,
    commit as commit_redemption,
    coupon_reward_snapshots,
    evaluate_coupon_or_reward,
    load_reward_products_map,
    lookup_offer_ids_for_exclusion,
    present_wallet_reward,
    release as release_redemption,
    reserve as reserve_redemption,
    resolve_storefront_customer,
    _reservation_is_stale,
)
from app.services.storefront_audit import record_storefront_audit
from app.services.customer_delete_service import (
    delete_loyalty_customer,
    finish_unomi_delete_after_commit,
)
from app.services.storefront_auth import (
    enforce_storefront_rate_limit,
    persist_storefront_session,
    require_idempotency_key,
    require_storefront_anonymize_scope,
    require_storefront_client,
)
from app.services.wallet_service import get_status_points_balance, resolve_points_expires_at

router = APIRouter(prefix="/v1/storefront", tags=["storefront"])

_HISTORY_TYPES = [
    "CUSTOMER_REGISTRATION",
    "TIER_UPGRADED",
    "TIER_DOWNGRADED",
    "TIER_RENEWED",
    "QUALIFICATION_MODE_CHANGED",
    "FIRST_PURCHASE_BYPASS",
    "STATUS_RESET",
    "ADMIN_SET_TIER",
]


class CartItemIn(BaseModel):
    productId: str | None = None
    sku: str | None = None
    matchKey: str | None = None
    name: str | None = None
    categoryIds: list[str] = Field(default_factory=list)
    unitPrice: int = 0
    quantity: int = 1


class CartIn(BaseModel):
    cartRef: str | None = None
    currency: str | None = None
    subtotal: int = 0
    subtotalExclTax: int | None = None
    shippingAmount: int = 0
    taxIncluded: bool | None = None
    items: list[CartItemIn] = Field(default_factory=list)
    shippingMethods: list[dict] = Field(default_factory=list)


class ValidateIn(BaseModel):
    profileId: str
    email: str | None = None
    code: str | None = None
    customerRewardId: str | None = None
    cart: CartIn = Field(default_factory=CartIn)
    appliedRedemptionIds: list[str] = Field(default_factory=list)


class ReserveIn(ValidateIn):
    ttlMinutes: int = 30


class CommitIn(BaseModel):
    orderRef: str


class ReleaseIn(BaseModel):
    reason: str = "cart_abandoned"


def _audit_redemption_response(result: dict | None) -> dict:
    result = result or {}
    return {
        "status": result.get("status"),
        "redemptionId": result.get("redemptionId"),
        "reasons": result.get("reasons"),
        "applicable": result.get("applicable"),
    }


def _cart_dict(cart: CartIn | None) -> dict:
    if cart is None:
        return {}
    return cart.model_dump() if hasattr(cart, "model_dump") else cart.dict()


def _tier_brief(tier: LoyaltyTier | None, customer=None) -> dict | None:
    if not tier:
        return None
    data = {"key": tier.key, "name": tier.name, "rank": int(tier.rank)}
    if customer is not None:
        data["assignedAt"] = customer.loyalty_status_assigned_at
        data["expiresAt"] = customer.loyalty_status_expires_at
    return data


def _wallet_effective_status(row, now: datetime) -> str:
    status = str(getattr(row, "status", "") or "")
    if status in {"USED", "EXPIRED"}:
        return status
    if status == "RESERVED" and _reservation_is_stale(getattr(row, "reserved_until", None), now):
        status = "ISSUED"
    expires_at = getattr(row, "expires_at", None)
    if status == "ISSUED" and expires_at is not None and expires_at <= now:
        return "EXPIRED"
    return status


def _wallet_reservation_index(db: Session, customer_id) -> tuple[dict, dict]:
    rows = (
        db.query(Redemption)
        .filter(Redemption.customer_id == customer_id)
        .filter(Redemption.status == "RESERVED")
        .order_by(Redemption.created_at.desc())
        .all()
    )
    by_coupon: dict = {}
    by_reward: dict = {}
    for row in rows:
        if row.customer_coupon_id is not None:
            by_coupon.setdefault(str(row.customer_coupon_id), row)
        if row.customer_reward_id is not None:
            by_reward.setdefault(str(row.customer_reward_id), row)
    return by_coupon, by_reward


def _wallet_reservation_fields(row, now: datetime, latest: dict | None = None) -> dict:
    if _wallet_effective_status(row, now) != "RESERVED":
        return {}
    rid = getattr(row, "reserved_by_redemption_id", None)
    until = getattr(row, "reserved_until", None)
    if rid is None and latest:
        found = latest.get(str(row.id))
        if found is not None:
            rid = found.id
            until = until or getattr(found, "reserved_until", None)
    if rid is None:
        return {}
    return {
        "reservedRedemptionId": str(rid),
        "reservedUntil": until,
    }


def _wallet_products_map(db: Session, customer_id) -> dict:
    ids = [
        row.reward_id
        for row in db.query(CustomerReward).filter(CustomerReward.customer_id == customer_id).all()
    ]
    return load_reward_products_map(db, ids)


def _wallet_coupon(
    db: Session,
    coupon: CustomerCoupon,
    *,
    status: str | None = None,
    products_map: dict | None = None,
    now: datetime | None = None,
    latest_by_coupon: dict | None = None,
    latest_by_reward: dict | None = None,
) -> dict:
    now = now or datetime.utcnow()
    ct = None
    if coupon.coupon_type_id:
        ct = db.query(CouponType).filter(CouponType.id == coupon.coupon_type_id).first()
    name = None
    if isinstance(coupon.payload, dict):
        name = (coupon.payload.get("couponType") or {}).get("name")
    nested = (
        db.query(CustomerReward).filter(CustomerReward.customer_coupon_id == coupon.id).all()
    )
    nested_by_id = {str(row.id): row for row in nested}
    snaps = coupon_reward_snapshots(db, coupon)
    rewards = []
    for snap in snaps:
        item = present_wallet_reward(snap, products_map or {})
        nested_row = nested_by_id.get(str(snap.get("customerRewardId") or ""))
        if nested_row is not None:
            nested_status = _wallet_effective_status(nested_row, now)
            item["status"] = nested_status
            item.update(_wallet_reservation_fields(nested_row, now, latest_by_reward))
        rewards.append(item)
    data = {
        "id": str(coupon.id),
        "code": coupon.code,
        "name": name or (ct.name if ct else None),
        "status": status or _wallet_effective_status(coupon, now),
        "expiresAt": coupon.expires_at,
        "usedAt": coupon.used_at,
        "shareable": bool(coupon.shareable),
        "rewards": rewards,
    }
    data.update(_wallet_reservation_fields(coupon, now, latest_by_coupon))
    return data


def _wallet_standalone_reward(
    row: CustomerReward,
    products_map: dict,
    *,
    status: str | None = None,
    now: datetime | None = None,
    latest_by_reward: dict | None = None,
) -> dict:
    now = now or datetime.utcnow()
    payload = row.payload if isinstance(row.payload, dict) else {}
    benefit = payload.get("benefit") if isinstance(payload.get("benefit"), dict) else {}
    snap = dict(benefit)
    if row.reward_id:
        snap["id"] = str(row.reward_id)
    snap["customerRewardId"] = str(row.id)
    snap["name"] = snap.get("name") or payload.get("name")
    item = present_wallet_reward(snap, products_map)
    item["status"] = status or _wallet_effective_status(row, now)
    item["expiresAt"] = row.expires_at
    item["usedAt"] = row.used_at
    item.update(_wallet_reservation_fields(row, now, latest_by_reward))
    return item


@router.get("/customers/{profile_id}/wallet")
def storefront_wallet(
    profile_id: str,
    request: Request,
    email: str | None = Query(default=None),
    include: str | None = Query(default=None),
    client: ApiClient = Depends(require_storefront_client),
    db: Session = Depends(get_db),
):
    customer = resolve_storefront_customer(db, brand=client.brand, profile_id=profile_id, email=email)
    tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == client.brand, LoyaltyTier.active.is_(True))
        .all()
    )
    current = next((t for t in tiers if t.key == customer.loyalty_status), None)
    now = datetime.utcnow()
    coupons = (
        db.query(CustomerCoupon)
        .filter(CustomerCoupon.customer_id == customer.id)
        .filter(CustomerCoupon.status.in_(("ISSUED", "RESERVED")))
        .order_by(CustomerCoupon.issued_at.desc())
        .all()
    )
    standalone = (
        db.query(CustomerReward)
        .filter(CustomerReward.customer_id == customer.id)
        .filter(CustomerReward.customer_coupon_id.is_(None))
        .filter(CustomerReward.status.in_(("ISSUED", "RESERVED")))
        .all()
    )
    products_map = _wallet_products_map(db, customer.id)
    latest_by_coupon, latest_by_reward = _wallet_reservation_index(db, customer.id)
    live_coupons = []
    for c in coupons:
        status = _wallet_effective_status(c, now)
        if status not in {"ISSUED", "RESERVED"}:
            continue
        live_coupons.append(
            _wallet_coupon(
                db,
                c,
                status=status,
                products_map=products_map,
                now=now,
                latest_by_coupon=latest_by_coupon,
                latest_by_reward=latest_by_reward,
            )
        )
    live_rewards = []
    for r in standalone:
        status = _wallet_effective_status(r, now)
        if status not in {"ISSUED", "RESERVED"}:
            continue
        live_rewards.append(
            _wallet_standalone_reward(
                r,
                products_map,
                status=status,
                now=now,
                latest_by_reward=latest_by_reward,
            )
        )
    if (include or "").strip().lower() == "past":
        past_coupons = (
            db.query(CustomerCoupon)
            .filter(CustomerCoupon.customer_id == customer.id)
            .filter(
                or_(
                    CustomerCoupon.status.in_(("USED", "EXPIRED", "RESERVED")),
                    and_(
                        CustomerCoupon.status == "ISSUED",
                        CustomerCoupon.expires_at.isnot(None),
                        CustomerCoupon.expires_at <= now,
                    ),
                )
            )
            .all()
        )
        past_rewards = (
            db.query(CustomerReward)
            .filter(CustomerReward.customer_id == customer.id)
            .filter(CustomerReward.customer_coupon_id.is_(None))
            .filter(
                or_(
                    CustomerReward.status.in_(("USED", "EXPIRED", "RESERVED")),
                    and_(
                        CustomerReward.status == "ISSUED",
                        CustomerReward.expires_at.isnot(None),
                        CustomerReward.expires_at <= now,
                    ),
                )
            )
            .all()
        )
        def _recency(row):
            return getattr(row, "used_at", None) or getattr(row, "expires_at", None) or getattr(row, "issued_at", None) or datetime.min

        past_coupons.sort(key=_recency, reverse=True)
        past_rewards.sort(key=_recency, reverse=True)
        seen_c = {c["id"] for c in live_coupons}
        extra_c = []
        for row in past_coupons:
            status = _wallet_effective_status(row, now)
            if status not in {"USED", "EXPIRED"}:
                continue
            item = _wallet_coupon(
                db,
                row,
                status=status,
                products_map=products_map,
                now=now,
                latest_by_coupon=latest_by_coupon,
                latest_by_reward=latest_by_reward,
            )
            if item["id"] in seen_c:
                continue
            extra_c.append(item)
            if len(extra_c) >= 20:
                break
        seen_r = {r["id"] for r in live_rewards}
        extra_r = []
        for row in past_rewards:
            status = _wallet_effective_status(row, now)
            if status not in {"USED", "EXPIRED"}:
                continue
            rid = str(row.id)
            if rid in seen_r:
                continue
            extra_r.append(
                _wallet_standalone_reward(
                    row,
                    products_map,
                    status=status,
                    now=now,
                    latest_by_reward=latest_by_reward,
                )
            )
            if len(extra_r) >= 20:
                break
        live_coupons.extend(extra_c)
        live_rewards.extend(extra_r)
    payload = {
        "profileId": customer.profile_id,
        "tier": _tier_brief(current),
        "coupons": live_coupons,
        "rewards": live_rewards,
    }
    persist_storefront_session(db, request)
    return payload


@router.get("/customers/{profile_id}/summary")
def storefront_summary(
    profile_id: str,
    request: Request,
    email: str | None = Query(default=None),
    client: ApiClient = Depends(require_storefront_client),
    db: Session = Depends(get_db),
):
    customer = resolve_storefront_customer(db, brand=client.brand, profile_id=profile_id, email=email)
    mode = get_qualification_mode(db, brand=client.brand)
    purchase_count, purchase_amount = get_purchase_balances(db, customer.id)
    sp = int(customer.status_points or 0)
    tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == client.brand, LoyaltyTier.active.is_(True))
        .order_by(LoyaltyTier.rank.asc())
        .all()
    )
    current = next((t for t in tiers if t.key == customer.loyalty_status), None)
    next_tier = None
    progress: dict = {}
    if mode == "PURCHASES":
        for t in tiers:
            need_count = int(getattr(t, "min_purchase_count", 0) or 0)
            need_amount = int(getattr(t, "min_purchase_amount", 0) or 0)
            if need_count > purchase_count or need_amount > purchase_amount:
                if current is None or int(t.rank) > int(current.rank):
                    next_tier = t
                    progress = {
                        "purchaseCount": purchase_count,
                        "purchaseAmountTotal": purchase_amount,
                        "purchasesToNextTier": max(0, need_count - purchase_count),
                        "amountToNextTier": max(0, need_amount - purchase_amount),
                    }
                    break
        if not progress:
            progress = {"purchaseCount": purchase_count, "purchaseAmountTotal": purchase_amount}
    else:
        current_min = int(current.min_status_points) if current else None
        if current_min is not None:
            next_tier = next((t for t in tiers if int(t.min_status_points) > current_min), None)
        points_to_next = None
        if next_tier:
            points_to_next = max(0, int(next_tier.min_status_points) - sp)
        progress = {
            "statusPoints": sp,
            "pointsToNextTier": points_to_next,
            "pointsExpiresAt": resolve_points_expires_at(db, customer.id),
        }

    available_coupons = (
        db.query(CustomerCoupon)
        .filter(CustomerCoupon.customer_id == customer.id, CustomerCoupon.status == "ISSUED")
        .count()
    )
    available_rewards = (
        db.query(CustomerReward)
        .filter(
            CustomerReward.customer_id == customer.id,
            CustomerReward.customer_coupon_id.is_(None),
            CustomerReward.status == "ISSUED",
        )
        .count()
    )
    data = {
        "profileId": customer.profile_id,
        "qualificationMode": mode,
        "tier": _tier_brief(current, customer),
        "nextTier": _tier_brief(next_tier),
        "progress": progress,
        "counts": {"availableCoupons": int(available_coupons or 0), "availableRewards": int(available_rewards or 0)},
        "earnedTier": compute_loyalty_status_from_tiers(
            db,
            client.brand,
            sp,
            purchase_count=purchase_count,
            purchase_amount=purchase_amount,
            qualification_mode=mode,
        ),
    }
    if mode != "PURCHASES":
        data["pointsBalance"] = get_status_points_balance(db, customer.id)
    persist_storefront_session(db, request)
    return data


@router.get("/customers/{profile_id}/history")
def storefront_history(
    profile_id: str,
    request: Request,
    email: str | None = Query(default=None),
    limit: int = 20,
    offset: int = 0,
    client: ApiClient = Depends(require_storefront_client),
    db: Session = Depends(get_db),
):
    customer = resolve_storefront_customer(db, brand=client.brand, profile_id=profile_id, email=email)
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    profile_ids = list_customer_unomi_profile_ids(db, customer)
    q = (
        db.query(Transaction)
        .filter(Transaction.brand == client.brand)
        .filter(Transaction.transaction_type.in_(_HISTORY_TYPES))
        .filter(
            or_(
                Transaction.customer_id == customer.id,
                and_(
                    Transaction.customer_id.is_(None),
                    Transaction.profile_id.in_(profile_ids or [customer.profile_id]),
                ),
            )
        )
    )
    total = q.count()
    items = q.order_by(Transaction.created_at.desc()).offset(offset).limit(limit).all()
    payload = {
        "profileId": customer.profile_id,
        "count": total,
        "items": [serialize_loyalty_history_item(db, tx, brand=client.brand) for tx in items],
    }
    persist_storefront_session(db, request)
    return payload


@router.post("/redemptions/validate")
def storefront_validate(
    body: ValidateIn,
    request: Request,
    client: ApiClient = Depends(require_storefront_client),
    db: Session = Depends(get_db),
):
    enforce_storefront_rate_limit(client)
    customer = resolve_storefront_customer(
        db, brand=client.brand, profile_id=body.profileId, email=body.email
    )
    cart = _cart_dict(body.cart)
    exclude_coupon_id, exclude_reward_id = lookup_offer_ids_for_exclusion(
        db,
        brand=client.brand,
        code=body.code,
        customer_reward_id=body.customerRewardId,
    )
    applied = applied_benefits_for_cart(
        db,
        brand=client.brand,
        customer_id=customer.id,
        cart_ref=cart.get("cartRef"),
        extra_ids=body.appliedRedemptionIds,
        exclude_coupon_id=exclude_coupon_id,
        exclude_customer_reward_id=exclude_reward_id,
    )
    result = evaluate_coupon_or_reward(
        db,
        brand=client.brand,
        customer=customer,
        code=body.code,
        customer_reward_id=body.customerRewardId,
        cart=cart,
        applied_benefits=applied,
    )
    out = {
        "applicable": bool(result.get("applicable")),
        "benefits": result.get("benefits") or [],
        "reasons": result.get("reasons") or [],
    }
    record_storefront_audit(
        db,
        brand=client.brand,
        site_id=str(client.id),
        action="validate",
        profile_id=customer.profile_id,
        request=body.model_dump() if hasattr(body, "model_dump") else None,
        response=out,
    )
    persist_storefront_session(db, request, force=True)
    return out


@router.post("/redemptions/reserve")
def storefront_reserve(
    body: ReserveIn,
    request: Request,
    client: ApiClient = Depends(require_storefront_client),
    db: Session = Depends(get_db),
):
    enforce_storefront_rate_limit(client)
    idempotency_key = require_idempotency_key(request)
    customer = resolve_storefront_customer(
        db, brand=client.brand, profile_id=body.profileId, email=body.email
    )
    cart = _cart_dict(body.cart)
    exclude_coupon_id, exclude_reward_id = lookup_offer_ids_for_exclusion(
        db,
        brand=client.brand,
        code=body.code,
        customer_reward_id=body.customerRewardId,
    )
    applied = applied_benefits_for_cart(
        db,
        brand=client.brand,
        customer_id=customer.id,
        cart_ref=cart.get("cartRef"),
        extra_ids=body.appliedRedemptionIds,
        exclude_coupon_id=exclude_coupon_id,
        exclude_customer_reward_id=exclude_reward_id,
    )
    result = reserve_redemption(
        db,
        brand=client.brand,
        site_id=str(client.id),
        customer=customer,
        code=body.code,
        customer_reward_id=body.customerRewardId,
        cart=cart,
        ttl_minutes=body.ttlMinutes,
        idempotency_key=f"{client.id}:{idempotency_key}",
        applied_benefits=applied,
    )
    record_storefront_audit(
        db,
        brand=client.brand,
        site_id=str(client.id),
        action="reserve",
        profile_id=customer.profile_id,
        redemption_id=str(result.get("redemptionId")) if result.get("redemptionId") else None,
        request=body.model_dump() if hasattr(body, "model_dump") else None,
        response=_audit_redemption_response(result),
    )
    db.commit()
    return result


@router.post("/redemptions/{redemption_id}/commit")
def storefront_commit(
    redemption_id: str,
    body: CommitIn,
    request: Request,
    client: ApiClient = Depends(require_storefront_client),
    db: Session = Depends(get_db),
):
    idempotency_key = require_idempotency_key(request)
    result = commit_redemption(
        db,
        redemption_id=redemption_id,
        order_ref=body.orderRef,
        idempotency_key=f"{client.id}:{idempotency_key}",
        brand=client.brand,
        site_id=str(client.id),
    )
    record_storefront_audit(
        db,
        brand=client.brand,
        site_id=str(client.id),
        action="commit",
        profile_id=None,
        redemption_id=redemption_id,
        request={"orderRef": body.orderRef},
        response=_audit_redemption_response(result),
    )
    db.commit()
    return result


@router.post("/redemptions/{redemption_id}/release")
def storefront_release(
    redemption_id: str,
    body: ReleaseIn,
    request: Request,
    client: ApiClient = Depends(require_storefront_client),
    db: Session = Depends(get_db),
):
    result = release_redemption(
        db,
        redemption_id=redemption_id,
        reason=body.reason,
        brand=client.brand,
        site_id=str(client.id),
    )
    record_storefront_audit(
        db,
        brand=client.brand,
        site_id=str(client.id),
        action="release",
        profile_id=None,
        redemption_id=redemption_id,
        request={"reason": body.reason},
        response=_audit_redemption_response(result),
    )
    db.commit()
    return result


@router.post("/customers/{profile_id}/anonymize")
def storefront_anonymize_customer(
    profile_id: str,
    request: Request,
    client: ApiClient = Depends(require_storefront_client),
    db: Session = Depends(get_db),
):
    """Anonymize a loyalty customer (opt-in API key scope storefront:anonymize)."""
    require_storefront_anonymize_scope(client)
    enforce_storefront_rate_limit(client)
    unomi_profile_id = None
    try:
        result = delete_loyalty_customer(
            db,
            brand=client.brand,
            profile_id=profile_id,
            skip_unomi=True,
            mode="anonymize",
        )
        if result.get("alreadyAnonymized"):
            db.rollback()
            return result
        if not result.get("deleted"):
            raise HTTPException(
                status_code=404,
                detail={"code": "CUSTOMER_NOT_FOUND", "message": "Customer not found"},
            )
        unomi_profile_id = result.pop("unomiProfileId", None)
        record_storefront_audit(
            db,
            brand=client.brand,
            site_id=str(client.id),
            action="anonymize",
            profile_id=profile_id,
            redemption_id=None,
            request={},
            response={"mode": "anonymize", "counts": result.get("counts")},
        )
        db.commit()
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=502, detail=f"Customer anonymize failed: {e}") from e

    unomi = finish_unomi_delete_after_commit(
        db,
        brand=client.brand,
        profile_id=unomi_profile_id,
        skip_unomi=False,
    )
    if unomi is not None:
        result["unomi"] = unomi
    return result
