from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.product import Product
from app.models.redemption import Redemption
from app.models.reward import Reward
from app.models.reward_product import RewardProduct
from app.services.benefit_service import (
    DISCOUNT_TYPES,
    SHIPPING_TYPES,
    benefit_scope,
    compute_benefit,
    describe_benefit,
    storefront_product_ref,
)
from app.services.contact_service import resolve_customer_for_lookup
from app.services.loyalty_settings_service import get_loyalty_settings


def resolve_storefront_customer(
    db: Session, *, brand: str, profile_id: str, email: str | None = None
) -> Customer:
    customer, _ = resolve_customer_for_lookup(
        db, brand=brand, profile_id=profile_id, email=email
    )
    if not customer:
        raise HTTPException(status_code=404, detail={"code": "CUSTOMER_NOT_FOUND", "message": "Customer not found"})
    return customer


def _reward_scope(db: Session, reward: Reward | None) -> tuple[set[str], set[str]]:
    if reward is None:
        return set(), set()
    links = db.query(RewardProduct).filter(RewardProduct.reward_id == reward.id).all()
    product_ids: set[str] = set()
    for link in links:
        product_ids.add(str(link.product_id))
        product = db.query(Product).filter(Product.id == link.product_id).first()
        if product is not None and getattr(product, "match_key", None):
            product_ids.add(str(product.match_key))
    return product_ids, set()


def _snapshot_from_reward(reward: Reward | None) -> dict:
    if reward is None:
        return {
            "benefitType": "CUSTOM",
            "benefitValue": None,
            "maxDiscountAmount": None,
            "minCartAmount": None,
            "appliesTo": "CART",
            "freeProductQuantity": 1,
            "stackable": False,
        }
    return {
        "benefitType": getattr(reward, "benefit_type", None) or "CUSTOM",
        "benefitValue": getattr(reward, "benefit_value", None),
        "maxDiscountAmount": getattr(reward, "max_discount_amount", None),
        "minCartAmount": getattr(reward, "min_cart_amount", None),
        "appliesTo": getattr(reward, "applies_to", None) or "CART",
        "freeProductQuantity": int(getattr(reward, "free_product_quantity", 1) or 1),
        "stackable": bool(getattr(reward, "stackable", False)),
        "name": reward.name,
        "id": str(reward.id),
    }


def coupon_reward_snapshots(db: Session, coupon: CustomerCoupon) -> list[dict]:
    rows = (
        db.query(CustomerReward)
        .filter(CustomerReward.customer_coupon_id == coupon.id)
        .all()
    )
    out = []
    for cr in rows:
        payload = cr.payload if isinstance(cr.payload, dict) else {}
        reward = db.query(Reward).filter(Reward.id == cr.reward_id).first() if cr.reward_id else None
        snap = payload.get("benefit") if isinstance(payload.get("benefit"), dict) else _snapshot_from_reward(reward)
        snap = dict(snap)
        snap["customerRewardId"] = str(cr.id)
        snap["name"] = snap.get("name") or (reward.name if reward else None)
        out.append(snap)
    if not out:
        out.append(_snapshot_from_reward(None))
    return out


def _count_discount_benefits(applied: list[dict]) -> int:
    n = 0
    for item in applied:
        btype = str((item or {}).get("type") or (item or {}).get("benefitType") or "")
        if btype in DISCOUNT_TYPES:
            n += 1
    return n


def _count_shipping_benefits(applied: list[dict]) -> int:
    n = 0
    for item in applied:
        btype = str((item or {}).get("type") or (item or {}).get("benefitType") or "")
        if btype in SHIPPING_TYPES:
            n += 1
    return n


def load_reward_products_map(db: Session, reward_ids: list) -> dict:
    ids = []
    for raw in reward_ids:
        if raw is None:
            continue
        try:
            ids.append(UUID(str(raw)))
        except Exception:
            continue
    if not ids:
        return {}
    links = db.query(RewardProduct).filter(RewardProduct.reward_id.in_(ids)).all()
    product_ids = [link.product_id for link in links]
    products = db.query(Product).filter(Product.id.in_(product_ids)).all() if product_ids else []
    by_id = {p.id: p for p in products}
    out: dict = {}
    for link in links:
        product = by_id.get(link.product_id)
        if product is None:
            continue
        ref = storefront_product_ref(product, int(link.quantity or 1))
        wallet_item = {
            "id": ref.get("productId") or ref.get("engineProductId") or str(product.id),
            "name": ref.get("name") or product.name,
            "quantity": int(link.quantity or 1),
        }
        out.setdefault(str(link.reward_id), []).append(wallet_item)
        out.setdefault(link.reward_id, []).append(ref)
    return out


def present_wallet_reward(snap: dict, products_map: dict) -> dict:
    btype = str(snap.get("benefitType") or "CUSTOM").upper()
    reward_id = str(snap.get("id") or "")
    products = []
    if reward_id:
        products = [
            item
            for item in (products_map.get(reward_id) or [])
            if isinstance(item, dict) and "id" in item
        ][:20]
    applies = str(snap.get("appliesTo") or "CART")
    return {
        "id": snap.get("customerRewardId"),
        "name": snap.get("name"),
        "benefitType": btype,
        "benefitValue": snap.get("benefitValue"),
        "maxDiscountAmount": snap.get("maxDiscountAmount"),
        "minCartAmount": snap.get("minCartAmount"),
        "scope": benefit_scope(btype, applies),
        "appliesTo": applies,
        "stackable": bool(snap.get("stackable")),
        "freeProductQuantity": int(snap.get("freeProductQuantity") or 1),
        "products": products,
        "description": describe_benefit(
            benefit_type=btype,
            benefit_value=snap.get("benefitValue"),
            max_discount_amount=snap.get("maxDiscountAmount"),
            min_cart_amount=snap.get("minCartAmount"),
            name=snap.get("name"),
            free_product_quantity=snap.get("freeProductQuantity"),
            applies_to=applies,
        ),
    }


def _reservation_is_stale(reserved_until, now: datetime | None = None) -> bool:
    now = now or datetime.utcnow()
    return reserved_until is None or reserved_until <= now


def _benefits_from_row(row: Redemption) -> list[dict]:
    raw = row.benefit
    if isinstance(raw, list):
        return [b for b in raw if isinstance(b, dict)]
    if isinstance(raw, dict):
        inner = raw.get("benefits")
        if isinstance(inner, list):
            return [b for b in inner if isinstance(b, dict)]
        return [raw]
    return []


def lookup_offer_ids_for_exclusion(
    db: Session,
    *,
    brand: str,
    code: str | None,
    customer_reward_id: str | None,
) -> tuple[UUID | None, UUID | None]:
    coupon_id = None
    reward_id = None
    if code and str(code).strip():
        coupon = (
            db.query(CustomerCoupon)
            .join(Customer, Customer.id == CustomerCoupon.customer_id)
            .filter(CustomerCoupon.code == str(code).strip().upper())
            .filter(Customer.brand == brand)
            .first()
        )
        if coupon is not None:
            coupon_id = coupon.id
    if customer_reward_id:
        try:
            reward_id = UUID(str(customer_reward_id))
        except Exception:
            reward_id = None
    return coupon_id, reward_id


def applied_benefits_for_cart(
    db: Session,
    *,
    brand: str,
    customer_id,
    cart_ref: str | None,
    extra_ids: list[str] | None = None,
    exclude_coupon_id=None,
    exclude_customer_reward_id=None,
) -> list[dict]:
    now = datetime.utcnow()
    rows: list[Redemption] = []
    if cart_ref:
        rows.extend(
            db.query(Redemption)
            .filter(Redemption.brand == brand)
            .filter(Redemption.customer_id == customer_id)
            .filter(Redemption.status == "RESERVED")
            .filter(Redemption.cart_ref == cart_ref)
            .all()
        )
    ids: list[UUID] = []
    for raw in extra_ids or []:
        try:
            ids.append(UUID(str(raw)))
        except Exception:
            continue
    if ids:
        extra = (
            db.query(Redemption)
            .filter(Redemption.brand == brand)
            .filter(Redemption.customer_id == customer_id)
            .filter(Redemption.status == "RESERVED")
            .filter(Redemption.id.in_(ids))
            .all()
        )
        seen = {r.id for r in rows}
        for row in extra:
            if row.id not in seen:
                rows.append(row)
    benefits: list[dict] = []
    for row in rows:
        if row.reserved_until is not None and row.reserved_until <= now:
            continue
        if exclude_coupon_id is not None and row.customer_coupon_id == exclude_coupon_id:
            continue
        if exclude_customer_reward_id is not None and row.customer_reward_id == exclude_customer_reward_id:
            continue
        benefits.extend(_benefits_from_row(row))
    return benefits


def _deny_locked_status(status: str, *, expired: bool) -> str | None:
    if status == "USED":
        return "ALREADY_USED"
    if status in {"EXPIRED", "INVALIDATED"} or expired:
        return "EXPIRED" if status != "INVALIDATED" else "INVALIDATED"
    return None


def evaluate_coupon_or_reward(
    db: Session,
    *,
    brand: str,
    customer: Customer,
    code: str | None,
    customer_reward_id: str | None,
    cart: dict,
    applied_benefits: list[dict] | None = None,
) -> dict:
    settings = get_loyalty_settings(db, brand=brand)
    max_discounts = int(getattr(settings, "max_discount_benefits_per_cart", 1) or 1) if settings else 1
    max_shipping = int(getattr(settings, "max_shipping_benefits_per_cart", 1) or 1) if settings else 1
    discount_base = getattr(settings, "discount_base", None) if settings else "SUBTOTAL_INCL_TAX"
    discount_base = discount_base or "SUBTOTAL_INCL_TAX"
    applied = applied_benefits or []
    now = datetime.utcnow()

    coupon = None
    customer_reward = None
    if code:
        coupon = (
            db.query(CustomerCoupon)
            .join(Customer, Customer.id == CustomerCoupon.customer_id)
            .filter(CustomerCoupon.code == code.strip().upper())
            .filter(Customer.brand == brand)
            .first()
        )
        if not coupon:
            return {"applicable": False, "benefits": [], "reasons": ["NOT_FOUND"]}
        owner = db.query(Customer).filter(Customer.id == coupon.customer_id).first()
        if owner is None or owner.brand != brand:
            return {"applicable": False, "benefits": [], "reasons": ["NOT_FOUND"]}
        if owner.id != customer.id and not bool(coupon.shareable):
            return {"applicable": False, "benefits": [], "reasons": ["NOT_OWNER"]}
        if coupon.expires_at is not None and coupon.expires_at <= now:
            return {"applicable": False, "benefits": [], "reasons": ["EXPIRED"]}
        if coupon.status == "USED":
            return {"applicable": False, "benefits": [], "reasons": ["ALREADY_USED"]}
        if coupon.status in {"EXPIRED", "INVALIDATED"}:
            return {"applicable": False, "benefits": [], "reasons": [coupon.status]}
        if coupon.status == "RESERVED" and not _reservation_is_stale(coupon.reserved_until, now):
            cart_ref = (cart or {}).get("cartRef")
            linked = (
                db.query(Redemption)
                .filter(Redemption.id == coupon.reserved_by_redemption_id)
                .first()
            )
            if linked and linked.cart_ref and linked.cart_ref != cart_ref:
                return {"applicable": False, "benefits": [], "reasons": ["RESERVED_ELSEWHERE"]}
        snapshots = coupon_reward_snapshots(db, coupon)
    elif customer_reward_id:
        try:
            rid = UUID(str(customer_reward_id))
        except Exception:
            return {"applicable": False, "benefits": [], "reasons": ["NOT_FOUND"]}
        customer_reward = db.query(CustomerReward).filter(CustomerReward.id == rid).first()
        if not customer_reward or customer_reward.customer_id != customer.id:
            return {"applicable": False, "benefits": [], "reasons": ["NOT_OWNER" if customer_reward else "NOT_FOUND"]}
        if customer_reward.expires_at is not None and customer_reward.expires_at <= now:
            return {"applicable": False, "benefits": [], "reasons": ["EXPIRED"]}
        if customer_reward.status == "USED":
            return {"applicable": False, "benefits": [], "reasons": ["ALREADY_USED"]}
        if customer_reward.status == "RESERVED" and not _reservation_is_stale(
            customer_reward.reserved_until, now
        ):
            return {"applicable": False, "benefits": [], "reasons": ["RESERVED_ELSEWHERE"]}
        reward = db.query(Reward).filter(Reward.id == customer_reward.reward_id).first() if customer_reward.reward_id else None
        snapshots = [_snapshot_from_reward(reward)]
        snapshots[0]["customerRewardId"] = str(customer_reward.id)
    else:
        return {"applicable": False, "benefits": [], "reasons": ["NOT_FOUND"]}

    benefits = []
    reasons: list[str] = []
    discount_so_far = _count_discount_benefits(applied)
    shipping_so_far = _count_shipping_benefits(applied)
    products_map = load_reward_products_map(db, [snap.get("id") for snap in snapshots])
    for snap in snapshots:
        btype = str(snap.get("benefitType") or "CUSTOM").upper()
        stackable = bool(snap.get("stackable"))
        if btype in DISCOUNT_TYPES and discount_so_far >= max_discounts and not stackable:
            reasons.append("NOT_STACKABLE")
            continue
        if btype in SHIPPING_TYPES and shipping_so_far >= max_shipping and not stackable:
            reasons.append("NOT_STACKABLE")
            continue
        reward = None
        if snap.get("id"):
            try:
                reward = db.query(Reward).filter(Reward.id == UUID(str(snap["id"]))).first()
            except Exception:
                reward = None
        product_ids, category_ids = _reward_scope(db, reward)
        product_refs = []
        if snap.get("id"):
            try:
                product_refs = [
                    item
                    for item in (products_map.get(UUID(str(snap["id"]))) or [])
                    if isinstance(item, dict) and "productId" in item
                ]
            except Exception:
                product_refs = []
        benefit, reason = compute_benefit(
            benefit_type=btype,
            benefit_value=snap.get("benefitValue"),
            max_discount_amount=snap.get("maxDiscountAmount"),
            min_cart_amount=snap.get("minCartAmount"),
            applies_to=str(snap.get("appliesTo") or "CART"),
            free_product_quantity=int(snap.get("freeProductQuantity") or 1),
            cart=cart or {},
            product_ids=product_ids,
            category_ids=category_ids,
            discount_base=discount_base,
            name=snap.get("name"),
            product_refs=product_refs or None,
        )
        if reason:
            reasons.append(reason)
            continue
        if benefit:
            benefits.append(benefit)
            if btype in DISCOUNT_TYPES:
                discount_so_far += 1
            if btype in SHIPPING_TYPES:
                shipping_so_far += 1

    if not benefits:
        return {"applicable": False, "benefits": [], "reasons": reasons or ["NOT_FOUND"]}
    return {
        "applicable": True,
        "benefits": benefits,
        "reasons": [],
        "coupon": coupon,
        "customerReward": customer_reward,
    }


def expire_stale_reservations(db: Session, *, brand: str | None = None) -> int:
    now = datetime.utcnow()
    q = db.query(Redemption).filter(Redemption.status == "RESERVED").filter(Redemption.reserved_until <= now)
    if brand:
        q = q.filter(Redemption.brand == brand)
    rows = q.all()
    released = 0
    for row in rows:
        _release_reservation(db, row, reason="ttl_expired")
        released += 1
    db.flush()
    return released


def _release_reservation(db: Session, row: Redemption, *, reason: str) -> None:
    if row.customer_coupon_id:
        coupon = (
            db.query(CustomerCoupon)
            .filter(CustomerCoupon.id == row.customer_coupon_id)
            .with_for_update()
            .first()
        )
        if coupon and coupon.status == "RESERVED":
            expired = coupon.expires_at is not None and coupon.expires_at <= datetime.utcnow()
            coupon.status = "EXPIRED" if expired else "ISSUED"
            coupon.reserved_until = None
            coupon.reserved_by_redemption_id = None
            for cr in db.query(CustomerReward).filter(CustomerReward.customer_coupon_id == coupon.id).all():
                if cr.status == "RESERVED":
                    cr.status = "ISSUED"
                    cr.reserved_until = None
                    cr.reserved_by_redemption_id = None
    if row.customer_reward_id:
        reward = (
            db.query(CustomerReward)
            .filter(CustomerReward.id == row.customer_reward_id)
            .with_for_update()
            .first()
        )
        if reward and reward.status == "RESERVED":
            reward.status = "ISSUED"
            reward.reserved_until = None
            reward.reserved_by_redemption_id = None
    row.status = "EXPIRED" if reason == "ttl_expired" else "RELEASED"
    row.updated_at = datetime.utcnow()


def reserve(
    db: Session,
    *,
    brand: str,
    site_id: str,
    customer: Customer,
    code: str | None,
    customer_reward_id: str | None,
    cart: dict,
    ttl_minutes: int,
    idempotency_key: str,
    applied_benefits: list[dict] | None = None,
) -> dict:
    existing = db.query(Redemption).filter(Redemption.idempotency_key == idempotency_key).first()
    if existing:
        return _redemption_payload(existing)

    ttl_minutes = max(1, min(int(ttl_minutes or 30), 120))
    evaluation = evaluate_coupon_or_reward(
        db,
        brand=brand,
        customer=customer,
        code=code,
        customer_reward_id=customer_reward_id,
        cart=cart or {},
        applied_benefits=applied_benefits,
    )
    if not evaluation.get("applicable"):
        return {"applicable": False, "benefits": [], "reasons": evaluation.get("reasons") or []}

    coupon: CustomerCoupon | None = evaluation.get("coupon")
    customer_reward: CustomerReward | None = evaluation.get("customerReward")
    now = datetime.utcnow()
    reserved_until = now + timedelta(minutes=ttl_minutes)
    cart_ref = (cart or {}).get("cartRef")

    if coupon:
        coupon = (
            db.query(CustomerCoupon)
            .filter(CustomerCoupon.id == coupon.id)
            .with_for_update()
            .one()
        )
        expired = coupon.expires_at is not None and coupon.expires_at <= now
        locked_reason = _deny_locked_status(coupon.status, expired=expired)
        if locked_reason:
            return {"applicable": False, "benefits": [], "reasons": [locked_reason]}
        if coupon.status == "RESERVED":
            previous = (
                db.query(Redemption).filter(Redemption.id == coupon.reserved_by_redemption_id).first()
                if coupon.reserved_by_redemption_id
                else None
            )
            stale = _reservation_is_stale(coupon.reserved_until, now)
            if previous and previous.status == "RESERVED" and stale:
                previous.status = "EXPIRED"
            elif previous and previous.status == "RESERVED" and previous.cart_ref == cart_ref:
                previous.status = "RELEASED"
            elif not stale:
                raise HTTPException(status_code=409, detail={"code": "RESERVED_ELSEWHERE", "reasons": ["RESERVED_ELSEWHERE"]})
        elif coupon.status != "ISSUED":
            return {"applicable": False, "benefits": [], "reasons": [coupon.status]}
        coupon.status = "RESERVED"
        coupon.reserved_until = reserved_until

    if customer_reward:
        customer_reward = (
            db.query(CustomerReward)
            .filter(CustomerReward.id == customer_reward.id)
            .with_for_update()
            .one()
        )
        expired = customer_reward.expires_at is not None and customer_reward.expires_at <= now
        locked_reason = _deny_locked_status(customer_reward.status, expired=expired)
        if locked_reason:
            return {"applicable": False, "benefits": [], "reasons": [locked_reason]}
        if customer_reward.status == "RESERVED":
            previous = (
                db.query(Redemption).filter(Redemption.id == customer_reward.reserved_by_redemption_id).first()
                if customer_reward.reserved_by_redemption_id
                else None
            )
            stale = _reservation_is_stale(customer_reward.reserved_until, now)
            if previous and previous.status == "RESERVED" and stale:
                previous.status = "EXPIRED"
            elif previous and previous.status == "RESERVED" and previous.cart_ref == cart_ref:
                previous.status = "RELEASED"
            elif not stale:
                raise HTTPException(status_code=409, detail={"code": "RESERVED_ELSEWHERE", "reasons": ["RESERVED_ELSEWHERE"]})
        elif customer_reward.status != "ISSUED":
            return {"applicable": False, "benefits": [], "reasons": [customer_reward.status]}
        customer_reward.status = "RESERVED"
        customer_reward.reserved_until = reserved_until

    row = Redemption(
        brand=brand,
        customer_id=customer.id,
        customer_coupon_id=coupon.id if coupon else None,
        customer_reward_id=customer_reward.id if customer_reward else None,
        site_id=site_id,
        cart_ref=cart_ref,
        status="RESERVED",
        benefit=evaluation.get("benefits"),
        currency=(cart or {}).get("currency"),
        cart_snapshot=cart,
        idempotency_key=idempotency_key,
        reserved_until=reserved_until,
    )
    db.add(row)
    db.flush()
    if coupon:
        coupon.reserved_by_redemption_id = row.id
    if customer_reward:
        customer_reward.reserved_by_redemption_id = row.id
    db.flush()
    return _redemption_payload(row, extra={"applicable": True, "benefits": evaluation.get("benefits")})


def commit(
    db: Session,
    *,
    redemption_id: str,
    order_ref: str,
    idempotency_key: str,
    brand: str,
    site_id: str | None = None,
) -> dict:
    existing = (
        db.query(Redemption)
        .filter(Redemption.idempotency_key == idempotency_key)
        .filter(Redemption.brand == brand)
        .first()
    )
    try:
        rid = UUID(str(redemption_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Redemption not found")
    row = (
        db.query(Redemption)
        .filter(Redemption.id == rid)
        .filter(Redemption.brand == brand)
        .with_for_update()
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Redemption not found")
    if row.status == "COMMITTED":
        return _redemption_payload(row)
    if existing and existing.id != row.id and existing.status == "COMMITTED":
        return _redemption_payload(existing)
    if row.status != "RESERVED":
        raise HTTPException(status_code=409, detail=f"Cannot commit redemption in status {row.status}")
    now = datetime.utcnow()
    if row.customer_coupon_id:
        coupon = db.query(CustomerCoupon).filter(CustomerCoupon.id == row.customer_coupon_id).with_for_update().one()
        coupon.status = "USED"
        coupon.used_at = now
        coupon.reserved_until = None
        for cr in db.query(CustomerReward).filter(CustomerReward.customer_coupon_id == coupon.id).all():
            cr.status = "USED"
            cr.used_at = now
    if row.customer_reward_id:
        reward = db.query(CustomerReward).filter(CustomerReward.id == row.customer_reward_id).with_for_update().one()
        reward.status = "USED"
        reward.used_at = now
        reward.reserved_until = None
    row.status = "COMMITTED"
    row.order_ref = order_ref
    row.updated_at = now
    if not row.idempotency_key:
        row.idempotency_key = idempotency_key
    if row.customer_coupon_id:
        from app.models.customer import Customer
        from app.services.webhook_service import enqueue_webhook_event

        owner = db.query(Customer).filter(Customer.id == row.customer_id).first()
        enqueue_webhook_event(
            db,
            brand=row.brand,
            event="coupon.used",
            payload={
                "couponId": str(row.customer_coupon_id),
                "code": coupon.code if row.customer_coupon_id else None,
                "profileId": owner.profile_id if owner else None,
                "orderRef": order_ref,
                "redemptionId": str(row.id),
            },
        )
    db.flush()
    return _redemption_payload(row)


def release(
    db: Session,
    *,
    redemption_id: str,
    reason: str,
    brand: str,
    site_id: str | None = None,
) -> dict:
    try:
        rid = UUID(str(redemption_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Redemption not found")
    row = (
        db.query(Redemption)
        .filter(Redemption.id == rid)
        .filter(Redemption.brand == brand)
        .with_for_update()
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Redemption not found")
    if row.status == "RELEASED":
        return _redemption_payload(row)
    if row.status == "COMMITTED" and reason not in {"order_cancelled", "order_refunded"}:
        raise HTTPException(status_code=409, detail="Committed redemption can only be released for cancel/refund")
    if row.status == "COMMITTED":
        if row.customer_coupon_id:
            coupon = db.query(CustomerCoupon).filter(CustomerCoupon.id == row.customer_coupon_id).with_for_update().one()
            coupon.status = "ISSUED"
            coupon.used_at = None
            coupon.reserved_until = None
            coupon.reserved_by_redemption_id = None
            for cr in db.query(CustomerReward).filter(CustomerReward.customer_coupon_id == coupon.id).all():
                if cr.status in {"USED", "RESERVED"}:
                    cr.status = "ISSUED"
                    cr.used_at = None
                    cr.reserved_until = None
                    cr.reserved_by_redemption_id = None
        if row.customer_reward_id:
            reward = db.query(CustomerReward).filter(CustomerReward.id == row.customer_reward_id).with_for_update().one()
            reward.status = "ISSUED"
            reward.used_at = None
            reward.reserved_until = None
            reward.reserved_by_redemption_id = None
        row.status = "RELEASED"
        payload = row.benefit if isinstance(row.benefit, dict) else {}
        if not isinstance(row.benefit, dict):
            row.benefit = {"benefits": row.benefit, "releaseReason": reason}
        else:
            payload = dict(row.benefit)
            payload["releaseReason"] = reason
            row.benefit = payload
        db.flush()
        return _redemption_payload(row)
    _release_reservation(db, row, reason=reason)
    db.flush()
    return _redemption_payload(row)


def _redemption_payload(row: Redemption, extra: dict | None = None) -> dict:
    data = {
        "redemptionId": str(row.id),
        "status": row.status,
        "reservedUntil": row.reserved_until,
        "orderRef": row.order_ref,
        "benefits": row.benefit if isinstance(row.benefit, list) else (row.benefit or {}).get("benefits") or row.benefit,
        "applicable": row.status in {"RESERVED", "COMMITTED"},
        "reasons": [],
    }
    if extra:
        data.update(extra)
    return data
