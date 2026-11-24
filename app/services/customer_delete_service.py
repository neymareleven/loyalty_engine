"""Anonymize or erase loyalty customers; Unomi profile delete after DB commit."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from sqlalchemy.orm import Session

from app.models.cash_movement import CashMovement
from app.models.customer import Customer
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_loyalty_guarantee import GUARANTEE_BYPASS, CustomerLoyaltyGuarantee
from app.models.customer_metrics import CustomerMetrics
from app.models.customer_reward import CustomerReward
from app.models.customer_unomi_profile_alias import CustomerUnomiProfileAlias
from app.models.erased_identity import ErasedIdentity, UnomiDeleteQueue
from app.models.point_movement import PointMovement
from app.models.purchase_movement import PurchaseMovement
from app.models.redemption import Redemption
from app.models.segment_member import SegmentMember
from app.models.storefront_audit_event import StorefrontAuditEvent
from app.models.transaction import Transaction
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.models.webhook_outbox import WebhookOutbox
from app.services.customer_identity_service import (
    ANONYMIZED_STATUS,
    customer_is_anonymized,
    email_identity_hash,
    scrub_pii,
)
from app.services.redemption_service import _release_reservation
from app.services.unomi_profile_service import delete_profile_from_unomi, reset_profile_sync_source, set_profile_sync_source

logger = logging.getLogger(__name__)

_EMPTY_COUNTS = {
    "transactions": 0,
    "redemptions": 0,
    "customer_coupons": 0,
    "customer_rewards": 0,
    "point_movements": 0,
    "purchase_movements": 0,
    "cash_movements": 0,
    "guarantees": 0,
    "aliases": 0,
    "segment_members": 0,
    "storefront_audit_events": 0,
    "webhook_outbox": 0,
}


def _bump(counts: dict, key: str, n: int = 1) -> None:
    counts[key] = int(counts.get(key) or 0) + int(n)


def _bypass_used_for_customer(db: Session, customer: Customer) -> bool:
    g = (
        db.query(CustomerLoyaltyGuarantee.id)
        .filter(CustomerLoyaltyGuarantee.customer_id == customer.id)
        .filter(CustomerLoyaltyGuarantee.source == GUARANTEE_BYPASS)
        .first()
    )
    if g:
        return True
    tx = (
        db.query(Transaction.id)
        .filter(Transaction.customer_id == customer.id)
        .filter(Transaction.transaction_type == GUARANTEE_BYPASS)
        .first()
    )
    return bool(tx)


def _record_erased_identity(
    db: Session,
    *,
    brand: str,
    email: str | None,
    bypass_used: bool,
    mode: str,
) -> None:
    digest = email_identity_hash(email)
    if not digest:
        return
    existing = (
        db.query(ErasedIdentity)
        .filter(ErasedIdentity.brand == brand)
        .filter(ErasedIdentity.email_hash == digest)
        .first()
    )
    if existing:
        if bypass_used:
            existing.bypass_used = True
        existing.mode = mode
        existing.erased_at = datetime.utcnow()
        return
    db.add(
        ErasedIdentity(
            brand=brand,
            email_hash=digest,
            bypass_used=bypass_used,
            mode=mode,
        )
    )


def _enqueue_unomi_delete(db: Session, *, brand: str, profile_id: str) -> None:
    pending = (
        db.query(UnomiDeleteQueue)
        .filter(UnomiDeleteQueue.brand == brand)
        .filter(UnomiDeleteQueue.profile_id == profile_id)
        .filter(UnomiDeleteQueue.status == "PENDING")
        .first()
    )
    if pending:
        return
    db.add(UnomiDeleteQueue(brand=brand, profile_id=profile_id, status="PENDING"))


def retry_pending_unomi_deletes(db: Session, *, brand: str | None = None, limit: int = 50) -> int:
    q = db.query(UnomiDeleteQueue).filter(UnomiDeleteQueue.status == "PENDING")
    if brand:
        q = q.filter(UnomiDeleteQueue.brand == brand)
    rows = q.order_by(UnomiDeleteQueue.created_at.asc()).limit(limit).all()
    done = 0
    for row in rows:
        token = set_profile_sync_source("loyalty")
        try:
            delete_profile_from_unomi(brand=row.brand, profile_id=row.profile_id)
            row.status = "DONE"
            done += 1
        except Exception as exc:
            row.attempts = int(row.attempts or 0) + 1
            row.last_error = str(exc)[:2000]
            logger.exception(
                "unomi profile delete retry failed brand=%s attempts=%s",
                row.brand,
                row.attempts,
            )
            if row.attempts >= 8:
                row.status = "FAILED"
        finally:
            reset_profile_sync_source(token)
    if rows:
        db.flush()
    return done


def finish_unomi_delete_after_commit(
    db: Session,
    *,
    brand: str,
    profile_id: str | None,
    skip_unomi: bool,
) -> dict | None:
    if skip_unomi or not profile_id:
        return None
    token = set_profile_sync_source("loyalty")
    try:
        return delete_profile_from_unomi(brand=brand, profile_id=profile_id)
    except Exception as exc:
        logger.exception("unomi profile delete failed after commit brand=%s", brand)
        _enqueue_unomi_delete(db, brand=brand, profile_id=profile_id)
        db.commit()
        return {"ok": False, "queued": True, "error": str(exc)[:500]}
    finally:
        reset_profile_sync_source(token)


def _scrub_customer_json(db: Session, customer: Customer, anon_profile_id: str) -> dict:
    counts = dict(_EMPTY_COUNTS)
    txs = db.query(Transaction).filter(Transaction.customer_id == customer.id).all()
    for tx in txs:
        if isinstance(tx.payload, dict):
            tx.payload = scrub_pii(tx.payload, anon_profile_id=anon_profile_id)
        tx.profile_id = anon_profile_id
        _bump(counts, "transactions")
    redemptions = db.query(Redemption).filter(Redemption.customer_id == customer.id).all()
    for row in redemptions:
        if isinstance(row.cart_snapshot, dict):
            row.cart_snapshot = scrub_pii(row.cart_snapshot, anon_profile_id=anon_profile_id)
        if isinstance(row.benefit, (dict, list)):
            row.benefit = scrub_pii(row.benefit, anon_profile_id=anon_profile_id)
        _bump(counts, "redemptions")
    old_ids = [customer.profile_id]
    for (pid,) in (
        db.query(CustomerUnomiProfileAlias.profile_id)
        .filter(CustomerUnomiProfileAlias.customer_id == customer.id)
        .all()
    ):
        if pid:
            old_ids.append(pid)
    audits = (
        db.query(StorefrontAuditEvent)
        .filter(StorefrontAuditEvent.brand == customer.brand)
        .filter(StorefrontAuditEvent.profile_id.in_(old_ids))
        .all()
    )
    for ev in audits:
        ev.profile_id = anon_profile_id
        if isinstance(ev.request, dict):
            ev.request = scrub_pii(ev.request, anon_profile_id=anon_profile_id)
        if isinstance(ev.response, dict):
            ev.response = scrub_pii(ev.response, anon_profile_id=anon_profile_id)
        _bump(counts, "storefront_audit_events")
    outbox = (
        db.query(WebhookOutbox)
        .filter(WebhookOutbox.brand == customer.brand)
        .filter(WebhookOutbox.status == "PENDING")
        .all()
    )
    for item in outbox:
        payload = item.payload if isinstance(item.payload, dict) else {}
        pid = str(payload.get("profileId") or "")
        if pid in old_ids or str(payload.get("customerId") or "") == str(customer.id):
            item.payload = scrub_pii(payload, anon_profile_id=anon_profile_id)
            _bump(counts, "webhook_outbox")
    return counts


def _anonymize_locked(db: Session, customer: Customer, *, mode: str) -> dict:
    now = datetime.utcnow()
    old_profile_id = customer.profile_id
    email = customer.email
    bypass_used = _bypass_used_for_customer(db, customer)
    _record_erased_identity(
        db, brand=customer.brand, email=email, bypass_used=bypass_used, mode=mode
    )

    counts = dict(_EMPTY_COUNTS)
    reserved = (
        db.query(Redemption)
        .filter(Redemption.customer_id == customer.id)
        .filter(Redemption.status == "RESERVED")
        .all()
    )
    for row in reserved:
        _release_reservation(db, row, reason="customer_anonymized")
        _bump(counts, "redemptions")

    coupons = db.query(CustomerCoupon).filter(CustomerCoupon.customer_id == customer.id).all()
    for coupon in coupons:
        if coupon.status in {"ISSUED", "RESERVED"}:
            coupon.status = "INVALIDATED"
            coupon.reserved_until = None
            coupon.reserved_by_redemption_id = None
            _bump(counts, "customer_coupons")
    rewards = db.query(CustomerReward).filter(CustomerReward.customer_id == customer.id).all()
    for reward in rewards:
        if reward.status in {"ISSUED", "RESERVED"}:
            reward.status = "CANCELLED"
            reward.reserved_until = None
            reward.reserved_by_redemption_id = None
            _bump(counts, "customer_rewards")

    guarantees = (
        db.query(CustomerLoyaltyGuarantee)
        .filter(CustomerLoyaltyGuarantee.customer_id == customer.id)
        .filter(CustomerLoyaltyGuarantee.revoked_at.is_(None))
        .all()
    )
    for g in guarantees:
        g.revoked_at = now
        _bump(counts, "guarantees")

    aliases = (
        db.query(CustomerUnomiProfileAlias)
        .filter(CustomerUnomiProfileAlias.customer_id == customer.id)
        .delete(synchronize_session=False)
    )
    _bump(counts, "aliases", aliases or 0)
    segs = (
        db.query(SegmentMember)
        .filter(SegmentMember.customer_id == customer.id)
        .delete(synchronize_session=False)
    )
    _bump(counts, "segment_members", segs or 0)

    anon_profile_id = f"anon_{uuid.uuid4()}"
    json_counts = _scrub_customer_json(db, customer, anon_profile_id)
    for key, n in json_counts.items():
        if n:
            counts[key] = max(int(counts.get(key) or 0), n)

    customer.email = None
    customer.gender = None
    customer.birthdate = None
    customer.birth_day = None
    customer.birth_month = None
    customer.birth_year = None
    customer.status = ANONYMIZED_STATUS
    customer.anonymized_at = now
    customer.email_hash = email_identity_hash(email)
    customer.profile_id = anon_profile_id
    db.flush()
    return {
        "mode": "anonymize",
        "alreadyAnonymized": False,
        "brand": customer.brand,
        "profileId": anon_profile_id,
        "previousProfileId": None,
        "counts": counts,
        "unomiProfileId": old_profile_id,
    }


def _erase_locked(
    db: Session,
    customer: Customer,
    *,
    keep_sales: bool,
    purge_fingerprint: bool,
) -> dict:
    now = datetime.utcnow()
    old_profile_id = customer.profile_id
    email = customer.email
    customer_id = customer.id
    brand = customer.brand
    bypass_used = _bypass_used_for_customer(db, customer)
    if not purge_fingerprint:
        _record_erased_identity(
            db, brand=brand, email=email, bypass_used=bypass_used, mode="erase"
        )

    counts = dict(_EMPTY_COUNTS)
    tx_ids = [row[0] for row in db.query(Transaction.id).filter(Transaction.customer_id == customer_id).all()]
    if tx_ids:
        n = (
            db.query(TransactionRuleExecution)
            .filter(TransactionRuleExecution.transaction_id.in_(tx_ids))
            .delete(synchronize_session=False)
        )
        counts["rule_executions"] = int(n or 0)

    _bump(counts, "redemptions", db.query(Redemption).filter(Redemption.customer_id == customer_id).delete(synchronize_session=False) or 0)
    _bump(counts, "customer_rewards", db.query(CustomerReward).filter(CustomerReward.customer_id == customer_id).delete(synchronize_session=False) or 0)
    _bump(counts, "customer_coupons", db.query(CustomerCoupon).filter(CustomerCoupon.customer_id == customer_id).delete(synchronize_session=False) or 0)

    if keep_sales:
        anon_profile_id = f"anon_{uuid.uuid4()}"
        json_counts = _scrub_customer_json(db, customer, anon_profile_id)
        for key, n in json_counts.items():
            if n:
                counts[key] = max(int(counts.get(key) or 0), n)
        detached = (
            db.query(Transaction)
            .filter(Transaction.customer_id == customer_id)
            .update(
                {Transaction.customer_id: None, Transaction.profile_id: anon_profile_id},
                synchronize_session=False,
            )
        )
        _bump(counts, "transactions", detached or 0)
    else:
        _bump(
            counts,
            "transactions",
            db.query(Transaction).filter(Transaction.customer_id == customer_id).delete(synchronize_session=False)
            or 0,
        )

    _bump(
        counts,
        "point_movements",
        db.query(PointMovement).filter(PointMovement.customer_id == customer_id).delete(synchronize_session=False)
        or 0,
    )
    _bump(
        counts,
        "purchase_movements",
        db.query(PurchaseMovement).filter(PurchaseMovement.customer_id == customer_id).delete(synchronize_session=False)
        or 0,
    )
    _bump(
        counts,
        "cash_movements",
        db.query(CashMovement).filter(CashMovement.customer_id == customer_id).delete(synchronize_session=False)
        or 0,
    )
    db.query(CustomerMetrics).filter(CustomerMetrics.customer_id == customer_id).delete(synchronize_session=False)
    _bump(
        counts,
        "guarantees",
        db.query(CustomerLoyaltyGuarantee).filter(CustomerLoyaltyGuarantee.customer_id == customer_id).delete(synchronize_session=False) or 0,
    )
    _bump(
        counts,
        "aliases",
        db.query(CustomerUnomiProfileAlias).filter(CustomerUnomiProfileAlias.customer_id == customer_id).delete(synchronize_session=False) or 0,
    )
    _bump(
        counts,
        "segment_members",
        db.query(SegmentMember).filter(SegmentMember.customer_id == customer_id).delete(synchronize_session=False) or 0,
    )

    old_ids = [old_profile_id]
    n_audit = (
        db.query(StorefrontAuditEvent)
        .filter(StorefrontAuditEvent.brand == brand)
        .filter(StorefrontAuditEvent.profile_id.in_([i for i in old_ids if i]))
        .delete(synchronize_session=False)
    )
    _bump(counts, "storefront_audit_events", n_audit or 0)
    pending = db.query(WebhookOutbox).filter(WebhookOutbox.brand == brand).filter(WebhookOutbox.status == "PENDING").all()
    for item in pending:
        payload = item.payload if isinstance(item.payload, dict) else {}
        if str(payload.get("profileId") or "") == str(old_profile_id) or str(payload.get("customerId") or "") == str(customer_id):
            db.delete(item)
            _bump(counts, "webhook_outbox")

    db.delete(customer)
    db.flush()
    return {
        "mode": "erase",
        "alreadyAnonymized": False,
        "brand": brand,
        "profileId": old_profile_id,
        "counts": counts,
        "unomiProfileId": old_profile_id,
        "fingerprintPurged": bool(purge_fingerprint),
        "keepSales": bool(keep_sales),
        "erasedAt": now.isoformat(),
    }


def delete_loyalty_customer(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    skip_unomi: bool = True,
    mode: str = "anonymize",
    confirm: str | None = None,
    purge_fingerprint: bool = False,
    keep_sales: bool = False,
) -> dict:
    mode = (mode or "anonymize").strip().lower()
    if mode not in {"anonymize", "erase"}:
        raise ValueError("mode must be anonymize or erase")
    if mode == "erase" and confirm != profile_id:
        raise ValueError("confirm must equal profile_id for erase")

    # Include ANONYMIZED rows (idempotent re-anonymize / legal erase of shell).
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(Customer.profile_id == profile_id)
        .with_for_update()
        .first()
    )
    if not customer:
        # Alias resolution for active customers only (aliases cleared on anonymize).
        alias = (
            db.query(CustomerUnomiProfileAlias)
            .filter(CustomerUnomiProfileAlias.brand == brand)
            .filter(CustomerUnomiProfileAlias.profile_id == profile_id)
            .first()
        )
        if alias:
            customer = (
                db.query(Customer)
                .filter(Customer.id == alias.customer_id)
                .with_for_update()
                .first()
            )
    if not customer:
        return {"deleted": False, "reason": "not_found"}

    if customer_is_anonymized(customer) and mode == "anonymize":
        logger.info("loyalty customer already anonymized brand=%s", brand)
        return {
            "deleted": False,
            "alreadyAnonymized": True,
            "mode": "anonymize",
            "brand": brand,
            "profileId": customer.profile_id,
            "counts": dict(_EMPTY_COUNTS),
            "unomiProfileId": None,
        }

    if mode == "anonymize":
        result = _anonymize_locked(db, customer, mode=mode)
    else:
        result = _erase_locked(
            db, customer, keep_sales=keep_sales, purge_fingerprint=purge_fingerprint
        )

    result["deleted"] = True
    result["skipUnomi"] = skip_unomi
    logger.info(
        "loyalty customer %s brand=%s counts=%s",
        mode,
        brand,
        result.get("counts"),
    )
    return result
