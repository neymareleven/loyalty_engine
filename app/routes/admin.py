import uuid
from datetime import datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, func, text
from sqlalchemy.orm import Session
from typing import Any

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.customer_reward import CustomerReward
from app.models.event_type import TransactionType
from app.models.internal_job import InternalJob
from app.models.loyalty_tier import LoyaltyTier
from app.models.point_movement import PointMovement
from app.models.rule import Rule
from app.models.reward import Reward
from app.models.coupon_type import CouponType
from app.models.product_category import ProductCategory
from app.models.product import Product
from app.models.segment import Segment
from app.models.transaction import Transaction
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.services.birthdate_targeting import BIRTHDATE_FIELD_META, BIRTHDATE_VALUE_PRESETS
from app.services.payload_schema_service import (
    get_transaction_type_rule_hints,
    payload_schema_field_catalog,
    payload_schema_field_paths,
)
from app.services.coupon_service import expire_coupons
from app.services.entitlement_history_service import build_global_entitlement_history
from app.services.loyalty_settings_service import ensure_brand_transaction_catalog, get_or_create_loyalty_settings
from app.services.loyalty_validity_service import initialize_validity_windows_for_existing_customers
from app.services.transaction_protection import delete_transaction_if_allowed
from app.schemas.loyalty_settings import LoyaltySettingsOut, LoyaltySettingsUpdate
from app.schemas.rule_condition_catalog import get_rule_conditions_catalog
from app.schemas.rule import RuleCreate, RuleUpdate
from app.schemas.rule_action_catalog import (
    EarnPointsAction,
    IssueCouponAction,
    ResetStatusPointsAction,
)


router = APIRouter(prefix="/admin", tags=["admin"])


def _resolve_admin_transaction(db: Session, *, brand: str, identifier: str) -> Transaction | None:
    try:
        tx_uuid = uuid.UUID(str(identifier))
    except Exception:
        tx_uuid = None
    q = db.query(Transaction).filter(Transaction.brand == brand)
    if tx_uuid is not None:
        return q.filter(Transaction.id == tx_uuid).first()
    return q.filter(Transaction.transaction_id == identifier).first()


@router.delete("/transactions/{transaction_id}")
def delete_transaction_admin(
    transaction_id: str,
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    tx = _resolve_admin_transaction(db, brand=brand, identifier=transaction_id)
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    delete_transaction_if_allowed(db, tx)
    db.commit()
    return {"deleted": True}


@router.get("/health/schema")
def admin_schema_health(db: Session = Depends(get_db)):
    """Quick check that prod DB has migrations required by segments + loyalty-settings."""
    checks = {
        "segments.provider": "SELECT provider FROM segments LIMIT 1",
        "segments.manual_profile_ids": "SELECT manual_profile_ids FROM segments LIMIT 1",
        "brand_loyalty_settings.segmentation_mode": (
            "SELECT segmentation_mode FROM brand_loyalty_settings LIMIT 1"
        ),
    }
    missing: list[str] = []
    errors: dict[str, str] = {}
    for name, sql in checks.items():
        try:
            db.execute(text(sql))
        except Exception as e:
            missing.append(name)
            errors[name] = str(e)[:300]
    if missing:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Database schema incomplete. Run: alembic upgrade head",
                "missingChecks": missing,
                "errors": errors,
            },
        )
    return {"ok": True, "checks": list(checks.keys())}


@router.get("/loyalty-settings", response_model=LoyaltySettingsOut)
def get_loyalty_settings(
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = get_or_create_loyalty_settings(db, brand=brand)
    db.commit()
    return {
        "brand": obj.brand,
        "points_validity_days": obj.points_validity_days,
        "loyalty_status_validity_days": obj.loyalty_status_validity_days,
    }


@router.get("/ui-options/coupon-types")
def list_ui_options_coupon_types(
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    db: Session = Depends(get_db),
):
    q = db.query(CouponType).filter(CouponType.brand == brand)
    if active is not None:
        q = q.filter(CouponType.active.is_(active))
    items = q.order_by(CouponType.name.asc()).all()
    from app.services.catalog_admin_service import coupon_type_deletion_meta
    from app.services.coupon_rewards_service import list_coupon_type_reward_ids

    out_items = []
    for ct in items:
        linked_ids = [str(rid) for rid in list_coupon_type_reward_ids(db, coupon_type_id=ct.id)]
        deletion = coupon_type_deletion_meta(db, coupon_type_id=ct.id)
        out_items.append(
            {
                "id": str(ct.id),
                "name": ct.name,
                "active": ct.active,
                "rewardIds": linked_ids,
                "rewardCount": len(linked_ids),
                "customerCouponCount": deletion["customer_coupon_count"],
                "canDelete": deletion["can_delete"],
                "recommendedAction": deletion["recommended_action"],
            }
        )
    return {"brand": brand, "items": out_items}


@router.get("/ui-options/coupon-types/{coupon_type_id}/rewards")
def list_ui_options_coupon_type_rewards(
    coupon_type_id: UUID,
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    db: Session = Depends(get_db),
):
    from app.models.coupon_type import CouponType
    from app.services.coupon_rewards_service import resolve_rewards_catalog

    ct = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not ct or ct.brand != brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")
    rewards = resolve_rewards_catalog(db, coupon_type=ct, active_only=active)
    return {
        "brand": brand,
        "couponTypeId": str(ct.id),
        "items": [
            {
                "id": str(r.id),
                "name": r.name,
                "active": r.active,
            }
            for r in rewards
        ],
    }


@router.get("/ui-options/segments")
def list_ui_options_segments(
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    db: Session = Depends(get_db),
):
    q = db.query(Segment).filter(Segment.brand == brand)
    if active is not None:
        q = q.filter(Segment.active.is_(active))
    items = q.order_by(Segment.name.asc()).all()
    return {
        "brand": brand,
        "items": [
            {
                "id": str(s.id),
                "name": s.name,
                "active": s.active,
                "is_dynamic": bool(s.is_dynamic),
            }
            for s in items
        ],
    }


@router.get("/ui-options/product-categories")
def list_ui_options_product_categories(
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    db: Session = Depends(get_db),
):
    q = db.query(ProductCategory).filter(ProductCategory.brand == brand)
    if active is not None:
        q = q.filter(ProductCategory.active.is_(active))
    items = q.order_by(ProductCategory.name.asc()).all()
    return {
        "brand": brand,
        "items": [
            {
                "id": str(c.id),
                "name": c.name,
                "active": c.active,
            }
            for c in items
        ],
    }


@router.get("/ui-options/products")
def list_ui_options_products(
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    category_id: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(Product).filter(Product.brand == brand)
    if active is not None:
        q = q.filter(Product.active.is_(active))
    if category_id:
        q = q.filter(Product.category_id == category_id)
    items = q.order_by(Product.name.asc()).all()
    return {
        "brand": brand,
        "items": [
            {
                "id": str(p.id),
                "name": p.name,
                "match_key": p.match_key,
                "points_value": p.points_value,
                "active": p.active,
                "category_id": str(p.category_id) if p.category_id else None,
            }
            for p in items
        ],
    }


@router.patch("/loyalty-settings", response_model=LoyaltySettingsOut)
def update_loyalty_settings(
    payload: LoyaltySettingsUpdate,
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = get_or_create_loyalty_settings(db, brand=brand)

    prev_points_days = obj.points_validity_days
    prev_status_days = obj.loyalty_status_validity_days

    data = payload.model_dump(exclude_unset=True)
    if "points_validity_days" in data and data["points_validity_days"] is not None:
        try:
            v = int(data["points_validity_days"])
        except Exception:
            raise HTTPException(status_code=400, detail="points_validity_days must be an integer")
        if v < 0:
            raise HTTPException(status_code=400, detail="points_validity_days must be >= 0")
        obj.points_validity_days = v
    elif "points_validity_days" in data and data["points_validity_days"] is None:
        obj.points_validity_days = None

    if "loyalty_status_validity_days" in data and data["loyalty_status_validity_days"] is not None:
        try:
            v = int(data["loyalty_status_validity_days"])
        except Exception:
            raise HTTPException(status_code=400, detail="loyalty_status_validity_days must be an integer")
        if v < 0:
            raise HTTPException(status_code=400, detail="loyalty_status_validity_days must be >= 0")
        obj.loyalty_status_validity_days = v
    elif "loyalty_status_validity_days" in data and data["loyalty_status_validity_days"] is None:
        obj.loyalty_status_validity_days = None

    should_backfill_points = obj.points_validity_days is not None and prev_points_days is None
    should_backfill_status = obj.loyalty_status_validity_days is not None and prev_status_days is None

    if should_backfill_points or should_backfill_status:
        initialize_validity_windows_for_existing_customers(db, brand=brand)

    db.commit()
    db.refresh(obj)
    return {
        "brand": obj.brand,
        "points_validity_days": obj.points_validity_days,
        "loyalty_status_validity_days": obj.loyalty_status_validity_days,
    }


@router.get("/brand-kpis")
def get_brand_kpis(
    windowDays: int = 30,
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    try:
        windowDays = int(windowDays or 30)
    except Exception:
        raise HTTPException(status_code=400, detail="windowDays must be an integer")
    if windowDays not in {7, 30}:
        raise HTTPException(status_code=400, detail="windowDays must be 7 or 30")

    now = datetime.utcnow()
    window_from = now - timedelta(days=windowDays)

    total_customers = (
        db.query(func.count(Customer.id))
        .filter(Customer.brand == brand)
        .scalar()
        or 0
    )
    new_customers = (
        db.query(func.count(Customer.id))
        .filter(Customer.brand == brand)
        .filter(Customer.created_at >= window_from)
        .scalar()
        or 0
    )
    active_customers = (
        db.query(func.count(Customer.id))
        .filter(Customer.brand == brand)
        .filter(Customer.last_activity_at.isnot(None))
        .filter(Customer.last_activity_at >= window_from)
        .scalar()
        or 0
    )
    configured_customers = (
        db.query(func.count(Customer.id))
        .filter(Customer.brand == brand)
        .filter(Customer.loyalty_status.isnot(None))
        .filter(Customer.loyalty_status != "UNCONFIGURED")
        .scalar()
        or 0
    )
    by_status_rows = (
        db.query(Customer.loyalty_status, func.count(Customer.id))
        .filter(Customer.brand == brand)
        .group_by(Customer.loyalty_status)
        .all()
    )
    customers_by_loyalty_status = [
        {"key": (row[0] or "UNCONFIGURED"), "count": int(row[1] or 0)} for row in by_status_rows
    ]

    ingested_in_window = (
        db.query(func.count(Transaction.id))
        .filter(Transaction.brand == brand)
        .filter(Transaction.created_at >= window_from)
        .scalar()
        or 0
    )
    tx_by_status_rows = (
        db.query(Transaction.status, func.count(Transaction.id))
        .filter(Transaction.brand == brand)
        .filter(Transaction.created_at >= window_from)
        .group_by(Transaction.status)
        .all()
    )
    tx_by_status = [
        {"status": (row[0] or "UNKNOWN"), "count": int(row[1] or 0)} for row in tx_by_status_rows
    ]

    top_types_rows = (
        db.query(Transaction.transaction_type, func.count(Transaction.id))
        .filter(Transaction.brand == brand)
        .filter(Transaction.created_at >= window_from)
        .group_by(Transaction.transaction_type)
        .order_by(desc(func.count(Transaction.id)))
        .limit(10)
        .all()
    )
    top_types = [
        {"transactionType": (row[0] or "UNKNOWN"), "count": int(row[1] or 0)} for row in top_types_rows
    ]

    top_error_codes_rows = (
        db.query(Transaction.error_code, func.count(Transaction.id))
        .filter(Transaction.brand == brand)
        .filter(Transaction.created_at >= window_from)
        .filter(Transaction.error_code.isnot(None))
        .filter(Transaction.error_code != "TRANSACTION_TYPE_CREATED")
        .group_by(Transaction.error_code)
        .order_by(desc(func.count(Transaction.id)))
        .limit(10)
        .all()
    )
    top_error_codes = [
        {"errorCode": row[0], "count": int(row[1] or 0)} for row in top_error_codes_rows if row[0]
    ]

    no_rules_count = (
        db.query(func.count(Transaction.id))
        .filter(Transaction.brand == brand)
        .filter(Transaction.created_at >= window_from)
        .filter(Transaction.error_code == "NO_RULES")
        .scalar()
        or 0
    )
    rules_applied_count = max(0, int(ingested_in_window) - int(no_rules_count))
    blocked_customer_not_found = (
        db.query(func.count(Transaction.id))
        .filter(Transaction.brand == brand)
        .filter(Transaction.created_at >= window_from)
        .filter(Transaction.error_code == "CUSTOMER_NOT_FOUND")
        .scalar()
        or 0
    )

    active_rules = (
        db.query(func.count(Rule.id))
        .filter(Rule.brand == brand)
        .filter(Rule.active.is_(True))
        .scalar()
        or 0
    )

    exec_window = (
        db.query(TransactionRuleExecution)
        .join(Transaction, Transaction.id == TransactionRuleExecution.transaction_id)
        .filter(Transaction.brand == brand)
        .filter(TransactionRuleExecution.executed_at >= window_from)
    )
    executions_in_window = exec_window.with_entities(func.count(TransactionRuleExecution.id)).scalar() or 0
    exec_by_result_rows = (
        exec_window.with_entities(TransactionRuleExecution.result, func.count(TransactionRuleExecution.id))
        .group_by(TransactionRuleExecution.result)
        .all()
    )
    exec_by_result = [
        {"result": (row[0] or "UNKNOWN"), "count": int(row[1] or 0)} for row in exec_by_result_rows
    ]
    top_failed_rules_rows = (
        exec_window.with_entities(TransactionRuleExecution.rule_id, func.count(TransactionRuleExecution.id))
        .filter(TransactionRuleExecution.result == "FAILED")
        .filter(TransactionRuleExecution.rule_id.isnot(None))
        .group_by(TransactionRuleExecution.rule_id)
        .order_by(desc(func.count(TransactionRuleExecution.id)))
        .limit(10)
        .all()
    )
    top_failed_rules = [
        {"ruleId": str(row[0]), "count": int(row[1] or 0)} for row in top_failed_rules_rows if row[0]
    ]

    pm_window = (
        db.query(PointMovement)
        .join(Customer, Customer.id == PointMovement.customer_id)
        .filter(Customer.brand == brand)
        .filter(PointMovement.created_at >= window_from)
    )
    points_issued = (
        pm_window.with_entities(func.coalesce(func.sum(PointMovement.points), 0))
        .filter(PointMovement.points > 0)
        .scalar()
        or 0
    )
    points_burned_signed = (
        pm_window.with_entities(func.coalesce(func.sum(PointMovement.points), 0))
        .filter(PointMovement.points < 0)
        .scalar()
        or 0
    )
    points_burned = abs(int(points_burned_signed or 0))
    points_net = int(points_issued or 0) - int(points_burned or 0)

    rewards_active_now = (
        db.query(func.count(CustomerReward.id))
        .join(Customer, Customer.id == CustomerReward.customer_id)
        .filter(Customer.brand == brand)
        .filter(CustomerReward.status == "ISSUED")
        .scalar()
        or 0
    )
    rewards_issued_in_window = (
        db.query(func.count(CustomerReward.id))
        .join(Customer, Customer.id == CustomerReward.customer_id)
        .filter(Customer.brand == brand)
        .filter(CustomerReward.issued_at >= window_from)
        .scalar()
        or 0
    )
    rewards_used_in_window = (
        db.query(func.count(CustomerReward.id))
        .join(Customer, Customer.id == CustomerReward.customer_id)
        .filter(Customer.brand == brand)
        .filter(CustomerReward.used_at.isnot(None))
        .filter(CustomerReward.used_at >= window_from)
        .scalar()
        or 0
    )
    rewards_expired_in_window = (
        db.query(func.count(CustomerReward.id))
        .join(Customer, Customer.id == CustomerReward.customer_id)
        .filter(Customer.brand == brand)
        .filter(CustomerReward.status == "EXPIRED")
        .filter(CustomerReward.expires_at.isnot(None))
        .filter(CustomerReward.expires_at >= window_from)
        .scalar()
        or 0
    )

    tier_upgraded = (
        db.query(func.count(Transaction.id))
        .filter(Transaction.brand == brand)
        .filter(Transaction.created_at >= window_from)
        .filter(Transaction.transaction_type == "TIER_UPGRADED")
        .scalar()
        or 0
    )
    tier_downgraded = (
        db.query(func.count(Transaction.id))
        .filter(Transaction.brand == brand)
        .filter(Transaction.created_at >= window_from)
        .filter(Transaction.transaction_type == "TIER_DOWNGRADED")
        .scalar()
        or 0
    )

    jobs_base = (
        db.query(InternalJob)
        .filter(InternalJob.brand == brand)
        .filter(InternalJob.job_key != "MAINT_EXPIRE_REWARDS")
    )
    jobs_active = jobs_base.filter(InternalJob.active.is_(True))
    active_jobs_count = jobs_active.with_entities(func.count(InternalJob.id)).scalar() or 0
    failing_jobs_count = (
        jobs_active.with_entities(func.count(InternalJob.id))
        .filter(InternalJob.last_status == "FAILED")
        .scalar()
        or 0
    )
    overdue_jobs_count = (
        jobs_active.with_entities(func.count(InternalJob.id))
        .filter(InternalJob.next_run_at.isnot(None))
        .filter(InternalJob.next_run_at < now)
        .scalar()
        or 0
    )
    jobs_items = (
        jobs_active.order_by(InternalJob.next_run_at.asc(), InternalJob.created_at.asc())
        .limit(20)
        .all()
    )
    jobs_out = [
        {
            "id": str(j.id),
            "name": j.name,
            "jobKey": j.job_key,
            "transactionType": j.transaction_type,
            "active": bool(j.active),
            "nextRunAt": j.next_run_at,
            "lastRunAt": j.last_run_at,
            "lastStatus": j.last_status,
            "lastError": j.last_error,
        }
        for j in jobs_items
    ]

    return {
        "brand": brand,
        "windowDays": windowDays,
        "window": {
            "from": window_from,
            "to": now,
        },
        "customers": {
            "total": int(total_customers),
            "newInWindow": int(new_customers),
            "activeInWindow": int(active_customers),
            "withLoyaltyStatusConfigured": int(configured_customers),
            "byLoyaltyStatus": customers_by_loyalty_status,
        },
        "transactions": {
            "ingestedInWindow": int(ingested_in_window),
            "byStatus": tx_by_status,
            "topTypesInWindow": top_types,
            "topErrorCodesInWindow": top_error_codes,
            "noRulesCountInWindow": int(no_rules_count),
            "rulesAppliedCountInWindow": int(rules_applied_count),
            "blockedCustomerNotFoundInWindow": int(blocked_customer_not_found),
        },
        "rules": {
            "activeRules": int(active_rules),
            "executionsInWindow": int(executions_in_window),
            "byResultInWindow": exec_by_result,
            "topFailedRulesInWindow": top_failed_rules,
        },
        "loyalty": {
            "points": {
                "issuedInWindow": int(points_issued),
                "burnedInWindow": int(points_burned),
                "netInWindow": int(points_net),
            },
            "rewards": {
                "activeNow": int(rewards_active_now),
                "issuedInWindow": int(rewards_issued_in_window),
                "usedInWindow": int(rewards_used_in_window),
                "expiredInWindow": int(rewards_expired_in_window),
            },
            "tierEventsInWindow": {
                "upgraded": int(tier_upgraded),
                "downgraded": int(tier_downgraded),
            },
        },
        "internalJobs": {
            "active": int(active_jobs_count),
            "failing": int(failing_jobs_count),
            "overdue": int(overdue_jobs_count),
            "items": jobs_out,
        },
    }


def _json_schema_paths(schema: Any, *, prefix: str) -> list[str]:
    return payload_schema_field_paths(schema, prefix=prefix)


def _reward_type_catalog_item(
    *,
    reward_type: str,
    title: str,
    description: str,
    required: list[str],
    visible: list[str],
    voucher_presets: list[dict[str, Any]] | None = None,
):
    props: dict[str, Any] = {
        "brand": {"type": ["string", "null"]},
        "name": {"type": "string"},
        "description": {"type": ["string", "null"]},
        "active": {"type": "boolean"},
    }

    ui_hints: dict[str, Any] = {
        "brand": {"widget": "hidden"},
        "description": {"widget": "textarea"},
        "active": {"widget": "switch"},
        "_ui": {
            "visibleFields": ["name", "description", "active"],
        },
    }

    return {
        "type": reward_type,
        "title": title,
        "description": description,
        "jsonSchema": {
            "type": "object",
            "properties": props,
            "required": ["name"],
            "additionalProperties": False,
        },
        "uiHints": ui_hints,
        "examples": [{"name": "Example"}],
    }


@router.get("/coupon-types/ui-catalog")
def get_coupon_types_ui_catalog():
    return {
        "workflow": {
            "order": [
                "create_coupon_type",
                "create_rewards_with_coupon_type_ids",
                "optional_put_coupon_type_rewards",
                "configure_rules_issue_coupon",
            ],
            "summary": (
                "Flux catalogue recommandé : créer le type de coupon, puis les rewards via coupon_type_ids. "
                "Gérer les liens depuis le coupon (PUT …/rewards) ; éviter de recâbler coupon_type_ids sur une reward existante sauf migration."
            ),
        },
        "create": {
            "fieldHelp": {
                "name": "Nom du type de coupon (ex. Anniversaire, VIP).",
                "validity_days": "Durée de validité des coupons émis (jours). Laisser vide = pas d'expiration automatique.",
            },
            "jsonSchema": {
                "note": "POST /admin/coupon-types — aucune reward à la création.",
            },
        },
        "update": {
            "fieldHelp": {
                "reward_ids": "Remplace toutes les rewards liées (coupon_type_rewards). Préférer PUT /admin/coupon-types/{id}/rewards.",
                "active": "Désactiver empêche les nouvelles émissions via issue_coupon.",
            },
        },
        "delete": {
            "policy": (
                "DELETE toujours possible après confirmation via GET …/delete-preview. "
                "Les coupons actifs (ISSUED) et rewards actives liées sont invalidés ; "
                "l'historique USED/EXPIRED reste en lecture seule. "
                "PATCH active=false désactive seulement les nouvelles émissions (sans invalider le portefeuille)."
            ),
            "previewEndpoint": "GET /admin/coupon-types/{coupon_type_id}/delete-preview",
            "fieldsOnList": ["canDelete", "customerCouponCount", "customerCouponsIssued", "recommendedAction"],
        },
        "linkRewards": {
            "summary": (
                "Liens coupon ↔ reward bidirectionnels via coupon_type_rewards. "
                "Modifier depuis le coupon type OU depuis la reward : les deux vues restent synchronisées."
            ),
            "fromCouponType": "PUT /admin/coupon-types/{coupon_type_id}/rewards",
            "fromReward": "PUT /rewards/{reward_id}/coupon-types",
            "alternateFromReward": "PATCH /rewards/{reward_id} avec coupon_type_ids",
            "readFromReward": "GET /rewards/{reward_id}/coupon-types",
        },
        "uiOptions": {
            "listCouponTypes": "/admin/ui-options/coupon-types",
            "listRewardsForCouponType": "/admin/ui-options/coupon-types/{coupon_type_id}/rewards",
        },
    }


@router.get("/rewards/ui-catalog")
def get_rewards_ui_catalog():
    return {
        "workflow": {
            "order": [
                "create_coupon_type_first",
                "create_reward_with_coupon_type_ids",
            ],
            "summary": (
                "Après création du coupon type, POST /rewards avec coupon_type_ids (obligatoire, min. 1). "
                "Les liens peuvent être modifiés depuis le coupon type (PUT …/rewards) "
                "ou depuis la reward (PUT …/coupon-types) — même table, toujours synchronisé."
            ),
        },
        "create": {
            "fieldHelp": {
                "name": "Nom lisible de la récompense (affiché dans le backoffice).",
                "description": "Description (optionnelle) affichée au support/ops.",
                "coupon_type_ids": (
                    "Types de coupon existants (obligatoire à la création). Créer le coupon type avant la reward."
                ),
            },
            "rewardTypes": [
                _reward_type_catalog_item(
                    reward_type="ENTITLEMENT",
                    title="Entitlement",
                    description="Static reward content attached to coupons.",
                    required=[],
                    visible=["name", "description", "active"],
                ),
            ],
        },
        "update": {
            "fieldHelp": {
                "coupon_type_ids": (
                    "Multi-select éditable à la création ET à la modification. "
                    "Remplace tous les liens coupon ↔ reward pour cette récompense. "
                    "Préférer PUT /rewards/{reward_id}/coupon-types pour ne mettre à jour que les liens."
                ),
                "active": "Désactiver pour stopper les nouvelles émissions ; ne supprime pas l'historique client.",
            },
            "couponTypeIdsField": {
                "editableOnCreate": True,
                "editableOnUpdate": True,
                "requiredOnCreate": True,
                "requiredOnUpdate": False,
                "widget": "remote_multi_select",
                "datasource": {
                    "endpoint": "/admin/ui-options/coupon-types",
                    "method": "GET",
                    "brandVia": "X-Brand",
                    "valueField": "id",
                    "labelField": "name",
                },
                "saveLinksEndpoint": "PUT /rewards/{reward_id}/coupon-types",
                "readLinksEndpoint": "GET /rewards/{reward_id}/coupon-types",
            },
        },
        "delete": {
            "policy": (
                "DELETE toujours possible après GET /rewards/{reward_id}/delete-preview. "
                "Invalidation granulaire : seules les attributions ISSUED de cette reward sont invalidées ; "
                "les autres rewards du même coupon client ne sont pas affectées. "
                "Si plus aucune reward active ne reste sur un coupon, le coupon est auto-invalidé."
            ),
            "previewEndpoint": "GET /rewards/{reward_id}/delete-preview",
        },
    }


@router.get("/customer-entitlements/ui-catalog")
def get_customer_entitlements_ui_catalog():
    return {
        "customerCoupons": {
            "summary": (
                "Les coupons émis chez un client ne se suppriment pas. "
                "Utiliser PATCH status uniquement si admin_actions_enabled=true."
            ),
            "endpoint": "PATCH /customers/{brand}/{profile_id}/coupons/{customer_coupon_id}/status",
            "listEndpoint": "GET /customers/{brand}/{profile_id}/coupons-with-rewards",
            "historyEndpoint": "GET /customers/{brand}/{profile_id}/entitlements/history",
            "statuses": ["ISSUED", "USED", "EXPIRED", "INVALIDATED"],
            "statusLabels": {
                "ISSUED": "Actif",
                "USED": "Utilisé",
                "EXPIRED": "Expiré",
                "INVALIDATED": "Invalidé",
            },
            "displayFields": [
                "display_label",
                "status_label",
                "catalog_removed",
                "admin_actions_enabled",
                "allowed_admin_transitions",
            ],
            "uiRules": [
                "Griser toutes les actions si admin_actions_enabled=false",
                "Afficher catalog_removed comme badge « Modèle retiré du catalogue »",
                "Ne jamais afficher les UUID bruts à l'utilisateur",
            ],
            "forbiddenActions": ["DELETE"],
        },
        "customerRewards": {
            "summary": (
                "Pas de DELETE sur les droits client. Libellés depuis snapshots payload "
                "(rewardSnapshot, couponTypeSnapshot, productSnapshots)."
            ),
            "displayFields": [
                "display_label",
                "status_label",
                "catalog_removed",
                "products",
                "coupon_type_name",
                "reward_name",
            ],
        },
        "catalogDeletion": {
            "summary": (
                "DELETE catalogue invalide les droits actifs (ISSUED) et conserve l'historique. "
                "Toujours afficher la modale via delete-preview avant DELETE."
            ),
            "previews": {
                "couponType": "GET /admin/coupon-types/{id}/delete-preview",
                "reward": "GET /rewards/{id}/delete-preview",
                "product": "GET /admin/products/{id}/delete-preview",
            },
        },
    }


@router.get("/entitlements/history")
def get_global_entitlements_history(
    active_brand: str = Depends(get_active_brand),
    profile_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    return build_global_entitlement_history(
        db,
        brand=active_brand,
        limit=limit,
        offset=offset,
        profile_id=profile_id,
    )


@router.get("/rules/ui-options/condition-fields")
def list_rule_condition_fields(
    transaction_type: str,
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if not (transaction_type or "").strip():
        raise HTTPException(status_code=400, detail="transaction_type is required")

    tt = (
        db.query(TransactionType)
        .filter(TransactionType.key == transaction_type)
        .filter((TransactionType.brand == brand) | (TransactionType.brand.is_(None)))
        .first()
    )
    if not tt:
        raise HTTPException(status_code=404, detail="TransactionType not found")

    payload_fields: list[str] = []
    payload_field_meta: dict[str, Any] = {}
    if tt.payload_schema is not None:
        payload_fields = _json_schema_paths(tt.payload_schema, prefix="payload")
        for item in payload_schema_field_catalog(tt.payload_schema, transaction_type_key=transaction_type):
            payload_field_meta[item["conditionField"]] = {
                "valueKind": item.get("type", "string"),
                "earnPointsPath": item.get("earnPointsPath"),
                "dynamicValue": item.get("dynamicValue"),
            }

    customer_fields = [
        "customer.gender",
        "customer.status",
        "customer.loyalty_status",
        "customer.status_points",
        "customer.created_at",
        "customer.last_activity_at",
        "customer.birthdate",
        "customer.rewards",
    ]

    system_fields: list[str] = []

    items = sorted({*payload_fields, *customer_fields})
    return {
        "brand": brand,
        "transaction_type": transaction_type,
        "sources": {
            "payload": payload_fields,
            "customer": customer_fields,
            "system": system_fields,
        },
        "valuePresets": {
            "birthdate": BIRTHDATE_VALUE_PRESETS,
            "datetime": [
                {"id": "system.now", "label": "Now", "value": {"$system": "now"}},
            ],
            "number": [
                {"id": "system.weekday", "label": "Weekday (0=Mon .. 6=Sun)", "value": {"$system": "weekday"}},
                {"id": "system.customer_created_days", "label": "Days since customer created", "value": {"$system": "customer_created_days"}},
                {"id": "system.customer_last_activity_days", "label": "Days since last activity", "value": {"$system": "customer_last_activity_days"}},
            ],
        },
        "fieldMeta": {
            "customer.loyalty_status": {
                "valueKind": "enum",
                "ui": {
                    "widget": "remote_select",
                    "datasource": {
                        "endpoint": "/admin/ui-options/loyalty-tiers",
                        "method": "GET",
                        "brandVia": "X-Brand",
                        "valueField": "key",
                        "labelField": "name",
                    },
                },
            },
            "customer.rewards": {
                "valueKind": "set",
                "ui": {
                    "widget": "remote_multi_select",
                    "datasource": {
                        "endpoint": "/admin/ui-options/rewards",
                        "method": "GET",
                        "brandVia": "X-Brand",
                        "valueField": "id",
                        "labelField": "name",
                    },
                    "operatorNotes": {
                        "in": "Expected value is a list of reward ids. Condition is true if customer has ANY of the selected rewards.",
                        "exists": "No values needed if you only check existence.",
                    },
                },
            },
            "customer.gender": {
                "valueKind": "enum",
                "ui": {
                    "widget": "select",
                    "options": ["M", "F", "OTHER", "UNKNOWN"],
                },
            },
            "customer.birthdate": dict(BIRTHDATE_FIELD_META),
            **payload_field_meta,
        },
        "payloadFields": payload_schema_field_catalog(tt.payload_schema, transaction_type_key=transaction_type)
        if tt.payload_schema
        else [],
        "ruleHints": get_transaction_type_rule_hints(transaction_type),
        "items": items,
    }


@router.get("/internal-jobs/ui-options/selector-fields")
def list_internal_job_selector_fields():
    customer_fields = [
        "customer.gender",
        "customer.status",
        "customer.loyalty_status",
        "customer.status_points",
        "customer.birthdate",
        "customer.created_at",
        "customer.last_activity_at",
        "customer.metrics.last_transaction_at",
        "customer.metrics.transactions_count_30d",
        "customer.metrics.transactions_count_90d",
    ]
    system_fields: list[str] = []
    items = sorted({*customer_fields, *system_fields})
    return {
        "sources": {
            "customer": customer_fields,
            "system": system_fields,
        },
        "fieldMeta": {
            "customer.gender": {
                "valueKind": "enum",
                "ui": {
                    "widget": "select",
                    "options": ["M", "F", "OTHER", "UNKNOWN"],
                },
            },
            "customer.birthdate": dict(BIRTHDATE_FIELD_META),
            "customer.created_at": {"valueKind": "datetime", "ui": {"widget": "datetime"}},
            "customer.last_activity_at": {"valueKind": "datetime", "ui": {"widget": "datetime"}},
            "customer.status_points": {"valueKind": "number", "ui": {"widget": "number"}},
            "customer.metrics.last_transaction_at": {"valueKind": "datetime", "ui": {"widget": "datetime"}},
            "customer.metrics.transactions_count_30d": {"valueKind": "number", "ui": {"widget": "number"}},
            "customer.metrics.transactions_count_90d": {"valueKind": "number", "ui": {"widget": "number"}},
        },
        "valuePresets": {
            "birthdate": BIRTHDATE_VALUE_PRESETS
            + [
                {
                    "id": "system.today_plus_7_mmdd",
                    "label": "Anniversaire dans 7 jours (MM-DD)",
                    "value": {"$system": "today", "add_days": 7, "format": "mmdd"},
                    "granularity": "day_month",
                },
                {
                    "id": "system.today_plus_30_mmdd",
                    "label": "Anniversaire dans 30 jours (MM-DD)",
                    "value": {"$system": "today", "add_days": 30, "format": "mmdd"},
                    "granularity": "day_month",
                },
            ],
            "datetime": [
                {"id": "system.now", "label": "Now", "value": {"$system": "now"}},
            ],
        },
        "items": items,
    }


@router.post("/rewards/expire")
def admin_expire_rewards(
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    expired_count = expire_rewards(db, brand=brand)
    db.commit()
    return {"brand": brand, "expired": expired_count}


@router.post("/coupons/expire")
def admin_expire_coupons(
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    expired_count = expire_coupons(db, brand=brand)
    db.commit()
    return {"brand": brand, "expired": expired_count}


@router.get("/ui-options/rewards")
def list_ui_options_rewards(
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    db: Session = Depends(get_db),
):
    q = db.query(Reward).filter(Reward.brand == brand)
    if active is not None:
        q = q.filter(Reward.active.is_(active))
    from app.services.coupon_rewards_service import list_reward_coupon_type_ids

    items = q.order_by(Reward.name.asc()).all()
    return {
        "brand": brand,
        "items": [
            {
                "id": str(r.id),
                "name": r.name,
                "active": r.active,
                "couponTypeIds": [str(cid) for cid in list_reward_coupon_type_ids(db, reward_id=r.id)],
            }
            for r in items
        ],
    }


@router.get("/ui-options/transaction-types")
def list_ui_options_transaction_types(
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    origin: str | None = None,
    db: Session = Depends(get_db),
):
    ensure_brand_transaction_catalog(db, brand=brand)
    db.flush()

    q = db.query(TransactionType).filter(TransactionType.brand == brand)
    q = q.filter(TransactionType.key != "ADMIN_SET_TIER")
    if active is not None:
        q = q.filter(TransactionType.active.is_(active))
    if origin:
        q = q.filter(TransactionType.origin == origin)
    items = q.order_by(TransactionType.key.asc()).all()
    return {
        "brand": brand,
        "items": [
            {
                "id": str(et.id),
                "key": et.key,
                "name": et.name,
                "origin": et.origin,
                "active": et.active,
                "hasPayloadSchema": bool(et.payload_schema),
            }
            for et in items
        ],
    }


@router.get("/ui-options/loyalty-tiers")
def list_ui_options_loyalty_tiers(
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    db: Session = Depends(get_db),
):
    q = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand)
    if active is not None:
        q = q.filter(LoyaltyTier.active.is_(active))
    items = q.order_by(LoyaltyTier.min_status_points.asc(), LoyaltyTier.created_at.asc()).all()
    return {
        "brand": brand,
        "items": [
            {
                "id": str(t.id),
                "key": t.key,
                "name": t.name,
                "rank": t.rank,
                "minStatusPoints": t.min_status_points,
                "active": t.active,
            }
            for t in items
        ],
    }


@router.get("/rule-actions")
def list_rule_actions_catalog():

    def _model_json_schema(model_cls):
        # pydantic v2: model_json_schema(); pydantic v1: schema()
        fn = getattr(model_cls, "model_json_schema", None)
        if callable(fn):
            return fn()
        return model_cls.schema()

    return {
        "flags": {
            "requiresPayloadSchema": {
                "meaning": "Dépend de la structure des données de l’événement (payload_schema) pour permettre une configuration guidée (sélecteur de champs) et/ou une exécution correcte.",
                "uiRecommendation": {
                    "ifTransactionTypeHasNoPayloadSchema": {
                        "disableActions": "Désactiver toute action qui a requiresPayloadSchema=true au niveau de l’action.",
                        "disableFields": "Désactiver tout champ dont uiHints.<field>.requiresPayloadSchema=true.",
                    }
                },
                "appliesTo": ["action", "field"],
            }
        },
        "actions": [
            {
                "type": "earn_points",
                "title": "Ajouter des points",
                "description": "Crédite un nombre de points au client.",
                "params": {"points": "int", "multiplier": "int (optional)"},
                "jsonSchema": _model_json_schema(EarnPointsAction),
                "examples": [
                    {"type": "earn_points", "points": 50},
                    {"type": "earn_points", "points": 50, "multiplier": 3},
                    {"type": "earn_points", "points": {"$path": "payload.total"}},
                    {"type": "earn_points", "points": {"$path": "total"}, "multiplier": 2},
                ],
                "uiHints": {
                    "points": {
                        "widget": "number_or_payload_path",
                        "min": 0,
                        "dynamicExample": {"$path": "payload.total"},
                        "help": "Nombre fixe OU {$path: 'payload.<champ>'} / {<champ>} depuis le payload de la transaction.",
                    },
                    "multiplier": {"widget": "number", "min": 1, "placeholder": "Optional"},
                },
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": False,
                    "sideEffects": ["Creates point movement", "Increments customer points", "May update loyalty status"],
                    "commonErrors": ["points missing/invalid", "customer not found"],
                },
            },
            {
                "type": "issue_coupon",
                "title": "Attribuer un coupon",
                "description": (
                    "Émet un coupon pour le client et les récompenses liées au type de coupon (coupon_type_rewards). "
                    "Flux catalogue : créer le coupon type, puis les rewards (coupon_type_ids). "
                    "Sans reward_ids: toutes les rewards liées actives. Avec reward_ids: sous-ensemble strict (erreur si ID hors bundle)."
                ),
                "params": {
                    "coupon_type_id": "uuid",
                    "frequency": "ALWAYS | ONCE_PER_CALENDAR_YEAR (1 calendar year) | ONCE_PER_CUSTOMER",
                    "reward_ids": "uuid[] (optional subset)",
                },
                "jsonSchema": _model_json_schema(IssueCouponAction),
                "examples": [
                    {"type": "issue_coupon", "coupon_type_id": "<uuid>"},
                    {"type": "issue_coupon", "coupon_type_id": "<uuid>", "frequency": "ALWAYS"},
                    {
                        "type": "issue_coupon",
                        "coupon_type_id": "<uuid>",
                        "reward_ids": ["<reward-uuid-1>", "<reward-uuid-2>"],
                    },
                ],
                "uiHints": {
                    "coupon_type_id": {
                        "widget": "remote_select",
                        "help": (
                            "Choisir un type de coupon actif. Créer le coupon type avant les rewards "
                            "(voir GET /admin/coupon-types/ui-catalog)."
                        ),
                        "datasource": {
                            "endpoint": "/admin/ui-options/coupon-types",
                            "method": "GET",
                            "valueField": "id",
                            "labelField": "name",
                            "brandVia": "X-Brand",
                        },
                    },
                    "reward_ids": {
                        "widget": "remote_multi_select",
                        "optional": True,
                        "dependsOn": {"coupon_type_id": "coupon_type_id"},
                        "datasource": {
                            "endpoint": "/admin/ui-options/coupon-types/{coupon_type_id}/rewards",
                            "method": "GET",
                            "brandVia": "X-Brand",
                            "valueField": "id",
                            "labelField": "name",
                        },
                        "help": "Laisser vide pour émettre toutes les récompenses par défaut du type de coupon.",
                    },
                    "frequency": {
                        "widget": "select",
                        "options": [
                            {"label": "Once per customer (lifetime)", "value": "ONCE_PER_CUSTOMER"},
                            {"label": "Once per calendar year (1 year)", "value": "ONCE_PER_CALENDAR_YEAR"},
                            {"label": "Always", "value": "ALWAYS"},
                        ],
                    },
                },
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": True,
                    "sideEffects": ["Creates customer coupon", "Creates customer rewards snapshot"],
                    "commonErrors": [
                        "coupon_type_id missing",
                        "coupon type not found",
                        "invalid frequency",
                        "reward_ids not linked to coupon type",
                        "reward_ids contains inactive rewards",
                    ],
                },
            },
            {
                "type": "reset_status_points",
                "title": "Réinitialiser les points de statut",
                "description": "Remet les points de statut du client à zéro.",
                "params": {},
                "jsonSchema": _model_json_schema(ResetStatusPointsAction),
                "examples": [{"type": "reset_status_points"}],
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": True,
                    "sideEffects": ["Sets customer.status_points=0", "May update loyalty status and emit internal tier event"],
                    "commonErrors": ["customer not found"],
                },
            },
        ]
    }


@router.get("/rules/ui-catalog")
def get_rules_ui_catalog():

    def _model_json_schema(model_cls):
        fn = getattr(model_cls, "model_json_schema", None)
        if callable(fn):
            return fn()
        return model_cls.schema()

    return {
        "create": {
            "jsonSchema": _model_json_schema(RuleCreate),
            "uiHints": {
                "brand": {"widget": "hidden"},
                "transaction_type": {
                    "widget": "remote_select",
                    "datasource": {
                        "endpoint": "/admin/ui-options/transaction-types",
                        "method": "GET",
                        "query": {"active": True},
                        "valueField": "key",
                        "labelField": "name",
                        "brandVia": "X-Brand",
                    },
                },
                "priority": {"widget": "number", "min": 0},
                "active": {"widget": "switch"},
                "conditions": {
                    "widget": "rule_condition_builder",
                    "catalog": {"endpoint": "/admin/rule-conditions", "method": "GET"},
                },
                "actions": {
                    "widget": "rule_action_builder",
                    "catalog": {"endpoint": "/admin/rule-actions", "method": "GET"},
                },
            },
            "examples": [
                {
                    "name": "Example rule: purchase >= 100 gives points",
                    "transaction_type": "PURCHASE",
                    "priority": 0,
                    "active": True,
                    "conditions": {"and": [{"field": "payload.amount", "operator": "gte", "value": 100}]},
                    "actions": [{"type": "earn_points", "points": 10}],
                }
            ],
        },
        "update": {
            "jsonSchema": _model_json_schema(RuleUpdate),
            "uiHints": {
                "priority": {"widget": "number", "min": 0},
                "active": {"widget": "switch"},
                "conditions": {
                    "widget": "rule_condition_builder",
                    "catalog": {"endpoint": "/admin/rule-conditions", "method": "GET"},
                },
                "actions": {
                    "widget": "rule_action_builder",
                    "catalog": {"endpoint": "/admin/rule-actions", "method": "GET"},
                },
            },
        },
        "dependencies": {
            "ruleActionsCatalog": {"endpoint": "/admin/rule-actions", "method": "GET"},
            "ruleConditionsCatalog": {"endpoint": "/admin/rule-conditions", "method": "GET"},
            "transactionTypesOptions": {
                "endpoint": "/admin/ui-options/transaction-types",
                "method": "GET",
                "brandVia": "X-Brand",
            },
        },
    }


@router.get("/rules/ui-bundle")
def get_rules_ui_bundle(
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    rewards = db.query(Reward).filter(Reward.brand == brand).order_by(Reward.name.asc()).all()
    event_types = db.query(TransactionType).filter(TransactionType.brand == brand).order_by(TransactionType.key.asc()).all()
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).order_by(LoyaltyTier.min_status_points.asc(), LoyaltyTier.created_at.asc()).all()
    return {
        "brand": brand,
        "rules": {"uiCatalog": get_rules_ui_catalog()},
        "ruleActions": list_rule_actions_catalog(),
        "ruleConditions": list_rule_conditions_catalog(),
        "uiOptions": {
            "rewards": {
                "brand": brand,
                "items": [
                    {
                        "id": str(r.id),
                        "name": r.name,
                        "active": r.active,
                    }
                    for r in rewards
                ],
            },
            "transactionTypes": {
                "brand": brand,
                "items": [
                    {
                        "id": str(et.id),
                        "key": et.key,
                        "name": et.name,
                        "origin": et.origin,
                        "active": et.active,
                    }
                    for et in event_types
                ],
            },
            "loyaltyTiers": {
                "brand": brand,
                "items": [
                    {
                        "id": str(t.id),
                        "key": t.key,
                        "name": t.name,
                        "rank": t.rank,
                        "minStatusPoints": t.min_status_points,
                        "active": t.active,
                    }
                    for t in tiers
                ],
            },
        },
    }


@router.get("/rule-conditions")
def list_rule_conditions_catalog():
    return get_rule_conditions_catalog()
