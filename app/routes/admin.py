from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, func
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
from app.models.transaction import Transaction
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.services.reward_service import expire_rewards
from app.services.coupon_service import expire_coupons
from app.services.loyalty_settings_service import get_or_create_loyalty_settings
from app.services.loyalty_validity_service import initialize_validity_windows_for_existing_customers
from app.schemas.loyalty_settings import LoyaltySettingsOut, LoyaltySettingsUpdate
from app.schemas.rule_condition_catalog import get_rule_conditions_catalog
from app.schemas.rule import RuleCreate, RuleUpdate
from app.schemas.rule_action_catalog import (
    EarnPointsAction,
    IssueCouponAction,
    ResetStatusPointsAction,
)


router = APIRouter(prefix="/admin", tags=["admin"])


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
        "coupon_validity_days": getattr(obj, "coupon_validity_days", None),
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
    return {
        "brand": brand,
        "items": [
            {
                "id": str(ct.id),
                "name": ct.name,
                "active": ct.active,
                "rewardCategoryId": str(
                    (
                        db.query(RewardCategory.id)
                        .filter(RewardCategory.brand == brand)
                        .filter(RewardCategory.coupon_type_id == ct.id)
                        .scalar()
                    )
                ),
            }
            for ct in items
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
    prev_coupon_days = getattr(obj, "coupon_validity_days", None)

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

    if "coupon_validity_days" in data and data["coupon_validity_days"] is not None:
        try:
            v = int(data["coupon_validity_days"])
        except Exception:
            raise HTTPException(status_code=400, detail="coupon_validity_days must be an integer")
        if v < 0:
            raise HTTPException(status_code=400, detail="coupon_validity_days must be >= 0")
        obj.coupon_validity_days = v
    elif "coupon_validity_days" in data and data["coupon_validity_days"] is None:
        obj.coupon_validity_days = None

    should_backfill_points = obj.points_validity_days is not None and prev_points_days is None
    should_backfill_status = obj.loyalty_status_validity_days is not None and prev_status_days is None
    should_backfill_coupon = getattr(obj, "coupon_validity_days", None) is not None and prev_coupon_days is None

    if should_backfill_points or should_backfill_status or should_backfill_coupon:
        initialize_validity_windows_for_existing_customers(db, brand=brand)

    db.commit()
    db.refresh(obj)
    return {
        "brand": obj.brand,
        "points_validity_days": obj.points_validity_days,
        "loyalty_status_validity_days": obj.loyalty_status_validity_days,
        "coupon_validity_days": getattr(obj, "coupon_validity_days", None),
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
    out: list[str] = []

    def normalize(node: Any) -> Any:
        if not isinstance(node, dict):
            return node

        # Manual format supported by TransactionTypeCreate.payload_schema:
        # { fieldName: {type, description?}, ... }
        # Normalize to JSON-Schema-like: {type: object, properties: ...}
        looks_like_manual_map = (
            "type" not in node
            and "properties" not in node
            and any(
                isinstance(k, str)
                and k
                and isinstance(v, dict)
                and ("type" in v or "description" in v)
                for k, v in node.items()
            )
        )
        if looks_like_manual_map:
            return {"type": "object", "properties": node}

        return node

    def walk(node: Any, path: str, depth: int = 0):
        if depth > 12:
            return
        if not isinstance(node, dict):
            return

        node_type = node.get("type")
        if node_type == "object" or "properties" in node:
            props = node.get("properties")
            if isinstance(props, dict):
                for k, v in props.items():
                    if not isinstance(k, str) or not k:
                        continue
                    next_path = f"{path}.{k}" if path else k
                    out.append(next_path)
                    walk(v, next_path, depth + 1)
            return

        if node_type == "array":
            items = node.get("items")
            if items is not None:
                walk(items, path, depth + 1)
            return

    walk(normalize(schema), prefix)
    uniq = sorted({p for p in out if isinstance(p, str) and p.startswith(prefix)})
    return uniq


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


@router.get("/rewards/ui-catalog")
def get_rewards_ui_catalog():
    return {
        "create": {
            "fieldHelp": {
                "name": "Nom lisible de la récompense (affiché dans le backoffice).",
                "description": "Description (optionnelle) affichée au support/ops.",
            },
            "rewardTypes": [
                _reward_type_catalog_item(
                    reward_type="ENTITLEMENT",
                    title="Entitlement",
                    description="Static reward content attached to coupons.",
                    required=[],
                    visible=["name", "description", "active"],
                ),
            ]
        }
    }


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
    if tt.payload_schema is not None:
        payload_fields = _json_schema_paths(tt.payload_schema, prefix="payload")

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
        },
        "items": items,
    }


@router.get("/internal-jobs/ui-options/selector-fields")
def list_internal_job_selector_fields():
    customer_fields = [
        "customer.gender",
        "customer.status",
        "customer.loyalty_status",
        "customer.birthdate",
        "customer.created_at",
        "customer.last_activity_at",
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
            "customer.birthdate": {"valueKind": "date", "ui": {"widget": "date"}},
            "customer.created_at": {"valueKind": "datetime", "ui": {"widget": "datetime"}},
            "customer.last_activity_at": {"valueKind": "datetime", "ui": {"widget": "datetime"}},
        },
        "valuePresets": {
            "datetime": [
                {"id": "system.now", "label": "Now", "value": {"$system": "now"}},
            ]
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
    items = q.order_by(Reward.name.asc()).all()
    return {
        "brand": brand,
        "items": [
            {
                "id": str(r.id),
                "name": r.name,
                "active": r.active,
                "type": r.type,
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
    q = db.query(TransactionType).filter(TransactionType.brand == brand)
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
                ],
                "uiHints": {
                    "points": {"widget": "number", "min": 0},
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
                "description": "Émet un coupon pour le client et lui attribue toutes les récompenses actives de la catégorie associée (snapshot au moment de l’émission).",
                "params": {"coupon_type_id": "uuid", "frequency": "ALWAYS | ONCE_PER_CALENDAR_YEAR"},
                "jsonSchema": _model_json_schema(IssueCouponAction),
                "examples": [
                    {"type": "issue_coupon", "coupon_type_id": "<uuid>"},
                    {"type": "issue_coupon", "coupon_type_id": "<uuid>", "frequency": "ALWAYS"},
                ],
                "uiHints": {
                    "coupon_type_id": {
                        "widget": "remote_select",
                        "datasource": {
                            "endpoint": "/admin/ui-options/coupon-types",
                            "method": "GET",
                            "valueField": "id",
                            "labelField": "name",
                            "brandVia": "X-Brand",
                        },
                    },
                    "frequency": {
                        "widget": "select",
                        "options": [
                            {"label": "Once per calendar year", "value": "ONCE_PER_CALENDAR_YEAR"},
                            {"label": "Always", "value": "ALWAYS"},
                        ],
                    },
                },
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": True,
                    "sideEffects": ["Creates customer coupon", "Creates customer rewards snapshot"],
                    "commonErrors": ["coupon_type_id missing", "coupon type not found", "invalid frequency"],
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
                        "query": {"origin": "EXTERNAL", "active": True},
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
                        "type": r.type,
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
