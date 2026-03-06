from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.event_type import TransactionType
from app.models.loyalty_tier import LoyaltyTier
from app.models.reward import Reward
from app.services.reward_service import expire_rewards
from app.schemas.rule_condition_catalog import get_rule_conditions_catalog
from app.schemas.rule import RuleCreate, RuleUpdate
from app.schemas.rule_action_catalog import (
    BurnPointsAction,
    EarnPointsAction,
    IssueRewardAction,
    ResetStatusPointsAction,
    SetRankAction,
)


router = APIRouter(prefix="/admin", tags=["admin"])


def _json_schema_paths(schema: Any, *, prefix: str) -> list[str]:
    out: list[str] = []

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

    walk(schema, prefix)
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
    base_required = ["name", "type"]
    req = sorted({*base_required, *required})

    props: dict[str, Any] = {
        "brand": {"type": ["string", "null"]},
        "name": {"type": "string"},
        "description": {"type": ["string", "null"]},
        "cost_points": {"type": ["integer", "null"], "minimum": 0},
        "type": {"type": "string", "enum": [reward_type]},
        "validity_days": {"type": ["integer", "null"], "minimum": 0},
        "currency": {"type": ["string", "null"], "minLength": 3, "maxLength": 3},
        "value_amount": {"type": ["integer", "null"], "minimum": 0},
        "value_percent": {"type": ["integer", "null"], "minimum": 1, "maximum": 100},
        "params": {"type": ["object", "null"]},
        "active": {"type": "boolean"},
    }

    ui_hints: dict[str, Any] = {
        "brand": {"widget": "hidden"},
        "type": {"widget": "select", "options": ["POINTS", "DISCOUNT", "CASHBACK", "VOUCHER"]},
        "description": {"widget": "textarea"},
        "cost_points": {"widget": "number", "min": 0},
        "validity_days": {"widget": "number", "min": 0, "placeholder": "Optional"},
        "currency": {"widget": "text", "placeholder": "ISO code (EUR, XAF, ...)"},
        "value_amount": {"widget": "number", "min": 0},
        "value_percent": {"widget": "number", "min": 1, "max": 100},
        "params": {
            "widget": "kv_object",
            "keyPlaceholder": "key",
            "valuePlaceholder": "value",
        },
        "active": {"widget": "switch"},
        "_ui": {
            "visibleFields": visible,
        },
    }

    if reward_type == "VOUCHER" and voucher_presets:
        ui_hints["params"] = {
            "widget": "voucher_params_builder",
            "presets": voucher_presets,
            "serialize": {
                "target": "params",
                "format": "object",
                "note": "Frontend should serialize preset form fields into RewardCreate.params (dict).",
            },
        }

    return {
        "type": reward_type,
        "title": title,
        "description": description,
        "jsonSchema": {
            "type": "object",
            "properties": props,
            "required": req,
            "additionalProperties": False,
        },
        "uiHints": ui_hints,
        "examples": [{"name": "Example", "type": reward_type}],
    }


@router.get("/rewards/ui-catalog")
def get_rewards_ui_catalog():
    common = ["name", "description", "type", "cost_points", "validity_days", "active"]

    voucher_presets: list[dict[str, Any]] = [
        {
            "key": "COUPON_FIXED",
            "title": "Coupon fixe",
            "description": "Un code/coupon avec une remise fixe (montant).",
            "defaultParams": {"kind": "COUPON_FIXED", "amount": 0, "currency": "XAF", "code_mode": "AUTO"},
            "form": {
                "jsonSchema": {
                    "type": "object",
                    "properties": {
                        "amount": {"type": "integer", "minimum": 0},
                        "currency": {"type": "string", "minLength": 3, "maxLength": 3},
                        "code_mode": {"type": "string", "enum": ["AUTO", "MANUAL"]},
                        "manual_code": {"type": ["string", "null"]},
                        "description": {"type": ["string", "null"]},
                    },
                    "required": ["amount", "currency", "code_mode"],
                    "additionalProperties": False,
                },
                "uiHints": {
                    "amount": {"widget": "number", "min": 0},
                    "currency": {"widget": "text", "placeholder": "ISO (XAF, EUR, ...)"},
                    "code_mode": {"widget": "select", "options": ["AUTO", "MANUAL"]},
                    "manual_code": {"widget": "text", "placeholder": "If MANUAL"},
                    "description": {"widget": "textarea", "placeholder": "Optional"},
                },
            },
            "serialize": {"kind": "COUPON_FIXED"},
        },
        {
            "key": "COUPON_PERCENT",
            "title": "Coupon %",
            "description": "Un code/coupon avec une remise en pourcentage.",
            "defaultParams": {"kind": "COUPON_PERCENT", "percent": 10, "code_mode": "AUTO"},
            "form": {
                "jsonSchema": {
                    "type": "object",
                    "properties": {
                        "percent": {"type": "integer", "minimum": 1, "maximum": 100},
                        "code_mode": {"type": "string", "enum": ["AUTO", "MANUAL"]},
                        "manual_code": {"type": ["string", "null"]},
                        "description": {"type": ["string", "null"]},
                    },
                    "required": ["percent", "code_mode"],
                    "additionalProperties": False,
                },
                "uiHints": {
                    "percent": {"widget": "number", "min": 1, "max": 100},
                    "code_mode": {"widget": "select", "options": ["AUTO", "MANUAL"]},
                    "manual_code": {"widget": "text", "placeholder": "If MANUAL"},
                    "description": {"widget": "textarea", "placeholder": "Optional"},
                },
            },
            "serialize": {"kind": "COUPON_PERCENT"},
        },
        {
            "key": "FREE_CODE",
            "title": "Code libre",
            "description": "Un voucher qui ne porte pas de valeur calculée ici (ex: cadeau externe, lien, instruction).",
            "defaultParams": {"kind": "FREE_CODE", "code_mode": "AUTO"},
            "form": {
                "jsonSchema": {
                    "type": "object",
                    "properties": {
                        "code_mode": {"type": "string", "enum": ["AUTO", "MANUAL"]},
                        "manual_code": {"type": ["string", "null"]},
                        "label": {"type": ["string", "null"]},
                        "instructions": {"type": ["string", "null"]},
                    },
                    "required": ["code_mode"],
                    "additionalProperties": False,
                },
                "uiHints": {
                    "code_mode": {"widget": "select", "options": ["AUTO", "MANUAL"]},
                    "manual_code": {"widget": "text", "placeholder": "If MANUAL"},
                    "label": {"widget": "text", "placeholder": "Optional"},
                    "instructions": {"widget": "textarea", "placeholder": "Optional"},
                },
            },
            "serialize": {"kind": "FREE_CODE"},
        },
    ]

    return {
        "create": {
            "fieldHelp": {
                "name": "Nom lisible de la récompense (affiché dans le backoffice).",
                "description": "Description (optionnelle) affichée au support/ops.",
                "cost_points": "Coût en points si la récompense est achetée via le catalogue. Laisser vide/null pour une récompense gratuite (marketing).",
                "validity_days": "Durée de validité après attribution (en jours). Laisser vide/null pour illimité.",
                "currency": "Devise ISO 3 lettres (XAF, EUR...). Utilisée uniquement si le type utilise un montant.",
                "value_amount": "Montant (ex: 500 XAF). Utilisé pour CASHBACK et pour DISCOUNT si remise fixe.",
                "value_percent": "Pourcentage (1..100). Utilisé pour DISCOUNT si remise en %.",
                "params": "Paramètres additionnels du type. Pour VOUCHER: utilisez un preset (formulaire guidé) plutôt qu'un JSON brut.",
            },
            "rewardTypes": [
                _reward_type_catalog_item(
                    reward_type="POINTS",
                    title="Points",
                    description="A simple reward redeemable with points.",
                    required=[],
                    visible=common,
                ),
                _reward_type_catalog_item(
                    reward_type="DISCOUNT",
                    title="Discount",
                    description="A discount reward (percentage or fixed amount).",
                    required=[],
                    visible=[*common, "currency", "value_amount", "value_percent"],
                ),
                _reward_type_catalog_item(
                    reward_type="CASHBACK",
                    title="Cashback",
                    description="A cashback reward (fixed amount + currency).",
                    required=["currency", "value_amount"],
                    visible=[*common, "currency", "value_amount"],
                ),
                _reward_type_catalog_item(
                    reward_type="VOUCHER",
                    title="Voucher",
                    description="A voucher reward. Params are entered as key/value pairs.",
                    required=["params"],
                    visible=[*common, "params"],
                    voucher_presets=voucher_presets,
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
        "customer.lifetime_points",
        "customer.status_points",
        "customer.created_at",
        "customer.last_activity_at",
        "customer.birthdate",
        "customer.rewards",
    ]
    system_fields = [
        "system.weekday",
        "system.customer_created_days",
        "system.customer_last_activity_days",
        "system.now",
    ]

    items = sorted({*payload_fields, *customer_fields, *system_fields})
    return {
        "brand": brand,
        "transaction_type": transaction_type,
        "sources": {
            "payload": payload_fields,
            "customer": customer_fields,
            "system": system_fields,
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
        "customer.lifetime_points",
        "customer.birthdate_month",
        "customer.birthdate_day",
        "customer.created_at_month",
        "customer.created_at_day",
        "customer.last_activity_at",
    ]
    system_fields = [
        "system.today_month",
        "system.today_day",
        "system.now",
    ]
    items = sorted({*customer_fields, *system_fields})
    return {
        "sources": {
            "customer": customer_fields,
            "system": system_fields,
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
                "costPoints": r.cost_points,
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
    items = q.order_by(LoyaltyTier.rank.asc()).all()
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
                "type": "burn_points",
                "title": "Retirer des points",
                "description": "Débite un nombre de points du portefeuille (wallet) du client.",
                "params": {"points": "int"},
                "jsonSchema": _model_json_schema(BurnPointsAction),
                "examples": [{"type": "burn_points", "points": 20}],
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": False,
                    "sideEffects": ["Creates point movement"],
                    "commonErrors": ["points missing/invalid", "not enough points", "customer not found"],
                },
            },
            {
                "type": "issue_reward",
                "title": "Attribuer une récompense",
                "description": "Crée une récompense pour le client (sans la consommer).",
                "params": {"reward_id": "uuid"},
                "jsonSchema": _model_json_schema(IssueRewardAction),
                "examples": [{"type": "issue_reward", "reward_id": "<uuid>"}],
                "uiHints": {
                    "reward_id": {
                        "widget": "remote_select",
                        "datasource": {
                            "endpoint": "/admin/ui-options/rewards",
                            "method": "GET",
                            "valueField": "id",
                            "labelField": "name",
                            "brandVia": "X-Brand",
                        },
                    }
                },
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": False,
                    "sideEffects": ["Creates customer reward"],
                    "commonErrors": ["reward_id missing", "reward not found"],
                },
            },
            {
                "type": "set_rank",
                "title": "Définir un niveau (tier)",
                "description": "Place le client sur un tier cible en ajustant automatiquement ses points de statut au minimum du tier.",
                "params": {"tier_key": "str"},
                "jsonSchema": _model_json_schema(SetRankAction),
                "examples": [{"type": "set_rank", "tier_key": "GOLD"}],
                "uiHints": {
                    "tier_key": {
                        "widget": "remote_select",
                        "datasource": {
                            "endpoint": "/admin/ui-options/loyalty-tiers",
                            "method": "GET",
                            "valueField": "key",
                            "labelField": "name",
                            "brandVia": "X-Brand",
                        },
                    }
                },
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": False,
                    "sideEffects": ["Adjusts customer.status_points", "Updates loyalty status"],
                    "commonErrors": ["tier_key missing/invalid", "tier not found"],
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
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).order_by(LoyaltyTier.rank.asc()).all()
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
                        "costPoints": r.cost_points,
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
