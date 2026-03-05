from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

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
