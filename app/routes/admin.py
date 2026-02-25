from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.customer_tag import CustomerTag
from app.models.event_type import EventType
from app.models.loyalty_tier import LoyaltyTier
from app.models.reward import Reward
from app.services.reward_service import expire_rewards
from app.schemas.rule_condition_catalog import get_rule_conditions_catalog
from app.schemas.rule import RuleCreate, RuleUpdate
from app.schemas.bonus_policy_catalog import get_bonus_award_policies_catalog
from app.schemas.rule_action_catalog import (
    AddCustomerTagAction,
    BurnPointsAction,
    DowngradeOneTierAction,
    EarnPointsAction,
    EarnPointsFromAmountAction,
    IssueRewardAction,
    RecordBonusAwardAction,
    RedeemRewardAction,
    ResetStatusPointsAction,
    SetCustomerStatusAction,
)


router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/rewards/expire")
def admin_expire_rewards(db: Session = Depends(get_db)):
    expired_count = expire_rewards(db)
    db.commit()
    return {"expired": expired_count}


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


@router.get("/ui-options/event-types")
def list_ui_options_event_types(
    brand: str = Depends(get_active_brand),
    active: bool | None = True,
    origin: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(EventType).filter(EventType.brand == brand)
    if active is not None:
        q = q.filter(EventType.active.is_(active))
    if origin:
        q = q.filter(EventType.origin == origin)
    items = q.order_by(EventType.key.asc()).all()
    return {
        "brand": brand,
        "items": [
            {
                "id": str(et.id),
                "key": et.key,
                "name": et.name,
                "origin": et.origin,
                "active": et.active,
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


@router.get("/ui-options/customer-tags")
def list_ui_options_customer_tags(
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    q = (
        db.query(CustomerTag.tag)
        .join(Customer, Customer.id == CustomerTag.customer_id)
        .filter(Customer.brand == brand)
        .distinct()
        .order_by(CustomerTag.tag.asc())
    )
    return {
        "brand": brand,
        "items": [{"tag": row[0]} for row in q.all()],
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
        "actions": [
            {
                "type": "earn_points",
                "title": "Ajouter des points",
                "description": "Crédite un nombre de points au client.",
                "params": {"points": "int", "from_payload": "str (optional)"},
                "jsonSchema": _model_json_schema(EarnPointsAction),
                "examples": [{"type": "earn_points", "points": 50}],
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
                "description": "Débite un nombre de points au client.",
                "params": {"points": "int", "from_payload": "str (optional)"},
                "jsonSchema": _model_json_schema(BurnPointsAction),
                "examples": [{"type": "burn_points", "points": 20}],
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": False,
                    "sideEffects": ["Creates point movement", "Decrements customer points", "May update loyalty status"],
                    "commonErrors": ["points missing/invalid", "not enough points", "customer not found"],
                },
            },
            {
                "type": "redeem_reward",
                "title": "Utiliser une récompense",
                "description": "Consomme une récompense (et peut débiter des points selon la config de la récompense).",
                "params": {"reward_id": "uuid"},
                "jsonSchema": _model_json_schema(RedeemRewardAction),
                "examples": [{"type": "redeem_reward", "reward_id": "<uuid>"}],
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
                    "sideEffects": ["Creates customer reward", "May burn points if reward has cost_points"],
                    "commonErrors": ["reward_id missing", "reward not found", "not enough points"],
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
                "type": "earn_points_from_amount",
                "title": "Ajouter des points selon un montant",
                "description": "Calcule des points à partir d'un montant dans le payload (ex: amount * rate).",
                "params": {
                    "amount_path": "str (optional, default: amount)",
                    "rate": "float",
                    "min_points": "int (optional)",
                    "max_points": "int (optional)",
                },
                "jsonSchema": _model_json_schema(EarnPointsFromAmountAction),
                "examples": [{"type": "earn_points_from_amount", "rate": 1.0, "amount_path": "amount"}],
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": False,
                    "sideEffects": ["Creates point movement", "Increments customer points", "May update loyalty status"],
                    "commonErrors": ["rate missing/invalid", "amount missing/invalid"],
                },
            },
            {
                "type": "record_bonus_award",
                "title": "Enregistrer l'attribution d'un bonus",
                "description": "Enregistre qu'un bonus a été attribué au client (utile pour l'idempotence selon la policy).",
                "params": {"bonusKey": "str"},
                "jsonSchema": _model_json_schema(RecordBonusAwardAction),
                "examples": [{"type": "record_bonus_award", "bonusKey": "BIRTHDAY_200"}],
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": True,
                    "sideEffects": ["Creates a bonus award record if not already awarded in period"],
                    "commonErrors": ["bonusKey missing", "bonusKey unknown", "brand mismatch"],
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
            {
                "type": "downgrade_one_tier",
                "title": "Rétrograder d'un niveau",
                "description": "Fait reculer le client d'un tier de fidélité (selon la configuration des tiers).",
                "params": {},
                "jsonSchema": _model_json_schema(DowngradeOneTierAction),
                "examples": [{"type": "downgrade_one_tier"}],
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": False,
                    "sideEffects": ["Adjusts customer.status_points to previous tier", "Updates loyalty status and emits internal tier event"],
                    "commonErrors": ["current tier not found", "no lower tier"],
                },
            },
            {
                "type": "set_customer_status",
                "title": "Définir le statut client",
                "description": "Met à jour le champ status du client (ex: VIP).",
                "params": {"status": "str"},
                "jsonSchema": _model_json_schema(SetCustomerStatusAction),
                "examples": [{"type": "set_customer_status", "status": "VIP"}],
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": True,
                    "sideEffects": ["Sets customer.status"],
                    "commonErrors": ["status missing"],
                },
            },
            {
                "type": "add_customer_tag",
                "title": "Ajouter un tag client",
                "description": "Ajoute un tag au client (et peut créer le tag s'il n'existe pas).",
                "params": {"tag": "str"},
                "jsonSchema": _model_json_schema(AddCustomerTagAction),
                "examples": [{"type": "add_customer_tag", "tag": "birthday"}],
                "uiHints": {
                    "tag": {
                        "widget": "remote_select",
                        "allowCreate": True,
                        "datasource": {
                            "endpoint": "/admin/ui-options/customer-tags",
                            "method": "GET",
                            "valueField": "tag",
                            "labelField": "tag",
                            "brandVia": "X-Brand",
                        },
                    }
                },
                "semantics": {
                    "atomicity": "Per-rule: all actions rollback if one fails.",
                    "idempotent": True,
                    "sideEffects": ["Creates customer tag if not existing"],
                    "commonErrors": ["tag missing"],
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
                "event_type": {
                    "widget": "remote_select",
                    "datasource": {
                        "endpoint": "/admin/ui-options/event-types",
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
                    "event_type": "PURCHASE",
                    "priority": 0,
                    "active": True,
                    "conditions": {"all": [{"amount_gte": 100}]},
                    "actions": [{"type": "earn_points_from_amount", "rate": 1.0, "amount_path": "amount"}],
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
            "eventTypesOptions": {
                "endpoint": "/admin/ui-options/event-types",
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
    event_types = db.query(EventType).filter(EventType.brand == brand).order_by(EventType.key.asc()).all()
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).order_by(LoyaltyTier.rank.asc()).all()
    tags_q = (
        db.query(CustomerTag.tag)
        .join(Customer, Customer.id == CustomerTag.customer_id)
        .filter(Customer.brand == brand)
        .distinct()
        .order_by(CustomerTag.tag.asc())
    )

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
            "eventTypes": {
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
            "customerTags": {
                "brand": brand,
                "items": [{"tag": row[0]} for row in tags_q.all()],
            },
        },
    }


@router.get("/rule-conditions")
def list_rule_conditions_catalog():
    return get_rule_conditions_catalog()


@router.get("/bonus-award-policies")
def list_bonus_award_policies_catalog():
    return get_bonus_award_policies_catalog()
