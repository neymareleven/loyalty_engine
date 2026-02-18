from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.reward_service import expire_rewards


router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/rewards/expire")
def admin_expire_rewards(db: Session = Depends(get_db)):
    expired_count = expire_rewards(db)
    db.commit()
    return {"expired": expired_count}


@router.get("/rule-actions")
def list_rule_actions_catalog():
    return {
        "actions": [
            {
                "type": "earn_points",
                "params": {
                    "points": "int",
                    "from_payload": "str (optional)",
                },
            },
            {
                "type": "burn_points",
                "params": {
                    "points": "int",
                    "from_payload": "str (optional)",
                },
            },
            {"type": "redeem_reward", "params": {}},
            {"type": "issue_reward", "params": {}},
            {
                "type": "earn_points_from_amount",
                "params": {
                    "amount_path": "str (optional, default: amount)",
                    "rate": "float",
                    "min_points": "int (optional)",
                    "max_points": "int (optional)",
                },
            },
            {
                "type": "record_bonus_award",
                "params": {
                    "bonusKey": "str",
                },
            },
            {"type": "reset_status_points", "params": {}},
            {"type": "downgrade_one_tier", "params": {}},
            {"type": "set_customer_status", "params": {"status": "str"}},
            {"type": "add_customer_tag", "params": {"tag": "str"}},
        ]
    }


@router.get("/rule-conditions")
def list_rule_conditions_catalog():
    return {
        "combinators": ["all", "any", "not"],
        "conditions": [
            {"type": "payload", "params": {"<path>": "<expectedValue>"}},
            {"type": "payload_present", "params": ["<path>"]},
            {"type": "payload_in", "params": {"<path>": ["<value>"]}},
            {"type": "payload_contains", "params": {"<path>": "<value>"}},
            {
                "type": "payload_cmp",
                "params": {"path": "<path>", "op": "eq|gte|lte|between", "value": "any"},
            },
            {"type": "amount_gte", "params": "int"},
            {"type": "points_gte", "params": "int"},
            {"type": "customer_loyalty_status_in", "params": ["<tierKey>"]},
            {"type": "customer_lifetime_points_gte", "params": "int"},
            {"type": "customer_status_in", "params": ["<status>"]},
            {"type": "customer_created_days_gte", "params": "int"},
            {"type": "customer_last_activity_days_gte", "params": "int"},
            {
                "type": "customer_cmp",
                "params": {"field": "<field>", "op": "eq|gte|lte|between", "value": "any"},
            },
            {"type": "weekday_in", "params": ["0..6"]},
            {"type": "first_purchase", "params": True},
            {"type": "birthday", "params": True},
            {"type": "birthday_bonus_points", "params": "int (optional)"},
            {
                "type": "earn_points_awarded_this_year",
                "params": "int | {points:int}",
            },
            {
                "type": "bonus_awarded",
                "params": "str | {bonusKey:str}",
            },
        ],
    }
