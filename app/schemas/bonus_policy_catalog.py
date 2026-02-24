from typing import Any, Dict


def get_bonus_award_policies_catalog() -> Dict[str, Any]:
    return {
        "policies": [
            {
                "type": "ONCE_EVER",
                "title": "Once ever",
                "description": "Award can be given only once per customer (no period).",
                "policyParamsSchema": {"type": "object", "additionalProperties": False, "properties": {}},
                "examples": [{"award_policy": "ONCE_EVER", "policy_params": {}}],
            },
            {
                "type": "ONCE_PER_YEAR",
                "title": "Once per year",
                "description": "Award can be given once per customer per calendar year.",
                "policyParamsSchema": {"type": "object", "additionalProperties": False, "properties": {}},
                "examples": [{"award_policy": "ONCE_PER_YEAR", "policy_params": {}}],
            },
            {
                "type": "ONCE_PER_MONTH",
                "title": "Once per month",
                "description": "Award can be given once per customer per calendar month.",
                "policyParamsSchema": {"type": "object", "additionalProperties": False, "properties": {}},
                "examples": [{"award_policy": "ONCE_PER_MONTH", "policy_params": {}}],
            },
            {
                "type": "ONCE_PER_WEEK",
                "title": "Once per week",
                "description": "Award can be given once per customer per ISO week.",
                "policyParamsSchema": {"type": "object", "additionalProperties": False, "properties": {}},
                "examples": [{"award_policy": "ONCE_PER_WEEK", "policy_params": {}}],
            },
            {
                "type": "ONCE_PER_DAY",
                "title": "Once per day",
                "description": "Award can be given once per customer per day.",
                "policyParamsSchema": {"type": "object", "additionalProperties": False, "properties": {}},
                "examples": [{"award_policy": "ONCE_PER_DAY", "policy_params": {}}],
            },
        ],
        "notes": [
            "policy_params is currently optional and unused by the engine for these policies.",
            "This catalog exists to let the frontend render a guided form instead of a raw JSON editor.",
        ],
    }
