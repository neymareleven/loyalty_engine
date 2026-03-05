def get_internal_job_selector_catalog():
    return {
        "combinators": ["and", "or", "not"],
        "uiHints": {
            "operator": {
                "widget": "select",
                "options": ["eq", "neq", "gt", "gte", "lt", "lte", "between", "in", "contains", "exists"],
            },
            "field": {
                "widget": "select",
                "options": [
                    "customer.status",
                    "customer.loyalty_status",
                    "customer.lifetime_points",
                    "customer.birthdate_month",
                    "customer.birthdate_day",
                    "customer.created_at_month",
                    "customer.created_at_day",
                    "customer.last_activity_at",
                    "system.today_month",
                    "system.today_day",
                ],
                "allowCreate": True,
            },
            "value": {"widget": "json"},
        },
        "leaf": [
            {
                "type": "leaf",
                "title": "Selector leaf (field/operator/value)",
                "description": "Feuille AST: compare un champ customer.* ou system.* via un opérateur.",
                "value": {"field": "customer.status", "operator": "in", "value": ["ACTIVE"]},
            }
        ],
        "examples": [
            {
                "and": [
                    {"field": "customer.status", "operator": "in", "value": ["ACTIVE"]},
                    {"field": "customer.birthdate_month", "operator": "eq", "value": 3},
                    {"field": "customer.birthdate_day", "operator": "eq", "value": 4},
                ]
            },
            {
                "and": [
                    {"field": "customer.status", "operator": "in", "value": ["ACTIVE"]},
                    {"field": "customer.last_activity_at", "operator": "lte", "value": "2026-01-01T00:00:00"},
                ]
            },
            {"or": [{"field": "customer.lifetime_points", "operator": "gte", "value": 1000}, {"field": "customer.loyalty_status", "operator": "in", "value": ["GOLD", "PLATINUM"]}]},
        ],
    }
