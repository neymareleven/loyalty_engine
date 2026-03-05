def get_rule_conditions_catalog():
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
                    "payload.amount",
                    "payload.points",
                    "payload.currency",
                    "customer.status",
                    "customer.loyalty_status",
                    "customer.lifetime_points",
                    "customer.status_points",
                    "customer.created_at",
                    "customer.last_activity_at",
                    "system.weekday",
                    "system.customer_created_days",
                    "system.customer_last_activity_days",
                ],
                "allowCreate": True,
            },
            "value": {"widget": "json"},
        },
        "conditions": [
            {
                "type": "leaf",
                "title": "Condition (field/operator/value)",
                "description": "Feuille AST: compare un champ (payload.*, customer.*, system.*) via un opérateur.",
                "params": {"field": "payload.amount", "operator": "gte", "value": 100},
            }
        ],
    }
