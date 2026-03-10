def get_internal_job_selector_catalog():
    return {
        "combinators": ["and", "or", "not"],
        "uiHints": {
            "operator": {
                "widget": "select",
                "options": ["eq", "neq", "gt", "gte", "lt", "lte", "between", "in", "contains", "exists"],
            },
            "field": {
                "widget": "remote_select",
                "datasource": {
                    "endpoint": "/admin/internal-jobs/ui-options/selector-fields",
                    "method": "GET",
                    "brandVia": "X-Brand",
                    "valueField": "items",
                    "labelField": "items",
                },
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
                    {"field": "customer.birthdate", "operator": "between", "value": ["2026-03-04", "2026-03-04"]},
                ]
            },
            {
                "and": [
                    {"field": "customer.status", "operator": "in", "value": ["ACTIVE"]},
                    {"field": "customer.last_activity_at", "operator": "lte", "value": "2026-01-01T00:00:00"},
                ]
            },
            {
                "and": [
                    {"field": "customer.status", "operator": "in", "value": ["ACTIVE"]},
                    {"field": "customer.created_at", "operator": "lte", "value": {"$system": "now"}},
                ]
            },
            {"or": [{"field": "customer.lifetime_points", "operator": "gte", "value": 1000}, {"field": "customer.loyalty_status", "operator": "in", "value": ["GOLD", "PLATINUM"]}]},
        ],
    }
