def get_rule_conditions_catalog():
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
                    "endpoint": "/admin/rules/ui-options/condition-fields",
                    "method": "GET",
                    "brandVia": "X-Brand",
                    "valueField": "items",
                    "labelField": "items",
                    "dependsOn": {"transaction_type": "transaction_type"},
                },
                "allowCreate": True,
            },
            "value": {
                "widget": "json",
                "presetsDatasource": {
                    "endpoint": "/admin/rules/ui-options/condition-fields",
                    "method": "GET",
                    "brandVia": "X-Brand",
                    "path": "valuePresets",
                    "dependsOn": {"transaction_type": "transaction_type"},
                },
            },
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
