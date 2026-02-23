def get_rule_conditions_catalog():
    return {
        "combinators": ["all", "any", "not"],
        "uiHints": {
            "payload_path": {"widget": "json_path"},
            "payload_cmp.op": {"widget": "select", "options": ["eq", "gte", "lte", "between"]},
            "customer_cmp.op": {"widget": "select", "options": ["eq", "gte", "lte", "between"]},
            "customer_cmp.field": {
                "widget": "select",
                "options": [
                    "lifetime_points",
                    "status_points",
                    "created_at",
                    "last_activity_at",
                    "loyalty_status",
                    "status",
                ],
            },
            "customer_loyalty_status_in": {
                "widget": "remote_multi_select",
                "datasource": {
                    "endpoint": "/admin/ui-options/loyalty-tiers",
                    "method": "GET",
                    "valueField": "key",
                    "labelField": "name",
                    "brandVia": "X-Brand",
                },
            },
            "weekday_in": {
                "widget": "multi_select",
                "options": [
                    {"value": 0, "label": "Sunday"},
                    {"value": 1, "label": "Monday"},
                    {"value": 2, "label": "Tuesday"},
                    {"value": 3, "label": "Wednesday"},
                    {"value": 4, "label": "Thursday"},
                    {"value": 5, "label": "Friday"},
                    {"value": 6, "label": "Saturday"},
                ],
            },
            "customer_status_in": {
                "widget": "multi_select",
                "options": ["ACTIVE", "INACTIVE", "VIP", "BANNED"],
            },
        },
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
