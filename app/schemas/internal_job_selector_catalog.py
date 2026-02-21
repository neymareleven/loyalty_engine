def get_internal_job_selector_catalog():
    return {
        "combinators": ["all", "any"],
        "leaf": [
            {"type": "birthdate_today", "value": True},
            {"type": "created_anniversary_today", "value": True},
            {"type": "inactive_days_gte", "value": 30},
            {"type": "status_in", "value": ["ACTIVE"]},
            {"type": "loyalty_status_in", "value": ["BRONZE"]},
            {"type": "lifetime_points_gte", "value": 1000},
        ],
        "uiHints": {
            "inactive_days_gte": {"widget": "number", "min": 0},
            "status_in": {
                "widget": "multi_select",
                "options": ["ACTIVE", "INACTIVE", "VIP", "BANNED"],
            },
            "loyalty_status_in": {
                "widget": "remote_multi_select",
                "datasource": {
                    "endpoint": "/admin/ui-options/loyalty-tiers",
                    "method": "GET",
                    "valueField": "key",
                    "labelField": "name",
                    "brandVia": "X-Brand",
                },
            },
            "lifetime_points_gte": {"widget": "number", "min": 0},
            "birthdate_today": {"widget": "switch"},
            "created_anniversary_today": {"widget": "switch"},
        },
        "examples": [
            {"all": [{"birthdate_today": True}, {"status_in": ["ACTIVE"]}]},
            {"any": [{"inactive_days_gte": 60}, {"loyalty_status_in": ["GOLD", "PLATINUM"]}]},
        ],
    }
