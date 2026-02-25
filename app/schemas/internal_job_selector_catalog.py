def get_internal_job_selector_catalog():
    return {
        "combinators": ["all", "any"],
        "leaf": [
            {
                "type": "birthdate_today",
                "title": "Anniversaire aujourd'hui",
                "description": "Cible les clients dont l'anniversaire est aujourd'hui.",
                "value": True,
            },
            {
                "type": "created_anniversary_today",
                "title": "Anniversaire d'inscription aujourd'hui",
                "description": "Cible les clients dont la date d'inscription a un anniversaire aujourd'hui.",
                "value": True,
            },
            {
                "type": "inactive_days_gte",
                "title": "Inactivité (jours)",
                "description": "Cible les clients dont la dernière activité date d'au moins N jours.",
                "value": 30,
            },
            {
                "type": "status_in",
                "title": "Statut client",
                "description": "Cible les clients dont le statut est dans une liste (ACTIVE/VIP/etc.).",
                "value": ["ACTIVE"],
            },
            {
                "type": "loyalty_status_in",
                "title": "Tier de fidélité",
                "description": "Cible les clients appartenant à un ou plusieurs tiers de fidélité.",
                "value": ["BRONZE"],
            },
            {
                "type": "lifetime_points_gte",
                "title": "Points cumulés minimum",
                "description": "Cible les clients dont les points cumulés (lifetime) dépassent un seuil.",
                "value": 1000,
            },
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
