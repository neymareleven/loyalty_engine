def get_rule_conditions_catalog():
    return {
        "combinators": ["all", "any", "not"],
        "uiHints": {
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
            {
                "type": "customer_loyalty_status_in",
                "title": "Statut de fidélité (tiers)",
                "description": "Vérifie que le client est dans un ou plusieurs tiers de fidélité.",
                "params": {"values": ["<tierKey>"]},
            },
            {
                "type": "customer_lifetime_points_gte",
                "title": "Points cumulés (lifetime) minimum",
                "description": "Vérifie que les points cumulés (historique) du client dépassent un seuil.",
                "params": {"value": 0},
            },
            {
                "type": "customer_status_in",
                "title": "Statut client",
                "description": "Vérifie que le statut du client est dans une liste (ACTIVE/VIP/etc.).",
                "params": {"values": ["<status>"]},
            },
            {
                "type": "customer_created_days_gte",
                "title": "Ancienneté du compte (jours)",
                "description": "Vérifie que le client a été créé il y a au moins N jours.",
                "params": {"value": 0},
            },
            {
                "type": "customer_last_activity_days_gte",
                "title": "Inactivité (jours)",
                "description": "Vérifie que la dernière activité du client date d'au moins N jours.",
                "params": {"value": 0},
            },
            {
                "type": "customer_cmp",
                "title": "Comparer un champ client",
                "description": "Compare un champ du client (eq/gte/lte/between) avec une valeur.",
                "params": {"field": ["<field>"], "op": "eq|gte|lte|between", "value": "any"},
            },
            {
                "type": "weekday_in",
                "title": "Jour de la semaine",
                "description": "Vérifie que la date de l'événement tombe sur un ou plusieurs jours (0=dimanche..6=samedi).",
                "params": {"values": [0]},
            },
            {
                "type": "first_purchase",
                "title": "Premier achat",
                "description": "Vérifie que c'est le premier achat du client.",
                "params": {"value": True},
            },
            {
                "type": "birthday",
                "title": "Anniversaire aujourd'hui",
                "description": "Vérifie que la date du jour correspond à l'anniversaire du client.",
                "params": {"value": True},
            },
            {
                "type": "birthday_bonus_points",
                "title": "Bonus anniversaire (points)",
                "description": "Condition spécifique anniversaire (valeur optionnelle selon votre logique métier).",
                "params": {"value": 0},
            },
            {
                "type": "earn_points_awarded_this_year",
                "title": "Points déjà gagnés cette année",
                "description": "Vérifie un total de points gagnés sur l'année en cours (selon votre implémentation).",
                "params": {"value": 0},
            },
            {
                "type": "bonus_awarded",
                "title": "Bonus déjà attribué",
                "description": "Vérifie qu'un bonus (bonusKey) a déjà été attribué au client.",
                "params": {"bonusKey": "<bonusKey>"},
            },
        ],
    }
