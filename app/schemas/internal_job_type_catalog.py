def get_internal_job_type_catalog():
    return {
        "jobTypes": [
            {
                "type": "PING_ACTIVE",
                "name": "Ping active customers",
                "description": "Emit an INTERNAL event for customers matching a selector.",
                "defaults": {
                    "event_type": "JOB_PING",
                    "selector": {"status_in": ["ACTIVE"]},
                    "payload_template": {"hello": "world"},
                    "schedule": {"type": "cron", "cron": "0 * * * *", "timezone": "UTC"},
                    "active": True,
                },
            },
            {
                "type": "BIRTHDAY",
                "name": "Birthday trigger",
                "description": "Emit an INTERNAL event for customers whose birthdate is today.",
                "defaults": {
                    "event_type": "BIRTHDAY",
                    "selector": {"birthdate_today": True},
                    "payload_template": {},
                    "schedule": {"type": "cron", "cron": "0 0 * * *", "timezone": "UTC"},
                    "active": True,
                },
            },
            {
                "type": "CREATED_ANNIVERSARY",
                "name": "Signup anniversary trigger",
                "description": "Emit an INTERNAL event for customers whose signup anniversary is today.",
                "defaults": {
                    "event_type": "SIGNUP_ANNIVERSARY",
                    "selector": {"created_anniversary_today": True},
                    "payload_template": {},
                    "schedule": {"type": "cron", "cron": "0 0 * * *", "timezone": "UTC"},
                    "active": True,
                },
            },
            {
                "type": "INACTIVITY_NUDGE",
                "name": "Inactivity nudge",
                "description": "Emit an INTERNAL event for customers inactive for N days.",
                "defaults": {
                    "event_type": "INACTIVITY_NUDGE",
                    "selector": {"inactive_days_gte": 30},
                    "payload_template": {"inactive_days": 30},
                    "schedule": {"type": "cron", "cron": "0 0 * * *", "timezone": "UTC"},
                    "active": True,
                },
            },
        ],
        "notes": [
            "These are UI presets only. Backend enforcement is done by InternalJobCreate schema + validation.",
            "Event types must exist (origin=INTERNAL) for the active brand before a job can be created.",
        ],
    }
