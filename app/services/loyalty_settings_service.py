from sqlalchemy.orm import Session

from app.models.brand_loyalty_settings import BrandLoyaltySettings
from app.models.event_type import TransactionType


def get_loyalty_settings(db: Session, *, brand: str) -> BrandLoyaltySettings | None:
    return db.query(BrandLoyaltySettings).filter(BrandLoyaltySettings.brand == brand).first()


def ensure_system_transaction_types(db: Session, *, brand: str) -> None:
    system_types = [
        {
            "key": "WELCOME",
            "origin": "INTERNAL",
            "name": "Welcome",
            "description": "System event emitted when the customer enters the loyalty program or a tier triggers a welcome gift.",
        },
        {
            "key": "TIER_UPGRADED",
            "origin": "INTERNAL",
            "name": "Tier upgraded",
            "description": "System event emitted when the customer's loyalty tier increases.",
        },
        {
            "key": "TIER_DOWNGRADED",
            "origin": "INTERNAL",
            "name": "Tier downgraded",
            "description": "System event emitted when the customer's loyalty tier decreases.",
        },
        {
            "key": "TIER_RENEWED",
            "origin": "INTERNAL",
            "name": "Tier renewed",
            "description": "System event emitted when the customer's loyalty tier validity window is refreshed without a tier change.",
        },
        {
            "key": "STATUS_RESET",
            "origin": "INTERNAL",
            "name": "Status reset",
            "description": "System event emitted when status points are reset.",
        },
        {
            "key": "ADMIN_SET_TIER",
            "origin": "INTERNAL",
            "name": "Admin set tier",
            "description": "Audit event for manual tier overrides performed via admin UI.",
        },
    ]

    for st in system_types:
        existing = (
            db.query(TransactionType.id)
            .filter(TransactionType.brand == brand)
            .filter(TransactionType.key == st["key"])
            .filter(TransactionType.origin == st["origin"])
            .first()
        )
        if existing:
            continue
        db.add(
            TransactionType(
                brand=brand,
                key=st["key"],
                origin=st["origin"],
                name=st["name"],
                description=st.get("description"),
                payload_schema=None,
                active=True,
            )
        )
    db.flush()


def get_or_create_loyalty_settings(db: Session, *, brand: str) -> BrandLoyaltySettings:
    obj = get_loyalty_settings(db, brand=brand)
    if obj:
        ensure_system_transaction_types(db, brand=brand)
        return obj
    obj = BrandLoyaltySettings(brand=brand)
    db.add(obj)
    db.flush()
    ensure_system_transaction_types(db, brand=brand)
    return obj
