from sqlalchemy.orm import Session

from app.models.brand_loyalty_settings import BrandLoyaltySettings
from app.models.event_type import TransactionType
from app.services.transaction_protection import SYSTEM_MANAGED_TRANSACTION_TYPE_KEYS

# Default EXTERNAL types provisioned per brand (mutable — not system-protected).
DEFAULT_EXTERNAL_TRANSACTION_TYPE_KEYS = frozenset({"sale"})


def get_loyalty_settings(db: Session, *, brand: str) -> BrandLoyaltySettings | None:
    return db.query(BrandLoyaltySettings).filter(BrandLoyaltySettings.brand == brand).first()


def ensure_system_transaction_types(db: Session, *, brand: str) -> None:
    descriptions = {
        "TIER_UPGRADED": "System event emitted when the customer's loyalty tier increases.",
        "TIER_DOWNGRADED": "System event emitted when the customer's loyalty tier decreases.",
        "TIER_RENEWED": (
            "System event emitted when the customer's loyalty tier validity window is "
            "refreshed without a tier change."
        ),
        "STATUS_RESET": "System event emitted when status points are reset.",
        "ADMIN_SET_TIER": "Audit event for manual tier overrides performed via admin UI.",
        "CUSTOMER_REGISTRATION": (
            "System event emitted once when a customer is created (first ingestion), not on updates."
        ),
    }
    names = {
        "TIER_UPGRADED": "Tier upgraded",
        "TIER_DOWNGRADED": "Tier downgraded",
        "TIER_RENEWED": "Tier renewed",
        "STATUS_RESET": "Status reset",
        "ADMIN_SET_TIER": "Admin set tier",
        "CUSTOMER_REGISTRATION": "Customer registration",
    }
    system_types = [
        {
            "key": key,
            "origin": "INTERNAL",
            "name": names[key],
            "description": descriptions[key],
        }
        for key in sorted(SYSTEM_MANAGED_TRANSACTION_TYPE_KEYS)
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


def ensure_default_external_transaction_types(db: Session, *, brand: str) -> None:
    """Provision standard EXTERNAL transaction types (editable in admin UI)."""
    defaults = [
        {
            "key": "sale",
            "origin": "EXTERNAL",
            "name": "Sale",
            "description": (
                "Default EXTERNAL event for CDP sale ingestion. "
                "payload_schema starts empty and is enriched as events are ingested."
            ),
            "payload_schema": None,
        },
    ]

    for item in defaults:
        existing = (
            db.query(TransactionType.id)
            .filter(TransactionType.brand == brand)
            .filter(TransactionType.key == item["key"])
            .filter(TransactionType.origin == item["origin"])
            .first()
        )
        if existing:
            continue
        db.add(
            TransactionType(
                brand=brand,
                key=item["key"],
                origin=item["origin"],
                name=item["name"],
                description=item.get("description"),
                payload_schema=item.get("payload_schema"),
                active=True,
            )
        )
    db.flush()


def ensure_brand_transaction_catalog(db: Session, *, brand: str) -> None:
    """System INTERNAL audit types + default EXTERNAL catalog for a brand."""
    ensure_system_transaction_types(db, brand=brand)
    ensure_default_external_transaction_types(db, brand=brand)


def get_or_create_loyalty_settings(db: Session, *, brand: str) -> BrandLoyaltySettings:
    obj = get_loyalty_settings(db, brand=brand)
    if obj:
        ensure_brand_transaction_catalog(db, brand=brand)
        return obj
    obj = BrandLoyaltySettings(brand=brand)
    db.add(obj)
    db.flush()
    ensure_brand_transaction_catalog(db, brand=brand)
    return obj
