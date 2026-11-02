from sqlalchemy.orm import Session

from app.models.brand_loyalty_settings import BrandLoyaltySettings
from app.models.event_type import TransactionType
from app.models.loyalty_tier import LoyaltyTier
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
        "QUALIFICATION_MODE_CHANGED": (
            "System event emitted when the brand switches qualification mode "
            "(POINTS/PURCHASES) and the customer's displayed tier is recalculated. "
            "This is not an upgrade, downgrade, or renew."
        ),
        "FIRST_PURCHASE_BYPASS": (
            "System event emitted when the customer's first purchase meets the "
            "configured amount and grants a configured loyalty tier."
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
        "QUALIFICATION_MODE_CHANGED": "Qualification mode changed",
        "FIRST_PURCHASE_BYPASS": "First purchase bypass",
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


def serialize_loyalty_settings(obj: BrandLoyaltySettings) -> dict:
    from app.services.purchase_service import normalize_qualification_mode

    tier_key = getattr(obj, "first_purchase_bypass_tier_key", None)
    return {
        "brand": obj.brand,
        "qualification_mode": normalize_qualification_mode(getattr(obj, "qualification_mode", None)),
        "points_validity_days": obj.points_validity_days,
        "loyalty_status_validity_days": obj.loyalty_status_validity_days,
        "purchase_validity_days": getattr(obj, "purchase_validity_days", None),
        "first_purchase_bypass_enabled": bool(getattr(obj, "first_purchase_bypass_enabled", False)),
        "first_purchase_bypass_min_amount": getattr(obj, "first_purchase_bypass_min_amount", None),
        "first_purchase_bypass_tier_key": (str(tier_key).strip() if tier_key else None) or None,
    }


def first_purchase_bypass_config_error(db: Session, *, brand: str, settings: BrandLoyaltySettings) -> str | None:
    if not bool(getattr(settings, "first_purchase_bypass_enabled", False)):
        return None
    amount = getattr(settings, "first_purchase_bypass_min_amount", None)
    try:
        amount_int = int(amount) if amount is not None else None
    except (TypeError, ValueError):
        return "first_purchase_bypass_min_amount must be an integer >= 1 when bypass is enabled"
    if amount_int is None or amount_int < 1:
        return "first_purchase_bypass_min_amount must be >= 1 when bypass is enabled"
    key = (getattr(settings, "first_purchase_bypass_tier_key", None) or "").strip()
    if not key:
        return "first_purchase_bypass_tier_key is required when bypass is enabled"
    tier = (
        db.query(LoyaltyTier.id)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .filter(LoyaltyTier.key == key)
        .first()
    )
    if not tier:
        return "first_purchase_bypass_tier_key must match an existing active loyalty tier"
    return None
