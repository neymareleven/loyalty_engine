from sqlalchemy.orm import Session
from app.models.customer import Customer
from app.services.loyalty_status_service import compute_loyalty_status_from_tiers


def get_customer(db: Session, brand: str, profile_id: str):
    return (
        db.query(Customer)
        .filter(
            Customer.brand == brand,
            Customer.profile_id == profile_id,
        )
        .first()
    )


def _normalize_gender(value: str) -> str:
    v = (value or "").strip().lower()

    if not v:
        return "UNKNOWN"

    if v in {"f", "female", "femme", "feminin", "féminin"}:
        return "F"
    if v in {"m", "male", "homme", "masculin"}:
        return "M"
    if v in {"other", "autre", "non-binaire", "non binaire", "nb"}:
        return "OTHER"
    if v in {"unknown", "inconnu"}:
        return "UNKNOWN"

    return "UNKNOWN"


def get_or_create_customer(db: Session, brand: str, profile_id: str, payload: dict | None = None):
    customer = (
        db.query(Customer)
        .filter(
            Customer.brand == brand,
            Customer.profile_id == profile_id,
        )
        .first()
    )

    if not customer:
        initial_status = compute_loyalty_status_from_tiers(db, brand, status_points=0)

        customer = Customer(
            brand=brand,
            profile_id=profile_id,
            status="ACTIVE",
            loyalty_status=(initial_status if initial_status else "UNCONFIGURED"),
        )
        db.add(customer)
        db.flush()

    # --- Mise à jour des attributs métier depuis le payload ---
    if payload:
        if "gender" in payload and payload["gender"]:
            customer.gender = _normalize_gender(payload["gender"])

        if "birthdate" in payload and payload["birthdate"]:
            customer.birthdate = payload["birthdate"]

    return customer
