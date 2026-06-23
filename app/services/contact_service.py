from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.customer import Customer
from app.services.loyalty_status_service import compute_loyalty_status_from_tiers

from datetime import date


def get_customer(db: Session, brand: str, profile_id: str):
    return (
        db.query(Customer)
        .filter(
            Customer.brand == brand,
            Customer.profile_id == profile_id,
        )
        .first()
    )


def _extract_email_from_payload(payload: dict | None, *, brand: str) -> str | None:
    """WooCommerce / Unomi sale payloads — billing_email, email, scopeEmail."""
    if not isinstance(payload, dict):
        return None
    for key in ("billing_email", "email", "recipientEmail", "billingEmail"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip() and "@" in val:
            return val.strip().lower()
    scope = payload.get("scopeEmail")
    if isinstance(scope, str) and scope.strip():
        prefix = f"{brand}-".lower()
        raw = scope.strip()
        if raw.lower().startswith(prefix) and "@" in raw:
            return raw[len(prefix) :].strip().lower()
    return None


def resolve_customer_for_transaction(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    payload: dict | None,
) -> Customer | None:
    """
    Match customer by Unomi profileId, else by email from sale payload.
    Handles profile merges (same email, new profileId) and guest checkout auto-create.
    """
    customer = get_customer(db, brand, profile_id)
    if customer:
        return customer

    email = _extract_email_from_payload(payload, brand=brand)
    if not email:
        return None

    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(func.lower(Customer.email) == email)
        .first()
    )
    if customer:
        if customer.profile_id != profile_id:
            customer.profile_id = profile_id
        return customer

    return get_or_create_customer(
        db,
        brand,
        profile_id,
        payload={"email": email},
        push_to_unomi=False,
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


from app.services.birthdate_targeting import parse_customer_birthdate_storage


def get_or_create_customer(
    db: Session,
    brand: str,
    profile_id: str,
    payload: dict | None = None,
    *,
    contact_properties: dict | None = None,
    push_to_unomi: bool = True,
):
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
        if "email" in payload and payload["email"]:
            customer.email = str(payload["email"]).strip()

        if "gender" in payload and payload["gender"]:
            customer.gender = _normalize_gender(payload["gender"])

        if "birthdate" in payload and payload["birthdate"]:
            raw_bd = payload["birthdate"]
            # Allow passing a python date (legacy) or string (new partial format)
            if isinstance(raw_bd, date):
                d = raw_bd
                customer.birthdate = d
                customer.birth_month = d.month
                customer.birth_day = d.day
                customer.birth_year = d.year
            else:
                full, mm, dd, yy = parse_customer_birthdate_storage(str(raw_bd))
                customer.birth_month = mm
                customer.birth_day = dd
                customer.birth_year = yy
                customer.birthdate = full

    from app.services.unomi_profile_service import sync_customer_profile_to_unomi

    if push_to_unomi:
        sync_customer_profile_to_unomi(
            db,
            customer=customer,
            reason="customer_upsert",
            extra_properties=contact_properties,
        )
    return customer
