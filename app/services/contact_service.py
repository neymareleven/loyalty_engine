from datetime import date, datetime

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.customer_unomi_profile_alias import CustomerUnomiProfileAlias
from app.models.transaction import Transaction
from app.services.loyalty_status_service import compute_loyalty_status_from_tiers


def get_customer(db: Session, brand: str, profile_id: str) -> Customer | None:
    """Resolve customer by master profile_id or any registered Unomi alias."""
    profile_id = (profile_id or "").strip()
    if not profile_id:
        return None

    customer = (
        db.query(Customer)
        .filter(
            Customer.brand == brand,
            Customer.profile_id == profile_id,
        )
        .first()
    )
    if customer:
        return customer

    alias = (
        db.query(CustomerUnomiProfileAlias)
        .filter(
            CustomerUnomiProfileAlias.brand == brand,
            CustomerUnomiProfileAlias.profile_id == profile_id,
        )
        .first()
    )
    if not alias:
        return None

    return db.query(Customer).filter(Customer.id == alias.customer_id).first()


def list_customer_unomi_profile_ids(db: Session, customer: Customer) -> list[str]:
    """Master profile_id plus all known Unomi aliases (deduplicated, stable order)."""
    ids: list[str] = []
    master = (customer.profile_id or "").strip()
    if master:
        ids.append(master)

    rows = (
        db.query(CustomerUnomiProfileAlias.profile_id)
        .filter(CustomerUnomiProfileAlias.customer_id == customer.id)
        .all()
    )
    for (profile_id,) in rows:
        pid = (profile_id or "").strip()
        if pid and pid not in ids:
            ids.append(pid)
    return ids


def normalize_lookup_email(value: str | None, *, brand: str) -> str | None:
    """Normalize email or scopeEmail (batira-user@x.com) for customer lookup."""
    if value is None or not str(value).strip():
        return None
    raw = str(value).strip()
    prefix = f"{brand}-".lower()
    if raw.lower().startswith(prefix) and "@" in raw:
        return raw[len(prefix) :].strip().lower()
    extracted = _extract_email_from_payload({"email": raw, "scopeEmail": raw}, brand=brand)
    return extracted


def register_unomi_profile_alias(
    db: Session,
    *,
    brand: str,
    customer: Customer,
    incoming_profile_id: str,
    source: str = "session",
) -> bool:
    """
    Record a Unomi profileId that maps to an existing customer without changing the master.

    Used when mergeProfilesOnEmail or a new session cookie sends a different profileId
    for the same email. Transactions keep the profile_id they were ingested with.
    """
    incoming_profile_id = (incoming_profile_id or "").strip()
    if not incoming_profile_id:
        return False

    master_profile_id = (customer.profile_id or "").strip()
    if not master_profile_id or incoming_profile_id == master_profile_id:
        return False

    existing_owner = get_customer(db, brand, incoming_profile_id)
    if existing_owner and existing_owner.id != customer.id:
        return False

    now = datetime.utcnow()
    row = (
        db.query(CustomerUnomiProfileAlias)
        .filter(
            CustomerUnomiProfileAlias.brand == brand,
            CustomerUnomiProfileAlias.customer_id == customer.id,
            CustomerUnomiProfileAlias.profile_id == incoming_profile_id,
        )
        .first()
    )
    if row:
        row.last_seen_at = now
        if source and row.source in (None, "", "session") and source != "session":
            row.source = source
        return False

    db.add(
        CustomerUnomiProfileAlias(
            brand=brand,
            customer_id=customer.id,
            profile_id=incoming_profile_id,
            source=(source or "session").strip() or "session",
            first_seen_at=now,
            last_seen_at=now,
        )
    )
    return True


def reconcile_customer_unomi_profile_id(
    db: Session,
    *,
    brand: str,
    customer: Customer,
    new_profile_id: str,
    source: str = "session",
) -> bool:
    """Backward-compatible name: registers an alias, never overwrites master or transactions."""
    return register_unomi_profile_alias(
        db,
        brand=brand,
        customer=customer,
        incoming_profile_id=new_profile_id,
        source=source,
    )


def customer_transaction_filters(db: Session, *, brand: str, customer: Customer):
    """
    Match transactions for a customer across Unomi profileId changes (pre/post merge).
    Each transaction keeps the profile_id from ingest time; listing spans master + aliases + email.
    """
    profile_ids = list_customer_unomi_profile_ids(db, customer)
    clauses = [Transaction.profile_id.in_(profile_ids)] if profile_ids else []

    email = (customer.email or "").strip().lower()
    if email:
        scope_email = f"{brand}-{email}".lower()
        clauses.extend(
            [
                func.lower(Transaction.payload["email"].as_string()) == email,
                func.lower(Transaction.payload["billing_email"].as_string()) == email,
                func.lower(Transaction.payload["scopeEmail"].as_string()) == scope_email,
            ]
        )
    if not clauses:
        return Transaction.profile_id == customer.profile_id
    return or_(*clauses)


def resolve_customer_for_lookup(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    email: str | None = None,
    reconcile_profile_id: bool = True,
) -> tuple[Customer | None, bool]:
    """
    Find loyalty customer by Unomi profileId (master or alias), else by email.
    Returns (customer, alias_registered).
    """
    customer = get_customer(db, brand, profile_id)
    if customer:
        return customer, False

    norm_email = normalize_lookup_email(email, brand=brand)
    if not norm_email:
        return None, False

    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(func.lower(Customer.email) == norm_email)
        .first()
    )
    if not customer:
        return None, False

    if not reconcile_profile_id:
        return customer, False

    registered = register_unomi_profile_alias(
        db,
        brand=brand,
        customer=customer,
        incoming_profile_id=profile_id,
    )
    return customer, registered


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
    Match customer for sale/business events.

    Email wins over profileId (same as upsert): Unomi sale.groovy may send the newest
    CDP profileId while Loyalty keeps the oldest master. Points always attach to the
    email-matched customer; incoming profileId is registered as alias when safe.
    """
    profile_id = (profile_id or "").strip()
    email = _extract_email_from_payload(payload, brand=brand)

    by_email = None
    if email:
        by_email = (
            db.query(Customer)
            .filter(Customer.brand == brand)
            .filter(func.lower(Customer.email) == email)
            .first()
        )

    by_profile = get_customer(db, brand, profile_id)

    if by_email:
        if by_email.profile_id != profile_id:
            register_unomi_profile_alias(
                db,
                brand=brand,
                customer=by_email,
                incoming_profile_id=profile_id,
                source="session",
            )
        return by_email

    if by_profile:
        return by_profile

    if email:
        return get_or_create_customer(
            db,
            brand,
            profile_id,
            payload={"email": email},
        )

    return None


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


def apply_customer_identity(
    customer: Customer,
    payload: dict | None,
) -> None:
    """Update loyalty identity fields on an existing customer row."""
    if not payload:
        return

    if payload.get("email"):
        customer.email = str(payload["email"]).strip()

    if payload.get("gender"):
        customer.gender = _normalize_gender(payload["gender"])

    if payload.get("birthdate"):
        raw_bd = payload["birthdate"]
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


def resolve_customer_for_upsert(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    identity_payload: dict | None,
) -> tuple[Customer, bool]:
    """
    Resolve the canonical loyalty customer for Unomi upsert.

    Email wins over profileId (master/alias). Incoming profileId is registered as an alias
    when it differs from the stored master. Returns (customer, is_new_registration).
    """
    profile_id = (profile_id or "").strip()
    norm_email = None
    if identity_payload and identity_payload.get("email"):
        norm_email = str(identity_payload["email"]).strip().lower() or None

    by_email = None
    if norm_email:
        by_email = (
            db.query(Customer)
            .filter(Customer.brand == brand)
            .filter(func.lower(Customer.email) == norm_email)
            .first()
        )

    by_profile = get_customer(db, brand, profile_id)

    if by_email:
        if by_email.profile_id != profile_id:
            register_unomi_profile_alias(
                db,
                brand=brand,
                customer=by_email,
                incoming_profile_id=profile_id,
                source="session",
            )
        apply_customer_identity(by_email, identity_payload)
        return by_email, False

    if by_profile:
        apply_customer_identity(by_profile, identity_payload)
        return by_profile, False

    customer = get_or_create_customer(
        db,
        brand,
        profile_id,
        payload=identity_payload,
    )
    return customer, True


def get_or_create_customer(
    db: Session,
    brand: str,
    profile_id: str,
    payload: dict | None = None,
):
    customer = get_customer(db, brand, profile_id)

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
    apply_customer_identity(customer, payload)

    return customer
