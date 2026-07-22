from datetime import date, datetime
import logging
from uuid import UUID

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.customer_unomi_profile_alias import CustomerUnomiProfileAlias
from app.models.transaction import Transaction
from app.services.loyalty_status_service import compute_loyalty_status_from_tiers

logger = logging.getLogger(__name__)


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


def _normalize_email(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    return str(value).strip().lower()


def _customer_email(customer: Customer | None) -> str | None:
    if customer is None:
        return None
    return _normalize_email(getattr(customer, "email", None))


def build_brand_profile_id_to_customer_map(
    db: Session,
    *,
    brand: str,
    customer_ids: list[UUID] | None = None,
) -> dict[str, UUID]:
    """Map master or alias Unomi profileId -> loyalty customer_id for a brand."""
    q = db.query(Customer.id, Customer.profile_id).filter(Customer.brand == brand)
    if customer_ids:
        q = q.filter(Customer.id.in_(customer_ids))

    customers = q.all()
    if not customers:
        return {}

    allowed_customer_ids = {row.id for row in customers}
    mapping: dict[str, UUID] = {}
    for row in customers:
        master = (row.profile_id or "").strip()
        if master:
            mapping[master] = row.id

    alias_q = (
        db.query(
            CustomerUnomiProfileAlias.profile_id,
            CustomerUnomiProfileAlias.customer_id,
        )
        .filter(CustomerUnomiProfileAlias.brand == brand)
        .filter(CustomerUnomiProfileAlias.customer_id.in_(allowed_customer_ids))
    )
    for profile_id, customer_id in alias_q.all():
        pid = (profile_id or "").strip()
        if pid:
            mapping[pid] = customer_id

    return mapping


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
    corroborating_email: str | None = None,
    caller: str = "unknown",
) -> bool:
    """
    Record a Unomi profileId that maps to an existing customer without changing the master.

    Session aliases require a corroborating email that exactly matches the target customer.
    """
    incoming_profile_id = (incoming_profile_id or "").strip()
    if not incoming_profile_id:
        return False

    master_profile_id = (customer.profile_id or "").strip()
    if not master_profile_id or incoming_profile_id == master_profile_id:
        return False

    source_norm = (source or "session").strip() or "session"
    customer_email = _customer_email(customer)
    corroborating = _normalize_email(corroborating_email)

    if source_norm == "session":
        if not customer_email or not corroborating:
            logger.warning(
                "unomi alias refused (session requires email corroboration): brand=%s caller=%s "
                "customer_id=%s master_profile_id=%s incoming_profile_id=%s customer_email=%s "
                "corroborating_email=%s",
                brand,
                caller,
                customer.id,
                master_profile_id,
                incoming_profile_id,
                customer_email,
                corroborating,
            )
            return False
        if customer_email != corroborating:
            logger.warning(
                "unomi alias refused (email mismatch — should not link different people): "
                "brand=%s caller=%s customer_id=%s master_profile_id=%s incoming_profile_id=%s "
                "customer_email=%s corroborating_email=%s",
                brand,
                caller,
                customer.id,
                master_profile_id,
                incoming_profile_id,
                customer_email,
                corroborating,
            )
            return False

    existing_owner = get_customer(db, brand, incoming_profile_id)
    if existing_owner and existing_owner.id != customer.id:
        owner_email = _customer_email(existing_owner)
        if owner_email and customer_email and owner_email != customer_email:
            logger.warning(
                "unomi alias refused (incoming profile owned by another customer with different email): "
                "brand=%s caller=%s target_customer_id=%s target_email=%s owner_customer_id=%s "
                "owner_email=%s incoming_profile_id=%s",
                brand,
                caller,
                customer.id,
                customer_email,
                existing_owner.id,
                owner_email,
                incoming_profile_id,
            )
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
        if source_norm and row.source in (None, "", "session") and source_norm != "session":
            row.source = source_norm
        return False

    db.add(
        CustomerUnomiProfileAlias(
            brand=brand,
            customer_id=customer.id,
            profile_id=incoming_profile_id,
            source=source_norm,
            first_seen_at=now,
            last_seen_at=now,
        )
    )
    logger.info(
        "unomi alias registered: brand=%s caller=%s customer_id=%s master_profile_id=%s "
        "incoming_profile_id=%s customer_email=%s corroborating_email=%s source=%s",
        brand,
        caller,
        customer.id,
        master_profile_id,
        incoming_profile_id,
        customer_email,
        corroborating,
        source_norm,
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

    When email is provided it wins over a stale profileId alias that belongs to another
    customer. Session alias registration requires matching corroborating email.
    Returns (customer, alias_registered).
    """
    profile_id = (profile_id or "").strip()
    norm_email = normalize_lookup_email(email, brand=brand)

    by_email = None
    if norm_email:
        by_email = (
            db.query(Customer)
            .filter(Customer.brand == brand)
            .filter(func.lower(Customer.email) == norm_email)
            .first()
        )

    by_profile = get_customer(db, brand, profile_id) if profile_id else None

    if by_email and by_profile and by_email.id != by_profile.id:
        profile_email = _customer_email(by_profile)
        logger.warning(
            "profile/email customer mismatch; trusting email: brand=%s caller=resolve_customer_for_lookup "
            "profile_id=%s profile_customer_id=%s profile_email=%s email_customer_id=%s lookup_email=%s",
            brand,
            profile_id,
            by_profile.id,
            profile_email,
            by_email.id,
            norm_email,
        )
        by_profile = None

    if by_email:
        registered = False
        if reconcile_profile_id and by_email.profile_id != profile_id:
            registered = register_unomi_profile_alias(
                db,
                brand=brand,
                customer=by_email,
                incoming_profile_id=profile_id,
                corroborating_email=norm_email,
                caller="resolve_customer_for_lookup",
            )
        return by_email, registered

    if by_profile:
        if norm_email:
            profile_email = _customer_email(by_profile)
            if profile_email and profile_email != norm_email:
                logger.warning(
                    "profile lookup rejected (email does not match profile owner): brand=%s "
                    "profile_id=%s customer_id=%s profile_email=%s lookup_email=%s caller=resolve_customer_for_lookup",
                    brand,
                    profile_id,
                    by_profile.id,
                    profile_email,
                    norm_email,
                )
                return None, False
        return by_profile, False

    return None, False


def _extract_trusted_identity_email_from_payload(payload: dict | None, *, brand: str) -> str | None:
    """Identity email for transaction matching — not billing/checkout fields.

    Uses only loyalty-trusted keys: ``email`` and brand-scoped ``scopeEmail``.
    ``billing_email`` and similar checkout fields are intentionally ignored.
    """
    if not isinstance(payload, dict):
        return None
    val = payload.get("email")
    if isinstance(val, str) and val.strip() and "@" in val:
        return val.strip().lower()
    scope = payload.get("scopeEmail")
    if isinstance(scope, str) and scope.strip():
        prefix = f"{brand}-".lower()
        raw = scope.strip()
        if raw.lower().startswith(prefix) and "@" in raw:
            return raw[len(prefix) :].strip().lower()
    return None


def _extract_email_from_payload(payload: dict | None, *, brand: str) -> str | None:
    """Broad email extraction (repair scripts / diagnostics). Includes billing fields."""
    if not isinstance(payload, dict):
        return None
    trusted = _extract_trusted_identity_email_from_payload(payload, brand=brand)
    if trusted:
        return trusted
    for key in ("billing_email", "recipientEmail", "billingEmail"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip() and "@" in val:
            return val.strip().lower()
    return None


def resolve_customer_for_transaction(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    payload: dict | None,
) -> Customer | None:
    """
    Match an enrolled customer for business transactions (brand-scoped).

    Resolution order:
    1. ``profileId`` (master or alias) + ``brand``
    2. If trusted ``email`` / ``scopeEmail`` is present, it must match the customer's email
       (never ``billing_email`` — checkout field, not identity).
    3. If ``profileId`` is unknown but trusted ``email`` matches an existing customer for this
       ``brand``, link the incoming profileId as an alias (same as upsert).

    Email alone never overrides a profileId that resolves to a different enrolled customer.
    """
    profile_id = (profile_id or "").strip()
    brand = (brand or "").strip()
    if not brand or not profile_id:
        return None

    email = _extract_trusted_identity_email_from_payload(payload, brand=brand)
    by_profile = get_customer(db, brand, profile_id)

    if by_profile:
        if email:
            profile_email = _customer_email(by_profile)
            if profile_email and profile_email != email:
                logger.warning(
                    "transaction profile/email mismatch rejected: brand=%s profile_id=%s "
                    "customer_id=%s profile_email=%s payload_email=%s caller=resolve_customer_for_transaction",
                    brand,
                    profile_id,
                    by_profile.id,
                    profile_email,
                    email,
                )
                return None
        return by_profile

    if not email:
        return None

    by_email = (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(func.lower(Customer.email) == email)
        .first()
    )
    if not by_email:
        return None

    if by_email.profile_id != profile_id:
        register_unomi_profile_alias(
            db,
            brand=brand,
            customer=by_email,
            incoming_profile_id=profile_id,
            source="session",
            corroborating_email=email,
            caller="resolve_customer_for_transaction",
        )
    return by_email


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
    *,
    caller: str = "unknown",
) -> None:
    """Update loyalty identity fields on an existing customer row.

    Email is never silently replaced when the customer already has a different one.
    """
    if not payload:
        return

    if payload.get("email"):
        incoming = str(payload["email"]).strip()
        existing = (customer.email or "").strip()
        if existing and existing.lower() != incoming.lower():
            logger.warning(
                "identity email change blocked: customer_id=%s brand=%s master_profile_id=%s "
                "existing_email=%s incoming_email=%s caller=%s",
                customer.id,
                customer.brand,
                customer.profile_id,
                existing,
                incoming,
                caller,
            )
        elif not existing:
            customer.email = incoming
        elif existing != incoming:
            customer.email = incoming

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
                corroborating_email=norm_email,
                caller="resolve_customer_for_upsert",
            )
        apply_customer_identity(by_email, identity_payload, caller="resolve_customer_for_upsert")
        return by_email, False

    if by_profile:
        apply_customer_identity(by_profile, identity_payload, caller="resolve_customer_for_upsert")
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
    apply_customer_identity(customer, payload, caller="get_or_create_customer")

    return customer
