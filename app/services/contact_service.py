from sqlalchemy.orm import Session
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


def _parse_partial_birthdate(value: str | None) -> tuple[date | None, int | None, int | None, int | None]:
    """Accept YYYY-MM-DD or MM-DD. Month/day required if provided, year optional.

    Returns (full_date_or_none, month, day, year).
    - If year is missing, full_date_or_none is None and year is None.
    """

    if value is None:
        return None, None, None, None

    s = str(value).strip()
    if not s:
        return None, None, None, None

    # YYYY-MM-DD
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try:
            d = date.fromisoformat(s)
        except Exception:
            raise ValueError("birthdate must be in format YYYY-MM-DD or MM-DD")
        return d, d.month, d.day, d.year

    # MM-DD
    if len(s) == 5 and s[2] == "-":
        try:
            mm = int(s[0:2])
            dd = int(s[3:5])
        except Exception:
            raise ValueError("birthdate must be in format YYYY-MM-DD or MM-DD")

        if mm < 1 or mm > 12:
            raise ValueError("birthdate month must be between 01 and 12")
        if dd < 1 or dd > 31:
            raise ValueError("birthdate day must be between 01 and 31")

        # Basic sanity check to reject impossible dates (e.g. 02-31)
        try:
            date(2000, mm, dd)
        except Exception:
            raise ValueError("birthdate MM-DD is not a valid calendar date")

        return None, mm, dd, None

    raise ValueError("birthdate must be in format YYYY-MM-DD or MM-DD")


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
            raw_bd = payload["birthdate"]
            # Allow passing a python date (legacy) or string (new partial format)
            if isinstance(raw_bd, date):
                d = raw_bd
                customer.birthdate = d
                customer.birth_month = d.month
                customer.birth_day = d.day
                customer.birth_year = d.year
            else:
                full, mm, dd, yy = _parse_partial_birthdate(str(raw_bd))
                customer.birth_month = mm
                customer.birth_day = dd
                customer.birth_year = yy
                customer.birthdate = full

    return customer
