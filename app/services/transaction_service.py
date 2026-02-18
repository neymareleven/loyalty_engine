from datetime import datetime

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.event_type import EventType
from app.models.transaction import Transaction
from app.services.rule_engine import process_transaction_rules


def _find_event_type(db: Session, *, brand: str, key: str):
    return (
        db.query(EventType)
        .filter(EventType.key == key)
        .filter(EventType.active.is_(True))
        .filter(EventType.brand == brand)
        .first()
    )


def create_internal_transaction(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    event_type: str,
    event_id: str,
    payload: dict | None = None,
    source: str = "SYSTEM",
    depth: int = 0,
    max_depth: int = 3,
):
    if depth >= max_depth:
        return None

    existing = db.query(Transaction).filter(Transaction.event_id == event_id).first()
    if existing:
        return existing

    transaction = Transaction(
        event_id=event_id,
        brand=brand,
        profile_id=profile_id,
        event_type=event_type,
        source=source,
        payload=payload or {"_ruleDepth": depth + 1},
        status="PENDING",
    )
    db.add(transaction)
    db.commit()
    db.refresh(transaction)

    try:
        process_transaction_rules(db, transaction)
        db.commit()
    except Exception as e:
        transaction.status = "FAILED"
        transaction.error_message = str(e)
        db.commit()

    return transaction


def create_transaction(db: Session, event_data):
    """
    Crée une transaction de manière idempotente.
    Si un event avec le même eventId existe déjà,
    on retourne la transaction existante sans retraitement.
    """

    # 🔐 IDPOTENCE — vérifier si l'événement existe déjà
    existing = (
        db.query(Transaction)
        .filter(Transaction.event_id == event_data.eventId)
        .first()
    )

    if existing:
        return existing

    # ------------------------------------------------------------
    # Option 1 (strict): customer profile events must not go through /events.
    # ------------------------------------------------------------
    if (event_data.eventType or "").upper() in {"CUSTOMER_PROFILE", "CONTACT", "CUSTOMER_UPSERT"}:
        raise HTTPException(
            status_code=400,
            detail="Customer profile events must use /customers/upsert (no rules executed).",
        )

    # validation minimale métier
    is_blocked = not all([
        event_data.brand and event_data.brand.strip(),
        event_data.profileId and event_data.profileId.strip(),
        event_data.eventType and event_data.eventType.strip(),
    ])

    status = "BLOCKED" if is_blocked else "PENDING"

    transaction = Transaction(
        event_id=event_data.eventId,   # 🔐 clé d'idempotence
        brand=event_data.brand,
        profile_id=event_data.profileId,
        event_type=event_data.eventType,
        source=event_data.source,
        payload=event_data.payload,
        status=status,
    )

    db.add(transaction)
    db.commit()
    db.refresh(transaction)

    et = _find_event_type(db, brand=transaction.brand, key=transaction.event_type)
    if et and et.origin == "EXTERNAL":
        customer = (
            db.query(Customer)
            .filter(Customer.brand == transaction.brand, Customer.profile_id == transaction.profile_id)
            .first()
        )
        if customer:
            customer.last_activity_at = datetime.utcnow()
            db.commit()

    # ⚠️ lancer le moteur seulement si PENDING
    if transaction.status == "PENDING":
        try:
            process_transaction_rules(db, transaction)
            db.commit()
        except Exception as e:
            transaction.status = "FAILED"
            transaction.error_message = str(e)
            db.commit()

    return transaction
