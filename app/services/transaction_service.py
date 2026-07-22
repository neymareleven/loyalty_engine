from datetime import datetime
import os
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.services.contact_service import resolve_customer_for_transaction
from app.models.customer import Customer
from app.models.event_type import TransactionType
from app.models.transaction import Transaction
from app.services.loyalty_status_service import update_customer_status
from app.services.payload_schema_service import enrich_payload_schema_on_ingest, infer_json_schema_from_payload
from app.services.rule_engine import process_transaction_rules
from app.services.sale_payload_service import normalize_sale_payload

_UNREGISTERED_CUSTOMER_ERROR_CODE = "CUSTOMER_NOT_REGISTERED"
_UNREGISTERED_CUSTOMER_MESSAGE = (
    "Customer not enrolled in loyalty program (ignored; register via /customers/upsert)."
)


def _is_unregistered_customer_transaction(transaction: Transaction) -> bool:
    """True when ingest was skipped because the loyalty customer does not exist yet."""
    code = transaction.error_code
    status = (transaction.status or "").upper()
    if code == _UNREGISTERED_CUSTOMER_ERROR_CODE and status == "IGNORED":
        return True
    # Legacy rows before IGNORED status was introduced.
    if code == "CUSTOMER_NOT_FOUND" and status == "BLOCKED":
        return True
    return False


def _ignore_unregistered_customer(transaction: Transaction) -> None:
    transaction.status = "IGNORED"
    transaction.error_code = _UNREGISTERED_CUSTOMER_ERROR_CODE
    transaction.error_message = _UNREGISTERED_CUSTOMER_MESSAGE
    transaction.processed_at = datetime.utcnow()


def _infer_json_schema_from_payload(value: Any, *, _depth: int = 0, _max_depth: int = 6) -> dict | None:
    return infer_json_schema_from_payload(value, _depth=_depth, _max_depth=_max_depth)


def _merge_json_schemas(a: dict | None, b: dict | None) -> dict | None:
    from app.services.payload_schema_service import merge_json_schemas

    return merge_json_schemas(a, b)


def _find_transaction_type(db: Session, *, brand: str, key: str):
    return (
        db.query(TransactionType)
        .filter(TransactionType.key == key)
        .filter(TransactionType.active.is_(True))
        .filter(TransactionType.brand == brand)
        .first()
    )


def create_internal_transaction(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    transaction_type: str,
    transaction_id: str,
    payload: dict | None = None,
    source: str = "SYSTEM",
    depth: int = 0,
    max_depth: int = 3,
    commit: bool = True,
):
    if depth >= max_depth:
        return None

    existing = (
        db.query(Transaction)
        .filter(Transaction.transaction_id == transaction_id)
        .filter(Transaction.brand == brand)
        .first()
    )
    if existing:
        return existing

    transaction = Transaction(
        transaction_id=transaction_id,
        brand=brand,
        profile_id=profile_id,
        transaction_type=transaction_type,
        source=source,
        payload=payload or {"_ruleDepth": depth + 1},
        status="PENDING",
    )
    db.add(transaction)
    if commit:
        db.commit()
        db.refresh(transaction)
    else:
        db.flush()

    try:
        process_transaction_rules(db, transaction)
        transaction.processed_at = datetime.utcnow()
        if commit:
            db.commit()
        else:
            db.flush()
    except Exception as e:
        transaction.status = "FAILED"
        transaction.error_message = str(e)
        transaction.processed_at = datetime.utcnow()
        if commit:
            db.commit()
        else:
            db.flush()

    return transaction


def _maybe_normalize_business_payload(*, transaction_type: str, payload: dict | None) -> dict | None:
    if (transaction_type or "").lower() == "sale":
        return normalize_sale_payload(payload)
    return payload


def _retry_ignored_unregistered_customer(db: Session, transaction: Transaction) -> Transaction:
    """Re-process if customer registered after an ignored ingest (idempotency + race)."""
    if not _is_unregistered_customer_transaction(transaction):
        return transaction

    customer = resolve_customer_for_transaction(
        db,
        brand=transaction.brand,
        profile_id=transaction.profile_id,
        payload=transaction.payload if isinstance(transaction.payload, dict) else None,
        transaction_type=transaction.transaction_type,
    )
    if not customer:
        return transaction

    transaction.status = "PENDING"
    transaction.error_code = None
    transaction.error_message = None
    transaction.processed_at = None
    db.commit()

    try:
        process_transaction_rules(db, transaction)
        transaction.processed_at = datetime.utcnow()
        db.commit()
    except Exception as e:
        db.rollback()
        msg = str(e)
        if "Customer not found" in msg or "not enrolled" in msg.lower():
            _ignore_unregistered_customer(transaction)
        else:
            transaction.status = "FAILED"
            transaction.error_message = msg
            transaction.processed_at = datetime.utcnow()
        db.commit()

    return transaction


# Backward-compatible alias for tests/scripts.
_retry_blocked_customer_not_found = _retry_ignored_unregistered_customer


def create_transaction(db: Session, event_data):
    """
    Crée une transaction de manière idempotente.
    Si un event avec le même eventId existe déjà,
    on retourne la transaction existante sans retraitement.
    """

    # 🔐 IDPOTENCE — vérifier si l'événement existe déjà
    existing = (
        db.query(Transaction)
        .filter(Transaction.transaction_id == event_data.eventId)
        .filter(Transaction.brand == event_data.brand)
        .first()
    )

    if existing:
        return _retry_ignored_unregistered_customer(db, existing)

    blocked_customer_profile_event = (event_data.eventType or "").upper() in {
        "CUSTOMER_PROFILE",
        "CONTACT",
        "CUSTOMER_UPSERT",
        "CONTACTINFOSUBMITTED",
        "SOCIALCONTACTS",
    }

    # validation minimale métier (strict)
    if not (event_data.brand and event_data.brand.strip()):
        raise HTTPException(status_code=400, detail="brand is required")
    if not (event_data.profileId and event_data.profileId.strip()):
        raise HTTPException(status_code=400, detail="profileId is required")
    if not (event_data.eventType and event_data.eventType.strip()):
        raise HTTPException(status_code=400, detail="eventType is required")
    if not (event_data.eventId and event_data.eventId.strip()):
        raise HTTPException(status_code=400, detail="eventId is required")

    status = "PENDING"

    if blocked_customer_profile_event:
        status = "BLOCKED"

    normalized_payload = _maybe_normalize_business_payload(
        transaction_type=event_data.eventType,
        payload=event_data.payload,
    )

    transaction = Transaction(
        transaction_id=event_data.eventId,   # 🔐 clé d'idempotence
        brand=event_data.brand,
        profile_id=event_data.profileId,
        transaction_type=event_data.eventType,
        source=event_data.source,
        payload=normalized_payload,
        status=status,
    )

    if status == "BLOCKED":
        transaction.error_code = "WRONG_INGESTION_ROUTE"
        transaction.error_message = "Customer profile events must use /customers/upsert (no rules executed)."
        transaction.processed_at = datetime.utcnow()

    if status != "PENDING":
        transaction.processed_at = datetime.utcnow()

    db.add(transaction)
    db.commit()
    db.refresh(transaction)

    auto_update_schema = (os.getenv("AUTO_UPDATE_TRANSACTIONTYPE_PAYLOAD_SCHEMA", "true") or "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    tt = _find_transaction_type(db, brand=transaction.brand, key=transaction.transaction_type)
    if not tt:
        inferred_schema = _infer_json_schema_from_payload(transaction.payload) if transaction.payload is not None else None
        tt = TransactionType(
            brand=transaction.brand,
            key=transaction.transaction_type,
            origin="EXTERNAL",
            name=transaction.transaction_type,
            description="Auto-created from inbound event",
            payload_schema=inferred_schema,
            active=True,
        )
        db.add(tt)
        db.commit()

    if auto_update_schema and tt:
        merged = enrich_payload_schema_on_ingest(tt.payload_schema, transaction.payload)
        if merged and merged != tt.payload_schema:
            tt.payload_schema = merged
            db.commit()

    if transaction.status == "PENDING":
        customer = resolve_customer_for_transaction(
            db,
            brand=transaction.brand,
            profile_id=transaction.profile_id,
            payload=transaction.payload if isinstance(transaction.payload, dict) else None,
        )
        if not customer:
            _ignore_unregistered_customer(transaction)
            db.commit()
            return transaction

        customer.last_activity_at = datetime.utcnow()
        db.commit()

        # If tiers are configured after some customers were created, they may still be
        # marked as UNCONFIGURED. Refresh their tier assignment opportunistically on
        # any external ingestion, even when no rules matched.
        if customer.loyalty_status in (None, "UNCONFIGURED"):
            update_customer_status(
                db,
                customer,
                reason="AUTO_TIER_REFRESH",
                source_transaction_id=transaction.id,
                depth=0,
                refresh_window=True,
                emit_events=False,
            )
            db.commit()

    if transaction.status == "PENDING":
        try:
            process_transaction_rules(db, transaction)
            transaction.processed_at = datetime.utcnow()
            db.commit()
        except Exception as e:
            db.rollback()
            msg = str(e)
            if "Customer not found" in msg or "not enrolled" in msg.lower():
                _ignore_unregistered_customer(transaction)
            else:
                transaction.status = "FAILED"
                transaction.error_message = msg
                transaction.processed_at = datetime.utcnow()
            db.commit()

    return transaction
