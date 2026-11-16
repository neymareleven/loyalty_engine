from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.loyalty_tier import LoyaltyTier
from app.services.loyalty_guarantee_service import cause_from_reason

_TYPE_LABELS = {
    "TIER_UPGRADED": "Montée de palier",
    "TIER_DOWNGRADED": "Descente de palier",
    "TIER_RENEWED": "Renouvellement",
    "FIRST_PURCHASE_BYPASS": "Bypass premier achat",
    "QUALIFICATION_MODE_CHANGED": "Changement de mode de qualification",
    "ADMIN_SET_TIER": "Modification admin",
    "STATUS_RESET": "Reset points statut",
    "CUSTOMER_REGISTRATION": "Inscription",
    "PURCHASE_RECORDED": "Achat enregistré",
}

_CAUSE_LABELS = {
    "thresholds": "Seuils atteints",
    "first_purchase_bypass": "Bypass premier achat",
    "mode_switch": "Changement de mode",
    "admin": "Modification admin",
    "expiration": "Expiration",
    "tier_config_changed": "Configuration des paliers",
    "purchases": "Achats",
    "points": "Points",
}


def infer_history_cause(payload: dict | None, transaction_type: str | None) -> str | None:
    payload = payload if isinstance(payload, dict) else {}
    cause = payload.get("cause")
    if cause:
        return str(cause)
    reason = str(payload.get("reason") or "")
    if reason in {"EARN_POINTS", "DEDUCT_STATUS_POINTS"}:
        return "points"
    if reason in {"LOYALTY_STATUS_EXPIRED", "POINTS_EXPIRED", "PURCHASES_EXPIRED"}:
        return "expiration"
    if reason == "PURCHASE_RECORDED":
        return "purchases"
    if reason.startswith("ADMIN"):
        return "admin"
    if transaction_type:
        mapped = cause_from_reason(transaction_type, default="")
        return mapped or None
    return None


def _tier_name_map(db: Session, brand: str) -> dict[str, str]:
    rows = db.query(LoyaltyTier.key, LoyaltyTier.name).filter(LoyaltyTier.brand == brand).all()
    return {str(k): str(n) for k, n in rows if k}


def serialize_loyalty_history_item(db: Session, tx, *, brand: str, names: dict[str, str] | None = None) -> dict:
    payload = tx.payload if isinstance(tx.payload, dict) else {}
    names = names if names is not None else _tier_name_map(db, brand)
    from_key = payload.get("fromTier") or payload.get("fromStatus")
    to_key = payload.get("toTier") or payload.get("toStatus") or payload.get("tier")
    cause = infer_history_cause(payload, tx.transaction_type)
    label = _TYPE_LABELS.get(tx.transaction_type) or tx.transaction_type
    if cause and cause in _CAUSE_LABELS and tx.transaction_type not in _TYPE_LABELS:
        label = _CAUSE_LABELS[cause]
    return {
        "id": str(tx.id),
        "transactionType": tx.transaction_type,
        "transactionId": tx.transaction_id,
        "status": tx.status,
        "source": tx.source,
        "createdAt": tx.created_at,
        "payload": payload,
        "fromTier": from_key,
        "toTier": to_key,
        "fromTierKey": from_key,
        "toTierKey": to_key,
        "fromTierName": names.get(str(from_key), from_key) if from_key else None,
        "toTierName": names.get(str(to_key), to_key) if to_key else None,
        "cause": cause,
        "label": label,
    }
