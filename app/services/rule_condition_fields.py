"""Shared rule-condition field lists for admin catalogs (UI hints only)."""

from __future__ import annotations

CUSTOMER_PURCHASE_FIELDS = (
    "customer.purchase_count",
    "customer.purchase_amount_total",
)

PURCHASE_RECORDED_PAYLOAD_FIELDS = (
    "payload.orderAmount",
    "payload.purchaseCount",
    "payload.purchaseAmountTotal",
    "payload.isFirstPurchase",
)

TIER_CHANGED_PAYLOAD_FIELDS = (
    "payload.cause",
    "payload.fromTier",
    "payload.toTier",
)

TIER_CHANGED_CAUSE_VALUES = (
    "thresholds",
    "first_purchase_bypass",
    "mode_switch",
    "admin",
    "expiration",
    "tier_config_changed",
)


def extra_payload_condition_fields(transaction_type: str | None) -> list[str]:
    key = str(transaction_type or "").strip().upper()
    if key == "PURCHASE_RECORDED":
        return list(PURCHASE_RECORDED_PAYLOAD_FIELDS)
    if key == "TIER_CHANGED":
        return list(TIER_CHANGED_PAYLOAD_FIELDS)
    return []


def extra_payload_field_meta(transaction_type: str | None) -> dict:
    key = str(transaction_type or "").strip().upper()
    if key == "PURCHASE_RECORDED":
        return {
            "payload.orderAmount": {"valueKind": "number", "ui": {"widget": "number"}, "label": "Montant de la commande"},
            "payload.purchaseCount": {"valueKind": "number", "ui": {"widget": "number"}, "label": "Nombre d'achats (après vente)"},
            "payload.purchaseAmountTotal": {
                "valueKind": "number",
                "ui": {"widget": "number"},
                "label": "Montant cumulé des achats (après vente)",
            },
            "payload.isFirstPurchase": {"valueKind": "boolean", "ui": {"widget": "boolean"}, "label": "Premier achat"},
        }
    if key == "TIER_CHANGED":
        return {
            "payload.cause": {
                "valueKind": "enum",
                "ui": {"widget": "select", "options": list(TIER_CHANGED_CAUSE_VALUES)},
                "label": "Cause du changement de palier",
            },
            "payload.fromTier": {"valueKind": "string", "ui": {"widget": "text"}, "label": "Palier précédent"},
            "payload.toTier": {"valueKind": "string", "ui": {"widget": "text"}, "label": "Palier atteint"},
        }
    return {}


def purchase_customer_field_meta() -> dict:
    return {
        "customer.purchase_count": {
            "valueKind": "number",
            "ui": {"widget": "number"},
            "label": "Nombre d'achats",
        },
        "customer.purchase_amount_total": {
            "valueKind": "number",
            "ui": {"widget": "number"},
            "label": "Montant cumulé des achats",
        },
        "customer.status_points": {
            "valueKind": "number",
            "ui": {"widget": "number"},
            "label": "Points de statut",
        },
    }
