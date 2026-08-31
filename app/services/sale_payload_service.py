"""Normalize WooCommerce / Unomi sale payloads before rule evaluation."""

from __future__ import annotations

from typing import Any

from app.services.payload_normalization_service import normalize_transaction_payload


def normalize_sale_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    return normalize_transaction_payload(payload, transaction_type="sale")
