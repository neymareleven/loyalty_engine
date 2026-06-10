"""Unomi connectivity and segmentation mode — configuration via .env only.

The **current brand** is always taken from the request (``X-Brand`` / ``?brand=``).
You do not list every brand in .env unless you need opt-in/opt-out rules.

Default when ``UNOMI_BASE_URL`` + ``UNOMI_PASSWORD`` are set:
  → every active brand uses Unomi (scope = brand key unless ``UNOMI_SCOPE_<BRAND>``).

Optional:
  - ``UNOMI_BRANDS`` — opt-in subset only (legacy / mixed deployments)
  - ``UNOMI_INTERNAL_BRANDS`` — opt-out (brands that stay on INTERNAL segmentation)
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from sqlalchemy.orm import Session

SEGMENTATION_MODE_INTERNAL = "INTERNAL"
SEGMENTATION_MODE_UNOMI = "UNOMI"


@dataclass(frozen=True)
class UnomiConnectionConfig:
    base_url: str
    username: str
    password: str
    scope: str


def _brand_env_suffix(brand: str) -> str:
    return re.sub(r"[^A-Z0-9]", "_", brand.strip().upper())


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_for_brand(brand: str, key: str, *, default: str | None = None) -> str | None:
    suffix = _brand_env_suffix(brand)
    for name in (f"UNOMI_{key}_{suffix}", f"UNOMI_{key}"):
        val = os.getenv(name)
        if val is not None and str(val).strip():
            return str(val).strip()
    return default


def _parse_brand_list(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {b.strip().lower() for b in raw.split(",") if b.strip()}


def _unomi_base_url_for_brand(brand: str) -> str | None:
    url = (_env_for_brand(brand, "BASE_URL") or "").rstrip("/")
    return url or None


def _unomi_password_for_brand(brand: str) -> str:
    return (_env_for_brand(brand, "PASSWORD") or "").strip()


def _unomi_credentials_configured(brand: str) -> bool:
    return bool(_unomi_base_url_for_brand(brand) and _unomi_password_for_brand(brand))


def _opt_in_brands() -> set[str]:
    return _parse_brand_list(os.getenv("UNOMI_BRANDS"))


def _opt_out_internal_brands() -> set[str]:
    return _parse_brand_list(os.getenv("UNOMI_INTERNAL_BRANDS"))


def _brand_uses_unomi(brand: str) -> bool:
    """True when the current X-Brand should use Unomi for this request."""
    if not _unomi_credentials_configured(brand):
        return False

    key = brand.strip().lower()
    if key in _opt_out_internal_brands():
        return False

    opt_in = _opt_in_brands()
    if opt_in:
        return key in opt_in

    # URL + password, no UNOMI_BRANDS list → all brands use Unomi (dynamic per X-Brand)
    return True


def _scope_for_brand(brand: str) -> str:
    return (_env_for_brand(brand, "SCOPE") or brand).strip()


def get_segmentation_mode(*, brand: str, db: Session | None = None) -> str:
    _ = db
    if _brand_uses_unomi(brand):
        return SEGMENTATION_MODE_UNOMI
    return SEGMENTATION_MODE_INTERNAL


def unomi_enabled_for_brand(*, brand: str, db: Session | None = None) -> bool:
    return get_segmentation_mode(brand=brand, db=db) == SEGMENTATION_MODE_UNOMI


def unomi_profile_sync_enabled_for_brand(*, brand: str) -> bool:
    """Profile sync is independent of segmentation mode (INTERNAL vs UNOMI)."""
    if not _unomi_credentials_configured(brand):
        return False
    if not _env_bool("UNOMI_PROFILE_SYNC", default=True):
        return False
    key = brand.strip().lower()
    disabled = _parse_brand_list(os.getenv("UNOMI_PROFILE_SYNC_DISABLED"))
    if key in disabled:
        return False
    opt_in = _parse_brand_list(os.getenv("UNOMI_PROFILE_SYNC_BRANDS"))
    if opt_in:
        return key in opt_in
    return True


def resolve_unomi_connection(*, brand: str, db: Session | None = None) -> UnomiConnectionConfig | None:
    _ = db
    if not _brand_uses_unomi(brand):
        return None

    base_url = _unomi_base_url_for_brand(brand)
    if not base_url:
        return None

    username = (_env_for_brand(brand, "USERNAME") or "karaf").strip()
    password = _unomi_password_for_brand(brand)
    if not password:
        return None

    return UnomiConnectionConfig(
        base_url=base_url,
        username=username,
        password=password,
        scope=_scope_for_brand(brand),
    )


def resolve_unomi_profile_connection(*, brand: str) -> UnomiConnectionConfig | None:
    """Unomi connection for profile sync (ignores segmentation mode / INTERNAL opt-out)."""
    if not unomi_profile_sync_enabled_for_brand(brand=brand):
        return None
    base_url = _unomi_base_url_for_brand(brand)
    if not base_url:
        return None
    password = _unomi_password_for_brand(brand)
    if not password:
        return None
    username = (_env_for_brand(brand, "USERNAME") or "karaf").strip()
    return UnomiConnectionConfig(
        base_url=base_url,
        username=username,
        password=password,
        scope=_scope_for_brand(brand),
    )


def unomi_env_status(*, brand: str) -> dict:
    opt_in = sorted(_opt_in_brands())
    opt_out = sorted(_opt_out_internal_brands())
    uses = _brand_uses_unomi(brand)
    conn = resolve_unomi_connection(brand=brand)

    if opt_in:
        policy = "opt_in"
        policy_note = "Only brands listed in UNOMI_BRANDS use Unomi."
    elif opt_out:
        policy = "opt_out"
        policy_note = "All brands use Unomi except UNOMI_INTERNAL_BRANDS."
    elif _unomi_credentials_configured(brand):
        policy = "all_brands"
        policy_note = "Every X-Brand uses Unomi (no UNOMI_BRANDS list required)."
    else:
        policy = "disabled"
        policy_note = "Set UNOMI_BASE_URL and UNOMI_PASSWORD in .env."

    return {
        "activeBrand": brand,
        "segmentationMode": get_segmentation_mode(brand=brand),
        "unomiConfigured": conn is not None,
        "unomiBaseUrl": conn.base_url if conn else _unomi_base_url_for_brand(brand),
        "unomiScope": conn.scope if conn else (_scope_for_brand(brand) if uses else None),
        "configSource": ".env",
        "brandContext": "Current brand from X-Brand header or brand query param (changes per user/session).",
        "unomiPolicy": policy,
        "unomiPolicyNote": policy_note,
        "unomiOptInBrands": opt_in,
        "unomiInternalOnlyBrands": opt_out,
        "currentBrandUsesUnomi": uses,
        "profileSyncEnabled": unomi_profile_sync_enabled_for_brand(brand=brand),
        "envKeys": [
            "UNOMI_BASE_URL + UNOMI_PASSWORD (required)",
            "UNOMI_INTERNAL_BRANDS (optional opt-out)",
            "UNOMI_BRANDS (optional opt-in — omit for all brands)",
            "UNOMI_SCOPE_<BRAND> (optional, else scope = active brand)",
        ],
    }
