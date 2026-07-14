"""Profile view reconciliation when Unomi CDP profileId differs from loyalty master."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.services.contact_service import _customer_email, normalize_lookup_email


@dataclass
class ProfileReconciliation:
    requested_profile_id: str
    loyalty_profile_id: str
    reconciled_by: str | None
    show_merge_notice: bool
    unomi_sync: dict[str, Any] | None = None

    def to_api_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "requestedProfileId": self.requested_profile_id,
            "loyaltyProfileId": self.loyalty_profile_id,
            "showMergeNotice": self.show_merge_notice,
        }
        if self.reconciled_by:
            out["reconciledBy"] = self.reconciled_by
        if self.unomi_sync is not None:
            out["unomiSync"] = self.unomi_sync
        return out


def reconcile_profile_view(
    db: Session,
    *,
    brand: str,
    requested_profile_id: str,
    email: str | None,
    customer: Customer,
    alias_registered: bool,
    sync_unomi: bool = True,
) -> ProfileReconciliation | None:
    """
    When the UI opens a CDP profileId that differs from the loyalty master, reconcile silently
    if email + brand match the canonical customer. Otherwise signal the UI to show a warning.
    """
    requested = (requested_profile_id or "").strip()
    master = (customer.profile_id or "").strip()
    if not requested or not master or requested == master:
        return None

    norm_email = normalize_lookup_email(email, brand=brand)
    cust_email = _customer_email(customer)
    email_match = bool(norm_email and cust_email and norm_email == cust_email)
    brand_ok = (customer.brand or "").strip() == (brand or "").strip()

    reconciled_by: str | None = None
    if email_match and brand_ok:
        reconciled_by = "email"
    elif alias_registered:
        reconciled_by = "alias"

    # Hide merge banner when email+brand identify the canonical loyalty customer.
    show_merge_notice = not (email_match and brand_ok)

    unomi_sync: dict[str, Any] | None = None
    if sync_unomi and email_match and brand_ok:
        from app.services.unomi_profile_service import sync_customer_profile_to_unomi

        unomi_sync = sync_customer_profile_to_unomi(
            db,
            customer=customer,
            reason="profile_reconcile",
            transport_override="profiles",
            target_profile_id=requested,
        )

    return ProfileReconciliation(
        requested_profile_id=requested,
        loyalty_profile_id=master,
        reconciled_by=reconciled_by,
        show_merge_notice=show_merge_notice,
        unomi_sync=unomi_sync,
    )
