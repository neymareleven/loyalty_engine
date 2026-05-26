"""Guards for system-managed transactions and transaction types."""

from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.event_type import TransactionType
from app.models.transaction import Transaction


# Keys provisioned by ensure_system_transaction_types (loyalty_settings_service).
SYSTEM_MANAGED_TRANSACTION_TYPE_KEYS = frozenset(
    {
        "TIER_UPGRADED",
        "TIER_DOWNGRADED",
        "TIER_RENEWED",
        "STATUS_RESET",
        "ADMIN_SET_TIER",
        "CUSTOMER_REGISTRATION",
    }
)

def is_system_transaction_type(obj: TransactionType | None) -> bool:
    if not obj:
        return False
    if obj.origin != "INTERNAL":
        return False
    desc = (obj.description or "").strip()
    return desc.startswith("System event emitted")


def is_hidden_transaction_type(obj: TransactionType | None) -> bool:
    if not obj:
        return False
    return obj.key == "ADMIN_SET_TIER"


def is_system_managed_transaction(tx: Transaction | None) -> bool:
    """True for audit rows auto-emitted by the loyalty engine (not deletable).

    INTERNAL origin or INTERNAL_JOB source alone does NOT imply protection:
    business events created via admin/internal jobs keep transaction_type keys
    chosen by ops (e.g. birthday_promo) and remain deletable.
    """
    if tx is None:
        return False

    tx_type = (tx.transaction_type or "").strip().upper()
    if tx_type in SYSTEM_MANAGED_TRANSACTION_TYPE_KEYS:
        return True
    # Admin coupon lifecycle audit (ADMIN_USE_COUPON, ADMIN_REOPEN_COUPON, …)
    if tx_type.startswith("ADMIN_"):
        return True

    return False


def transaction_deletion_meta(tx: Transaction | None) -> dict:
    protected = is_system_managed_transaction(tx)
    return {
        "can_delete": not protected,
        "is_system_managed": protected,
        "recommended_action": None if not protected else "protected",
    }


def assert_transaction_deletable(tx: Transaction | None) -> None:
    if tx is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    if is_system_managed_transaction(tx):
        raise HTTPException(
            status_code=403,
            detail=(
                "Cette transaction système ne peut pas être supprimée "
                "(audit fidélité / événement interne)."
            ),
        )


def assert_transaction_type_mutable(obj: TransactionType | None) -> None:
    if obj is None:
        raise HTTPException(status_code=404, detail="Transaction type not found")
    if is_hidden_transaction_type(obj) or is_system_transaction_type(obj):
        raise HTTPException(
            status_code=403,
            detail="This transaction type is managed by the system and cannot be modified",
        )


def assert_transaction_type_deletable(obj: TransactionType | None) -> None:
    if obj is None:
        raise HTTPException(status_code=404, detail="Transaction type not found")
    if is_hidden_transaction_type(obj) or is_system_transaction_type(obj):
        raise HTTPException(
            status_code=403,
            detail="This transaction type is managed by the system and cannot be deleted",
        )


def delete_transaction_if_allowed(db: Session, tx: Transaction) -> None:
    """Delete a non-system transaction and its rule executions (admin only)."""
    from app.models.customer_coupon import CustomerCoupon
    from app.models.customer_reward import CustomerReward
    from app.models.point_movement import PointMovement
    from app.models.transaction_rule_execution import TransactionRuleExecution

    assert_transaction_deletable(tx)

    blocking: list[str] = []
    if (
        db.query(PointMovement.id)
        .filter(PointMovement.source_transaction_id == tx.id)
        .first()
    ):
        blocking.append("point_movements")
    if (
        db.query(CustomerReward.id)
        .filter(CustomerReward.source_transaction_id == tx.id)
        .first()
    ):
        blocking.append("customer_rewards")
    if (
        db.query(CustomerCoupon.id)
        .filter(CustomerCoupon.source_transaction_id == tx.id)
        .first()
    ):
        blocking.append("customer_coupons")

    if blocking:
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    "Impossible de supprimer cette transaction car elle est référencée "
                    "par d'autres données fidélité."
                ),
                "blockingReferences": blocking,
            },
        )

    db.query(TransactionRuleExecution).filter(
        TransactionRuleExecution.transaction_id == tx.id
    ).delete(synchronize_session=False)
    db.delete(tx)
