from datetime import datetime

from app.services.customer_coupon_service import _admin_audit_transaction_id


def test_admin_audit_transaction_id_fits_varchar_100():
    cid = "bb6dcc84-1234-5678-9abc-def012345678"
    tx_id = _admin_audit_transaction_id(
        customer_coupon_id=cid,
        transaction_type="ADMIN_USE_COUPON",
        now=datetime(2025, 5, 28, 12, 0, 0, 123456),
    )
    assert len(tx_id) <= 100
    assert tx_id.startswith("admcp_")
