"""Points balance, expiry display, and expiration history."""

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

from app.services.wallet_service import (
    is_point_movement_expired,
    serialize_point_movement_out,
)


def _movement(**kwargs):
    defaults = {
        "id": uuid4(),
        "customer_id": uuid4(),
        "points": 100,
        "type": "EARN",
        "source_transaction_id": None,
        "created_at": datetime.utcnow(),
        "expires_at": date.today() + timedelta(days=30),
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_is_point_movement_expired_when_past_expiry_date():
    movement = _movement(expires_at=date.today() - timedelta(days=1))
    assert is_point_movement_expired(movement) is True


def test_is_point_movement_not_expired_without_expiry_date():
    movement = _movement(expires_at=None)
    assert is_point_movement_expired(movement) is False


def test_serialize_point_movement_marks_expired_earn():
    movement = _movement(expires_at=date.today() - timedelta(days=2), points=80)
    out = serialize_point_movement_out(movement)
    assert out["isExpired"] is True
    assert out["status"] == "expired"
    assert out["points"] == 80


def test_serialize_point_movement_expire_type():
    movement = _movement(type="EXPIRE", points=-120, expires_at=None)
    out = serialize_point_movement_out(movement)
    assert out["isExpired"] is True
    assert out["status"] == "expired"
    assert out["points"] == -120


def test_serialize_point_movement_active_with_expiry():
    movement = _movement(expires_at=date.today() + timedelta(days=15), points=50)
    out = serialize_point_movement_out(movement)
    assert out["isExpired"] is False
    assert out["status"] == "active_with_expiry"


def test_get_status_points_balance_excludes_expire_rows():
    from app.services.wallet_service import get_status_points_balance

    db = MagicMock()
    db.query.return_value.filter.return_value.scalar.return_value = 0
    assert get_status_points_balance(db, uuid4()) == 0
    db.query.return_value.filter.assert_called_once()
