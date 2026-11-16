from datetime import date, datetime

from app.services.loyalty_status_service import (
    loyalty_status_expired_on,
    status_window_expires_at,
)
from app.services.wallet_service import is_point_movement_expired


def test_status_window_expires_at_midnight_of_assigned_plus_days():
    assigned = datetime(2026, 11, 5, 0, 0, 1)
    expires = status_window_expires_at(assigned, 1)
    assert expires == datetime(2026, 11, 6, 0, 0, 0)


def test_job_at_midnight_on_expiry_day_expires_status():
    expires = datetime(2026, 11, 6, 0, 0, 0)
    assert loyalty_status_expired_on(expires_at=expires, now=datetime(2026, 11, 6, 0, 0, 0)) is True
    assert loyalty_status_expired_on(expires_at=expires, now=datetime(2026, 11, 5, 23, 59, 59)) is False


def test_point_lot_expires_on_its_calendar_day():
    class Movement:
        type = "EARN"
        points = 10
        expires_at = date(2026, 11, 6)

    assert is_point_movement_expired(Movement(), today=date(2026, 11, 6)) is True
    assert is_point_movement_expired(Movement(), today=date(2026, 11, 5)) is False
