from types import SimpleNamespace
from uuid import uuid4

from fastapi import Response
from starlette.middleware.cors import CORSMiddleware

from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.point_movement import PointMovement
from app.models.purchase_movement import PurchaseMovement
from app.models.transaction import Transaction


class _Q:
    def __init__(self, rows):
        self.rows = list(rows)
        self._offset = 0
        self._limit = None

    def filter(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def offset(self, n):
        self._offset = int(n)
        return self

    def limit(self, n):
        self._limit = int(n)
        return self

    def count(self):
        return len(self.rows)

    def all(self):
        end = None if self._limit is None else self._offset + self._limit
        return self.rows[self._offset : end]


class _DB:
    def __init__(self, mapping: dict):
        self.mapping = mapping

    def query(self, model):
        return _Q(self.mapping.get(model, []))

    def commit(self):
        return None


def _rows(n):
    return [SimpleNamespace(id=uuid4(), n=i) for i in range(n)]


def test_cors_exposes_total_count():
    from app.main import app

    cors = next(m for m in app.user_middleware if m.cls is CORSMiddleware)
    exposed = {str(h).lower() for h in (cors.kwargs.get("expose_headers") or [])}
    assert "x-total-count" in exposed
    assert "x-unomi-sync" in exposed
    assert "x-unomi-sync-detail" in exposed


def test_openapi_documents_total_count_header():
    from app.main import app

    schema = app.openapi()
    paths = [
        "/customers/{brand}/{profile_id}/point-movements",
        "/customers/{brand}/{profile_id}/purchase-movements",
        "/transactions/by-user",
        "/transactions/by-user-and-type",
    ]
    for path in paths:
        headers = schema["paths"][path]["get"]["responses"]["200"].get("headers") or {}
        assert "X-Total-Count" in headers, path
    coupons = schema["paths"]["/customers/{brand}/{profile_id}/coupons-with-rewards"]["get"]
    assert "count" in (coupons["responses"]["200"].get("description") or "")


def test_point_movements_page_and_total(monkeypatch):
    from app.routes.customers import list_point_movements

    customer = SimpleNamespace(id=uuid4(), profile_id="wp_1")
    monkeypatch.setattr("app.routes.customers._require_customer", lambda *a, **k: customer)
    monkeypatch.setattr("app.routes.customers.serialize_point_movement_out", lambda row: {"id": str(row.id)})
    response = Response()
    result = list_point_movements(
        brand="batira",
        profile_id="wp_1",
        email=None,
        active_brand="batira",
        limit=50,
        offset=100,
        db=_DB({PointMovement: _rows(120)}),
        response=response,
    )
    assert len(result) == 20
    assert response.headers["X-Total-Count"] == "120"


def test_purchase_movements_page_and_total(monkeypatch):
    from app.routes.customers import list_purchase_movements

    customer = SimpleNamespace(id=uuid4(), profile_id="wp_1")
    monkeypatch.setattr("app.routes.customers._require_customer", lambda *a, **k: customer)
    monkeypatch.setattr("app.routes.customers.backfill_customer_purchase_movements_from_sales", lambda *a, **k: None)
    monkeypatch.setattr("app.routes.customers.serialize_purchase_movement_out", lambda row: {"id": str(row.id)})
    response = Response()
    result = list_purchase_movements(
        brand="batira",
        profile_id="wp_1",
        email=None,
        active_brand="batira",
        limit=50,
        offset=100,
        db=_DB({PurchaseMovement: _rows(120)}),
        response=response,
    )
    assert len(result) == 20
    assert response.headers["X-Total-Count"] == "120"


def test_transactions_by_user_page_and_total(monkeypatch):
    from app.routes.transactions import list_transactions_by_user

    customer = SimpleNamespace(id=uuid4(), profile_id="wp_1")
    monkeypatch.setattr(
        "app.routes.transactions._resolve_customer_for_transaction_list",
        lambda *a, **k: customer,
    )
    monkeypatch.setattr("app.routes.transactions.customer_transaction_filters", lambda *a, **k: True)
    monkeypatch.setattr("app.routes.transactions._serialize_transaction_out", lambda tx: {"id": str(tx.id)})
    response = Response()
    result = list_transactions_by_user(
        brand="batira",
        profileId="wp_1",
        email=None,
        active_brand="batira",
        limit=50,
        offset=100,
        db=_DB({Transaction: _rows(120)}),
        response=response,
    )
    assert len(result) == 20
    assert response.headers["X-Total-Count"] == "120"


def test_transactions_by_user_and_type_page_and_total(monkeypatch):
    from app.routes.transactions import list_transactions_by_user_and_type

    customer = SimpleNamespace(id=uuid4(), profile_id="wp_1")
    monkeypatch.setattr(
        "app.routes.transactions._resolve_customer_for_transaction_list",
        lambda *a, **k: customer,
    )
    monkeypatch.setattr("app.routes.transactions.customer_transaction_filters", lambda *a, **k: True)
    monkeypatch.setattr("app.routes.transactions._serialize_transaction_out", lambda tx: {"id": str(tx.id)})
    response = Response()
    result = list_transactions_by_user_and_type(
        brand="batira",
        transactionType="SALE",
        profileId="wp_1",
        email=None,
        active_brand="batira",
        limit=50,
        offset=100,
        db=_DB({Transaction: _rows(120)}),
        response=response,
    )
    assert len(result) == 20
    assert response.headers["X-Total-Count"] == "120"


def test_coupons_with_rewards_count(monkeypatch):
    from app.routes.customers import list_customer_coupons_with_rewards

    customer = SimpleNamespace(id=uuid4(), profile_id="wp_1")
    coupons = _rows(120)
    monkeypatch.setattr("app.routes.customers._require_customer", lambda *a, **k: customer)
    monkeypatch.setattr(
        "app.routes.customers.serialize_customer_coupon_out",
        lambda db, coupon: {"id": str(coupon.id)},
    )
    result = list_customer_coupons_with_rewards(
        brand="batira",
        profile_id="wp_1",
        email=None,
        active_brand="batira",
        status=None,
        coupon_limit=50,
        coupon_offset=100,
        reward_status=None,
        db=_DB({CustomerCoupon: coupons, CustomerReward: []}),
    )
    assert len(result["items"]) == 20
    assert result["limit"] == 50
    assert result["offset"] == 100
    assert result["count"] == 120
    assert result["profileId"] == "wp_1"
