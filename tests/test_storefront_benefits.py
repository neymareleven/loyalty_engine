from types import SimpleNamespace
from uuid import uuid4

from fastapi import HTTPException

from app.services.benefit_service import compute_benefit, describe_benefit
from app.services.coupon_code import generate_coupon_code
from app.services.loyalty_status_service import last_change_from_tier_event
from app.services.redemption_service import evaluate_coupon_or_reward
from app.services.storefront_auth import hash_api_key


def test_percent_discount_capped_and_integer():
    cart = {"subtotal": 50000, "items": []}
    benefit, reason = compute_benefit(
        benefit_type="PERCENT_DISCOUNT",
        benefit_value=10,
        max_discount_amount=20000,
        min_cart_amount=15000,
        applies_to="CART",
        free_product_quantity=1,
        cart=cart,
        product_ids=set(),
        category_ids=set(),
    )
    assert reason is None
    assert benefit["discountAmount"] == 5000


def test_percent_discount_respects_max_and_min_cart():
    too_small, reason = compute_benefit(
        benefit_type="PERCENT_DISCOUNT",
        benefit_value=10,
        max_discount_amount=None,
        min_cart_amount=30000,
        applies_to="CART",
        free_product_quantity=1,
        cart={"subtotal": 10000},
        product_ids=set(),
        category_ids=set(),
    )
    assert too_small is None
    assert reason == "MIN_CART_NOT_REACHED"

    capped, _ = compute_benefit(
        benefit_type="PERCENT_DISCOUNT",
        benefit_value=50,
        max_discount_amount=4000,
        min_cart_amount=None,
        applies_to="CART",
        free_product_quantity=1,
        cart={"subtotal": 20000},
        product_ids=set(),
        category_ids=set(),
    )
    assert capped["discountAmount"] == 4000


def test_fixed_discount_and_free_shipping():
    fixed, _ = compute_benefit(
        benefit_type="FIXED_DISCOUNT",
        benefit_value=5000,
        max_discount_amount=None,
        min_cart_amount=30000,
        applies_to="CART",
        free_product_quantity=1,
        cart={"subtotal": 40000},
        product_ids=set(),
        category_ids=set(),
    )
    assert fixed["discountAmount"] == 5000
    ship, _ = compute_benefit(
        benefit_type="FREE_SHIPPING",
        benefit_value=None,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="CART",
        free_product_quantity=1,
        cart={"subtotal": 10000, "shippingAmount": 2500},
        product_ids=set(),
        category_ids=set(),
    )
    assert ship["shippingDiscount"] == 2500


def test_free_product_requires_catalog_link():
    none, reason = compute_benefit(
        benefit_type="FREE_PRODUCT",
        benefit_value=None,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="PRODUCTS",
        free_product_quantity=1,
        cart={"subtotal": 10000},
        product_ids=set(),
        category_ids=set(),
    )
    assert none is None
    assert reason == "NO_ELIGIBLE_ITEMS"
    ok, _ = compute_benefit(
        benefit_type="FREE_PRODUCT",
        benefit_value=None,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="PRODUCTS",
        free_product_quantity=2,
        cart={"subtotal": 10000},
        product_ids={"sku-1"},
        category_ids=set(),
    )
    assert ok["freeItems"] == [{"productId": "sku-1", "quantity": 2}]


def test_coupon_code_format():
    code = generate_coupon_code()
    assert code.startswith("LOY-")
    parts = code.split("-")
    assert len(parts) == 3
    assert all(ch not in "01OI" for ch in "".join(parts[1:]))


def test_api_key_hash_is_sha256_hex():
    digest = hash_api_key("le_live_secret")
    assert len(digest) == 64
    assert digest == hash_api_key("le_live_secret")
    assert digest != hash_api_key("other")


def test_not_owner_when_coupon_belongs_to_other_customer(monkeypatch):
    owner_id = uuid4()
    other_id = uuid4()
    coupon = SimpleNamespace(
        id=uuid4(),
        customer_id=owner_id,
        code="LOY-AAAA-BBBB",
        shareable=False,
        expires_at=None,
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=owner_id, brand="batira")
    customer = SimpleNamespace(id=other_id, brand="batira")

    class Q:
        def __init__(self, result):
            self.result = result

        def join(self, *args, **kwargs):
            return self

        def filter(self, *args, **kwargs):
            return self

        def first(self):
            return self.result

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon

            if model is CustomerCoupon:
                return Q(coupon)
            if model is Customer:
                return Q(owner)
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    result = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-AAAA-BBBB",
        customer_reward_id=None,
        cart={"subtotal": 50000},
    )
    assert result["applicable"] is False
    assert "NOT_OWNER" in result["reasons"]


def test_storefront_401_without_bearer():
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    response = client.get("/v1/storefront/customers/wp_1/wallet")
    assert response.status_code == 401


def test_last_change_renewed_and_registration():
    assert last_change_from_tier_event("TIER_RENEWED") == "renewed"
    assert last_change_from_tier_event("CUSTOMER_REGISTRATION") == "registration"


def test_customer_not_found_code():
    from app.services.redemption_service import resolve_storefront_customer

    class Q:
        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return None

        def all(self):
            return []

    class DB:
        def query(self, *a, **k):
            return Q()

    try:
        resolve_storefront_customer(DB(), brand="batira", profile_id="missing")
        assert False, "expected 404"
    except HTTPException as exc:
        assert exc.status_code == 404
        assert exc.detail["code"] == "CUSTOMER_NOT_FOUND"


def test_second_discount_is_not_stackable_by_default(monkeypatch):
    customer_id = uuid4()
    coupon = SimpleNamespace(
        id=uuid4(),
        customer_id=customer_id,
        code="LOY-CCCC-DDDD",
        shareable=False,
        expires_at=None,
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class Q:
        def __init__(self, result):
            self.result = result

        def join(self, *args, **kwargs):
            return self

        def filter(self, *args, **kwargs):
            return self

        def first(self):
            return self.result

        def all(self):
            return []

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon
            from app.models.customer_reward import CustomerReward

            if model is CustomerCoupon:
                return Q(coupon)
            if model is Customer:
                return Q(owner)
            if model is CustomerReward:
                return Q(None)
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: [
            {
                "benefitType": "PERCENT_DISCOUNT",
                "benefitValue": 10,
                "maxDiscountAmount": None,
                "minCartAmount": None,
                "appliesTo": "CART",
                "freeProductQuantity": 1,
                "stackable": False,
            }
        ],
    )
    result = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-CCCC-DDDD",
        customer_reward_id=None,
        cart={"subtotal": 50000},
        applied_benefits=[{"type": "PERCENT_DISCOUNT", "discountAmount": 3000}],
    )
    assert result["applicable"] is False
    assert "NOT_STACKABLE" in result["reasons"]


def test_percent_plus_free_product_can_stack(monkeypatch):
    customer_id = uuid4()
    coupon = SimpleNamespace(
        id=uuid4(),
        customer_id=customer_id,
        code="LOY-EEEE-FFFF",
        shareable=True,
        expires_at=None,
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class Q:
        def __init__(self, result):
            self.result = result

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return []

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon

            if model is CustomerCoupon:
                return Q(coupon)
            if model is Customer:
                return Q(owner)
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: [
            {
                "benefitType": "FREE_PRODUCT",
                "benefitValue": None,
                "appliesTo": "PRODUCTS",
                "freeProductQuantity": 1,
                "stackable": False,
                "id": str(uuid4()),
            }
        ],
    )
    monkeypatch.setattr(
        "app.services.redemption_service._reward_scope",
        lambda db, reward: ({"sku-9"}, set()),
    )
    result = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-EEEE-FFFF",
        customer_reward_id=None,
        cart={"subtotal": 50000},
        applied_benefits=[{"type": "PERCENT_DISCOUNT", "discountAmount": 5000}],
    )
    assert result["applicable"] is True
    assert result["benefits"][0]["type"] == "FREE_PRODUCT"


def test_expired_coupon_reason(monkeypatch):
    from datetime import datetime, timedelta

    customer_id = uuid4()
    coupon = SimpleNamespace(
        id=uuid4(),
        customer_id=customer_id,
        code="LOY-GONE-0001",
        shareable=False,
        expires_at=datetime.utcnow() - timedelta(days=1),
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class Q:
        def __init__(self, result):
            self.result = result

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon

            if model is CustomerCoupon:
                return Q(coupon)
            if model is Customer:
                return Q(owner)
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    result = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-GONE-0001",
        customer_reward_id=None,
        cart={"subtotal": 50000},
    )
    assert result["applicable"] is False
    assert "EXPIRED" in result["reasons"]


def test_commit_other_brand_is_404():
    from app.services.redemption_service import commit, release

    class Q:
        def filter(self, *a, **k):
            return self

        def with_for_update(self):
            return self

        def first(self):
            return None

    class DB:
        def query(self, *a, **k):
            return Q()

    rid = str(uuid4())
    try:
        commit(DB(), redemption_id=rid, order_ref="ord-1", idempotency_key="k1", brand="brand-a")
        assert False, "expected 404"
    except HTTPException as exc:
        assert exc.status_code == 404
    try:
        release(DB(), redemption_id=rid, reason="abandoned", brand="brand-a")
        assert False, "expected 404"
    except HTTPException as exc:
        assert exc.status_code == 404


def test_reserve_never_moves_used_to_reserved(monkeypatch):
    from app.services.redemption_service import reserve

    customer_id = uuid4()
    coupon_id = uuid4()
    issued = SimpleNamespace(
        id=coupon_id,
        customer_id=customer_id,
        code="LOY-USED-LOCK",
        shareable=False,
        expires_at=None,
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
        coupon_type_id=None,
        payload=None,
    )
    used = SimpleNamespace(
        id=coupon_id,
        customer_id=customer_id,
        code="LOY-USED-LOCK",
        shareable=False,
        expires_at=None,
        status="USED",
        reserved_until=None,
        reserved_by_redemption_id=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class CouponQ:
        def __init__(self):
            self._locked = False

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def with_for_update(self):
            self._locked = True
            return self

        def first(self):
            return issued

        def one(self):
            return used if self._locked else issued

    class Q:
        def __init__(self, result=None):
            self.result = result

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return []

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon
            from app.models.redemption import Redemption

            if model is CustomerCoupon:
                return CouponQ()
            if model is Customer:
                return Q(owner)
            if model is Redemption:
                return Q(None)
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: [
            {
                "benefitType": "PERCENT_DISCOUNT",
                "benefitValue": 10,
                "maxDiscountAmount": None,
                "minCartAmount": None,
                "appliesTo": "CART",
                "freeProductQuantity": 1,
                "stackable": False,
            }
        ],
    )
    result = reserve(
        DB(),
        brand="batira",
        site_id="site-1",
        customer=customer,
        code="LOY-USED-LOCK",
        customer_reward_id=None,
        cart={"subtotal": 50000, "cartRef": "cart-used"},
        ttl_minutes=15,
        idempotency_key="idemp-used",
    )
    assert result["applicable"] is False
    assert "ALREADY_USED" in result["reasons"]
    assert used.status == "USED"


def test_stale_reservation_can_be_reserved_again(monkeypatch):
    from datetime import datetime, timedelta

    from app.services.redemption_service import reserve

    customer_id = uuid4()
    coupon_id = uuid4()
    previous = SimpleNamespace(id=uuid4(), status="RESERVED", cart_ref="old-cart")
    stale_until = datetime.utcnow() - timedelta(minutes=5)
    coupon = SimpleNamespace(
        id=coupon_id,
        customer_id=customer_id,
        code="LOY-STALE-0001",
        shareable=False,
        expires_at=None,
        status="RESERVED",
        reserved_until=stale_until,
        reserved_by_redemption_id=previous.id,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")
    added = []

    class CouponQ:
        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def with_for_update(self):
            return self

        def first(self):
            return coupon

        def one(self):
            return coupon

    class RedemptionQ:
        def __init__(self):
            self.n = 0

        def filter(self, *a, **k):
            self.n += 1
            return self

        def first(self):
            if self.n <= 1:
                return None
            return previous

    class Q:
        def __init__(self, result=None):
            self.result = result

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return []

    class DB:
        def __init__(self):
            self._redemptions = RedemptionQ()

        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon
            from app.models.redemption import Redemption

            if model is CustomerCoupon:
                return CouponQ()
            if model is Customer:
                return Q(owner)
            if model is Redemption:
                return self._redemptions
            return Q(None)

        def add(self, obj):
            added.append(obj)

        def flush(self):
            if added and getattr(added[0], "id", None) is None:
                added[0].id = uuid4()

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: [
            {
                "benefitType": "PERCENT_DISCOUNT",
                "benefitValue": 10,
                "maxDiscountAmount": None,
                "minCartAmount": None,
                "appliesTo": "CART",
                "freeProductQuantity": 1,
                "stackable": False,
            }
        ],
    )
    result = reserve(
        DB(),
        brand="batira",
        site_id="site-1",
        customer=customer,
        code="LOY-STALE-0001",
        customer_reward_id=None,
        cart={"subtotal": 50000, "cartRef": "new-cart"},
        ttl_minutes=15,
        idempotency_key="idemp-stale",
    )
    assert result["applicable"] is True
    assert result.get("status") == "RESERVED"
    assert previous.status == "EXPIRED"
    assert coupon.status == "RESERVED"
    assert coupon.reserved_by_redemption_id == added[0].id


def test_applied_cart_reservation_blocks_second_discount(monkeypatch):
    from datetime import datetime, timedelta

    from app.services.redemption_service import applied_benefits_for_cart

    customer_id = uuid4()
    reserved = SimpleNamespace(
        id=uuid4(),
        reserved_until=datetime.utcnow() + timedelta(minutes=20),
        benefit=[{"type": "PERCENT_DISCOUNT", "discountAmount": 3000}],
    )
    coupon = SimpleNamespace(
        id=uuid4(),
        customer_id=customer_id,
        code="LOY-STACK-0002",
        shareable=False,
        expires_at=None,
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class CouponQ:
        def __init__(self, result):
            self.result = result

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return []

    class RedemptionQ:
        def filter(self, *a, **k):
            return self

        def all(self):
            return [reserved]

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon
            from app.models.redemption import Redemption

            if model is Redemption:
                return RedemptionQ()
            if model is CustomerCoupon:
                return CouponQ(coupon)
            if model is Customer:
                return CouponQ(owner)
            return CouponQ(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: [
            {
                "benefitType": "PERCENT_DISCOUNT",
                "benefitValue": 15,
                "maxDiscountAmount": None,
                "minCartAmount": None,
                "appliesTo": "CART",
                "freeProductQuantity": 1,
                "stackable": False,
            }
        ],
    )
    applied = applied_benefits_for_cart(
        DB(),
        brand="batira",
        customer_id=customer_id,
        cart_ref="same-cart",
        extra_ids=None,
    )
    result = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-STACK-0002",
        customer_reward_id=None,
        cart={"subtotal": 50000, "cartRef": "same-cart"},
        applied_benefits=applied,
    )
    assert result["applicable"] is False
    assert "NOT_STACKABLE" in result["reasons"]


def test_empty_scopes_forbidden():
    from starlette.requests import Request

    from app.services.storefront_auth import require_storefront_client

    request = Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [(b"authorization", b"Bearer le_live_test")],
            "client": ("127.0.0.1", 123),
            "server": ("test", 80),
        }
    )

    class Q:
        def filter(self, *a, **k):
            return self

        def first(self):
            return SimpleNamespace(
                id=uuid4(),
                brand="batira",
                scopes=[],
                active=True,
                last_used_at=None,
            )

    class DB:
        def query(self, *a, **k):
            return Q()

    try:
        require_storefront_client(request, DB())
        assert False, "expected 403"
    except HTTPException as exc:
        assert exc.status_code == 403


def test_last_used_at_throttled_and_validate_audit_commits():
    from datetime import datetime, timedelta

    from starlette.requests import Request

    from app.models.storefront_audit_event import StorefrontAuditEvent
    from app.services.storefront_audit import record_storefront_audit
    from app.services.storefront_auth import persist_storefront_session, require_storefront_client

    recent = datetime.utcnow() - timedelta(seconds=10)
    client = SimpleNamespace(
        id=uuid4(),
        brand="batira",
        scopes=["storefront"],
        active=True,
        last_used_at=recent,
    )
    request = Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [(b"authorization", b"Bearer le_live_test")],
            "client": ("127.0.0.1", 123),
            "server": ("test", 80),
        }
    )

    class Q:
        def filter(self, *a, **k):
            return self

        def first(self):
            return client

    class DB:
        def query(self, *a, **k):
            return Q()

        def add(self, obj):
            self.added = obj

        def flush(self):
            self.flushed = True

        def commit(self):
            self.committed = True

    db = DB()
    require_storefront_client(request, db)
    assert client.last_used_at == recent
    assert request.state.storefront_dirty is False

    record_storefront_audit(
        db,
        brand="batira",
        site_id=str(client.id),
        action="validate",
        profile_id="wp_1",
        request={"code": "LOY-AAAA-BBBB"},
        response={"applicable": True},
    )
    persist_storefront_session(db, request, force=True)
    assert isinstance(db.added, StorefrontAuditEvent)
    assert db.added.action == "validate"
    assert db.committed is True


def _percent_snapshot():
    return [
        {
            "benefitType": "PERCENT_DISCOUNT",
            "benefitValue": 10,
            "maxDiscountAmount": None,
            "minCartAmount": None,
            "appliesTo": "CART",
            "freeProductQuantity": 1,
            "stackable": False,
        }
    ]


def test_revalidate_same_reserved_coupon_recalculates(monkeypatch):
    from datetime import datetime, timedelta

    from app.services.redemption_service import applied_benefits_for_cart

    coupon_id = uuid4()
    customer_id = uuid4()
    reserved = SimpleNamespace(
        id=uuid4(),
        customer_coupon_id=coupon_id,
        customer_reward_id=None,
        cart_ref="cart-x",
        reserved_until=datetime.utcnow() + timedelta(minutes=20),
        benefit=[{"type": "PERCENT_DISCOUNT", "discountAmount": 5000}],
    )
    coupon = SimpleNamespace(
        id=coupon_id,
        customer_id=customer_id,
        code="LOY-SAME-0001",
        shareable=False,
        expires_at=None,
        status="RESERVED",
        reserved_until=reserved.reserved_until,
        reserved_by_redemption_id=reserved.id,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class Q:
        def __init__(self, result=None, rows=None):
            self.result = result
            self.rows = rows or []

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return self.rows

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon
            from app.models.redemption import Redemption

            if model is CustomerCoupon:
                return Q(coupon)
            if model is Customer:
                return Q(owner)
            if model is Redemption:
                return Q(reserved, rows=[reserved])
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: _percent_snapshot(),
    )
    applied = applied_benefits_for_cart(
        DB(),
        brand="batira",
        customer_id=customer_id,
        cart_ref="cart-x",
        exclude_coupon_id=coupon_id,
    )
    assert applied == []
    result = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-SAME-0001",
        customer_reward_id=None,
        cart={"subtotal": 80000, "cartRef": "cart-x"},
        applied_benefits=applied,
    )
    assert result["applicable"] is True
    assert result["benefits"][0]["discountAmount"] == 8000


def test_rereserve_same_coupon_replaces_reservation(monkeypatch):
    from datetime import datetime, timedelta

    from app.services.redemption_service import applied_benefits_for_cart, reserve

    coupon_id = uuid4()
    customer_id = uuid4()
    previous = SimpleNamespace(
        id=uuid4(),
        status="RESERVED",
        cart_ref="cart-x",
        order_ref=None,
        customer_coupon_id=coupon_id,
        customer_reward_id=None,
        reserved_until=datetime.utcnow() + timedelta(minutes=20),
        benefit=[{"type": "PERCENT_DISCOUNT", "discountAmount": 5000}],
    )
    coupon = SimpleNamespace(
        id=coupon_id,
        customer_id=customer_id,
        code="LOY-SAME-0002",
        shareable=False,
        expires_at=None,
        status="RESERVED",
        reserved_until=previous.reserved_until,
        reserved_by_redemption_id=previous.id,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")
    added = []

    class CouponQ:
        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def with_for_update(self):
            return self

        def first(self):
            return coupon

        def one(self):
            return coupon

    class RedemptionQ:
        def __init__(self):
            self.firsts = 0

        def filter(self, *a, **k):
            return self

        def first(self):
            self.firsts += 1
            if self.firsts == 1:
                return None
            return previous

        def all(self):
            return [previous]

    class Q:
        def __init__(self, result=None):
            self.result = result

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return []

    class DB:
        def __init__(self):
            self._redemptions = RedemptionQ()

        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon
            from app.models.redemption import Redemption

            if model is CustomerCoupon:
                return CouponQ()
            if model is Customer:
                return Q(owner)
            if model is Redemption:
                return self._redemptions
            return Q(None)

        def add(self, obj):
            added.append(obj)

        def flush(self):
            if added and getattr(added[0], "id", None) is None:
                added[0].id = uuid4()

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: _percent_snapshot(),
    )
    db = DB()
    applied = applied_benefits_for_cart(
        db,
        brand="batira",
        customer_id=customer_id,
        cart_ref="cart-x",
        exclude_coupon_id=coupon_id,
    )
    assert applied == []
    result = reserve(
        db,
        brand="batira",
        site_id="site-1",
        customer=customer,
        code="LOY-SAME-0002",
        customer_reward_id=None,
        cart={"subtotal": 80000, "cartRef": "cart-x"},
        ttl_minutes=15,
        idempotency_key="idemp-rereserve",
        applied_benefits=applied,
    )
    assert "NOT_STACKABLE" not in (result.get("reasons") or [])
    assert result["applicable"] is True
    assert result.get("status") == "RESERVED"
    assert previous.status == "RELEASED"
    assert coupon.status == "RESERVED"
    assert coupon.reserved_by_redemption_id == added[0].id


def test_second_coupon_on_same_cart_is_not_stackable(monkeypatch):
    from datetime import datetime, timedelta

    from app.services.redemption_service import applied_benefits_for_cart

    coupon_a_id = uuid4()
    coupon_b_id = uuid4()
    customer_id = uuid4()
    reserved_a = SimpleNamespace(
        id=uuid4(),
        customer_coupon_id=coupon_a_id,
        customer_reward_id=None,
        reserved_until=datetime.utcnow() + timedelta(minutes=20),
        benefit=[{"type": "PERCENT_DISCOUNT", "discountAmount": 3000}],
    )
    coupon_b = SimpleNamespace(
        id=coupon_b_id,
        customer_id=customer_id,
        code="LOY-B-0001",
        shareable=False,
        expires_at=None,
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class Q:
        def __init__(self, result=None, rows=None):
            self.result = result
            self.rows = rows or []

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return self.rows

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon
            from app.models.redemption import Redemption

            if model is CustomerCoupon:
                return Q(coupon_b)
            if model is Customer:
                return Q(owner)
            if model is Redemption:
                return Q(reserved_a, rows=[reserved_a])
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: _percent_snapshot(),
    )
    applied = applied_benefits_for_cart(
        DB(),
        brand="batira",
        customer_id=customer_id,
        cart_ref="cart-x",
        exclude_coupon_id=coupon_b_id,
    )
    result = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-B-0001",
        customer_reward_id=None,
        cart={"subtotal": 50000, "cartRef": "cart-x"},
        applied_benefits=applied,
    )
    assert result["applicable"] is False
    assert "NOT_STACKABLE" in result["reasons"]


def test_free_shipping_reward_accepted_with_reserved_discount(monkeypatch):
    from datetime import datetime, timedelta

    from app.services.redemption_service import applied_benefits_for_cart

    coupon_a_id = uuid4()
    customer_id = uuid4()
    reward_id = uuid4()
    reserved_a = SimpleNamespace(
        id=uuid4(),
        customer_coupon_id=coupon_a_id,
        customer_reward_id=None,
        reserved_until=datetime.utcnow() + timedelta(minutes=20),
        benefit=[{"type": "PERCENT_DISCOUNT", "discountAmount": 3000}],
    )
    customer_reward = SimpleNamespace(
        id=reward_id,
        customer_id=customer_id,
        expires_at=None,
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
        reward_id=uuid4(),
    )
    shipping_reward = SimpleNamespace(
        id=customer_reward.reward_id,
        benefit_type="FREE_SHIPPING",
        benefit_value=None,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="CART",
        free_product_quantity=1,
        stackable=False,
        name="Livraison offerte",
    )
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class Q:
        def __init__(self, result=None, rows=None):
            self.result = result
            self.rows = rows or []

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return self.rows

    class DB:
        def query(self, model):
            from app.models.customer_reward import CustomerReward
            from app.models.redemption import Redemption
            from app.models.reward import Reward

            if model is Redemption:
                return Q(reserved_a, rows=[reserved_a])
            if model is CustomerReward:
                return Q(customer_reward)
            if model is Reward:
                return Q(shipping_reward)
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(max_discount_benefits_per_cart=1, discount_base="SUBTOTAL_INCL_TAX"),
    )
    monkeypatch.setattr(
        "app.services.redemption_service._reward_scope",
        lambda db, reward: (set(), set()),
    )
    applied = applied_benefits_for_cart(
        DB(),
        brand="batira",
        customer_id=customer_id,
        cart_ref="cart-x",
        exclude_customer_reward_id=reward_id,
    )
    result = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code=None,
        customer_reward_id=str(reward_id),
        cart={"subtotal": 50000, "shippingAmount": 1500, "cartRef": "cart-x"},
        applied_benefits=applied,
    )
    assert result["applicable"] is True
    assert result["benefits"][0]["type"] == "FREE_SHIPPING"
    assert result["benefits"][0]["scope"] == "SHIPPING"


def test_shipping_percent_fixed_and_zero_shipping():
    cart = {"subtotal": 20000, "shippingAmount": 5000}
    pct, reason = compute_benefit(
        benefit_type="SHIPPING_PERCENT_DISCOUNT",
        benefit_value=50,
        max_discount_amount=1000,
        min_cart_amount=None,
        applies_to="CART",
        free_product_quantity=1,
        cart=cart,
        product_ids=set(),
        category_ids=set(),
    )
    assert reason is None
    assert pct["discountAmount"] == 0
    assert pct["shippingDiscount"] == 1000
    assert pct["scope"] == "SHIPPING"
    assert "livraison" in pct["description"]

    fixed, _ = compute_benefit(
        benefit_type="SHIPPING_FIXED_DISCOUNT",
        benefit_value=8000,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="PRODUCTS",
        free_product_quantity=3,
        cart=cart,
        product_ids=set(),
        category_ids=set(),
    )
    assert fixed["shippingDiscount"] == 5000
    assert fixed["scope"] == "SHIPPING"

    methods, _ = compute_benefit(
        benefit_type="SHIPPING_PERCENT_DISCOUNT",
        benefit_value=10,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="CART",
        free_product_quantity=1,
        cart={"subtotal": 10000, "shippingAmount": 9999, "shippingMethods": [{"amount": 2000}, {"amount": 500}]},
        product_ids=set(),
        category_ids=set(),
    )
    assert methods["shippingDiscount"] == 250

    zero, zreason = compute_benefit(
        benefit_type="SHIPPING_PERCENT_DISCOUNT",
        benefit_value=20,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="CART",
        free_product_quantity=1,
        cart={"subtotal": 10000, "shippingAmount": 0},
        product_ids=set(),
        category_ids=set(),
    )
    assert zreason is None
    assert zero["shippingDiscount"] == 0


def test_describe_benefit_french_without_currency():
    text = describe_benefit(
        benefit_type="PERCENT_DISCOUNT",
        benefit_value=10,
        max_discount_amount=5000,
        min_cart_amount=20000,
    )
    assert text == "-10 % sur le panier, dès 20 000 de panier (plafonné à 5 000)"
    assert "€" not in text
    assert "XAF" not in text
    assert describe_benefit(benefit_type="FREE_SHIPPING") == "Livraison offerte"
    assert describe_benefit(benefit_type="SHIPPING_PERCENT_DISCOUNT", benefit_value=15) == "-15 % sur la livraison"
    assert describe_benefit(benefit_type="FREE_PRODUCT", free_product_quantity=2) == "2 produits offerts"


def test_free_product_prefers_storefront_refs():
    ok, _ = compute_benefit(
        benefit_type="FREE_PRODUCT",
        benefit_value=None,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="PRODUCTS",
        free_product_quantity=1,
        cart={"subtotal": 10000},
        product_ids={"engine-fallback"},
        category_ids=set(),
        product_refs=[{"productId": "woo-1", "name": "Mug", "quantity": 1}],
    )
    assert ok["freeItems"] == [{"productId": "woo-1", "name": "Mug", "quantity": 1}]
    assert ok["scope"] == "PRODUCT"


def test_shipping_not_stackable_against_free_shipping(monkeypatch):
    customer_id = uuid4()
    coupon = SimpleNamespace(
        id=uuid4(),
        customer_id=customer_id,
        code="LOY-SHIP-0001",
        shareable=True,
        expires_at=None,
        status="ISSUED",
        reserved_until=None,
        reserved_by_redemption_id=None,
        coupon_type_id=None,
        payload=None,
    )
    owner = SimpleNamespace(id=customer_id, brand="batira")
    customer = SimpleNamespace(id=customer_id, brand="batira")

    class Q:
        def __init__(self, result):
            self.result = result

        def join(self, *a, **k):
            return self

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return []

    class DB:
        def query(self, model):
            from app.models.customer import Customer
            from app.models.customer_coupon import CustomerCoupon

            if model is CustomerCoupon:
                return Q(coupon)
            if model is Customer:
                return Q(owner)
            return Q(None)

    monkeypatch.setattr(
        "app.services.redemption_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(
            max_discount_benefits_per_cart=1,
            max_shipping_benefits_per_cart=1,
            discount_base="SUBTOTAL_INCL_TAX",
        ),
    )
    monkeypatch.setattr(
        "app.services.redemption_service.coupon_reward_snapshots",
        lambda db, coupon: [
            {
                "benefitType": "SHIPPING_PERCENT_DISCOUNT",
                "benefitValue": 50,
                "appliesTo": "CART",
                "freeProductQuantity": 1,
                "stackable": False,
            }
        ],
    )
    blocked = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-SHIP-0001",
        customer_reward_id=None,
        cart={"subtotal": 50000, "shippingAmount": 4000},
        applied_benefits=[{"type": "FREE_SHIPPING", "shippingDiscount": 4000}],
    )
    assert blocked["applicable"] is False
    assert "NOT_STACKABLE" in blocked["reasons"]

    stacked = evaluate_coupon_or_reward(
        DB(),
        brand="batira",
        customer=customer,
        code="LOY-SHIP-0001",
        customer_reward_id=None,
        cart={"subtotal": 50000, "shippingAmount": 4000},
        applied_benefits=[{"type": "PERCENT_DISCOUNT", "discountAmount": 5000}],
    )
    assert stacked["applicable"] is True
    assert stacked["benefits"][0]["type"] == "SHIPPING_PERCENT_DISCOUNT"
    assert stacked["benefits"][0]["shippingDiscount"] == 2000


def test_present_wallet_reward_includes_scope_and_products():
    from app.services.redemption_service import present_wallet_reward

    reward_id = str(uuid4())
    presented = present_wallet_reward(
        {
            "id": reward_id,
            "customerRewardId": str(uuid4()),
            "name": "Mug offert",
            "benefitType": "FREE_PRODUCT",
            "freeProductQuantity": 1,
            "appliesTo": "PRODUCTS",
            "stackable": False,
        },
        {reward_id: [{"id": "woo-1", "name": "Mug", "quantity": 1}]},
    )
    assert presented["scope"] == "PRODUCT"
    assert presented["appliesTo"] == "PRODUCTS"
    assert presented["stackable"] is False
    assert presented["products"] == [{"id": "woo-1", "name": "Mug", "quantity": 1}]
    assert presented["description"] == "Produit offert"


def _release_db(*, row, coupon=None, nested=None, standalone=None):
    from app.models.customer_coupon import CustomerCoupon
    from app.models.customer_reward import CustomerReward
    from app.models.redemption import Redemption

    nested = nested or []

    class Chain:
        def __init__(self, result, many=None):
            self.result = result
            self.many = many if many is not None else ([result] if result is not None else [])

        def filter(self, *a, **k):
            return self

        def with_for_update(self):
            return self

        def first(self):
            return self.result

        def one(self):
            return self.result

        def all(self):
            return self.many

    class DB:
        def __init__(self):
            self.flushed = False

        def query(self, model):
            if model is Redemption:
                return Chain(row)
            if model is CustomerCoupon:
                return Chain(coupon)
            if model is CustomerReward:
                if standalone is not None:
                    return Chain(standalone, many=[standalone])
                return Chain(nested[0] if nested else None, many=nested)
            return Chain(None)

        def flush(self):
            self.flushed = True

    return DB()


def test_release_reserved_coupon_sets_issued_and_clears_reservation():
    from datetime import datetime, timedelta

    from app.services.redemption_service import release

    rid = uuid4()
    coupon_id = uuid4()
    until = datetime.utcnow() + timedelta(minutes=20)
    nested = SimpleNamespace(
        status="RESERVED",
        reserved_until=until,
        reserved_by_redemption_id=rid,
        used_at=None,
    )
    coupon = SimpleNamespace(
        id=coupon_id,
        status="RESERVED",
        expires_at=None,
        used_at=None,
        reserved_until=until,
        reserved_by_redemption_id=rid,
    )
    row = SimpleNamespace(
        id=rid,
        brand="batira",
        status="RESERVED",
        customer_coupon_id=coupon_id,
        customer_reward_id=None,
        order_ref=None,
        reserved_until=until,
        benefit=[],
    )
    db = _release_db(row=row, coupon=coupon, nested=[nested])
    result = release(db, redemption_id=str(rid), reason="cart_abandoned", brand="batira")
    assert result["status"] == "RELEASED"
    assert coupon.status == "ISSUED"
    assert coupon.reserved_until is None
    assert coupon.reserved_by_redemption_id is None
    assert nested.status == "ISSUED"
    assert nested.reserved_until is None
    assert nested.reserved_by_redemption_id is None


def test_release_committed_order_cancelled_reissues_coupon():
    from datetime import datetime

    from app.services.redemption_service import release

    rid = uuid4()
    coupon_id = uuid4()
    nested = SimpleNamespace(
        status="USED",
        used_at=datetime.utcnow(),
        reserved_until=None,
        reserved_by_redemption_id=None,
    )
    coupon = SimpleNamespace(
        id=coupon_id,
        status="USED",
        expires_at=None,
        used_at=datetime.utcnow(),
        reserved_until=None,
        reserved_by_redemption_id=None,
    )
    row = SimpleNamespace(
        id=rid,
        brand="batira",
        status="COMMITTED",
        customer_coupon_id=coupon_id,
        customer_reward_id=None,
        order_ref="wc_order_1",
        reserved_until=None,
        benefit={"benefits": []},
    )
    db = _release_db(row=row, coupon=coupon, nested=[nested])
    result = release(db, redemption_id=str(rid), reason="order_cancelled", brand="batira")
    assert result["status"] == "RELEASED"
    assert coupon.status == "ISSUED"
    assert coupon.used_at is None
    assert coupon.reserved_until is None
    assert coupon.reserved_by_redemption_id is None
    assert nested.status == "ISSUED"
    assert nested.used_at is None


def test_storefront_release_creates_json_safe_audit(monkeypatch):
    from datetime import datetime, timedelta

    from starlette.requests import Request

    from app.models.storefront_audit_event import StorefrontAuditEvent
    from app.routes.storefront import ReleaseIn, storefront_release

    until = datetime.utcnow() + timedelta(minutes=15)
    rid = uuid4()
    coupon = SimpleNamespace(status="RESERVED", reserved_until=until, reserved_by_redemption_id=rid)

    def fake_release(db, **kwargs):
        coupon.status = "ISSUED"
        coupon.reserved_until = None
        coupon.reserved_by_redemption_id = None
        return {
            "redemptionId": str(rid),
            "status": "RELEASED",
            "reservedUntil": until,
            "orderRef": None,
            "benefits": [],
            "applicable": False,
            "reasons": [],
        }

    monkeypatch.setattr("app.routes.storefront.release_redemption", fake_release)

    class DB:
        def __init__(self):
            self.added = None
            self.committed = False

        def add(self, obj):
            self.added = obj

        def flush(self):
            pass

        def commit(self):
            self.committed = True

    db = DB()
    request = Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 123),
            "server": ("test", 80),
        }
    )
    client = SimpleNamespace(id=uuid4(), brand="batira")
    result = storefront_release(str(rid), ReleaseIn(reason="cart_abandoned"), request, client, db)
    assert result["status"] == "RELEASED"
    assert coupon.status == "ISSUED"
    assert db.committed is True
    assert isinstance(db.added, StorefrontAuditEvent)
    assert db.added.action == "release"
    assert db.added.response["status"] == "RELEASED"
    assert db.added.response["redemptionId"] == str(rid)
    assert "reservedUntil" not in db.added.response


def test_storefront_commit_then_release_order_cancelled_audits(monkeypatch):
    from datetime import datetime

    from starlette.requests import Request

    from app.models.storefront_audit_event import StorefrontAuditEvent
    from app.routes.storefront import CommitIn, ReleaseIn, storefront_commit, storefront_release

    rid = str(uuid4())
    coupon = SimpleNamespace(
        status="RESERVED",
        used_at=None,
        reserved_until=datetime.utcnow(),
        reserved_by_redemption_id=rid,
    )

    def fake_commit(db, **kwargs):
        coupon.status = "USED"
        coupon.used_at = datetime.utcnow()
        coupon.reserved_until = None
        coupon.reserved_by_redemption_id = None
        return {
            "redemptionId": rid,
            "status": "COMMITTED",
            "reservedUntil": datetime.utcnow(),
            "orderRef": kwargs.get("order_ref"),
            "reasons": [],
            "applicable": True,
        }

    def fake_release(db, **kwargs):
        assert kwargs.get("reason") == "order_cancelled"
        coupon.status = "ISSUED"
        coupon.used_at = None
        return {
            "redemptionId": rid,
            "status": "RELEASED",
            "reservedUntil": datetime.utcnow(),
            "reasons": [],
            "applicable": False,
        }

    monkeypatch.setattr("app.routes.storefront.commit_redemption", fake_commit)
    monkeypatch.setattr("app.routes.storefront.release_redemption", fake_release)

    class DB:
        def __init__(self):
            self.events = []
            self.committed = 0

        def add(self, obj):
            self.events.append(obj)

        def flush(self):
            pass

        def commit(self):
            self.committed += 1

    db = DB()
    request = Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [(b"idempotency-key", b"commit-key-1")],
            "client": ("127.0.0.1", 123),
            "server": ("test", 80),
        }
    )
    client = SimpleNamespace(id=uuid4(), brand="batira")
    committed = storefront_commit(rid, CommitIn(orderRef="wc_order_9"), request, client, db)
    assert committed["status"] == "COMMITTED"
    released = storefront_release(rid, ReleaseIn(reason="order_cancelled"), request, client, db)
    assert released["status"] == "RELEASED"
    assert coupon.status == "ISSUED"
    assert db.committed == 2
    assert [e.action for e in db.events] == ["commit", "release"]
    assert all(isinstance(e, StorefrontAuditEvent) for e in db.events)
    assert all("reservedUntil" not in (e.response or {}) for e in db.events)


def test_audit_failure_does_not_block_release(monkeypatch):
    from datetime import datetime

    from starlette.requests import Request

    from app.routes.storefront import ReleaseIn, storefront_release

    coupon = SimpleNamespace(status="RESERVED", reserved_until=datetime.utcnow(), reserved_by_redemption_id="x")

    def fake_release(db, **kwargs):
        coupon.status = "ISSUED"
        coupon.reserved_until = None
        coupon.reserved_by_redemption_id = None
        return {"redemptionId": "x", "status": "RELEASED", "reservedUntil": datetime.utcnow(), "reasons": []}

    monkeypatch.setattr("app.routes.storefront.release_redemption", fake_release)

    class DB:
        def __init__(self):
            self.committed = False

        def add(self, obj):
            raise TypeError("datetime is not JSON serializable")

        def flush(self):
            raise TypeError("datetime is not JSON serializable")

        def commit(self):
            self.committed = True

    db = DB()
    request = Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 123),
            "server": ("test", 80),
        }
    )
    result = storefront_release(
        "x",
        ReleaseIn(reason="cart_abandoned"),
        request,
        SimpleNamespace(id=uuid4(), brand="batira"),
        db,
    )
    assert result["status"] == "RELEASED"
    assert coupon.status == "ISSUED"
    assert db.committed is True


def test_record_storefront_audit_encodes_datetime():
    from datetime import datetime

    from app.models.storefront_audit_event import StorefrontAuditEvent
    from app.services.storefront_audit import record_storefront_audit

    class DB:
        def __init__(self):
            self.added = None

        def add(self, obj):
            self.added = obj

        def flush(self):
            pass

    db = DB()
    record_storefront_audit(
        db,
        brand="batira",
        site_id="1",
        action="release",
        profile_id=None,
        redemption_id="abc",
        request={"reason": "cart_abandoned"},
        response={"status": "RELEASED", "reservedUntil": datetime(2026, 1, 1, 12, 0, 0)},
    )
    assert isinstance(db.added, StorefrontAuditEvent)
    assert isinstance(db.added.response["reservedUntil"], str)
