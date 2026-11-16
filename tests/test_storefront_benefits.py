from types import SimpleNamespace
from uuid import uuid4

from fastapi import HTTPException

from app.services.benefit_service import compute_benefit
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
