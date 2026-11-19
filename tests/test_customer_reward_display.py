from types import SimpleNamespace
from uuid import uuid4

from app.models.reward import Reward
from app.services.customer_entitlement_serialization import serialize_customer_reward_out


class _Q:
    def __init__(self, result=None):
        self.result = result

    def filter(self, *a, **k):
        return self

    def first(self):
        return self.result


class _DB:
    def __init__(self, catalog=None):
        self.catalog = catalog

    def query(self, model):
        if model is Reward:
            return _Q(self.catalog)
        return _Q(None)


def test_serialize_customer_reward_exposes_benefit_details():
    reward = SimpleNamespace(
        id=uuid4(),
        customer_id=uuid4(),
        reward_id=uuid4(),
        customer_coupon_id=None,
        status="ISSUED",
        issued_at=None,
        expires_at=None,
        used_at=None,
        source_transaction_id=None,
        rule_id=None,
        rule_execution_id=None,
        payload={
            "rewardSnapshot": {
                "id": "r1",
                "name": "-10 % Savon",
                "description": "Remise fidélité sur le savon",
            },
            "benefit": {
                "benefitType": "PERCENT_DISCOUNT",
                "benefitValue": 10,
                "maxDiscountAmount": 5000,
                "minCartAmount": 20000,
                "appliesTo": "PRODUCTS",
                "freeProductQuantity": 1,
                "stackable": False,
            },
            "productSnapshots": [
                {"id": "p1", "name": "Savon", "matchKey": "savon", "quantity": 1}
            ],
        },
    )
    data = serialize_customer_reward_out(_DB(), reward=reward)
    assert data["reward_name"] == "-10 % Savon"
    assert data["description"] == "Remise fidélité sur le savon"
    assert data["benefit_type"] == "PERCENT_DISCOUNT"
    assert data["benefit_type_label"] == "Remise en %"
    assert data["benefit_value"] == 10
    assert data["max_discount_amount"] == 5000
    assert data["min_cart_amount"] == 20000
    assert data["applies_to"] == "PRODUCTS"
    assert data["applies_to_label"] == "Produits spécifiques"
    assert data["scope"] == "PRODUCTS"
    assert data["stackable"] is False
    assert data["stackable_label"] == "Non cumulable"
    assert data["benefit_description"] == "-10 % sur le panier, dès 20 000 de panier (plafonné à 5 000)"
    assert data["usage_conditions"]["summary"] == data["benefit_description"]
    assert data["products"][0]["name"] == "Savon"


def test_serialize_customer_reward_falls_back_to_catalog():
    catalog_id = uuid4()
    catalog = SimpleNamespace(
        id=catalog_id,
        name="Livraison",
        description="Port offert dès un palier",
        benefit_type="FREE_SHIPPING",
        benefit_value=None,
        max_discount_amount=None,
        min_cart_amount=15000,
        applies_to="CART",
        free_product_quantity=1,
        stackable=True,
    )
    reward = SimpleNamespace(
        id=uuid4(),
        customer_id=uuid4(),
        reward_id=catalog_id,
        customer_coupon_id=None,
        status="ISSUED",
        issued_at=None,
        expires_at=None,
        used_at=None,
        source_transaction_id=None,
        rule_id=None,
        rule_execution_id=None,
        payload={"rewardSnapshot": {"name": "Livraison"}},
    )
    data = serialize_customer_reward_out(_DB(catalog), reward=reward)
    assert data["benefit_type"] == "FREE_SHIPPING"
    assert data["benefit_type_label"] == "Livraison offerte"
    assert data["description"] == "Port offert dès un palier"
    assert data["min_cart_amount"] == 15000
    assert data["stackable"] is True
    assert data["benefit_description"] == "Livraison offerte, dès 15 000 de panier"
