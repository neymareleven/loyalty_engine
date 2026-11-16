import json
from types import SimpleNamespace
from uuid import uuid4

from app.services.webhook_service import SIGNATURE_HEADER, enqueue_webhook_event, sign_payload


def test_webhook_signature_is_hmac_sha256():
    body = b'{"event":"coupon.issued"}'
    sig = sign_payload("super-secret", body)
    assert sig.startswith("sha256=")
    assert sig == sign_payload("super-secret", body)
    assert sig != sign_payload("other", body)


def test_enqueue_respects_event_filter():
    captured = []

    class Q:
        def filter(self, *a, **k):
            return self

        def all(self):
            return [
                SimpleNamespace(
                    id=uuid4(),
                    events=["coupon.issued"],
                    active=True,
                )
            ]

    class DB:
        def query(self, model):
            return Q()

        def add(self, row):
            captured.append(row)

        def flush(self):
            return None

    n = enqueue_webhook_event(
        DB(),
        brand="batira",
        event="coupon.used",
        payload={"couponId": "x"},
    )
    assert n == 0
    assert captured == []

    n = enqueue_webhook_event(
        DB(),
        brand="batira",
        event="coupon.issued",
        payload={"couponId": "x"},
    )
    assert n == 1
    assert captured[0].event == "coupon.issued"


def test_signed_body_header_name():
    assert SIGNATURE_HEADER == "X-Loyalty-Signature"
    body = json.dumps({"ok": True}, separators=(",", ":")).encode()
    assert len(sign_payload("k", body)) == len("sha256=") + 64


def test_storefront_openapi_is_versioned():
    from pathlib import Path

    path = Path("openapi/storefront.v1.json")
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["info"]["version"] == "1.0.0"
    assert "/v1/storefront/redemptions/validate" in data["paths"]
