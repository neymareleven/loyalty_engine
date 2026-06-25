"""Customer upsert field parsing."""

from app.schemas.customer import CustomerUpsert
from app.services.customer_upsert_service import customer_identity_payload, parse_customer_upsert_payload


def test_parse_upsert_top_level_identity_fields():
    parsed = parse_customer_upsert_payload(
        CustomerUpsert(
            brand="batira",
            profileId="uuid-1",
            email="kevine@gmail.com",
            gender="F",
            birthdate="1990-06-15",
        )
    )
    assert parsed["brand"] == "batira"
    assert parsed["email"] == "kevine@gmail.com"
    assert parsed["gender"] == "F"
    assert parsed["birthdate"] == "1990-06-15"
    assert parsed["extra_properties"] == {}


def test_parse_upsert_from_properties_bag():
    parsed = parse_customer_upsert_payload(
        CustomerUpsert(
            brand="batira",
            profileId="uuid-2",
            properties={
                "email": "a@b.com",
                "gender": "M",
                "firstName": "Ada",
            },
        )
    )
    assert parsed["email"] == "a@b.com"
    assert parsed["gender"] == "M"
    assert parsed["extra_properties"] == {"firstName": "Ada"}


def test_parse_upsert_cf7_form_fields():
    parsed = parse_customer_upsert_payload(
        CustomerUpsert(
            profileId="uuid-3",
            properties={
                "your-brand": "batira",
                "email": "new@test.com",
                "firstName": "New",
                "_wpcf7": "123",
                "phone-cf7it-national": "687894563",
            },
        )
    )
    assert parsed["brand"] == "batira"
    assert parsed["email"] == "new@test.com"
    assert parsed["extra_properties"] == {"firstName": "New"}


def test_customer_identity_payload():
    parsed = parse_customer_upsert_payload(
        CustomerUpsert(
            brand="batira",
            profileId="x",
            email="z@loyalty.local",
            gender="F",
            birthdate="1990-06-15",
        )
    )
    identity = customer_identity_payload(parsed)
    assert identity == {
        "email": "z@loyalty.local",
        "gender": "F",
        "birthdate": "1990-06-15",
    }
