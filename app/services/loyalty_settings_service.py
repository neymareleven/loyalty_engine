from sqlalchemy.orm import Session

from app.models.brand_loyalty_settings import BrandLoyaltySettings


def get_loyalty_settings(db: Session, *, brand: str) -> BrandLoyaltySettings | None:
    return db.query(BrandLoyaltySettings).filter(BrandLoyaltySettings.brand == brand).first()


def get_or_create_loyalty_settings(db: Session, *, brand: str) -> BrandLoyaltySettings:
    obj = get_loyalty_settings(db, brand=brand)
    if obj:
        return obj
    obj = BrandLoyaltySettings(brand=brand)
    db.add(obj)
    db.flush()
    return obj
