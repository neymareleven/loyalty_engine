from datetime import datetime
from typing import Optional

from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from app.services.benefit_service import BENEFIT_TYPES, SHIPPING_TYPES


def _normalize_benefit_type(value: str | None) -> str:
    return str(value or "CUSTOM").strip().upper()


def _validate_shipping_value(benefit_type: str, benefit_value: int | None) -> None:
    if benefit_type == "SHIPPING_PERCENT_DISCOUNT":
        if benefit_value is None:
            raise ValueError("benefit_value est obligatoire pour une remise pourcentage sur la livraison.")
        if int(benefit_value) < 1 or int(benefit_value) > 100:
            raise ValueError("Pour un pourcentage sur la livraison, benefit_value doit être entre 1 et 100.")
    if benefit_type == "SHIPPING_FIXED_DISCOUNT":
        if benefit_value is None:
            raise ValueError("benefit_value est obligatoire pour une remise fixe sur la livraison.")
        if int(benefit_value) <= 0:
            raise ValueError("Pour une remise fixe sur la livraison, benefit_value doit être supérieur à 0.")


class RewardProductItem(BaseModel):
    product_id: UUID
    quantity: int = 1


class RewardProductItemOut(BaseModel):
    product_id: UUID
    quantity: int
    name: Optional[str] = None
    match_key: Optional[str] = None
    points_value: Optional[int] = None


class CouponTypeLinkSummary(BaseModel):
    id: UUID
    name: str
    active: bool


class RewardCreate(BaseModel):
    brand: Optional[str] = None
    name: str
    description: Optional[str] = None
    active: bool = True
    products: Optional[list[RewardProductItem]] = None
    coupon_type_ids: list[UUID] = Field(
        ...,
        min_length=1,
        description="Types de coupon existants auxquels rattacher cette récompense.",
    )
    benefit_type: str = "CUSTOM"
    benefit_value: Optional[int] = None
    max_discount_amount: Optional[int] = None
    min_cart_amount: Optional[int] = None
    applies_to: Optional[str] = "CART"
    free_product_quantity: Optional[int] = 1
    stackable: bool = False

    @field_validator("benefit_type")
    @classmethod
    def _benefit_type_allowed(cls, value: str) -> str:
        normalized = _normalize_benefit_type(value)
        if normalized not in BENEFIT_TYPES:
            raise ValueError(
                "benefit_type invalide. Valeurs autorisées : "
                + ", ".join(sorted(BENEFIT_TYPES))
            )
        return normalized

    @model_validator(mode="after")
    def _shipping_rules(self):
        btype = _normalize_benefit_type(self.benefit_type)
        _validate_shipping_value(btype, self.benefit_value)
        if btype in SHIPPING_TYPES:
            self.applies_to = "CART"
            self.free_product_quantity = 1
        return self


class RewardCouponTypesReplace(BaseModel):
    """Replace all coupon-type links for a reward (bidirectional with PUT coupon-type rewards)."""

    coupon_type_ids: list[UUID] = Field(
        default_factory=list,
        description="Liste complète des types de coupon liés (vide = aucun lien).",
    )


class RewardUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    active: Optional[bool] = None
    products: Optional[list[RewardProductItem]] = None
    coupon_type_ids: Optional[list[UUID]] = Field(
        default=None,
        description="Si fourni, remplace tous les liens coupon ↔ reward pour cette récompense.",
    )
    benefit_type: Optional[str] = None
    benefit_value: Optional[int] = None
    max_discount_amount: Optional[int] = None
    min_cart_amount: Optional[int] = None
    applies_to: Optional[str] = None
    free_product_quantity: Optional[int] = None
    stackable: Optional[bool] = None

    @field_validator("benefit_type")
    @classmethod
    def _benefit_type_allowed(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = _normalize_benefit_type(value)
        if normalized not in BENEFIT_TYPES:
            raise ValueError(
                "benefit_type invalide. Valeurs autorisées : "
                + ", ".join(sorted(BENEFIT_TYPES))
            )
        return normalized

    @model_validator(mode="after")
    def _shipping_rules(self):
        if self.benefit_type:
            _validate_shipping_value(self.benefit_type, self.benefit_value)
            if self.benefit_type in SHIPPING_TYPES:
                self.applies_to = "CART"
                self.free_product_quantity = 1
        return self


class RewardOut(BaseModel):
    id: UUID
    brand: str
    name: str
    description: Optional[str] = None
    active: bool
    coupon_type_ids: list[UUID] = Field(default_factory=list)
    coupon_types: list[CouponTypeLinkSummary] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    products: list[RewardProductItemOut] = []
    benefit_type: Optional[str] = None
    benefit_value: Optional[int] = None
    max_discount_amount: Optional[int] = None
    min_cart_amount: Optional[int] = None
    applies_to: Optional[str] = None
    free_product_quantity: Optional[int] = None
    stackable: Optional[bool] = None

    class Config:
        from_attributes = True
