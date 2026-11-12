from typing import Literal, Optional

from pydantic import BaseModel, Field


QualificationMode = Literal["POINTS", "PURCHASES"]


class LoyaltySettingsOut(BaseModel):
    brand: str
    qualification_mode: QualificationMode = "POINTS"
    points_validity_days: Optional[int] = None
    loyalty_status_validity_days: Optional[int] = None
    purchase_validity_days: Optional[int] = None
    first_purchase_bypass_enabled: bool = False
    first_purchase_bypass_min_amount: Optional[int] = None
    first_purchase_bypass_tier_key: Optional[str] = None
    mode_switch_policy: str = "IMMEDIATE"


class LoyaltySettingsUpdate(BaseModel):
    qualification_mode: Optional[QualificationMode] = Field(
        default=None,
        description="POINTS = paliers par points; PURCHASES = paliers par nombre d'achats + montant total.",
    )
    points_validity_days: Optional[int] = None
    loyalty_status_validity_days: Optional[int] = None
    purchase_validity_days: Optional[int] = Field(
        default=None,
        description="Durée de validité (jours) des lots d'achats/montants. null = pas d'expiration.",
    )
    first_purchase_bypass_enabled: Optional[bool] = Field(
        default=None,
        description="Si true, le tout premier achat d'un montant suffisant accède au palier configuré.",
    )
    first_purchase_bypass_min_amount: Optional[int] = Field(
        default=None,
        description="Montant minimum du premier achat pour déclencher le bypass (requis si activé).",
    )
    first_purchase_bypass_tier_key: Optional[str] = Field(
        default=None,
        description="Clé d'un palier existant (actif) attribué par le bypass (requis si activé).",
    )
    mode_switch_policy: Optional[Literal["IMMEDIATE", "HOLD_UNTIL_WINDOW"]] = Field(
        default=None,
        description="IMMEDIATE = palier mérité tout de suite (les garanties restent). HOLD_UNTIL_WINDOW = garder le palier actuel jusqu'à la fin de fenêtre.",
    )
    confirm_mode_switch: Optional[bool] = Field(
        default=None,
        description="Requis à true pour appliquer un changement de qualification_mode (après preview).",
    )
