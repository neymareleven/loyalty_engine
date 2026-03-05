from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel, Field


class TransactionTypeCreate(BaseModel):
    brand: Optional[str] = None
    key: Optional[str] = None
    origin: str

    name: str
    description: Optional[str] = None

    payload_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional. Describes the expected payload structure. "
            "For manual configuration, use a dictionary where each key is a payload field name and the value describes "
            "the field type and an optional description. "
            "UI guidance: treat payload_schema as a map of fieldName -> {type, description}. "
            "Recommended type values for a dropdown: string, number, boolean, array, object, integer. "
            "Note: for EXTERNAL events, the system may auto-populate this with an inferred JSON schema (type/object/properties/etc.)."
        ),
        examples=[
            {
                "votre_champ_perso_1": {"type": "string", "description": "Description du champ"},
                "votre_champ_perso_2": {"type": "number", "description": "Description du champ"},
                "votre_champ_perso_3": {"type": "boolean"},
            },
            {
                "votre_champ_perso_1": {"type": "string", "description": "Type choisi via une liste déroulante"},
                "votre_champ_perso_2": {"type": "array", "description": "Liste de valeurs"},
                "votre_champ_perso_3": {"type": "object", "description": "Objet imbriqué"},
            },
            {
                "type": "object",
                "properties": {
                    "votre_champ_perso_1": {"type": "string"},
                    "votre_champ_perso_2": {"type": "number"},
                    "votre_champ_perso_3": {"type": "boolean"},
                },
            },
        ],
    )

    active: bool = True


class TransactionTypeUpdate(BaseModel):
    brand: Optional[str] = None
    key: Optional[str] = None
    origin: Optional[str] = None

    name: Optional[str] = None
    description: Optional[str] = None

    payload_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional. Same format as on create. "
            "Can be a simple field dictionary for manual configuration or an inferred JSON schema for EXTERNAL events."
        ),
        examples=[
            {
                "votre_champ_perso_1": {"type": "string", "description": "Description du champ"},
                "votre_champ_perso_2": {"type": "number", "description": "Description du champ"},
                "votre_champ_perso_3": {"type": "boolean"},
            }
        ],
    )

    active: Optional[bool] = None


class TransactionTypeOut(BaseModel):
    id: UUID
    brand: Optional[str] = None
    key: str
    origin: str

    name: str
    description: Optional[str] = None

    payload_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional payload schema documentation. "
            "May be provided manually (field dictionary) or inferred automatically (JSON schema)."
        ),
    )

    active: bool

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
