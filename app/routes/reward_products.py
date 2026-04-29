from fastapi import APIRouter
from sqlalchemy.exc import IntegrityError


router = APIRouter(prefix="/admin/rewards", tags=["admin-reward-products"])


def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None



