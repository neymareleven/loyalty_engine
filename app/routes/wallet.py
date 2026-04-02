from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/wallet", tags=["wallet"])


@router.get("/{brand}/{profile_id}")
def read_wallet(
    brand: str,
    profile_id: str,
):
    raise HTTPException(
        status_code=410,
        detail="Wallet is deprecated. Use customer loyalty status/coupons endpoints.",
    )
