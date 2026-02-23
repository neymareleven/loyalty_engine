from fastapi import Header, HTTPException, Query


def get_active_brand(
    brand_query: str | None = Query(default=None, alias="brand"),
    x_brand: str | None = Header(default=None, alias="X-Brand"),
) -> str:
    active = x_brand or brand_query
    if not active:
        raise HTTPException(
            status_code=400,
            detail="Missing brand context. Provide X-Brand header or brand query param.",
        )
    return active
