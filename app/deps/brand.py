from fastapi import Header, HTTPException, Query


def normalize_brand(brand: str | None) -> str:
    return (brand or "").strip().lower()


def brands_match(a: str | None, b: str | None) -> bool:
    return normalize_brand(a) == normalize_brand(b)


def assert_brand_matches(*, path_or_query_brand: str, active_brand: str) -> None:
    if not brands_match(path_or_query_brand, active_brand):
        raise HTTPException(status_code=400, detail="brand does not match active brand context")


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
    return normalize_brand(active)
