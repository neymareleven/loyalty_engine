import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def main():
    print("This script is deprecated: coupon validity is now configured per CouponType (coupon_types.validity_days).")
    return

    # Load environment variables
    load_dotenv(encoding="utf-8")

    # Try local .env fallback when launched from another working directory
    if not os.getenv("DATABASE_URL"):
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not found in environment variables or .env file.")
        return

    connect_args = {}
    if db_url.startswith("postgres"):
        connect_args = {"options": "-c timezone=utc"}

    print("Connecting to database...")
    engine = create_engine(db_url, connect_args=connect_args)

    try:
        with engine.begin() as conn:
            print("Ensuring coupon_validity_days exists on brand_loyalty_settings...")
            conn.execute(
                text(
                    "ALTER TABLE brand_loyalty_settings "
                    "ADD COLUMN IF NOT EXISTS coupon_validity_days INTEGER;"
                )
            )
            print("Column check complete (added if missing).")
            print("Schema fix applied successfully.")
    except Exception as e:
        print(f"Error updating schema: {e}")


if __name__ == "__main__":
    main()
