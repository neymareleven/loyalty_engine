import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def main():
    # Load environment variables
    load_dotenv(encoding="utf-8")

    if not os.getenv("DATABASE_URL"):
        # Fallback to local .env in this directory if needed
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not found in environment variables or .env file.")
        return

    # Connection arguments (aligned with existing scripts)
    connect_args = {}
    if db_url.startswith("postgres"):
        connect_args = {"options": "-c timezone=utc"}

    print("Connecting to database...")
    engine = create_engine(db_url, connect_args=connect_args)

    try:
        with engine.begin() as conn:
            print("Ensuring transactions.event_type is aligned with transaction_type...")

            # 1) Make sure event_type column exists (idempotent for most PostgreSQL setups)
            #    If the column already exists, this will raise; in that case you can comment this out.
            #    Left here for safety on older schemas.
            try:
                conn.execute(
                    text(
                        "ALTER TABLE transactions "
                        "ADD COLUMN IF NOT EXISTS event_type VARCHAR(50);"
                    )
                )
                print("Verified/created event_type column on transactions table.")
            except Exception as e:
                print(f"Warning: could not ADD COLUMN event_type (it likely already exists): {e}")

            # 2) Drop NOT NULL constraint temporarily to avoid insert failures
            try:
                conn.execute(
                    text(
                        "ALTER TABLE transactions "
                        "ALTER COLUMN event_type DROP NOT NULL;"
                    )
                )
                print("Dropped NOT NULL constraint on event_type (if it existed).")
            except Exception as e:
                print(f"Warning: could not DROP NOT NULL on event_type: {e}")

            # 3) Backfill existing NULL event_type values from transaction_type
            print("Backfilling event_type from transaction_type where event_type IS NULL...")
            conn.execute(
                text(
                    "UPDATE transactions "
                    "SET event_type = transaction_type "
                    "WHERE event_type IS NULL AND transaction_type IS NOT NULL;"
                )
            )

            # 4) (Optional) You can re‑enforce NOT NULL if your application
            #    now writes event_type explicitly. For now, we keep it nullable
            #    to avoid future failures from existing code that only sets transaction_type.
            #
            # Uncomment this block once the API / ORM is updated to always set event_type:
            #
            # print('Re‑applying NOT NULL constraint on event_type...')
            # conn.execute(
            #     text(
            #         'ALTER TABLE transactions '
            #         'ALTER COLUMN event_type SET NOT NULL;'
            #     )
            # )

            print("Successfully fixed transactions.event_type column.")

    except Exception as e:
        print(f"Error while updating transactions table schema: {e}")


if __name__ == "__main__":
    main()

