import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def main():
    load_dotenv(encoding="utf-8")
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
            print("Normalizing duplicate priorities per brand...")
            conn.execute(
                text(
                    """
                    WITH ranked AS (
                        SELECT
                            id,
                            ROW_NUMBER() OVER (
                                PARTITION BY brand
                                ORDER BY priority ASC, created_at ASC, id ASC
                            ) - 1 AS new_priority
                        FROM rules
                    )
                    UPDATE rules r
                    SET priority = ranked.new_priority
                    FROM ranked
                    WHERE r.id = ranked.id
                    """
                )
            )

            print("Dropping old uniqueness constraint if it exists...")
            conn.execute(text("ALTER TABLE rules DROP CONSTRAINT IF EXISTS uq_rules_brand_priority;"))
            conn.execute(text("ALTER TABLE rules DROP CONSTRAINT IF EXISTS uq_rules_brand_tx_type_priority;"))

            print("Adding unique constraint on (brand, priority)...")
            conn.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM pg_constraint
                            WHERE conname = 'uq_rules_brand_priority'
                        ) THEN
                            ALTER TABLE rules
                            ADD CONSTRAINT uq_rules_brand_priority
                            UNIQUE (brand, priority);
                        END IF;
                    END $$;
                    """
                )
            )

            print("Done: rule priorities are normalized and unique per brand.")
    except Exception as e:
        print(f"Error updating rules priority uniqueness: {e}")


if __name__ == "__main__":
    main()
