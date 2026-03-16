import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


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
            print("Fixing customer_rewards schema (rule_id / rule_execution_id)...")

            # Add missing columns (idempotent)
            conn.execute(
                text("ALTER TABLE customer_rewards ADD COLUMN IF NOT EXISTS rule_id UUID;")
            )
            conn.execute(
                text(
                    "ALTER TABLE customer_rewards "
                    "ADD COLUMN IF NOT EXISTS rule_execution_id UUID;"
                )
            )

            # Add missing idempotency_key / payload if schema is behind
            conn.execute(
                text(
                    "ALTER TABLE customer_rewards "
                    "ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);"
                )
            )
            conn.execute(
                text("ALTER TABLE customer_rewards ADD COLUMN IF NOT EXISTS payload JSON;")
            )

            # Best-effort foreign keys (safe to skip if referenced tables don't exist yet)
            # PostgreSQL doesn't support "ADD CONSTRAINT IF NOT EXISTS", so we guard via pg_constraint.
            conn.execute(
                text(
                    """
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rules')
     AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'customer_rewards_rule_id_fkey') THEN
    ALTER TABLE customer_rewards
      ADD CONSTRAINT customer_rewards_rule_id_fkey
      FOREIGN KEY (rule_id) REFERENCES rules(id) ON DELETE SET NULL;
  END IF;
END $$;
"""
                )
            )

            conn.execute(
                text(
                    """
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'transaction_rule_execution')
     AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'customer_rewards_rule_execution_id_fkey') THEN
    ALTER TABLE customer_rewards
      ADD CONSTRAINT customer_rewards_rule_execution_id_fkey
      FOREIGN KEY (rule_execution_id) REFERENCES transaction_rule_execution(id) ON DELETE SET NULL;
  END IF;
END $$;
"""
                )
            )

            # Optional indexes to keep listing fast
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_customer_rewards_customer_id_issued_at "
                    "ON customer_rewards (customer_id, issued_at DESC);"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_customer_rewards_rule_id "
                    "ON customer_rewards (rule_id);"
                )
            )

            print("Successfully fixed customer_rewards schema.")

    except Exception as e:
        print(f"Error while updating customer_rewards table schema: {e}")


if __name__ == "__main__":
    main()

