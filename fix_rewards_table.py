import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv(encoding='utf-8')
    
    # Try different paths for .env if needed
    if not os.getenv("DATABASE_URL"):
        load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), encoding='utf-8')
        
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not found in environment variables or .env file.")
        return

    # Connection arguments (similar to db.py)
    connect_args = {}
    if db_url.startswith("postgres"):
        connect_args = {"options": "-c timezone=utc"}

    print(f"Connecting to database...")
    engine = create_engine(db_url, connect_args=connect_args)

    try:
        with engine.begin() as conn:
            print("Adding max_attributions column to rewards table...")
            conn.execute(text("ALTER TABLE rewards ADD COLUMN IF NOT EXISTS max_attributions INTEGER;"))
            print("Successfully added max_attributions.")

            print("Adding reset_period column to rewards table...")
            conn.execute(text("ALTER TABLE rewards ADD COLUMN IF NOT EXISTS reset_period VARCHAR(20);"))
            print("Successfully added reset_period.")

            print("Adding reward_category_id column to rewards table...")
            conn.execute(text("ALTER TABLE rewards ADD COLUMN IF NOT EXISTS reward_category_id UUID;"))
            print("Successfully added reward_category_id.")

            print("Adding index on rewards.reward_category_id (if missing)...")
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_rewards_reward_category_id "
                    "ON rewards (reward_category_id);"
                )
            )
            print("Successfully added/verified index ix_rewards_reward_category_id.")

            print("Adding FK rewards.reward_category_id -> reward_categories.id (if missing)...")
            conn.execute(
                text(
                    "DO $$ "
                    "BEGIN "
                    "IF NOT EXISTS ("
                    "    SELECT 1 FROM pg_constraint "
                    "    WHERE conname = 'fk_rewards_reward_category_id'"
                    ") THEN "
                    "    ALTER TABLE rewards "
                    "    ADD CONSTRAINT fk_rewards_reward_category_id "
                    "    FOREIGN KEY (reward_category_id) "
                    "    REFERENCES reward_categories(id) "
                    "    ON DELETE RESTRICT; "
                    "END IF; "
                    "END $$;"
                )
            )
            print("Successfully added/verified FK fk_rewards_reward_category_id.")
            
            print("Successfully updated the rewards table schema!")
    except Exception as e:
        print(f"Error updating schema: {e}")

if __name__ == "__main__":
    main()
