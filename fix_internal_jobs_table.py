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
            print("Adding name column to internal_jobs table...")
            # We add name as VARCHAR(200). Since it's nullable=False in the model, 
            # we should either provide a default or make it nullable for existing rows first.
            # Best to add it as nullable, populate it (e.g. from job_key), and then make it non-nullable.
            conn.execute(text("ALTER TABLE internal_jobs ADD COLUMN IF NOT EXISTS name VARCHAR(200);"))
            print("Successfully added name column.")

            print("Populating name column from job_key for existing rows...")
            conn.execute(text("UPDATE internal_jobs SET name = job_key WHERE name IS NULL;"))
            
            print("Making name column NOT NULL...")
            conn.execute(text("ALTER TABLE internal_jobs ALTER COLUMN name SET NOT NULL;"))

            print("Adding description column to internal_jobs table...")
            conn.execute(text("ALTER TABLE internal_jobs ADD COLUMN IF NOT EXISTS description VARCHAR(1000);"))
            print("Successfully added description column.")
            
            print("Successfully updated the internal_jobs table schema!")
    except Exception as e:
        print(f"Error updating schema: {e}")

if __name__ == "__main__":
    main()
