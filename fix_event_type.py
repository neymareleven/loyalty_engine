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

    # Connection arguments
    connect_args = {}
    if db_url.startswith("postgres"):
        connect_args = {"options": "-c timezone=utc"}

    print(f"Connecting to database...")
    engine = create_engine(db_url, connect_args=connect_args)

    try:
        with engine.begin() as conn:
            print("Dropping NOT NULL constraint from event_type...")
            conn.execute(text("ALTER TABLE rules ALTER COLUMN event_type DROP NOT NULL;"))
            print("Successfully dropped event_type constraint!")
            
    except Exception as e:
        print(f"Error dropping constraint: {e}")

if __name__ == "__main__":
    main()
