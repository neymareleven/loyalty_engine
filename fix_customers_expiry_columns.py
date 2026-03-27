import os
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
            print("Checking/Adding expiry and status columns to customers table...")
            
            # Missing columns identified from recent error logs and model comparison
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS status_points_reset_at TIMESTAMP;"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS points_expires_at TIMESTAMP;"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS loyalty_status_assigned_at TIMESTAMP;"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS loyalty_status_expires_at TIMESTAMP;"))
            
            print("Successfully checked/added columns (if they did not exist).")
            print("Successfully updated the customers table schema for longevity!")
    except Exception as e:
        print(f"Error updating schema: {e}")

if __name__ == "__main__":
    main()
