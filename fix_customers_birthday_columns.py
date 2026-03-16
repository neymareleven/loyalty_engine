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
            print("Checking/Adding birth_month, birth_day, birth_year columns to customers table...")
            
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS birth_month INTEGER;"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS birth_day INTEGER;"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS birth_year INTEGER;"))
            
            print("Successfully added columns (if they did not exist).")

            print("Populating birthday components from birthdate column for existing rows...")
            # Extract month, day, year from birthdate if it's set
            conn.execute(text("""
                UPDATE customers 
                SET 
                    birth_month = EXTRACT(MONTH FROM birthdate),
                    birth_day = EXTRACT(DAY FROM birthdate),
                    birth_year = EXTRACT(YEAR FROM birthdate)
                WHERE birthdate IS NOT NULL AND birth_month IS NULL;
            """))
            
            print("Successfully updated existing data!")
            print("Successfully updated the customers table schema!")
    except Exception as e:
        print(f"Error updating schema: {e}")

if __name__ == "__main__":
    main()
