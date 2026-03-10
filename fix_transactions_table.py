import os
import sys
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Try to find a .env file searching upwards from the script location
current_path = os.path.dirname(os.path.abspath(__file__))
while current_path != os.path.dirname(current_path):
    env_path = os.path.join(current_path, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path, encoding='utf-8')
        break
    current_path = os.path.dirname(current_path)

# Fallback to current directory .env
load_dotenv(encoding='utf-8')

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("Error: DATABASE_URL not found in environment variables or .env file.")
    sys.exit(1)

try:
    parsed = urllib.parse.urlparse(DATABASE_URL)
    DATABASE_URL = urllib.parse.urlunparse(parsed)
except Exception:
    DATABASE_URL = DATABASE_URL.encode('utf-8', errors='replace').decode('utf-8')

connect_args = None
if DATABASE_URL and DATABASE_URL.startswith("postgres"):
    connect_args = {"options": "-c timezone=utc"}

engine = create_engine(DATABASE_URL, connect_args=connect_args or {})

def main():
    try:
        with engine.begin() as conn:
            print("Checking if 'transaction_type' column exists in 'transactions' table...")
            
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='transactions' and column_name='transaction_type';
            """)).fetchone()
            
            if not result:
                print("Column 'transaction_type' not found. Adding it...")
                conn.execute(text("ALTER TABLE transactions ADD COLUMN transaction_type VARCHAR(50);"))
                conn.execute(text("UPDATE transactions SET transaction_type = 'UNKNOWN' WHERE transaction_type IS NULL;"))
                conn.execute(text("ALTER TABLE transactions ALTER COLUMN transaction_type SET NOT NULL;"))
                print("Successfully added 'transaction_type' column.")
            else:
                print("Column 'transaction_type' already exists.")
                
    except Exception as e:
        print(f"Error while fixing database: {e}")

if __name__ == "__main__":
    main()
