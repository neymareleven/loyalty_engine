import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import urllib.parse

# Load .env file with explicit UTF-8 encoding
load_dotenv(encoding='utf-8')

DATABASE_URL = os.getenv("DATABASE_URL")
# Ensure proper encoding by parsing and reconstructing the URL
if DATABASE_URL:
    try:
        # Parse the URL to ensure all components are properly encoded
        parsed = urllib.parse.urlparse(DATABASE_URL)
        # Reconstruct with proper encoding - this will handle any encoding issues
        DATABASE_URL = urllib.parse.urlunparse(parsed)
    except Exception:
        # If parsing fails, ensure it's a valid UTF-8 string by replacing invalid bytes
        DATABASE_URL = DATABASE_URL.encode('utf-8', errors='replace').decode('utf-8')

connect_args = None
if DATABASE_URL and DATABASE_URL.startswith("postgres"):
    connect_args = {"options": "-c timezone=utc"}

engine = create_engine(DATABASE_URL, connect_args=connect_args or {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
