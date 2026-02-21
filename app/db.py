import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

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
