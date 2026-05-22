"""Inspect the rules table schema (sync SQLAlchemy, same as the API)."""

import os
import sys

from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db import SessionLocal


def check_schema():
    with SessionLocal() as session:
        result = session.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'rules' ORDER BY ordinal_position;"
            )
        )
        for col in result.fetchall():
            print(f"- {col[0]}: {col[1]}")


if __name__ == "__main__":
    check_schema()
