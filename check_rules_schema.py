import asyncio
import os
import sys

from sqlalchemy import text

# Add backend directory to sys.path so we can import app modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from app.db import async_session_maker

async def check_schema():
    async with async_session_maker() as session:
        result = await session.execute(
            text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'rules';")
        )
        columns = result.fetchall()
        for col in columns:
            print(f"- {col[0]}: {col[1]}")

if __name__ == "__main__":
    asyncio.run(check_schema())
