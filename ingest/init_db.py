import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise SystemExit("DATABASE_URL is missing. Add it to .env")

engine = create_engine(DB_URL)

schema_path = Path("sql/01_schema.sql")
sql = schema_path.read_text(encoding="utf-8")

with engine.begin() as conn:
    # Split on semicolons safely enough for simple schema files
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        conn.execute(text(stmt))
    print("✅ Schema applied successfully.")
