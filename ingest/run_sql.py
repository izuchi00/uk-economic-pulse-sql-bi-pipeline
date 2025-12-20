import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(os.environ["DATABASE_URL"])

def run_file(path: str):
    sql = Path(path).read_text(encoding="utf-8")
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))
    print(f"✅ Ran {path}")

if __name__ == "__main__":
    run_file("sql/02_staging.sql")
