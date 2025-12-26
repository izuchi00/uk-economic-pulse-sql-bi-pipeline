from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def run_file(path: str) -> None:
    """Run a .sql file against DATABASE_URL."""
    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url, pool_pre_ping=True)

    sql_path = Path(path)
    sql_text = sql_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text(sql_text))

    print(f"✅ Ran {path}")
