# data/ingest/load_postgres.py
from __future__ import annotations

import os
import pandas as pd
from sqlalchemy import create_engine, text

UPSERT_DATE_SQL = """
INSERT INTO dim_date (date_id, year, quarter, month)
VALUES (:date_id, :year, :quarter, :month)
ON CONFLICT (date_id) DO NOTHING;
"""

UPSERT_FACT_SQL = """
INSERT INTO fact_observation (series_id, date_id, value, release_date)
VALUES (:series_id, :date_id, :value, :release_date)
ON CONFLICT (series_id, date_id)
DO UPDATE SET
  value = EXCLUDED.value,
  release_date = EXCLUDED.release_date,
  fetched_at = NOW();
"""


def upsert_observations(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL missing. Put it in .env")

    # Build dim_date rows from df
    dates = pd.to_datetime(df["date_id"])
    dim_date_df = pd.DataFrame({
        "date_id": dates.dt.date,
        "year": dates.dt.year.astype(int),
        "quarter": (((dates.dt.month - 1) // 3) + 1).astype(int),
        "month": dates.dt.month.astype(int),
    }).drop_duplicates()

    engine = create_engine(db_url, pool_pre_ping=True)

    with engine.begin() as conn:
        # 1) upsert dim_date first (so FK passes)
        conn.execute(text(UPSERT_DATE_SQL), dim_date_df.to_dict(orient="records"))

        # 2) then upsert facts
        conn.execute(text(UPSERT_FACT_SQL), df.to_dict(orient="records"))

    return len(df)
