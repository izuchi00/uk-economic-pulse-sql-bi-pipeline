# data/ingest/load_postgres.py
from __future__ import annotations

import os
import pandas as pd
from dotenv import load_dotenv

from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import Date

load_dotenv()

def upsert_observations(df: pd.DataFrame) -> int:
    """
    Upsert into:
      - dim_date(date_id)
      - fact_observation(series_id, date_id, value, release_date, fetched_at)

    Expects df columns:
      series_id (str), date_id (date), value (float), release_date (date|None)
    """
    if df is None or df.empty:
        return 0

    required = {"series_id", "date_id", "value", "release_date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"upsert_observations() missing columns: {missing}")

    df = df.copy()
    df["series_id"] = df["series_id"].astype(str).str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # ✅ ensure Python date types (important for ARRAY(Date))
    df["date_id"] = pd.to_datetime(df["date_id"], errors="coerce").dt.date
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date
    df["release_date"] = df["release_date"].apply(lambda x: None if pd.isna(x) else x)

    df = df.dropna(subset=["series_id", "date_id", "value"])

    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        # ✅ Upsert dates into dim_date
        unique_dates = sorted({r["date_id"] for r in rows if r.get("date_id") is not None})
        if unique_dates:
            stmt_dates = text("""
                INSERT INTO dim_date (date_id)
                SELECT DISTINCT d::date
                FROM unnest(:dates) AS t(d)
                ON CONFLICT (date_id) DO NOTHING;
            """).bindparams(
                bindparam("dates", type_=ARRAY(Date))
            )

            conn.execute(stmt_dates, {"dates": unique_dates})

        # Upsert observations
        conn.execute(
            text("""
                INSERT INTO fact_observation (series_id, date_id, value, release_date)
                VALUES (:series_id, :date_id, :value, :release_date)
                ON CONFLICT (series_id, date_id)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    release_date = EXCLUDED.release_date,
                    fetched_at = NOW();
            """),
            rows,
        )

    return len(rows)
