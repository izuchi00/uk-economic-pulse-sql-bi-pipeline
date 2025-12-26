# data/ingest/load_postgres.py
from __future__ import annotations

import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

    # Ensure types
    df = df.copy()
    df["series_id"] = df["series_id"].astype(str).str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["series_id", "date_id", "value"])

    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

    # Insert/update rows
    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        # Upsert dates into dim_date
        unique_dates = sorted({r["date_id"] for r in rows if r.get("date_id") is not None})
        if unique_dates:
            conn.execute(
                text("""
                    INSERT INTO dim_date (date_id)
                    SELECT unnest(:dates::date[])
                    ON CONFLICT (date_id) DO NOTHING;
                """),
                {"dates": unique_dates},
            )

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
