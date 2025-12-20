# ingest/run_pipeline.py
from __future__ import annotations

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from ingest.run_sql import run_file
from data.ingest.fetch_boe import fetch_boe_series
from data.ingest.load_postgres import upsert_observations

load_dotenv()


def main():
    # 1) ensure dim_series exists / is updated
    run_file("sql/02_staging.sql")

    # 2) fetch BoE series and upsert
    SERIES = ["IUMABEDR", "IUMAMNPY", "XUMAGBD"]

    df = fetch_boe_series(
    ["IUMABEDR", "IUMBV34", "IUMBV37", "IUMBV42", "IUMBV45"],
    date_from="01/Jan/1990",
    date_to="now",
)

    n = upsert_observations(df)
    print(f"✅ Upserted {n} observations into fact_observation")

    # 3) create/update views
    run_file("sql/03_views_reporting.sql")

    # 4) sanity check: latest value
    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT series_id, latest_date, latest_value
                FROM vw_latest_value_per_series
                WHERE series_id = 'IUMABEDR'
            """)
        ).fetchone()
    print("Latest:", row)


if __name__ == "__main__":
    main()
