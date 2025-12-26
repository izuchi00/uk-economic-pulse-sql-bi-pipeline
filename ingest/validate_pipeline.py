# ingest/validate_pipeline.py
from __future__ import annotations

import os
from datetime import date, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    engine = create_engine(db_url, pool_pre_ping=True)

    series_id = os.getenv("VALIDATE_SERIES_ID", "IUMABEDR")

    with engine.begin() as conn:
        # Basic row count check
        row_count = conn.execute(
            text("SELECT COUNT(*) FROM fact_observation WHERE series_id = :sid"),
            {"sid": series_id},
        ).scalar_one()

        # Latest date check
        latest_date = conn.execute(
            text("SELECT MAX(date_id) FROM fact_observation WHERE series_id = :sid"),
            {"sid": series_id},
        ).scalar_one()

    print(f"🔎 Validation | series={series_id} | rows={row_count} | latest_date={latest_date}")

    if row_count == 0 or latest_date is None:
        raise SystemExit("❌ Validation failed: no data loaded.")

    # Optional freshness check (don’t be too strict for monthly series)
    # Fail only if older than ~120 days (you can tune it)
    if isinstance(latest_date, date):
        if latest_date < (date.today() - timedelta(days=120)):
            raise SystemExit(f"❌ Validation failed: data too old (latest_date={latest_date}).")

    print("✅ Validation passed.")


if __name__ == "__main__":
    main()
