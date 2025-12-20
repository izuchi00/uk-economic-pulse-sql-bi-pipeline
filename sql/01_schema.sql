CREATE TABLE IF NOT EXISTS dim_series (
  series_id TEXT PRIMARY KEY,
  series_name TEXT NOT NULL,
  source TEXT NOT NULL,
  unit TEXT,
  frequency TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_id DATE PRIMARY KEY,
  year INT,
  quarter INT,
  month INT
);

CREATE TABLE IF NOT EXISTS fact_observation (
  series_id TEXT REFERENCES dim_series(series_id),
  date_id DATE REFERENCES dim_date(date_id),
  value NUMERIC,
  release_date DATE,
  fetched_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (series_id, date_id)
);
