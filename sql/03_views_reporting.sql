CREATE OR REPLACE VIEW vw_latest_value_per_series AS
SELECT
  s.series_id,
  s.series_name,
  s.source,
  s.unit,
  s.frequency,
  o.date_id AS latest_date,
  o.value  AS latest_value,
  o.fetched_at
FROM dim_series s
JOIN LATERAL (
  SELECT *
  FROM fact_observation fo
  WHERE fo.series_id = s.series_id
  ORDER BY fo.date_id DESC
  LIMIT 1
) o ON true;

CREATE OR REPLACE VIEW vw_change_vs_prev_period AS
WITH ranked AS (
  SELECT
    fo.series_id,
    fo.date_id,
    fo.value,
    ROW_NUMBER() OVER (PARTITION BY fo.series_id ORDER BY fo.date_id DESC) AS rn
  FROM fact_observation fo
)
SELECT
  cur.series_id,
  cur.date_id AS latest_date,
  cur.value   AS latest_value,
  prev.date_id AS prev_date,
  prev.value   AS prev_value,
  (cur.value - prev.value) AS change_abs,
  CASE
    WHEN prev.value IS NULL OR prev.value = 0 THEN NULL
    ELSE (cur.value - prev.value) / prev.value
  END AS change_pct
FROM ranked cur
LEFT JOIN ranked prev
  ON prev.series_id = cur.series_id
 AND prev.rn = 2
WHERE cur.rn = 1;

CREATE OR REPLACE VIEW vw_data_freshness AS
SELECT
  series_id,
  MAX(fetched_at) AS last_fetched_at,
  MAX(date_id) AS latest_observation_date
FROM fact_observation
GROUP BY series_id;
