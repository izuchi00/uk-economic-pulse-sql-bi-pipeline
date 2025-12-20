INSERT INTO dim_series (series_id, series_name, source, unit, frequency, notes)
VALUES
  ('CPIH', 'Consumer Prices Index incl. housing (CPIH)', 'ONS', 'Index', 'Monthly', 'UK CPIH headline'),
  ('UNEMP', 'Unemployment rate (UK)', 'ONS', 'Percent', 'Monthly', 'UK unemployment rate'),
  ('BANK_RATE', 'Bank of England Bank Rate', 'BoE', 'Percent', 'Ad-hoc', 'Official Bank Rate')
ON CONFLICT (series_id) DO UPDATE
SET series_name = EXCLUDED.series_name,
    source      = EXCLUDED.source,
    unit        = EXCLUDED.unit,
    frequency   = EXCLUDED.frequency,
    notes       = EXCLUDED.notes;

INSERT INTO dim_series (series_id, series_name, source, unit, frequency, notes)
VALUES
  ('BOE_BANK_RATE', 'Bank of England Bank Rate', 'BoE', 'Percent', 'Monthly', 'Official Bank Rate')
ON CONFLICT (series_id) DO UPDATE
SET series_name = EXCLUDED.series_name,
    source      = EXCLUDED.source,
    unit        = EXCLUDED.unit,
    frequency   = EXCLUDED.frequency,
    notes       = EXCLUDED.notes;

INSERT INTO dim_series (series_id, series_name, source, unit, frequency, notes)
VALUES
  ('IUMABEDR', 'Monthly average of official Bank Rate', 'BoE', 'Percent', 'Monthly', 'Official Bank Rate (monthly average)')
ON CONFLICT (series_id) DO UPDATE
SET series_name = EXCLUDED.series_name,
    source      = EXCLUDED.source,
    unit        = EXCLUDED.unit,
    frequency   = EXCLUDED.frequency,
    notes       = EXCLUDED.notes;
