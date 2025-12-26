# data/ingest/fetch_boe.py
from __future__ import annotations

import io
import time
from typing import List, Tuple

import pandas as pd
import requests


BOE_ENDPOINTS = [
    "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp",
    "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp",
]


def _download_text(url: str, params: dict) -> str:
    headers = {
        "User-Agent": "uk-economic-pulse/1.0 (+powerbi; contact: you)",
        "Accept": "text/csv,text/plain,*/*",
        "Referer": "https://www.bankofengland.co.uk/",
    }
    r = requests.get(url, params=params, headers=headers, timeout=60, allow_redirects=True)
    r.raise_for_status()
    return r.text


def _is_html(text: str) -> bool:
    t = (text or "").lstrip().lower()
    return t.startswith("<!doctype") or t.startswith("<html") or "<html" in t[:2000]


def _parse_boe_csv(csv_text: str) -> pd.DataFrame:
    """
    Returns df with columns: series_id, date_id, value, release_date
    """
    # Some BoE responses include header row: DATE,SERIES,VALUE
    # Some have weird leading lines. We'll read flexibly.
    buf = io.StringIO(csv_text)

    df = pd.read_csv(
        buf,
        sep=",",
        engine="python",
        on_bad_lines="skip",
    )

    # Try to standardise columns
    cols = [c.strip().upper() for c in df.columns]
    df.columns = cols

    # If it came without headers, it may be 3 unnamed cols
    if set(["DATE", "SERIES", "VALUE"]).issubset(set(df.columns)):
        date_col, series_col, value_col = "DATE", "SERIES", "VALUE"
    elif df.shape[1] >= 3:
        # fallback: first 3 cols
        df = df.iloc[:, :3].copy()
        df.columns = ["DATE", "SERIES", "VALUE"]
        date_col, series_col, value_col = "DATE", "SERIES", "VALUE"
    else:
        raise ValueError(f"Unexpected BoE CSV shape: {df.shape}")

    # Clean + parse
    df[series_col] = df[series_col].astype(str).str.strip()
    raw_dates = df[date_col].astype(str).str.strip()

    # Handles both "31 Jan 1990" and "31/Jan/1990" etc.
    dt = pd.to_datetime(raw_dates, dayfirst=True, errors="coerce", utc=False)

    values = (
        df[value_col]
        .astype(str)
        .str.strip()
        .replace({"..": None, "n/a": None, "NA": None, "": None})
    )
    val = pd.to_numeric(values, errors="coerce")

    out = pd.DataFrame(
        {
            "series_id": df[series_col],
            "date_id": dt.dt.date,
            "value": val,
            "release_date": None,
        }
    )

    out = out.dropna(subset=["series_id", "date_id", "value"]).reset_index(drop=True)
    return out


def fetch_boe_series(
    series_codes: List[str],
    date_from: str = "01/Jan/1990",
    date_to: str = "now",
    sleep_between: float = 0.4,
) -> pd.DataFrame:
    """
    Fetch BoE series in ONE request (BoE supports multiple SeriesCodes).
    Returns dataframe: series_id, date_id, value, release_date.
    Raises if BoE returns HTML or nothing parseable.
    """
    params = {
        "csv.x": "yes",
        "Datefrom": date_from,
        "Dateto": date_to,
        "SeriesCodes": ",".join(series_codes),
        "UsingCodes": "Y",
        "CSVF": "CN",
        "VPD": "Y",
    }

    last_err: Exception | None = None
    text_payload: str | None = None

    for ep in BOE_ENDPOINTS:
        try:
            time.sleep(sleep_between)
            text_payload = _download_text(ep, params)
            if _is_html(text_payload):
                raise RuntimeError("BoE returned HTML (access denied / blocked / invalid code / redirected).")
            break
        except Exception as e:
            last_err = e
            text_payload = None

    if text_payload is None:
        raise RuntimeError(f"Failed to download BoE CSV. Last error: {last_err}")

    df = _parse_boe_csv(text_payload)
    return df
