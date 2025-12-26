from __future__ import annotations

import io
import time
from typing import List, Optional

import pandas as pd
import requests

BOE_ENDPOINTS = [
    # These are “query endpoints”. Visiting directly in a browser (without params) can show an error.
    "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp",
    "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp",
]


def _download_text(session: requests.Session, url: str, params: dict) -> str:
    headers = {
        "User-Agent": "uk-economic-pulse/1.0 (+data-pipeline)",
        "Accept": "text/csv,text/plain,*/*",
        "Referer": "https://www.bankofengland.co.uk/",
        "Accept-Language": "en-GB,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = session.get(url, params=params, headers=headers, timeout=60, allow_redirects=True)
    r.raise_for_status()
    return r.text


def _is_html(text: str) -> bool:
    t = (text or "").lstrip().lower()
    # quick checks
    return (
        t.startswith("<!doctype")
        or t.startswith("<html")
        or "<html" in t[:2000]
        or "<head" in t[:2000]
    )


def _slice_to_csv_table(csv_text: str) -> str:
    """
    BoE sometimes returns:
      - a normal CSV with header DATE,SERIES,VALUE
      - CSV with a few leading lines (notes) before the header
      - CSV with inconsistent parsing if we start too early

    This function finds the first line that looks like the real table header,
    otherwise falls back to the first line that looks like 3 columns.
    """
    if not csv_text:
        return ""

    lines = [ln.strip("\ufeff").rstrip() for ln in csv_text.splitlines() if ln.strip()]
    if not lines:
        return ""

    # Preferred: find header line containing DATE and SERIES and VALUE
    for i, ln in enumerate(lines[:200]):  # search early section only
        u = ln.upper().replace(" ", "")
        if "DATE" in u and "SERIES" in u and "VALUE" in u and ("," in ln):
            return "\n".join(lines[i:])

    # Fallback: first line that looks like 3+ comma-separated fields
    for i, ln in enumerate(lines[:200]):
        if ln.count(",") >= 2:
            return "\n".join(lines[i:])

    # Worst case: return original
    return "\n".join(lines)


def _parse_boe_csv(csv_text: str) -> pd.DataFrame:
    """
    Returns df with columns:
      series_id, date_id, value, release_date
    """
    sliced = _slice_to_csv_table(csv_text)
    if not sliced:
        return pd.DataFrame(columns=["series_id", "date_id", "value", "release_date"])

    buf = io.StringIO(sliced)

    # Read flexibly
    df = pd.read_csv(
        buf,
        sep=",",
        engine="python",
        on_bad_lines="skip",
    )

    # Standardize column names
    df.columns = [str(c).strip().upper() for c in df.columns]

    # Identify columns
    if {"DATE", "SERIES", "VALUE"}.issubset(df.columns):
        date_col, series_col, value_col = "DATE", "SERIES", "VALUE"
    elif df.shape[1] >= 3:
        df = df.iloc[:, :3].copy()
        df.columns = ["DATE", "SERIES", "VALUE"]
        date_col, series_col, value_col = "DATE", "SERIES", "VALUE"
    else:
        raise ValueError(f"Unexpected BoE CSV shape after slicing: {df.shape}")

    # Clean
    df[series_col] = df[series_col].astype(str).str.strip()

    raw_dates = df[date_col].astype(str).str.strip()

    # BoE commonly uses: "31 Jan 1990"
    # We try a format first, then fall back to general parsing.
    dt = pd.to_datetime(raw_dates, format="%d %b %Y", errors="coerce")
    if dt.isna().all():
        dt = pd.to_datetime(raw_dates, dayfirst=True, errors="coerce")

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
    retries_per_endpoint: int = 2,
    sleep_between: float = 0.6,
) -> pd.DataFrame:
    """
    Fetch BoE series in ONE request (BoE supports multiple SeriesCodes).
    Returns dataframe: series_id, date_id, value, release_date.

    Raises RuntimeError if BoE returns HTML for all attempts/endpoints.
    Returns empty df if CSV is valid but contains no rows.
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

    last_err: Optional[Exception] = None

    with requests.Session() as session:
        for ep in BOE_ENDPOINTS:
            for attempt in range(retries_per_endpoint + 1):
                try:
                    if attempt > 0:
                        # simple backoff
                        time.sleep(sleep_between * (attempt + 1))
                    else:
                        time.sleep(sleep_between)

                    payload = _download_text(session, ep, params)

                    if _is_html(payload):
                        # include a small preview to help debug redirects/blocks
                        preview = (payload or "")[:200].replace("\n", " ")
                        raise RuntimeError(
                            f"BoE returned HTML (blocked/redirected/invalid request). Preview: {preview}"
                        )

                    df = _parse_boe_csv(payload)
                    return df

                except Exception as e:
                    last_err = e
                    continue

    raise RuntimeError(f"Failed to download BoE CSV from all endpoints. Last error: {last_err}")
