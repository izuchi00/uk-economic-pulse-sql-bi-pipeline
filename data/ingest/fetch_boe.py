from __future__ import annotations

import io
import requests
import pandas as pd


def _download_csv(url: str, params: dict) -> str:
    headers = {"User-Agent": "uk-economic-pulse/1.0"}
    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.text


def fetch_boe_series(
    series_codes: list[str],
    date_from: str = "01/Jan/1990",
    date_to: str = "now",
) -> pd.DataFrame:
    """
    Fetch BoE IADB CSV for one or more series codes.
    Returns dataframe with: series_id, date_id, value, release_date
    """
    endpoints = [
        "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp",
        "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp",
    ]

    params = {
        "csv.x": "yes",
        "Datefrom": date_from,
        "Dateto": date_to,
        "SeriesCodes": ",".join(series_codes),
        "UsingCodes": "Y",
        "CSVF": "CN",
        "VPD": "Y",
    }

    last_err = None
    csv_text = None

    for ep in endpoints:
        try:
            csv_text = _download_csv(ep, params)
            break
        except Exception as e:
            last_err = e

    if csv_text is None:
        raise RuntimeError(f"Failed to download BoE CSV. Last error: {last_err}")

    df = pd.read_csv(io.StringIO(csv_text), header=None)

    if df.shape[1] < 3:
        raise ValueError(f"Unexpected BoE CSV shape: {df.shape}. Head:\n{df.head()}")

    df = df.iloc[:, :3].copy()
    df.columns = ["date_raw", "series_id", "value_raw"]

    cleaned = (
        df["value_raw"]
        .astype(str)
        .str.strip()
        .replace({"..": None, "n/a": None, "NA": None})
    )
    df["value"] = pd.to_numeric(cleaned, errors="coerce")
    df["date_id"] = pd.to_datetime(df["date_raw"], format="%d/%b/%Y", errors="coerce").dt.date

    out = df.loc[df["date_id"].notna(), ["series_id", "date_id", "value"]].copy()
    out["release_date"] = None
    out = out.loc[out["value"].notna()].reset_index(drop=True)

    return out
