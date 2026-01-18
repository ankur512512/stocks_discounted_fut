#!/usr/bin/env python3
"""
Download NSE UDiFF CM + FO bhavcopy (zip), then list TOP N stock futures trading
at PREMIUM to SPOT (FUT_CLOSE > SPOT_CLOSE), sorted by highest premium %.

Usage:
  python nse_futures_premium_top30.py --date 2025-12-24
  python nse_futures_premium_top30.py               # defaults to yesterday
  python nse_futures_premium_top30.py --top 30
"""

import argparse
import io
import sys
import time
import zipfile
from datetime import datetime, timedelta, date

import pandas as pd
import requests


BASE_CM = "https://nsearchives.nseindia.com/content/cm/"
BASE_FO = "https://nsearchives.nseindia.com/content/fo/"

_SESSION = requests.Session()
_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com/",
    "Accept": "application/zip,application/octet-stream,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def build_urls(d: date) -> tuple[str, str]:
    ds = yyyymmdd(d)
    cm_name = f"BhavCopy_NSE_CM_0_0_0_{ds}_F_0000.csv.zip"
    fo_name = f"BhavCopy_NSE_FO_0_0_0_{ds}_F_0000.csv.zip"
    return (BASE_CM + cm_name, BASE_FO + fo_name)


def download_zip(url: str, timeout: int = 45, retries: int = 4) -> bytes:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = _SESSION.get(url, headers=_HEADERS, timeout=(8, timeout))
            r.raise_for_status()
            return r.content
        except Exception as e:
            last_err = e
            time.sleep(0.8 * attempt)
    raise RuntimeError(f"Failed to download after {retries} attempts: {url} | Last error: {last_err}")


def unzip_single_csv(zip_bytes: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("Zip contains no CSV.")
        with zf.open(csv_names[0]) as f:
            return pd.read_csv(f)


def get_for_trading_day(target: date, max_backtrack_days: int = 7) -> tuple[date, pd.DataFrame, pd.DataFrame]:
    last_err = None
    for i in range(max_backtrack_days + 1):
        d = target - timedelta(days=i)
        cm_url, fo_url = build_urls(d)
        print(f"Trying date: {d.isoformat()}")
        try:
            print("Downloading CM...")
            cm_zip = download_zip(cm_url)
            print("Downloading FO...")
            fo_zip = download_zip(fo_url)
            cm_df = unzip_single_csv(cm_zip)
            fo_df = unzip_single_csv(fo_zip)
            return d, cm_df, fo_df
        except Exception as e:
            last_err = e
            print(f"  Failed for {d.isoformat()}: {e}")
    raise RuntimeError(f"Failed to download CM/FO bhavcopy within {max_backtrack_days} days. Last error: {last_err}")


def compute_futures_premium(cm_df: pd.DataFrame, fo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Spot equities: Sgmt='CM', FinInstrmTp='STK', SctySrs='EQ'
    Stock futures: Sgmt='FO', FinInstrmTp='STF'
    """
    all_df = pd.concat([cm_df, fo_df], ignore_index=True)

    # Validate minimum columns
    required_cols = {"Sgmt", "FinInstrmTp", "TckrSymb", "ClsPric"}
    missing = required_cols - set(all_df.columns)
    if missing:
        raise RuntimeError(f"Unexpected CSV format. Missing columns: {sorted(missing)}")

    if "SctySrs" not in all_df.columns:
        raise RuntimeError("Unexpected CM format: missing 'SctySrs' column.")

    # SPOT
    spot = all_df[
        (all_df["Sgmt"] == "CM") &
        (all_df["FinInstrmTp"] == "STK") &
        (all_df["SctySrs"] == "EQ")
    ].copy()

    spot = spot[["TckrSymb", "ClsPric"]].rename(columns={"TckrSymb": "SYMBOL", "ClsPric": "SPOT_CLOSE"})
    spot = spot.dropna(subset=["SYMBOL", "SPOT_CLOSE"]).drop_duplicates(subset=["SYMBOL"])

    # FUTURES
    fut_required = {"XpryDt", "OpnIntrst"}
    missing_fut = fut_required - set(all_df.columns)
    if missing_fut:
        raise RuntimeError(f"Unexpected FO format. Missing columns: {sorted(missing_fut)}")

    fut = all_df[
        (all_df["Sgmt"] == "FO") &
        (all_df["FinInstrmTp"] == "STF")
    ].copy()

    fut = fut[["TckrSymb", "XpryDt", "ClsPric", "OpnIntrst"]].rename(
        columns={"TckrSymb": "SYMBOL", "XpryDt": "EXPIRY", "ClsPric": "FUT_CLOSE", "OpnIntrst": "OPEN_INT"}
    )
    fut = fut.dropna(subset=["SYMBOL", "EXPIRY", "FUT_CLOSE"])

    # Merge
    m = fut.merge(spot, on="SYMBOL", how="left")
    m = m[m["SPOT_CLOSE"].notna()].copy()

    # Premium %
    m["PREMIUM_%"] = (m["FUT_CLOSE"] - m["SPOT_CLOSE"]) / m["SPOT_CLOSE"] * 100

    # Keep only premium > 0
    out = m[m["PREMIUM_%"] > 0].copy()

    # Highest premium first
    out = out.sort_values("PREMIUM_%", ascending=False)

    return out[["SYMBOL", "EXPIRY", "SPOT_CLOSE", "FUT_CLOSE", "PREMIUM_%", "OPEN_INT"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Target date in YYYY-MM-DD. Defaults to yesterday.")
    ap.add_argument("--outdir", default=".", help="Output directory. Default: current directory.")
    ap.add_argument("--backtrack", type=int, default=7, help="Max backtrack days for holidays/weekends. Default: 7.")
    ap.add_argument("--top", type=int, default=30, help="How many rows to keep. Default: 30.")
    args = ap.parse_args()

    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target = datetime.now().date() - timedelta(days=1)

    trade_day, cm_df, fo_df = get_for_trading_day(target, max_backtrack_days=args.backtrack)
    prem = compute_futures_premium(cm_df, fo_df).head(args.top)

    ds = yyyymmdd(trade_day)
    outdir = args.outdir.rstrip("/")

    xlsx_path = f"{outdir}/top_premium_{ds}.xlsx"
    csv_path = f"{outdir}/top_premium_{ds}.csv"

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
        prem.to_excel(w, index=False, sheet_name="TOP_PREMIUM")

    prem.to_csv(csv_path, index=False)

    print(f"\nTrading day used: {trade_day.isoformat()}")
    print(f"Rows (Top premium): {len(prem)}")
    print(f"Wrote: {xlsx_path}")
    print(f"Wrote: {csv_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
