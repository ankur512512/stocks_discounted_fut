# Swing Short Screener — Phase 1 (research version)

This add-on deliberately **does not modify** the two existing NSE scanners. It builds an end-of-day, stock-futures bearish candidate list from historical NSE **CM + FO UDiFF** ZIP bhavcopies. There is **no trade execution**.

## Install and run (repository root)

```powershell
# create branch in YOUR existing clone
 git checkout -b feature/swing-short-screener
.\venv\Scripts\python.exe -m pip install -r requirements.txt
.\venv\Scripts\python.exe -m swing_short.screener --date 2026-10-30
```

Linux: `python -m swing_short.screener --date 2026-10-30`.

First run downloads up to 125 calendar days (weekdays only), which may take time and can be throttled or blocked by NSE. Files persist under `data/nse_cache/cm` and `data/nse_cache/fo`, and subsequent executions reuse existing ZIPs. Run with `--offline` to use only pre-cached ZIPs, or with `--cache PATH`. **Your existing discount Excel alone cannot produce a valid multi-day screen.** Output appears in `reports/swing_short_YYYYMMDD.xlsx` (SHORTLIST, ALL_SCREENED, READ_ME) and `.txt`.

## Signal conditions

Nearest futures expiry with >=7 days until expiry; daily stock history and exact-expiry historical OI are required. A watchlist setup requires all of: spot/futures raw discount < -0.25%, basis widening by >0.1 percentage points versus five contract sessions earlier, 3-session OI gain >5% accompanied by futures price drop >1%, spot below EMA20 below EMA50, RSI(14) 35–50 and falling. Liquidity filter: average historical spot turnover >= Rs 10 crore/day, futures day's contract volume >=100 units (as reported by NSE), positive OI. Confirmed end-of-day breakdown also requires spot closing below previous 10-session low and day volume >1.2 times preceding 20-session average.

Output *spot reference levels*: breakdown = previous 10-session low; invalidation = greater of previous 10-session high or breakdown + 1.5 ATR14; two illustrative targets at 1R and 2R. **These are not executable futures prices.** All signal weights and thresholds are hypotheses, not validated trading rules.

## Missing before live use

- **Dividend/carry adjustment and corporate-action/earnings calendar**: discounts can reflect dividends; adjustments are currently not implemented. Treat ALL findings as preliminary research.
- Separate tradable futures entry/SL/targets, tick rounding, bid-ask spreads, contract lots, margin and physical settlement handling.
- Index/sector relative strength, ADX, complete data-health checks, data vendor fallback, and a proper backtest.
- Daily automated schedule / Telegram notifications (deployment phase).

For now, **paper trade only**. If exchange data cannot be retrieved or history is insufficient, the program fails closed without fabricating picks. No individual stocks are guaranteed to qualify.
