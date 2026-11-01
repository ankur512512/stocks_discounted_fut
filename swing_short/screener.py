"""End-of-day NSE stock-futures short watchlist. Research tool, not an order system."""
from __future__ import annotations

import argparse
import io
import sys
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

CM_BASE = 'https://nsearchives.nseindia.com/content/cm/'
FO_BASE = 'https://nsearchives.nseindia.com/content/fo/'
HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.nseindia.com/', 'Accept': 'application/zip,*/*'}
SPOT_COLS = ['Sgmt','FinInstrmTp','SctySrs','TckrSymb','OpnPric','HghPric','LwPric','ClsPric','TtlTradgVol']
FUT_COLS = ['Sgmt','FinInstrmTp','TckrSymb','XpryDt','ClsPric','OpnIntrst','TtlTradgVol']


def read_zip(path_or_bytes):
    payload = Path(path_or_bytes).read_bytes() if isinstance(path_or_bytes, (str, Path)) else path_or_bytes
    with zipfile.ZipFile(io.BytesIO(payload)) as z:
        names = [n for n in z.namelist() if n.lower().endswith('.csv')]
        if not names:
            raise ValueError('No CSV in NSE zip')
        with z.open(names[0]) as f:
            return pd.read_csv(f, low_memory=False)


def fetch_day(kind: str, day: date, cache: Path, offline: bool, session: requests.Session) -> pd.DataFrame | None:
    stamp = day.strftime('%Y%m%d')
    filename = f'BhavCopy_NSE_{kind}_0_0_0_{stamp}_F_0000.csv.zip'
    folder = cache / kind.lower()
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / filename
    if path.exists():
        try:
            return read_zip(path)
        except (ValueError, zipfile.BadZipFile):
            if offline:
                raise
            path.unlink()
    if offline:
        return None
    base = CM_BASE if kind == 'CM' else FO_BASE
    for attempt in range(3):
        try:
            response = session.get(base + filename, headers=HEADERS, timeout=(8, 30))
            if response.status_code in (403, 404):
                if response.status_code == 403:
                    print(f'Access denied: {filename}', file=sys.stderr)
                return None
            response.raise_for_status()
            data = read_zip(response.content)
            path.write_bytes(response.content)
            return data
        except (requests.RequestException, zipfile.BadZipFile, ValueError) as exc:
            if attempt == 2:
                print(f'Unavailable {filename}: {exc}', file=sys.stderr)
                return None
            time.sleep(attempt + 1)
    return None


def normalize_day(day: date, cm: pd.DataFrame, fo: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    needed_cm, needed_fo = set(SPOT_COLS), set(FUT_COLS)
    if not needed_cm.issubset(cm.columns) or not needed_fo.issubset(fo.columns):
        raise ValueError(f'Unexpected UDiFF schema; CM missing {needed_cm - set(cm.columns)}, FO missing {needed_fo - set(fo.columns)}')
    s = cm.loc[(cm.Sgmt == 'CM') & (cm.FinInstrmTp == 'STK') & (cm.SctySrs == 'EQ'), SPOT_COLS[3:]].copy()
    s.columns = ['symbol', 'open', 'high', 'low', 'close', 'volume']
    f = fo.loc[(fo.Sgmt == 'FO') & (fo.FinInstrmTp == 'STF'), FUT_COLS[2:]].copy()
    f.columns = ['symbol', 'expiry', 'fut_close', 'oi', 'fut_volume']
    for col in ['open', 'high', 'low', 'close', 'volume']:
        s[col] = pd.to_numeric(s[col], errors='coerce')
    for col in ['fut_close', 'oi', 'fut_volume']:
        f[col] = pd.to_numeric(f[col], errors='coerce')
    f['expiry'] = pd.to_datetime(f.expiry, errors='coerce', dayfirst=False).dt.date
    s['symbol'] = s.symbol.astype(str).str.strip().str.upper()
    f['symbol'] = f.symbol.astype(str).str.strip().str.upper()
    s = s.dropna(subset=['close', 'high', 'low']).query('close > 0').drop_duplicates('symbol')
    f = f.dropna(subset=['expiry', 'fut_close', 'oi']).query('fut_close > 0 and oi >= 0')
    s['date'], f['date'] = day, day
    return s, f


def load_history(end: date, calendar_days: int, cache: Path, offline: bool) -> tuple[pd.DataFrame, pd.DataFrame, list[date]]:
    spots, futures, days = [], [], []
    with requests.Session() as session:
        for delta in range(calendar_days - 1, -1, -1):
            day = end - timedelta(days=delta)
            if day.weekday() >= 5:
                continue
            cm = fetch_day('CM', day, cache, offline, session)
            fo = fetch_day('FO', day, cache, offline, session)
            if cm is None or fo is None:
                continue
            s, f = normalize_day(day, cm, fo)
            if s.empty or f.empty:
                continue
            spots.append(s)
            futures.append(f)
            days.append(day)
            print(f'Loaded {day}: {len(s)} equities, {len(f)} futures')
    if not days:
        raise RuntimeError('No matched CM/FO trading sessions found; populate cache or verify NSE archive access')
    return pd.concat(spots, ignore_index=True), pd.concat(futures, ignore_index=True), days


def rsi(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0).ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    down = (-delta.clip(upper=0)).ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = up / down.replace(0, np.nan)
    out = 100 - 100 / (1 + rs)
    return out.mask((down == 0) & (up > 0), 100).mask((up == 0) & (down > 0), 0).mask((up == 0) & (down == 0), 50)


def atr(frame: pd.DataFrame, length: int = 14) -> pd.Series:
    previous = frame.close.shift(1)
    true_range = pd.concat([frame.high-frame.low, (frame.high-previous).abs(), (frame.low-previous).abs()], axis=1).max(axis=1)
    return true_range.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()


def indicators(spot: pd.DataFrame) -> pd.DataFrame:
    pieces = []
    for symbol, g in spot.groupby('symbol', sort=False):
        g = g.sort_values('date').copy()
        g['ema20'] = g.close.ewm(span=20, adjust=False, min_periods=20).mean()
        g['ema50'] = g.close.ewm(span=50, adjust=False, min_periods=50).mean()
        g['rsi14'] = rsi(g.close)
        g['atr14'] = atr(g)
        g['support10'] = g.low.shift(1).rolling(10, min_periods=10).min()
        g['resistance10'] = g.high.shift(1).rolling(10, min_periods=10).max()
        g['vol20'] = g.volume.shift(1).rolling(20, min_periods=20).mean()
        g['rsi_prev'] = g.rsi14.shift(1)
        g['turnover20_cr'] = (g.close*g.volume).shift(1).rolling(20, min_periods=20).mean() / 1e7
        pieces.append(g)
    return pd.concat(pieces, ignore_index=True)


def prior_contract_row(contract: pd.DataFrame, latest: date, lag: int) -> pd.Series | None:
    earlier = contract.loc[contract.date < latest].sort_values('date')
    if len(earlier) < lag:
        return None
    return earlier.iloc[-lag]


def scan(spots: pd.DataFrame, futures: pd.DataFrame, end: date, min_sessions: int = 55, min_dte: int = 7,
         min_turnover_cr: float = 10.0, min_fut_volume: int = 100) -> pd.DataFrame:
    trade_days = sorted(set(spots.date) & set(futures.date))
    if len(trade_days) < min_sessions:
        raise ValueError(f'Only {len(trade_days)} sessions; need >= {min_sessions} for EMA50 and screening')
    last_day = trade_days[-1]
    # Do not silently emit potentially stale trading candidates.
    if (end - last_day).days > 4:
        raise ValueError(f'Latest data {last_day} is stale relative to requested {end}')
    technical = indicators(spots)
    current_spot = technical.loc[technical.date == last_day].set_index('symbol')
    current_fut = futures.loc[futures.date == last_day].copy()
    current_fut['dte'] = current_fut.expiry.map(lambda x: (x - last_day).days)
    current_fut = current_fut.loc[current_fut.dte >= min_dte]
    # Nearest eligible monthly contract only, one row per stock.
    current_fut = current_fut.sort_values(['symbol', 'expiry']).drop_duplicates('symbol')
    results = []
    for row in current_fut.itertuples(index=False):
        if row.symbol not in current_spot.index:
            continue
        s = current_spot.loc[row.symbol]
        required = ['ema20','ema50','rsi14','rsi_prev','atr14','support10','resistance10','vol20','turnover20_cr']
        if any(not np.isfinite(float(s[col])) for col in required):
            continue
        contract = futures.loc[(futures.symbol == row.symbol) & (futures.expiry == row.expiry)].sort_values('date')
        p3 = prior_contract_row(contract, last_day, 3)
        p5 = prior_contract_row(contract, last_day, 5)
        if p3 is None or p5 is None or p3.oi <= 0:
            continue
        # Require uninterrupted observations on the same contract for valid OI/basis comparisons.
        if p3.date != trade_days[-4] or p5.date != trade_days[-6]:
            continue
        prevspot3 = technical.loc[(technical.symbol == row.symbol) & (technical.date == p3.date)]
        prevspot5 = technical.loc[(technical.symbol == row.symbol) & (technical.date == p5.date)]
        if prevspot3.empty or prevspot5.empty:
            continue
        basis = 100*(row.fut_close/s.close-1)
        prev_basis = 100*(p5.fut_close/prevspot5.iloc[0].close-1)
        oi_change = 100*(row.oi/p3.oi-1)
        fut_return = 100*(row.fut_close/p3.fut_close-1)
        price_return = 100*(s.close/prevspot3.iloc[0].close-1)
        has_discount = basis < -0.25
        widening = basis < prev_basis - 0.10
        short_buildup = oi_change > 5 and fut_return < -1
        ema_bear = s.close < s.ema20 < s.ema50
        rsi_bear = 35 <= s.rsi14 <= 50 and s.rsi14 < s.rsi_prev
        vol_confirm = s.volume > 1.2 * s.vol20
        rel_break = s.close < s.support10
        # Non-compensable eligibility gates: no technical-only 'short' suggestions.
        eligible_setup = has_discount and widening and short_buildup and ema_bear and rsi_bear
        liquid = s.turnover20_cr >= min_turnover_cr and row.fut_volume >= min_fut_volume and row.oi > 0
        points = (15*has_discount + 15*widening + 25*short_buildup + 20*ema_bear +
                  10*rsi_bear + 10*vol_confirm + 5*rel_break)
        entry = float(s.support10)  # Underlying trigger on daily close, NOT executable future price.
        stop = max(float(s.resistance10), entry + 1.5*float(s.atr14))
        risk = stop-entry
        if risk <= 0:
            continue
        # Targets based on underlying spot structure, not a filled futures trade.
        target1, target2 = entry-risk, entry-2*risk
        if target2 <= 0:
            continue
        if not liquid:
            status = 'REJECT_LIQUIDITY'
        elif not eligible_setup:
            status = 'NOT_ELIGIBLE'
        elif rel_break and vol_confirm:
            status = 'BREAKDOWN_CONFIRMED_EOD'
        else:
            status = 'WATCH_BREAKDOWN'
        results.append(dict(SYMBOL=row.symbol, STATUS=status, SCORE=points, TRADE_DATE=str(last_day),
                            EXPIRY=str(row.expiry), DAYS_TO_EXPIRY=row.dte, SPOT_CLOSE=round(s.close,2),
                            FUT_CLOSE=round(row.fut_close,2), RAW_BASIS_PCT=round(basis,3),
                            BASIS_5D_AGO_PCT=round(prev_basis,3), OI_CHANGE_3D_PCT=round(oi_change,2),
                            FUT_RETURN_3D_PCT=round(fut_return,2), SPOT_RETURN_3D_PCT=round(price_return,2),
                            EMA20=round(s.ema20,2), EMA50=round(s.ema50,2), RSI14=round(s.rsi14,1),
                            ATR14=round(s.atr14,2), SPOT_BREAKDOWN_CLOSE_BELOW=round(entry,2),
                            SPOT_INVALIDATION_ABOVE=round(stop,2), SPOT_TARGET_R1=round(target1,2),
                            SPOT_TARGET_R2=round(target2,2), AVG_TURNOVER_CR=round(s.turnover20_cr,2),
                            FUT_VOLUME=row.fut_volume, OPEN_INTEREST=row.oi,
                            HIGH_VOLUME=bool(vol_confirm), BASIS_WIDENING=bool(widening), SHORT_BUILDUP=bool(short_buildup),
                            COMMENT='Spot-only levels; convert to tradable futures levels before placing orders. '
                                    'Dividend/carry/corporate-action checks are not automated.'))
    output = pd.DataFrame(results)
    if output.empty:
        raise ValueError('No analyzable stock futures. Verify contracts, history, and NSE data completeness')
    rank = {'BREAKDOWN_CONFIRMED_EOD':0,'WATCH_BREAKDOWN':1,'NOT_ELIGIBLE':2,'REJECT_LIQUIDITY':3}
    output['_rank'] = output.STATUS.map(rank)
    output = output.sort_values(['_rank','SCORE','RAW_BASIS_PCT'], ascending=[True,False,True]).drop(columns='_rank')
    return output.reset_index(drop=True)


def export(result: pd.DataFrame, destination: Path, trading_day: date, limit: int = 10):
    destination.mkdir(parents=True, exist_ok=True)
    stamp = trading_day.strftime('%Y%m%d')
    path = destination/f'swing_short_{stamp}.xlsx'
    shortlist = result.loc[result.STATUS.isin(['BREAKDOWN_CONFIRMED_EOD','WATCH_BREAKDOWN'])].head(limit)
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        shortlist.to_excel(writer, sheet_name='SHORTLIST', index=False)
        result.to_excel(writer, sheet_name='ALL_SCREENED', index=False)
        notes = pd.DataFrame({'LIMITATION': [
            'Research shortlist only. Not verified trading advice or an automated order system.',
            'RAW_BASIS_PCT is NOT adjusted for dividend, cost of carry, or corporate actions.',
            'Triggers, stops and targets refer to SPOT; future order prices must be computed separately.',
            'EOD breakdown requires a closing price below the previous 10-day low and above-average volume.',
            'OI comparisons use the same symbol and exact contract expiry (not rollover comparisons).',
            'Verify announcements, earnings, physical settlement, liquidity, and tick size before trading.',
            'Scoring weights and thresholds are hypotheses, NOT backtested or validated.',
            'Only NSE UDiFF stock futures and spot EQ are analyzed; no NIFTY regime filter yet.',
        ]})
        notes.to_excel(writer, sheet_name='READ_ME', index=False)
        for sheet in writer.book:
            ws = writer.book[sheet]
            ws.freeze_panes = 'A2'
            ws.auto_filter.ref = ws.dimensions
            for cell in ws[1]:
                cell.font = cell.font.copy(bold=True)
            for column in ws.columns:
                col_letter = column[0].column_letter
                ws.column_dimensions[col_letter].width = min(58, max(14, max(len(str(c.value or '')) for c in column[:min(50, len(column))])+2))
    text = destination/f'swing_short_{stamp}.txt'
    lines = [f'NSE swing short research list | data {trading_day}', 'EOD candidates, NOT automatic trade instructions', '']
    if shortlist.empty:
        lines.append('No eligible setups today. Do not force a trade.')
    for r in shortlist.itertuples(index=False):
        lines.append(f'{r.SYMBOL} | {r.STATUS} | score {r.SCORE}/100 | basis {r.RAW_BASIS_PCT}% | '
                     f'spot close below {r.SPOT_BREAKDOWN_CLOSE_BELOW} | invalidation {r.SPOT_INVALIDATION_ABOVE} | '
                     f'targets {r.SPOT_TARGET_R1}, {r.SPOT_TARGET_R2}')
    lines.append('\nIMPORTANT: spot levels only; futures trade mapping + dividend/corporate-action checks pending.')
    text.write_text('\n'.join(lines), encoding='utf-8')
    return path, text


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', help='Latest requested trading date (YYYY-MM-DD); defaults yesterday')
    parser.add_argument('--days', type=int, default=125, help='Calendar-day lookback for at least 55 trading sessions')
    parser.add_argument('--cache', type=Path, default=Path('data/nse_cache'))
    parser.add_argument('--offline', action='store_true', help='Use only previously cached NSE ZIP files')
    parser.add_argument('--outdir', type=Path, default=Path('reports'))
    parser.add_argument('--min-turnover-cr', type=float, default=10)
    args = parser.parse_args(argv)
    if args.days < 85:
        parser.error('--days must be >= 85 calendar days to allow a 50-session EMA')
    end = date.fromisoformat(args.date) if args.date else date.today()-timedelta(days=1)
    spots, futures, trade_days = load_history(end, args.days, args.cache, args.offline)
    if len(trade_days) < 55:
        raise RuntimeError(f'Only {len(trade_days)} sessions loaded; at least 55 required; no report produced')
    result = scan(spots, futures, end, min_turnover_cr=args.min_turnover_cr)
    book, text = export(result, args.outdir, trade_days[-1])
    print(f'Sessions: {len(trade_days)} | last: {trade_days[-1]} | analyzed: {len(result)}')
    print(result.STATUS.value_counts().to_string())
    print(f'Written: {book}\nWritten: {text}')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, requests.RequestException) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
