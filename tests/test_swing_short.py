from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
import pandas as pd
from openpyxl import load_workbook
from swing_short.screener import export, indicators, prior_contract_row, scan


def sample_data():
    days = [date(2026, 6, 1) + timedelta(days=i) for i in range(85)]
    days = [d for d in days if d.weekday() < 5]
    # Consistent decline -> EMA bearish, contract data stay on same expiry.
    spot, fut = [], []
    for i, d in enumerate(days):
        p = 120 - i*.45
        spot.append(dict(symbol='TEST', date=d, open=p+1, high=p+2, low=p-2, close=p, volume=1000000 + i*10000))
        fut.append(dict(symbol='TEST', date=d, expiry=date(2026, 9, 30), fut_close=p-(.4+i*.03), oi=500000+i*3000, fut_volume=1000,))
    return pd.DataFrame(spot), pd.DataFrame(fut)


def test_indicators_and_no_future_leakage():
    s, f = sample_data()
    calculated = indicators(s)
    assert calculated.iloc[-1].ema20 > calculated.iloc[-1].close
    assert calculated.iloc[-1].support10 == min(s.iloc[-11:-1].low)
    assert pd.isna(calculated.iloc[0].support10)


def test_same_expiry_history():
    _, f = sample_data()
    prior = prior_contract_row(f, f.iloc[-1].date, 3)
    assert prior.date == f.iloc[-4].date


def test_report_needs_sufficient_history():
    s, f = sample_data()
    try:
        scan(s.head(10), f.head(10), s.iloc[9].date)
    except ValueError as exc:
        assert 'sessions' in str(exc)
    else:
        raise AssertionError('Expected insufficient history failure')


def test_screen_runs_with_history():
    s, f = sample_data()
    out = scan(s, f, s.iloc[-1].date)
    assert len(out) == 1
    assert out.iloc[0].SYMBOL == 'TEST'
    assert out.iloc[0].SPOT_BREAKDOWN_CLOSE_BELOW < s.iloc[-1].close + 5


def test_export_formats_all_worksheets():
    result = pd.DataFrame([{
        'SYMBOL': 'TEST', 'STATUS': 'WATCH_BREAKDOWN', 'SCORE': 75,
        'RAW_BASIS_PCT': -0.5, 'SPOT_BREAKDOWN_CLOSE_BELOW': 100,
        'SPOT_INVALIDATION_ABOVE': 105, 'SPOT_TARGET_R1': 95,
        'SPOT_TARGET_R2': 90,
    }])
    with TemporaryDirectory() as directory:
        workbook_path, text_path = export(result, Path(directory), date(2026, 6, 1))
        workbook = load_workbook(workbook_path)
        assert workbook.sheetnames == ['SHORTLIST', 'ALL_SCREENED', 'READ_ME']
        for sheet in workbook.worksheets:
            assert sheet.freeze_panes == 'A2'
            assert sheet.auto_filter.ref == sheet.dimensions
            assert sheet['A1'].font.bold
            assert sheet.column_dimensions['A'].width >= 14
        assert workbook['SHORTLIST']['A2'].value == 'TEST'
        assert 'TEST' in text_path.read_text(encoding='utf-8')
