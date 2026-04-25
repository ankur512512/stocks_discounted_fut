# Script to download list of stocks having high difference with their Future prices:

- **nse_futures_discount.py**: To find discounted futures.
- **nse_futures_premium_top30.py**: To find top 30 stocks with very high future premiums.

Both scripts now write only an `.xlsx` output file. The final sheet also includes an `IS_NIFTY_50` column that marks whether each symbol is part of the NIFTY 50 index.

### Installation for Linux/WSL

Clone this repo and execute below commands from the root of the repo.
If you don't provide any date -- by default yesterday's date will be taken and will automatically backtrack up to 7 days if yesterday was a holiday/weekend.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python nse_futures_discount.py --date 2025-12-24
deactivate # Once done
```

### Installation for Windows

```bash
python3 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python nse_futures_discount.py --date 2025-12-24
deactivate # Once done
```

Follow the same for other script.
