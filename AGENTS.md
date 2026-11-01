# Project: NSE Futures Swing Short Screener

## Objective

Develop an automated NSE stock futures screener that identifies
potential short-selling opportunities for swing trading.

Trading horizon: 3-15 trading days.

The system must NOT execute trades automatically.

## Existing Repository

Two original scripts:
- nse_futures_discount.py
- nse_futures_premium_top30.py

Keep existing scripts working and avoid unnecessary changes.

## Swing Short Screener

Location: swing_short/

Current implementation:
- NSE equity and futures historical data retrieval
- Futures discount calculation
- Historical open interest comparison
- EMA 20 and EMA 50
- RSI 14
- ATR 14
- Breakdown detection
- Bearish scoring
- Excel and text reports

## Trading Strategy

Identify bearish candidates using:
1. Futures trading below spot after accounting for carry.
2. Widening futures discount.
3. Falling futures price with increasing open interest.
4. Spot price below EMA 20 and EMA 50.
5. RSI between 35 and 50.
6. Confirmed breakdown or failed support retest.
7. Adequate liquidity.

Use technical confirmation before considering a short.

## Outputs

Generate a daily shortlist containing:
- Stock symbol
- Futures contract expiry
- Bearish score
- Entry trigger
- Stop-loss
- Targets
- Reward-to-risk ratio
- Supporting technical indicators

## Development Phases

Phase 1:
Build and validate the daily screener.

Phase 2:
Historical backtesting and strategy optimization.

Phase 3:
Daily scheduling and Telegram notifications.

## Critical Requirements

- Never introduce look-ahead bias.
- Handle NSE holidays and missing data.
- Compare open interest on matching futures contracts.
- Account for futures expiry and rollovers.
- Avoid treating futures discount as automatically bearish.
- Include brokerage, slippage and settlement risks.
- Do not recommend trades solely from an unvalidated score.
- Write automated tests for new functionality.
- Never automatically execute orders.