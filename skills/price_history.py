#!/usr/bin/env python3
"""Fetch historical price data and compute moving average indicators.

Uses yfinance for multi-year historical data. Returns MA directions,
alignment (bullish/bearish/mixed), and deviation percentages.
No trading recommendations — data only.

Usage:
    python skills/price_history.py '{"ticker": "NVDA", "period": "5y"}'
    python skills/price_history.py '{"ticker": "AAPL"}'
"""
from __future__ import annotations

import json
import sys
from datetime import datetime

import yfinance as yf

MA_WINDOWS = [5, 20, 60, 120, 250]
DIRECTION_LOOKBACK = 5  # days to determine MA slope direction


def compute_ma_direction(series, window: int) -> dict | None:
    """Compute MA value and its recent direction."""
    if len(series) < window + DIRECTION_LOOKBACK:
        return None
    ma = series.rolling(window=window).mean()
    ma_clean = ma.dropna()
    if len(ma_clean) < DIRECTION_LOOKBACK:
        return None
    current = float(ma_clean.iloc[-1])
    prev = float(ma_clean.iloc[-DIRECTION_LOOKBACK])
    diff_pct = (current - prev) / prev * 100 if prev != 0 else 0
    if diff_pct > 0.3:
        direction = "up"
    elif diff_pct < -0.3:
        direction = "down"
    else:
        direction = "flat"
    return {"value": round(current, 2), "direction": direction}


def analyze(ticker: str, period: str = "5y") -> dict:
    tk = yf.Ticker(ticker)
    hist = tk.history(period=period)

    if hist.empty:
        return {"error": f"No data found for ticker '{ticker}'"}

    close = hist["Close"]
    current_price = round(float(close.iloc[-1]), 2)

    # Compute MAs
    ma_data = {}
    ma_values = {}
    for w in MA_WINDOWS:
        result = compute_ma_direction(close, w)
        key = f"MA{w}"
        if result:
            ma_data[key] = result
            ma_values[w] = result["value"]
        else:
            ma_data[key] = None

    # Alignment: check if short-term MAs are above long-term MAs
    available = [w for w in MA_WINDOWS if w in ma_values]
    if len(available) >= 3:
        is_bullish = all(
            ma_values[available[i]] >= ma_values[available[i + 1]]
            for i in range(len(available) - 1)
        )
        is_bearish = all(
            ma_values[available[i]] <= ma_values[available[i + 1]]
            for i in range(len(available) - 1)
        )
        if is_bullish:
            alignment = "bullish"
        elif is_bearish:
            alignment = "bearish"
        else:
            alignment = "mixed"
    else:
        alignment = "insufficient_data"

    # Deviation percentages
    dev_ma250 = None
    dev_ma20 = None
    if 250 in ma_values and ma_values[250] != 0:
        dev_ma250 = round((current_price - ma_values[250]) / ma_values[250] * 100, 1)
    if 20 in ma_values and ma_values[20] != 0:
        dev_ma20 = round((current_price - ma_values[20]) / ma_values[20] * 100, 1)

    # 52-week high/low
    one_year = close.tail(252) if len(close) >= 252 else close
    high_52w = round(float(one_year.max()), 2)
    low_52w = round(float(one_year.min()), 2)

    info = tk.info or {}

    return {
        "ticker": ticker,
        "current_price": current_price,
        "currency": info.get("currency", "USD"),
        "ma": ma_data,
        "alignment": alignment,
        "deviation_from_ma250_pct": dev_ma250,
        "deviation_from_ma20_pct": dev_ma20,
        "52w_high": high_52w,
        "52w_low": low_52w,
        "data_period": period,
        "data_points": len(close),
        "last_updated": str(close.index[-1].date()),
    }


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: price_history.py '{\"ticker\": \"NVDA\", \"period\": \"5y\"}'"}, indent=2))
        sys.exit(1)

    params = json.loads(sys.argv[1])
    ticker = params.get("ticker")
    period = params.get("period", "5y")

    if not ticker:
        print(json.dumps({"error": "ticker is required"}))
        sys.exit(1)

    result = analyze(ticker, period)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
