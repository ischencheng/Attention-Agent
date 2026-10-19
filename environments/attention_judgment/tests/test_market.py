from datetime import datetime, timezone
import json
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from environments.attention_judgment import market


NY = ZoneInfo("America/New_York")


def instant(text):
    return datetime.fromisoformat(text).replace(tzinfo=NY)


def response(symbol="ABC", days=("2025-11-26", "2025-11-28"), opens=None, closes=None, events=None):
    n = len(days)
    return {"chart": {"error": None, "result": [{
        "meta": {"symbol": symbol, "exchangeTimezoneName": "America/New_York"},
        "timestamp": [int(instant(day + "T09:30:00").timestamp()) for day in days],
        "indicators": {
            "quote": [{"open": opens if opens is not None else [100] * n,
                       "close": closes if closes is not None else [110] * n}],
            "adjclose": [{"adjclose": [109] * n}],
        },
        "events": events or {},
    }]}}


def snapshot(symbol, bars, actions=None, as_of="2025-11-28T14:00:00-05:00"):
    return {"symbol": symbol, "as_of": as_of, "bars": bars,
            "corporate_actions": actions or []}


def bar(day, opening=100, close=110, **kwargs):
    return {"session": day, "open": opening, "close": close,
            "adjusted_close": 999, "split": 1, "dividend": 0, **kwargs}


class HorizonTests(unittest.TestCase):
    def test_contract_can_be_frozen_before_prices_and_is_not_shared_mutable_state(self):
        original = market.market_contract()
        frozen = json.loads(json.dumps(original))
        original["calendar"] = "other"
        self.assertEqual(frozen["calendar"], "XNYS")
        self.assertEqual(market.market_contract()["calendar"], "XNYS")
        self.assertIn("calendar_version", frozen)
        self.assertIn("without_overwrite", frozen["vintage_policy"])
        with self.assertRaises(TypeError):
            market.MARKET_CONTRACT["version"] = 99

    def test_open_must_be_strictly_later(self):
        before = market.horizon_dates(instant("2025-11-26T09:29:59"))
        at_open = market.horizon_dates(instant("2025-11-26T09:30:00"))
        self.assertEqual(before, {"entry": "2025-11-26", "day": "2025-11-26",
                                  "week": "2025-11-28", "month": "2025-11-28"})
        self.assertEqual(at_open["entry"], "2025-11-28")  # Thanksgiving skipped.

    def test_month_and_year_rollover(self):
        horizons = market.horizon_dates(instant("2025-12-31T16:01:00"))
        self.assertEqual(horizons, {"entry": "2026-01-02", "day": "2026-01-02",
                                    "week": "2026-01-02", "month": "2026-01-30"})

    def test_weekend_dst_and_naive_datetime(self):
        self.assertEqual(market.horizon_dates(datetime(2025, 3, 9, 15, tzinfo=timezone.utc))["entry"], "2025-03-10")
        with self.assertRaises(ValueError):
            market.horizon_dates(datetime(2025, 3, 10, 9))

    def test_completion_uses_regular_or_early_close_not_just_date(self):
        self.assertFalse(market.horizon_complete("2025-11-26", instant("2025-11-26T15:59:59")))
        self.assertTrue(market.horizon_complete("2025-11-26", instant("2025-11-26T16:00:00")))
        self.assertFalse(market.horizon_complete("2025-11-28", instant("2025-11-28T12:59:59")))
        self.assertTrue(market.horizon_complete("2025-11-28", instant("2025-11-28T13:00:00")))
        with self.assertRaises(ValueError):
            market.horizon_complete("2025-11-27", instant("2025-11-27T17:00:00"))


class FetchTests(unittest.TestCase):
    def test_early_close_filters_partial_bar_and_retains_raw(self):
        raw = response()
        with patch.object(market, "_request_chart", return_value=raw):
            partial = market.fetch_prices("abc", "2025-11-26", "2025-11-28", instant("2025-11-28T12:59:59"))
            complete = market.fetch_prices("ABC", "2025-11-26", "2025-11-28", instant("2025-11-28T13:00:00"))
        self.assertEqual([b["session"] for b in partial["bars"]], ["2025-11-26"])
        self.assertEqual([b["session"] for b in complete["bars"]], ["2025-11-26", "2025-11-28"])
        self.assertEqual(partial["raw_response"], raw)
        self.assertNotIn("2025-11-27", complete["missing_sessions"])
        self.assertEqual(complete["calendar"], "XNYS")

    def test_missing_and_invalid_bars_are_never_filled(self):
        raw = response(days=("2025-11-25", "2025-11-28"), closes=[None, 110])
        with patch.object(market, "_request_chart", return_value=raw):
            result = market.fetch_prices("ABC", "2025-11-25", "2025-11-28", instant("2025-11-28T14:00:00"))
        self.assertEqual(result["missing_sessions"], ["2025-11-25", "2025-11-26"])
        self.assertEqual(result["invalid_sessions"], ["2025-11-25"])
        self.assertEqual(len(result["bars"]), 1)

    def test_actions_survive_even_when_their_price_bar_is_missing(self):
        stamp = int(instant("2025-11-26T09:30:00").timestamp())
        events = {"splits": {str(stamp): {"date": stamp, "numerator": 2, "denominator": 1}},
                  "dividends": {str(stamp): {"date": stamp, "amount": 0.5}}}
        with patch.object(market, "_request_chart", return_value=response(days=("2025-11-28",), events=events)):
            result = market.fetch_prices("ABC", "2025-11-26", "2025-11-28", instant("2025-11-28T14:00:00"))
        self.assertEqual(len(result["corporate_actions"]), 2)
        self.assertEqual(result["missing_sessions"], ["2025-11-26"])

    def test_provider_errors_and_wrong_exchange_do_not_become_empty_success(self):
        with patch.object(market, "_request_chart", return_value={"chart": {"result": None, "error": {"code": "Not Found"}}}):
            with self.assertRaises(market.MarketDataError):
                market.fetch_prices("ABC", "2025-11-26", "2025-11-28", instant("2025-11-28T14:00:00"))
        raw = response()
        raw["chart"]["result"][0]["meta"]["exchangeTimezoneName"] = "Asia/Tokyo"
        with patch.object(market, "_request_chart", return_value=raw):
            with self.assertRaises(market.MarketDataError):
                market.fetch_prices("ABC", "2025-11-26", "2025-11-28", instant("2025-11-28T14:00:00"))


class PerformanceTests(unittest.TestCase):
    def setUp(self):
        self.asset = snapshot("ABC", [bar("2025-11-26", 100, 105), bar("2025-11-28", 106, 120)])
        self.spy = snapshot("SPY", [bar("2025-11-26", 200, 202), bar("2025-11-28", 203, 210)])

    def evaluate(self):
        return market.evaluate_horizon(self.asset, self.spy, "2025-11-26", "2025-11-28")

    def test_raw_open_to_close_against_spy_not_adjusted_close(self):
        result = self.evaluate()
        self.assertEqual(result["status"], "resolved")
        self.assertAlmostEqual(result["symbol_return"], 0.2)
        self.assertAlmostEqual(result["benchmark_return"], 0.05)
        self.assertAlmostEqual(result["excess_return"], 0.15)
        self.assertFalse(result["dividends_included"])
        self.assertEqual(result["max_close_drawdown"], 0)
        self.assertEqual(result["worst_close_return"], 0)

    def test_profitable_endpoint_does_not_hide_peak_drawdown_or_loss_from_entry(self):
        days = ["2025-11-24", "2025-11-25", "2025-11-26", "2025-11-28"]
        asset = snapshot("ABC", [bar(day, 100, close) for day, close in zip(days, [120, 90, 105, 130])])
        spy = snapshot("SPY", [bar(day, 200, close) for day, close in zip(days, [210, 200, 205, 220])])
        result = market.evaluate_horizon(asset, spy, days[0], days[-1])
        self.assertAlmostEqual(result["symbol_return"], 0.30)
        self.assertAlmostEqual(result["max_close_drawdown"], 0.25)  # 120 -> 90.
        self.assertAlmostEqual(result["worst_close_return"], -0.10)  # Entry 100 -> 90.
        self.assertAlmostEqual(result["benchmark_max_close_drawdown"], 1 - 200 / 210)
        self.assertEqual(result["benchmark_worst_close_return"], 0)
        self.assertEqual(result["drawdown_basis"], "entry_open_then_completed_daily_closes_only")

    def test_same_day_loss_uses_entry_open_and_not_intraday_low(self):
        day = "2025-11-28"
        asset = snapshot("ABC", [bar(day, 100, 80, low=1)])
        spy = snapshot("SPY", [bar(day, 200, 210)])
        result = market.evaluate_horizon(asset, spy, day, day)
        self.assertAlmostEqual(result["max_close_drawdown"], 0.20)
        self.assertAlmostEqual(result["worst_close_return"], -0.20)

    def test_later_price_revision_has_new_vintage_without_mutating_prior_result(self):
        self.asset["fetched_at"] = "2025-11-29T00:00:00+00:00"
        first = self.evaluate()
        stored = json.loads(json.dumps(first))
        self.asset["bars"][1]["close"] = 125
        self.asset["fetched_at"] = "2025-12-01T00:00:00+00:00"
        revised = self.evaluate()
        self.assertEqual(first, stored)
        self.assertNotEqual(first["symbol_return"], revised["symbol_return"])
        self.assertNotEqual(first["data_vintages"][0]["snapshot_sha256"], revised["data_vintages"][0]["snapshot_sha256"])
        self.assertEqual(first["data_vintages"][0]["fetched_at"], "2025-11-29T00:00:00+00:00")

    def test_no_partial_or_missing_horizon_settlement(self):
        self.asset["as_of"] = "2025-11-28T12:59:59-05:00"
        self.assertEqual(self.evaluate()["reason"], "horizon-not-complete")
        self.asset["as_of"] = "2025-11-28T14:00:00-05:00"
        self.asset["bars"].pop()
        self.assertEqual(self.evaluate()["reason"], "missing-bars")

    def test_split_and_dividend_in_either_series_require_adjustment(self):
        self.asset["bars"][1]["split"] = 4
        result = self.evaluate()
        self.assertEqual(result["reason"], "action-adjustment-required")
        self.assertNotIn("symbol_return", result)
        self.asset["bars"][1]["split"] = 1
        self.spy["corporate_actions"] = [{"session": "2025-11-26", "type": "dividend", "amount": 1.2}]
        self.assertEqual(self.evaluate()["affected_symbol"], "SPY")

    def test_invalid_prices_and_duplicate_bars_fail_closed(self):
        self.asset["bars"][0]["open"] = float("nan")
        self.assertEqual(self.evaluate()["reason"], "invalid-prices")
        self.asset["bars"][0]["open"] = 100
        self.asset["bars"].append(self.asset["bars"][0])
        self.assertEqual(self.evaluate()["reason"], "duplicate-bars")

    def test_non_session_is_not_rolled_to_convenient_date(self):
        with self.assertRaises(ValueError):
            market.evaluate_horizon(self.asset, self.spy, "2025-11-27", "2025-11-28")


if __name__ == "__main__":
    unittest.main()
