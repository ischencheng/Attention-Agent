"""Paper-account invariants using invented prices, never market performance."""
from __future__ import annotations

import copy
import json
import unittest

from environments.research_portfolio.market import Market


START = "2025-01-02T21:00:00+00:00"
OPEN1 = "2025-01-03T14:30:00+00:00"
CLOSE1 = "2025-01-03T21:00:00+00:00"
OPEN2 = "2025-01-06T14:30:00+00:00"
END = "2025-01-06T21:00:00+00:00"


def manifest(*, fee=0, slippage=0, max_weight=1):
    return {
        "timeline": [START, CLOSE1, END], "price_basis": "raw",
        "portfolio": {"initial_cash": 1000, "fee_bps": fee,
                      "slippage_bps": slippage, "max_weight": max_weight},
        "assets": [{"asset_id": key, "name": f"Fictional {key}", "symbol": key,
                    "available_at": START, "terms": ["datacenter"]} for key in ("A", "B")],
        "bars": [{"asset_id": key, "open_at": opened, "close_at": closed,
                  "open": opening, "close": closing}
                 for key, first_open, first_close, second_open, second_close in [
                     ("A", 10, 12, 15, 16), ("B", 20, 20, 10, 10)]
                 for opened, closed, opening, closing in [
                     (OPEN1, CLOSE1, first_open, first_close), (OPEN2, END, second_open, second_close)]],
    }


class MarketTests(unittest.TestCase):
    def test_cash_account_and_marked_unrealized_return(self):
        market = Market(manifest())
        self.assertEqual(market.snapshot()["nav"], 1000)
        order = market.submit({"A": 0.5}, START, ["thesis-v1"])
        self.assertEqual(order["execute_at"], OPEN1)
        self.assertEqual(market.snapshot()["positions"], {})
        first = market.advance(CLOSE1)
        self.assertEqual(first["cash"], 500)
        self.assertEqual(first["positions"]["A"]["quantity"], 50)
        self.assertEqual(first["nav"], 1100)
        final = market.advance(END)
        self.assertEqual(final["positions"]["A"]["quantity"], 50)
        self.assertEqual(final["nav"], 1300)
        self.assertAlmostEqual(final["net_return"], 0.3)
        self.assertEqual(len([event for event in final["ledger"] if event["type"] == "fill"]), 1)

    def test_fees_and_slippage_are_charged_once(self):
        market = Market(manifest(fee=100, slippage=100))
        market.submit({"A": 1}, START, ["thesis-v1"])
        snapshot = market.advance(CLOSE1)
        quantity = 1000 / (10 * 1.01 * 1.01)
        self.assertAlmostEqual(snapshot["positions"]["A"]["quantity"], quantity)
        self.assertAlmostEqual(snapshot["cash"], 0)
        self.assertAlmostEqual(snapshot["nav"], quantity * 12)
        fill = next(event for event in snapshot["ledger"] if event["type"] == "fill")
        self.assertAlmostEqual(fill["execution_price"], 10.1)
        self.assertAlmostEqual(fill["fee"], quantity * 10.1 * 0.01)
        self.assertEqual(fill["decision_refs"], ["thesis-v1"])
        market.submit({}, CLOSE1, ["thesis-v2"])
        final = market.advance(END)
        expected = quantity * 15 * 0.99 * 0.99
        self.assertEqual(final["positions"], {})
        self.assertAlmostEqual(final["cash"], expected)
        self.assertAlmostEqual(final["net_return"], expected / 1000 - 1)

    def test_whole_allocation_sells_before_buying(self):
        market = Market(manifest())
        market.submit({"A": 1}, START, ["first"])
        market.advance(CLOSE1)
        market.submit({"B": 1}, CLOSE1, ["revised"])
        final = market.advance(END)
        self.assertEqual(final["cash"], 0)
        self.assertEqual(final["positions"], {"B": {"quantity": 150, "mark_price": 10, "market_value": 1500}})
        last_fills = [event for event in final["ledger"] if event["type"] == "fill"][-2:]
        self.assertEqual([event["side"] for event in last_fills], ["sell", "buy"])
        self.assertEqual([event["at"] for event in last_fills], [OPEN2, OPEN2])
        self.assertTrue(all(event["cash_after"] >= 0 for event in last_fills))

    def test_price_history_and_future_events_are_hidden(self):
        data = manifest()
        data["corporate_actions"] = [{"type": "dividend", "asset_id": "A", "ex_at": OPEN2,
                                      "pay_at": END, "amount_per_share": 1}]
        data["bars"][1]["close"] = 987654321
        market = Market(data)
        self.assertEqual(market.snapshot()["bars"], [])
        market.submit({"A": 0.5}, START, ["thesis"])
        during = market.advance(OPEN1)
        self.assertEqual(during["bars"], [])
        self.assertEqual(during["positions"]["A"]["mark_price"], 10)
        serialized = json.dumps(during)
        self.assertNotIn("987654321", serialized)
        self.assertNotIn("dividend", serialized)
        self.assertEqual(len(market.advance(CLOSE1)["bars"]), 2)

    def test_at_open_decision_cannot_fill_at_the_same_open(self):
        market = Market(manifest())
        market.advance(OPEN1)
        market.submit({"A": 1}, OPEN1, ["thesis"])
        self.assertEqual(market.advance(CLOSE1)["positions"], {})
        final = market.advance(END)
        fill = next(event for event in final["ledger"] if event["type"] == "fill")
        self.assertEqual(fill["at"], OPEN2)
        self.assertEqual(fill["raw_price"], 15)

    def test_split_preserves_value_with_raw_prices(self):
        data = manifest()
        data["bars"][1].update(open=6, close=6)
        data["corporate_actions"] = [{"type": "split", "asset_id": "A", "at": OPEN2, "ratio": 2}]
        market = Market(data)
        market.submit({"A": 1}, START, ["thesis"])
        previous = market.advance(CLOSE1)
        final = market.advance(END)
        self.assertEqual(previous["nav"], final["nav"])
        self.assertEqual(final["positions"]["A"]["quantity"], 200)
        self.assertEqual(len([item for item in final["ledger"] if item["type"] == "split"]), 1)

    def test_dividend_receivable_then_cash_without_double_counting(self):
        data = manifest()
        data["bars"][1].update(open=11, close=11)
        data["corporate_actions"] = [{"type": "dividend", "asset_id": "A", "ex_at": OPEN2,
                                      "pay_at": END, "amount_per_share": 1}]
        market = Market(data)
        market.submit({"A": 1}, START, ["thesis"])
        before = market.advance(CLOSE1)
        ex = market.advance(OPEN2)
        self.assertEqual(ex["cash"], 0)
        self.assertEqual(sum(item["amount"] for item in ex["receivables"].values()), 100)
        self.assertEqual(ex["nav"], before["nav"])
        paid = market.advance(END)
        self.assertEqual(paid["cash"], 100)
        self.assertEqual(paid["receivables"], {})
        self.assertEqual(paid["nav"], before["nav"])

    def test_unpaid_dividend_is_in_terminal_nav(self):
        data = manifest()
        data["corporate_actions"] = [{"type": "dividend", "asset_id": "A", "ex_at": OPEN2,
                                      "pay_at": "2025-01-09T21:00:00Z", "amount_per_share": 1}]
        market = Market(data)
        market.submit({"A": 1}, START, ["thesis"])
        final = market.advance(END)
        self.assertEqual(final["cash"], 0)
        self.assertEqual(final["nav"], 1700)
        self.assertEqual(sum(item["amount"] for item in final["receivables"].values()), 100)
        self.assertNotIn("2025-01-09", json.dumps(final))

    def test_ex_dividend_buy_does_not_get_entitlement(self):
        data = manifest()
        data["corporate_actions"] = [{"type": "dividend", "asset_id": "A", "ex_at": OPEN1,
                                      "pay_at": END, "amount_per_share": 1}]
        market = Market(data)
        market.submit({"A": 1}, START, ["thesis"])
        final = market.advance(END)
        self.assertEqual(final["cash"], 0)
        self.assertEqual(final["receivables"], {})

    def test_pending_replacement_and_cancel_are_explicit(self):
        market = Market(manifest())
        market.submit({"A": 1}, START, ["first"])
        before = market.snapshot()
        with self.assertRaisesRegex(ValueError, "explicit"):
            market.submit({"B": 1}, START, ["second"])
        self.assertEqual(market.snapshot(), before)
        market.submit({"B": 0.5}, START, ["second"], replace=True)
        self.assertEqual([item["type"] for item in market.snapshot()["ledger"]],
                         ["allocation_submitted", "allocation_cancelled", "allocation_submitted"])
        market.cancel(START, "Policy stops before exposure is opened")
        final = market.advance(END)
        self.assertEqual(final["net_return"], 0)
        self.assertEqual(final["positions"], {})

    def test_invalid_replacement_preserves_existing_order(self):
        market = Market(manifest())
        market.submit({"A": 0.5}, START, ["first"])
        before = market.snapshot()
        with self.assertRaises(ValueError):
            market.submit({"A": 1.1}, START, ["second"], replace=True)
        self.assertEqual(market.snapshot(), before)

    def test_reduce_only_quantity_cap_survives_a_gap_down(self):
        data = manifest()
        data["bars"][1].update(open=1, close=1)
        market = Market(data)
        market.submit({"A": 0.5}, START, ["first"])
        first = market.advance(CLOSE1)
        self.assertEqual(first["positions"]["A"]["quantity"], 50)
        # 40% is below the currently marked weight, yet without an execution-time
        # quantity cap the lower price would turn this request into a share buy.
        market.submit({"A": 0.4}, CLOSE1, ["risk-gate-failed"], reduce_only_assets=["A"])
        final = market.advance(END)
        self.assertEqual(final["positions"]["A"]["quantity"], 50)
        self.assertEqual(final["cash"], 500)
        caps = [event for event in final["ledger"] if event["type"] == "allocation_reduction_cap"]
        self.assertEqual(len(caps), 1)
        self.assertEqual(caps[0]["allowed_quantity"], 50)
        self.assertGreater(caps[0]["requested_quantity"], 50)

    def test_reduce_only_allows_sells_and_other_asset_buys(self):
        market = Market(manifest())
        market.submit({"A": 1}, START, ["first"])
        market.advance(CLOSE1)
        market.submit({"A": 0.25, "B": 0.5}, CLOSE1, ["revised"], reduce_only_assets=["A"])
        final = market.advance(END)
        self.assertEqual(final["positions"]["A"]["quantity"], 25)
        self.assertEqual(final["positions"]["B"]["quantity"], 75)
        self.assertEqual(final["cash"], 375)

    def test_invalid_allocations_leave_state_unchanged(self):
        market = Market(manifest(max_weight=0.6))
        for allocation in ({"A": -0.1}, {"A": 0.7}, {"A": 0.6, "B": 0.6},
                           {"A": float("nan")}, {"A": True}, {"unknown": 0.1}):
            before = market.snapshot()
            with self.subTest(allocation=allocation), self.assertRaises(ValueError):
                market.submit(allocation, START, ["thesis"])
            self.assertEqual(market.snapshot(), before)

    def test_asset_availability_blocks_early_allocation(self):
        data = manifest()
        data["assets"][0]["available_at"] = CLOSE1
        market = Market(data)
        with self.assertRaisesRegex(ValueError, "not yet available"):
            market.submit({"A": 0.5}, START, ["thesis"])

    def test_every_asset_requires_complete_terminal_coverage(self):
        data = manifest()
        data["bars"].pop()
        with self.assertRaisesRegex(ValueError, "complete session grid"):
            Market(data)
        data = manifest()
        data["timeline"][-1] = "2025-01-07T21:00:00Z"
        with self.assertRaisesRegex(ValueError, "terminal cutoff"):
            Market(data)

    def test_bad_market_data_fails_before_a_policy_runs(self):
        for change in (lambda data: data.update(price_basis="adjusted"),
                       lambda data: data["bars"][0].update(close=0),
                       lambda data: data["bars"][0].update(open=float("nan")),
                       lambda data: data["bars"][0].update(open_at="2025-01-03T14:30:00"),
                       lambda data: data["bars"].append(copy.deepcopy(data["bars"][0])),
                       lambda data: data.update(corporate_actions=[{"type": "merger", "asset_id": "A"}])):
            data = manifest()
            change(data)
            with self.assertRaises(ValueError):
                Market(data)

    def test_past_future_snapshots_and_action_backdating_rejected(self):
        market = Market(manifest())
        market.advance(CLOSE1)
        for stamp in (START, END):
            with self.assertRaises(ValueError):
                market.snapshot(stamp)
            with self.assertRaises(ValueError):
                market.submit({}, stamp, ["thesis"])
        with self.assertRaises(ValueError):
            market.advance(START)
        market.advance(END)
        with self.assertRaisesRegex(ValueError, "no eligible open"):
            market.submit({}, END, ["thesis"])

    def test_public_results_do_not_alias_private_state(self):
        market = Market(manifest())
        order = market.submit({"A": 0.5}, START, ["thesis"])
        order["weights"]["A"] = 0
        snapshot = market.advance(CLOSE1)
        snapshot["positions"]["A"]["quantity"] = 0
        snapshot["ledger"].clear()
        self.assertEqual(market.snapshot()["positions"]["A"]["quantity"], 50)
        self.assertTrue(market.snapshot()["ledger"])

    def test_multiple_full_buy_sell_cycles_conserve_net_cash(self):
        market = Market(manifest(fee=12, slippage=7))
        market.submit({"A": 0.5, "B": 0.5}, START, ["first"])
        first = market.advance(CLOSE1)
        self.assertGreaterEqual(first["cash"], 0)
        market.submit({}, CLOSE1, ["second"])
        final = market.advance(END)
        self.assertEqual(final["positions"], {})
        deltas = sum(item["cash_delta"] for item in final["ledger"] if item["type"] == "fill")
        self.assertAlmostEqual(final["cash"], 1000 + deltas)
        self.assertAlmostEqual(final["net_return"], final["cash"] / 1000 - 1)


if __name__ == "__main__":
    unittest.main()
