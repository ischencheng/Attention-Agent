"""Reconcile serialized fractional fills without inventing holdings at exit."""
import copy
from decimal import Decimal
import math
import unittest

from environments.research_portfolio.market import Market
from environments.research_portfolio.reconcile import reconcile
from environments.research_portfolio.tests.test_env import fixture, TIMES


def rebalance(final_weight, *, initial_cash=1000, price_scale=1, split_ratio=None):
    manifest = fixture()
    manifest["portfolio"].update(initial_cash=initial_cash, fee_bps=10, slippage_bps=5)
    for bar in manifest["bars"]:
        bar["open"] *= price_scale
        bar["close"] *= price_scale
    if split_ratio is not None:
        manifest["corporate_actions"] = [{"type": "split", "asset_id": "power",
                                          "at": manifest["bars"][2]["open_at"], "ratio": split_ratio}]
    market = Market(manifest)
    for index, weight in enumerate((0.3333333333, 0.2, final_weight)):
        market.submit({"power": weight} if weight else {}, TIMES[index], ["fixture-thesis"])
        account = market.advance(TIMES[index + 1])
    return manifest, {"account": account, "reward": account["net_return"]}


class ReconcileTests(unittest.TestCase):
    def test_fractional_partial_reduction_then_full_exit(self):
        manifest, summary = rebalance(0)
        self.assertEqual(summary["account"]["positions"], {})
        fills = [item for item in summary["account"]["ledger"] if item["type"] == "fill"]
        self.assertEqual([item["side"] for item in fills], ["buy", "sell", "sell"])
        self.assertTrue(all(item["quantity"] > 0 and item["fee"] > 0 for item in fills))
        residue = sum(Decimal(str(item["quantity"])) * (1 if item["side"] == "buy" else -1)
                      for item in fills)
        self.assertNotEqual(residue, 0, "Fixture must exercise serialized float residue")
        self.assertLess(abs(residue), Decimal("1e-12"))

        result = reconcile(manifest, summary)
        self.assertEqual(result["status"], "passed", result["errors"])
        self.assertEqual(result["fills_checked"], 3)
        self.assertAlmostEqual(result["recomputed_nav"], summary["account"]["cash"])

    def test_full_exit_across_cash_and_price_scales(self):
        for cash in (1000, 10_000_000, 100_000_000_000):
            for price_scale in (0.001, 1, 1000):
                with self.subTest(initial_cash=cash, price_scale=price_scale):
                    manifest, summary = rebalance(0, initial_cash=cash, price_scale=price_scale)
                    self.assertEqual(summary["account"]["positions"], {})
                    result = reconcile(manifest, summary)
                    self.assertEqual(result["status"], "passed", result["errors"])
                    self.assertEqual(result["fills_checked"], 3)
                    self.assertTrue(math.isclose(result["recomputed_nav"], summary["account"]["cash"],
                                                 rel_tol=1e-12, abs_tol=1e-10))

    def test_split_preserves_the_fill_precision_bound(self):
        for final_weight in (0, 1e-10):
            with self.subTest(final_weight=final_weight):
                manifest, summary = rebalance(final_weight, initial_cash=10_000_000,
                                               price_scale=0.001, split_ratio=2.5)
                result = reconcile(manifest, summary)
                self.assertEqual(result["status"], "passed", result["errors"])

    def test_retained_position_cannot_be_omitted(self):
        for cash, price_scale in ((1000, 1), (10_000_000, 1), (100_000_000_000, 0.001)):
            for target in (0.01, 1e-10, 1e-20):
                with self.subTest(initial_cash=cash, price_scale=price_scale, target_weight=target):
                    manifest, summary = rebalance(target, initial_cash=cash, price_scale=price_scale)
                    position = summary["account"]["positions"]["power"]
                    self.assertGreater(position["quantity"], 0)
                    self.assertEqual(reconcile(manifest, summary)["status"], "passed")

                    changed = copy.deepcopy(summary)
                    changed["account"]["positions"] = {}
                    result = reconcile(manifest, changed)
                    self.assertEqual(result["status"], "failed")
                    self.assertIn("final position set differs", result["errors"])
                    self.assertTrue(any(error.startswith("final mark power:") for error in result["errors"]))

    def test_small_position_is_not_erased_by_an_absolute_share_floor(self):
        manifest = fixture()
        market = Market(manifest)
        market.submit({"power": 1e-20}, TIMES[0], ["fixture-thesis"])
        account = market.advance(TIMES[-1])
        self.assertLess(account["positions"]["power"]["quantity"], 1e-12)
        summary = {"account": account, "reward": account["net_return"]}
        self.assertEqual(reconcile(manifest, summary)["status"], "passed")
        summary["account"]["positions"] = {}
        self.assertIn("final position set differs", reconcile(manifest, summary)["errors"])

    def test_tiny_reopened_holding_survives_a_large_liquidation(self):
        manifest = copy.deepcopy(fixture())
        manifest["portfolio"].update(initial_cash=10_000_000, fee_bps=10, slippage_bps=5)
        manifest["timeline"].append("2025-06-06T20:00:00+00:00")
        manifest["bars"].append({"asset_id": "power", "open_at": "2025-06-06T13:30:00+00:00",
                                  "close_at": manifest["timeline"][-1], "open": 100, "close": 100})
        market = Market(manifest)
        for index, weight in enumerate((0.3333333333, 0.2, 0, 1e-20)):
            market.submit({"power": weight} if weight else {}, manifest["timeline"][index], ["fixture-thesis"])
            account = market.advance(manifest["timeline"][index + 1])
            if weight == 0:
                self.assertEqual(account["positions"], {})
        self.assertLess(account["positions"]["power"]["quantity"], 1e-12)
        summary = {"account": account, "reward": account["net_return"]}
        result = reconcile(manifest, summary)
        self.assertEqual(result["status"], "passed", result["errors"])
        summary["account"]["positions"] = {}
        self.assertIn("final position set differs", reconcile(manifest, summary)["errors"])

    def test_false_zero_checkpoint_cannot_erase_a_real_residual(self):
        manifest, summary = rebalance(0.01, initial_cash=10_000_000)
        fills = [item for item in summary["account"]["ledger"] if item["type"] == "fill"]
        fills[-1]["quantity_after"] = 0
        summary["account"]["positions"] = {}
        result = reconcile(manifest, summary)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(any(error.startswith("quantity after fill:") for error in result["errors"]))
        self.assertIn("final position set differs", result["errors"])


if __name__ == "__main__":
    unittest.main()
