"""Reconcile serialized fractional fills without inventing holdings at exit."""
import copy
from decimal import Decimal
import unittest

from environments.research_portfolio.market import Market
from environments.research_portfolio.reconcile import reconcile
from environments.research_portfolio.tests.test_env import fixture, TIMES


def rebalance(final_weight):
    manifest = fixture()
    manifest["portfolio"].update(fee_bps=10, slippage_bps=5)
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

    def test_retained_position_cannot_be_omitted(self):
        for target in (0.01, 1e-10):
            with self.subTest(target_weight=target):
                manifest, summary = rebalance(target)
                position = summary["account"]["positions"]["power"]
                self.assertGreater(position["quantity"], 1e-12)
                self.assertEqual(reconcile(manifest, summary)["status"], "passed")

                changed = copy.deepcopy(summary)
                changed["account"]["positions"] = {}
                result = reconcile(manifest, changed)
                self.assertEqual(result["status"], "failed")
                self.assertIn("final position set differs", result["errors"])
                self.assertTrue(any(error.startswith("final mark power:") for error in result["errors"]))


if __name__ == "__main__":
    unittest.main()
