"""Independent account arithmetic from fills and the frozen market tape.

This checker does not call Market or trust its cash deltas, marks or return.
It verifies accounting for the recorded fill quantities, not optimal decisions
or the broker's target-weight sizing algorithm.
"""
from decimal import Decimal
import math

from .scenario import timestamp


QUANTITY_TOLERANCE = Decimal("1e-12")


def reconcile(manifest, summary):
    number = lambda value: Decimal(str(value))
    initial = number(manifest["portfolio"]["initial_cash"])
    cash, positions, receivables = initial, {}, {}
    terminal = timestamp(manifest["timeline"][-1])
    events, errors = [], []
    fills = [item for item in summary["account"]["ledger"] if item["type"] == "fill"]
    ids = [item["event_id"] for item in fills]
    if len(ids) != len(set(ids)):
        errors.append("duplicate fill event IDs")
    for index, item in enumerate(fills):
        events.append((timestamp(item["at"]), 3, index, "fill", item))
    for index, item in enumerate(manifest["corporate_actions"]):
        if item["type"] == "split":
            events.append((timestamp(item["at"]), 0, index, "split", item))
        else:
            events.append((timestamp(item["ex_at"]), 1, index, "dividend_ex", item))
            events.append((timestamp(item["pay_at"]), 2, index, "dividend_pay", item))

    def check(label, actual, expected):
        if not math.isclose(float(actual), float(expected), rel_tol=1e-9, abs_tol=1e-7):
            errors.append(f"{label}: recorded {actual}, recomputed {expected}")

    for at, _, index, kind, item in sorted(events):
        if at > terminal:
            errors.append("fill after terminal horizon") if kind == "fill" else None
            continue
        asset = item["asset_id"]
        quantity = positions.get(asset, Decimal(0))
        if kind == "split":
            positions[asset] = quantity * number(item["ratio"])
        elif kind == "dividend_ex":
            receivables[index] = quantity * number(item["amount_per_share"])
        elif kind == "dividend_pay":
            cash += receivables.pop(index, Decimal(0))
        else:
            bar = next((bar for bar in manifest["bars"] if bar["asset_id"] == asset
                        and timestamp(bar["open_at"]) == at), None)
            if bar is None:
                errors.append("fill has no matching market open")
                continue
            signed = number(item["quantity"]) * (1 if item["side"] == "buy" else -1)
            if item["side"] not in {"buy", "sell"} or number(item["quantity"]) <= 0:
                errors.append("invalid fill side or quantity")
            slip = number(manifest["portfolio"]["slippage_bps"]) / 10000
            price = number(bar["open"]) * (1 + slip if signed > 0 else 1 - slip)
            fee = abs(signed) * price * number(manifest["portfolio"]["fee_bps"]) / 10000
            cash += -signed * price - fee
            positions[asset] = quantity + signed
            check("execution price", item["execution_price"], price)
            check("fee", item["fee"], fee)
            check("cash after fill", item["cash_after"], cash)
            check("quantity after fill", item["quantity_after"], positions[asset])
            if cash < Decimal("-0.0000001") or positions[asset] < Decimal("-0.0000001"):
                errors.append("negative cash or shares")
    values = {}
    for asset, quantity in positions.items():
        # Serialized fractional fills can leave Decimal residue after a full
        # exit. Apply the same share tolerance to valuation and membership.
        if abs(quantity) <= QUANTITY_TOLERANCE:
            continue
        mark = next(bar["close"] for bar in manifest["bars"]
                    if bar["asset_id"] == asset and timestamp(bar["close_at"]) == terminal)
        values[asset] = quantity * number(mark)
        actual = summary["account"]["positions"].get(asset, {})
        check("final quantity " + asset, actual.get("quantity", 0), quantity)
        check("final mark " + asset, actual.get("mark_price", 0), mark)
    expected_assets = set(values)
    if set(summary["account"]["positions"]) != expected_assets:
        errors.append("final position set differs")
    nav = cash + sum(values.values()) + sum(receivables.values())
    reward = nav / initial - 1
    check("final cash", summary["account"]["cash"], cash)
    check("final receivables", sum(item["amount"] for item in summary["account"]["receivables"].values()), sum(receivables.values()))
    check("terminal NAV", summary["account"]["nav"], nav)
    check("terminal reward", summary["reward"], reward)
    return {"status": "passed" if not errors else "failed", "errors": errors,
            "recomputed_cash": float(cash), "recomputed_nav": float(nav),
            "recomputed_reward": float(reward), "fills_checked": len(fills),
            "scope": "Independent fill, fee, corporate-action and terminal valuation arithmetic; fill quantities are inputs."}
