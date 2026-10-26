"""Deterministic, offline paper accounting using unadjusted prices.

An allocation is a complete set of requested weights in pre-trade net asset
value, including any dividend receivables. Unspecified assets have target zero.
At the next shared market open we sell first, then proportionally scale buys to
available cash, including fees. Fractional shares are allowed. Costs therefore
make realized weights differ from requested weights; ``max_weight`` limits the
request, and does not cause hidden rebalances after price movements.

This module never fetches prices or submits orders outside this process. Its
manifest is evaluator data, not a policy observation or a security boundary.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json
from math import isfinite


def _time(value):
    if not isinstance(value, str):
        raise ValueError("timestamps must be timezone-aware ISO strings")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("timestamps must be timezone-aware ISO strings") from None
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware ISO strings")
    return stamp.astimezone(timezone.utc)


def _iso(value):
    return value.isoformat()


def _number(value, name, *, positive=False):
    if type(value) not in (int, float):
        raise ValueError(f"{name} must be finite numeric data")
    try:
        finite = isfinite(value)
    except OverflowError:
        finite = False
    if not finite:
        raise ValueError(f"{name} must be finite numeric data")
    result = Decimal(str(value))
    if result < 0 or (positive and result == 0):
        raise ValueError(f"{name} must be {'positive' if positive else 'nonnegative'}")
    return result


def _text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} requires nonempty text")
    return value


class Market:
    """A chronological, long-only cash account with point-in-time snapshots."""

    def __init__(self, manifest):
        try:
            json.dumps(manifest, allow_nan=False)
        except (TypeError, ValueError, OverflowError):
            raise ValueError("market manifest must contain finite JSON data") from None
        if not isinstance(manifest, dict) or manifest.get("price_basis") != "raw":
            raise ValueError("price_basis must be raw; adjusted prices are unsupported")
        timeline = manifest.get("timeline")
        if not isinstance(timeline, list) or len(timeline) < 2:
            raise ValueError("timeline requires at least two cutoffs")
        times = [_time(stamp) for stamp in timeline]
        if any(a >= b for a, b in zip(times, times[1:])):
            raise ValueError("timeline cutoffs must be strictly increasing")
        self.start, self.terminal = times[0], times[-1]
        config = manifest.get("portfolio")
        if not isinstance(config, dict) or set(config) != {
            "initial_cash", "fee_bps", "slippage_bps", "max_weight"
        }:
            raise ValueError("portfolio requires initial_cash, fee_bps, slippage_bps, max_weight")
        self.initial_cash = _number(config["initial_cash"], "initial_cash", positive=True)
        self.fee = _number(config["fee_bps"], "fee_bps") / Decimal(10000)
        self.slippage = _number(config["slippage_bps"], "slippage_bps") / Decimal(10000)
        self.max_weight = _number(config["max_weight"], "max_weight", positive=True)
        if self.fee >= 1 or self.slippage >= 1 or self.max_weight > 1:
            raise ValueError("fees/slippage must be below 10000 bps and max_weight at most one")

        assets = manifest.get("assets")
        if not isinstance(assets, list) or not assets:
            raise ValueError("assets require a nonempty data-coverage universe")
        self._assets = {}
        for asset in assets:
            if not isinstance(asset, dict) or set(asset) != {
                "asset_id", "symbol", "name", "available_at", "terms"
            }:
                raise ValueError("invalid asset fields")
            asset_id = _text(asset["asset_id"], "asset_id")
            for key in ("symbol", "name"):
                _text(asset[key], key)
            _time(asset["available_at"])
            if not isinstance(asset["terms"], list) or any(
                not isinstance(term, str) or not term.strip() for term in asset["terms"]
            ):
                raise ValueError("asset terms must be strings")
            if asset_id in self._assets:
                raise ValueError("duplicate asset_id")
            self._assets[asset_id] = deepcopy(asset)

        bars = manifest.get("bars")
        if not isinstance(bars, list) or not bars:
            raise ValueError("bars require complete raw-price coverage")
        grouped = {key: [] for key in self._assets}
        self._bars = []
        self._open_prices = {}
        events = []
        for bar in bars:
            if not isinstance(bar, dict) or set(bar) != {
                "asset_id", "open_at", "close_at", "open", "close"
            } or not isinstance(bar["asset_id"], str) or bar["asset_id"] not in self._assets:
                raise ValueError("invalid raw bar fields or asset")
            opened, closed = _time(bar["open_at"]), _time(bar["close_at"])
            if opened >= closed:
                raise ValueError("a bar must open strictly before it closes")
            opening = _number(bar["open"], "open", positive=True)
            closing = _number(bar["close"], "close", positive=True)
            asset_id = bar["asset_id"]
            grouped[asset_id].append((opened, closed))
            self._open_prices.setdefault(opened, {})[asset_id] = opening
            self._bars.append(deepcopy(bar))
            events.append((closed, 4, asset_id, "close", closing))
        grids = []
        for grid in grouped.values():
            grid.sort()
            if not grid or len(grid) != len(set(grid)):
                raise ValueError("missing or duplicate bars in session grid")
            if any(a[1] >= b[0] for a, b in zip(grid, grid[1:])):
                raise ValueError("sessions must not overlap")
            grids.append(grid)
        if any(grid != grids[0] for grid in grids[1:]):
            raise ValueError("all assets must share the complete session grid")
        # A final completed bar is required even if the policy never buys an asset.
        # This rejects incomplete episodes before their return is used as a reward.
        if not any(closed == self.terminal for _, closed in grids[0]):
            raise ValueError("all assets require a closing bar at the terminal cutoff")
        for opened, prices in self._open_prices.items():
            events.append((opened, 3, "", "open", prices))
        self._bars.sort(key=lambda bar: (_time(bar["close_at"]), bar["asset_id"]))

        actions = manifest.get("corporate_actions", [])
        if not isinstance(actions, list):
            raise ValueError("corporate_actions must be a list")
        seen = set()
        for index, action in enumerate(actions):
            if not isinstance(action, dict) or not isinstance(action.get("asset_id"), str) or action["asset_id"] not in self._assets:
                raise ValueError("invalid corporate action asset")
            identity = json.dumps(action, sort_keys=True)
            if identity in seen:
                raise ValueError("duplicate corporate action")
            seen.add(identity)
            asset_id = action["asset_id"]
            if action.get("type") == "split" and set(action) == {"type", "asset_id", "at", "ratio"}:
                stamp = _time(action["at"])
                ratio = _number(action["ratio"], "split ratio", positive=True)
                events.append((stamp, 0, str(index), "split", (asset_id, ratio)))
            elif action.get("type") == "dividend" and set(action) == {
                "type", "asset_id", "ex_at", "pay_at", "amount_per_share"
            }:
                ex, pay = _time(action["ex_at"]), _time(action["pay_at"])
                if pay < ex:
                    raise ValueError("dividend payment precedes entitlement")
                amount = _number(action["amount_per_share"], "amount_per_share")
                dividend_id = f"dividend-{index + 1}"
                events.append((ex, 1, dividend_id, "dividend_ex", (asset_id, amount, dividend_id)))
                events.append((pay, 2, dividend_id, "dividend_pay", (asset_id, dividend_id)))
            else:
                raise ValueError("unsupported corporate action or fields")

        self._events = sorted(events, key=lambda event: event[:3])
        self._event_index = 0
        self._cash = self.initial_cash
        self._quantities = {key: Decimal(0) for key in self._assets}
        self._marks = {}
        self._receivables = {}
        self._pending = None
        self._ledger = []
        self._order_number = 0
        self._current = self.start
        self.advance(_iso(self.start))

    def _record(self, kind, stamp, **fields):
        item = {"event_id": f"market-{len(self._ledger) + 1}", "type": kind,
                "at": _iso(stamp), **fields}
        self._ledger.append(item)
        return item

    def _check_current(self, stamp):
        parsed = _time(stamp)
        if parsed != self._current:
            raise ValueError("action timestamp must equal the current market cutoff")
        return parsed

    def _nav(self):
        total = self._cash + sum((entry["amount"] for entry in self._receivables.values()), Decimal(0))
        for asset_id, quantity in self._quantities.items():
            if quantity:
                if asset_id not in self._marks:
                    raise ValueError(f"missing valuation price for {asset_id}")
                total += quantity * self._marks[asset_id]
        return total

    def submit(self, weights, at, decision_refs, *, replace=False, reduce_only_assets=None):
        """Queue a complete allocation; invalid requests do not change state."""
        stamp = self._check_current(at)
        if not isinstance(weights, dict) or any(key not in self._assets for key in weights):
            raise ValueError("allocation contains unknown assets")
        parsed = {key: _number(value, "weight") for key, value in weights.items()}
        if sum(parsed.values(), Decimal(0)) > 1 or any(weight > self.max_weight for weight in parsed.values()):
            raise ValueError("allocation violates cash or requested concentration limit")
        if any(_time(self._assets[key]["available_at"]) > stamp for key in parsed):
            raise ValueError("asset is not yet available to the policy")
        if not isinstance(decision_refs, list) or not decision_refs or any(
            not isinstance(ref, str) or not ref.strip() for ref in decision_refs
        ) or len(set(decision_refs)) != len(decision_refs):
            raise ValueError("allocation requires unique decision references")
        if type(replace) is not bool:
            raise ValueError("replace must be a boolean")
        protected = [] if reduce_only_assets is None else reduce_only_assets
        if not isinstance(protected, list) or any(
            not isinstance(key, str) or key not in self._assets for key in protected
        ) or len(set(protected)) != len(protected):
            raise ValueError("reduce_only_assets must contain unique known asset IDs")
        if self._pending is not None and not replace:
            raise ValueError("pending allocation requires explicit replace or cancel")
        opens = [opened for opened in self._open_prices if stamp < opened <= self.terminal]
        if not opens:
            raise ValueError("no eligible open remains before the terminal cutoff")
        # All validation above precedes cancellation and ID allocation.
        if self._pending is not None:
            self.cancel(at, "Explicit allocation replacement")
        self._order_number += 1
        self._pending = {
            "order_id": f"allocation-{self._order_number}", "submitted_at": _iso(stamp),
            "execute_at": _iso(min(opens)), "weights": {key: float(value) for key, value in parsed.items()},
            "decision_refs": list(decision_refs),
            "reduce_only_assets": list(protected),
        }
        self._record("allocation_submitted", stamp, **deepcopy(self._pending))
        return deepcopy(self._pending)

    def cancel(self, at, reason):
        stamp = self._check_current(at)
        _text(reason, "cancellation reason")
        if self._pending is None:
            raise ValueError("no pending allocation to cancel")
        event = self._record("allocation_cancelled", stamp, order_id=self._pending["order_id"],
                             decision_refs=list(self._pending["decision_refs"]), reason=reason)
        self._pending = None
        return deepcopy(event)

    def _fill(self, stamp, prices):
        order = self._pending
        nav_before = self._nav()
        desired = {key: Decimal(str(order["weights"].get(key, 0))) * nav_before / prices[key]
                   for key in self._assets}
        for key in sorted(order["reduce_only_assets"]):
            if desired[key] > self._quantities[key]:
                self._record("allocation_reduction_cap", stamp, order_id=order["order_id"],
                             decision_refs=list(order["decision_refs"]), asset_id=key,
                             requested_quantity=float(desired[key]),
                             allowed_quantity=float(self._quantities[key]))
                desired[key] = self._quantities[key]
        for key in sorted(self._assets):
            quantity = self._quantities[key] - desired[key]
            if quantity > 0:
                self._trade(key, -quantity, prices[key], stamp, order)
        buy_quantities = {key: max(Decimal(0), desired[key] - self._quantities[key]) for key in self._assets}
        requested_cash = sum((quantity * prices[key] * (1 + self.slippage) * (1 + self.fee)
                              for key, quantity in buy_quantities.items()), Decimal(0))
        scale = min(Decimal(1), self._cash / requested_cash) if requested_cash else Decimal(1)
        for key in sorted(self._assets):
            quantity = buy_quantities[key] * scale
            if quantity > 0:
                # Cap each fill to remaining cash: Decimal division can round up by
                # a final unit, and a cash account must never go negative.
                unit_cost = prices[key] * (1 + self.slippage) * (1 + self.fee)
                quantity = min(quantity, self._cash / unit_cost)
                self._trade(key, quantity, prices[key], stamp, order)
        self._record("allocation_completed", stamp, order_id=order["order_id"],
                     decision_refs=list(order["decision_refs"]), nav_before=float(nav_before),
                     nav_after=float(self._nav()), buy_scale=float(scale), cash=float(self._cash))
        self._pending = None

    def _trade(self, asset_id, quantity, raw_price, stamp, order):
        side = "buy" if quantity > 0 else "sell"
        execution_price = raw_price * (1 + self.slippage if quantity > 0 else 1 - self.slippage)
        gross = abs(quantity) * execution_price
        fee = gross * self.fee
        cash_delta = -gross - fee if quantity > 0 else gross - fee
        self._cash += cash_delta
        # Decimal rounding at the final digit is not an economic overdraft.
        if self._cash < 0 and abs(self._cash) < Decimal("1e-20"):
            self._cash = Decimal(0)
        if self._cash < 0:
            raise ValueError("cash-account invariant violated")
        self._quantities[asset_id] += quantity
        self._record("fill", stamp, order_id=order["order_id"],
                     decision_refs=list(order["decision_refs"]), asset_id=asset_id, side=side,
                     quantity=float(abs(quantity)), raw_price=float(raw_price),
                     execution_price=float(execution_price), fee=float(fee),
                     cash_delta=float(cash_delta), cash_after=float(self._cash),
                     quantity_after=float(self._quantities[asset_id]))

    def advance(self, cutoff):
        """Process all events through cutoff and return the current public state."""
        stamp = _time(cutoff)
        if stamp < self._current or stamp > self.terminal:
            raise ValueError("market time must advance monotonically within the episode")
        with localcontext() as context:
            context.prec = 40
            while self._event_index < len(self._events):
                event_at, _, _, kind, payload = self._events[self._event_index]
                if event_at > stamp:
                    break
                self._event_index += 1
                if kind == "open":
                    self._marks.update(payload)
                    if self._pending is not None and _time(self._pending["execute_at"]) == event_at:
                        self._fill(event_at, payload)
                elif kind == "close":
                    asset_id = self._events[self._event_index - 1][2]
                    self._marks[asset_id] = payload
                elif kind == "split":
                    asset_id, ratio = payload
                    previous = self._quantities[asset_id]
                    self._quantities[asset_id] *= ratio
                    if asset_id in self._marks:
                        self._marks[asset_id] /= ratio
                    if previous:
                        self._record("split", event_at, asset_id=asset_id, ratio=float(ratio),
                                     quantity_before=float(previous), quantity_after=float(self._quantities[asset_id]))
                elif kind == "dividend_ex":
                    asset_id, per_share, dividend_id = payload
                    amount = self._quantities[asset_id] * per_share
                    if amount:
                        self._receivables[dividend_id] = {"asset_id": asset_id, "amount": amount,
                                                         "ex_at": _iso(event_at)}
                        self._record("dividend_entitlement", event_at, asset_id=asset_id,
                                     dividend_id=dividend_id, amount=float(amount))
                elif kind == "dividend_pay":
                    asset_id, dividend_id = payload
                    receivable = self._receivables.pop(dividend_id, None)
                    if receivable:
                        self._cash += receivable["amount"]
                        self._record("dividend_payment", event_at, asset_id=asset_id,
                                     dividend_id=dividend_id, amount=float(receivable["amount"]),
                                     cash_after=float(self._cash))
            self._current = stamp
            return self.snapshot()

    def snapshot(self, cutoff=None):
        if cutoff is not None:
            self._check_current(cutoff)
        nav = self._nav()
        positions = {key: {"quantity": float(quantity), "mark_price": float(self._marks[key]),
                           "market_value": float(quantity * self._marks[key])}
                     for key, quantity in self._quantities.items() if quantity}
        receivables = {key: {**entry, "amount": float(entry["amount"])}
                       for key, entry in self._receivables.items()}
        return deepcopy({
            "as_of": _iso(self._current), "cash": float(self._cash), "positions": positions,
            "receivables": receivables, "nav": float(nav),
            "net_return": float(nav / self.initial_cash - 1),
            "current_weights": {key: float(quantity * self._marks[key] / nav) if nav else 0.0
                                for key, quantity in self._quantities.items() if quantity},
            "pending_order": self._pending, "ledger": self._ledger,
            "bars": [bar for bar in self._bars if _time(bar["close_at"]) <= self._current
                     and _time(self._assets[bar["asset_id"]]["available_at"]) <= self._current],
        })
