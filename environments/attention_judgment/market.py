"""Auditable US-equity daily prices and calendar-based evaluation horizons.

Yahoo's chart endpoint is an unauthenticated data source, not an execution
venue. Its historical data can be revised. Persist the returned raw_response
and timestamps; an as_of filter does not turn a later download into a historical
point-in-time data vintage. XNYS supplies the US equity holiday/early-close
schedule. Returns are hypothetical open-to-close price returns, not fills,
portfolio returns, or total returns. Dividends are not added to returns; any
reported dividend or split within a horizon requires explicit adjustment first.
"""
from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import exchange_calendars as xcals


NEW_YORK = ZoneInfo("America/New_York")
SOURCE = "Yahoo Finance chart API"
MARKET_CONTRACT = MappingProxyType({
    "version": 1,
    "source": SOURCE,
    "calendar": "XNYS",
    "calendar_version": xcals.__version__,
    "timezone": "America/New_York",
    "benchmark": "SPY",
    "entry_rule": "first_regular_market_open_strictly_after_decision",
    "day_rule": "entry_session_close",
    "week_rule": "last_session_of_entry_calendar_week",
    "month_rule": "last_session_of_entry_calendar_month",
    "price_basis": "entry_open_to_exit_close_price_only",
    "drawdown_basis": "entry_open_then_completed_daily_closes_only",
    "drawdown_sign": "positive_peak_to_trough_loss_fraction",
    "worst_close_return_basis": "minimum_return_from_entry_including_initial_zero",
    "dividends_included": False,
    "corporate_actions": "unresolved_until_explicitly_adjusted",
    "missing_prices": "unresolved_no_imputed_fills",
    "vintage_policy": "freeze_first_settlement; append_later_corrections_without_overwrite",
})


def market_contract() -> dict[str, Any]:
    """JSON-serializable copy to freeze inside a judgment BEFORE evaluation.

    Later package or contract versions must not silently replace the saved
    contract. Persistence and append-only correction enforcement belong to
    the caller, not this price-fetching module.
    """
    return dict(MARKET_CONTRACT)


class MarketDataError(RuntimeError):
    """The provider did not return a usable, identifiable daily-price series."""


def _date(value: str | date) -> date:
    if isinstance(value, datetime):
        raise ValueError("Expected a session date, not a datetime")
    return value if isinstance(value, date) else date.fromisoformat(value)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("A timezone-aware datetime is required")
    return value.astimezone(timezone.utc)


@lru_cache(maxsize=16)
def _calendar(first_year: int, last_year: int):
    return xcals.get_calendar(
        "XNYS", start=f"{first_year}-01-01", end=f"{last_year}-12-31"
    )


def _sessions(start: date, end: date):
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    cal = _calendar(start.year, end.year)
    return cal, cal.sessions_in_range(start.isoformat(), end.isoformat())


def horizon_complete(session_date: str | date, as_of: datetime) -> bool:
    """Whether the specified session's actual regular/early close has passed.

    Use before fetching settlement prices. A non-session date is a contract
    error, rather than a reason to choose an earlier or later price silently.
    """
    day = _date(session_date)
    cutoff = _utc(as_of)
    cal, sessions = _sessions(day, day)
    if len(sessions) != 1:
        raise ValueError("Horizon date must be a US equity session")
    return cal.session_close(sessions[0]).to_pydatetime() <= cutoff


def horizon_dates(decided_at: datetime) -> dict[str, str]:
    """Entry is the first regular open strictly AFTER the decision timestamp.

    'week' and 'month' mean the last session in the entry's calendar week and
    calendar month. They are not rolling five-/twenty-session horizons. A
    decision at the opening instant therefore enters on the next session.
    """
    instant = _utc(decided_at)
    local_day = instant.astimezone(NEW_YORK).date()
    cal, sessions = _sessions(local_day, local_day + timedelta(days=40))
    entry = next(
        (session.date() for session in sessions if cal.session_open(session).to_pydatetime() > instant),
        None,
    )
    if entry is None:
        raise ValueError("No subsequent market open in calendar search window")
    week_end = entry + timedelta(days=6 - entry.weekday())
    month_end = date(entry.year, entry.month, calendar.monthrange(entry.year, entry.month)[1])
    _, week = _sessions(entry, week_end)
    _, month = _sessions(entry, month_end)
    return {
        "entry": entry.isoformat(),
        "day": entry.isoformat(),
        "week": week[-1].date().isoformat(),
        "month": month[-1].date().isoformat(),
    }


def _request_chart(url: str) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": "Attention-Agent/1.0 (research snapshots)", "Accept": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            result = json.load(response)
    except Exception as exc:
        raise MarketDataError(f"Yahoo chart request failed: {exc}") from exc
    if not isinstance(result, dict):
        raise MarketDataError("Yahoo returned a non-object response")
    return result


def _number(value: Any, *, positive: bool = False) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    if not math.isfinite(value) or (positive and value <= 0):
        return None
    return value


def _event_session(timestamp: Any) -> str:
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
        raise MarketDataError("Invalid corporate-action timestamp")
    try:
        return datetime.fromtimestamp(timestamp, timezone.utc).astimezone(NEW_YORK).date().isoformat()
    except (ValueError, OverflowError, OSError) as exc:
        raise MarketDataError("Invalid corporate-action timestamp") from exc


def fetch_prices(
    symbol: str, start_date: str | date, end_date: str | date, as_of: datetime
) -> dict[str, Any]:
    """Fetch an inclusive date range; expose only closed, valid US sessions.

    A missing daily bar remains missing, including halts or provider gaps.
    raw_response intentionally retains the full provider response for audit;
    do not expose it to an agent during historical replay because it can
    contain observations newer than as_of. The caller owns persistence.
    """
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a nonempty string")
    symbol = symbol.strip().upper()
    start, end = _date(start_date), _date(end_date)
    cal, sessions = _sessions(start, end)
    cutoff = _utc(as_of)
    period1 = int(datetime.combine(start, datetime.min.time(), NEW_YORK).timestamp())
    period2 = int(datetime.combine(end + timedelta(days=1), datetime.min.time(), NEW_YORK).timestamp())
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + quote(symbol, safe="") + "?" + urlencode({
        "period1": period1, "period2": period2, "interval": "1d",
        "events": "div,splits", "includeAdjustedClose": "true",
    })
    raw = _request_chart(url)
    fetched = datetime.now(timezone.utc)
    effective_cutoff = min(cutoff, fetched)
    chart = raw.get("chart", {})
    if not isinstance(chart, dict) or chart.get("error"):
        raise MarketDataError(f"Yahoo chart error: {chart.get('error') if isinstance(chart, dict) else chart}")
    results = chart.get("result")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        raise MarketDataError("Yahoo chart result is absent or ambiguous")
    result = results[0]
    meta = result.get("meta", {})
    if not isinstance(meta, dict) or str(meta.get("symbol", "")).upper() != symbol:
        raise MarketDataError("Yahoo result symbol does not match request")
    if meta.get("exchangeTimezoneName") not in {"America/New_York", "US/Eastern"}:
        raise MarketDataError("This evaluator supports US Eastern equity sessions only")

    completed = {
        session.date().isoformat()
        for session in sessions
        if cal.session_close(session).to_pydatetime() <= effective_cutoff
    }
    actions: list[dict[str, Any]] = []
    events = result.get("events") or {}
    if not isinstance(events, dict):
        raise MarketDataError("Malformed corporate-action events")
    for name, kind in (("splits", "split"), ("dividends", "dividend")):
        entries = events.get(name) or {}
        if not isinstance(entries, dict):
            raise MarketDataError("Malformed corporate-action event collection")
        for event in entries.values():
            if not isinstance(event, dict):
                raise MarketDataError("Malformed corporate-action event")
            session = _event_session(event.get("date"))
            if session not in completed:
                continue
            action: dict[str, Any] = {"session": session, "type": kind}
            if kind == "split":
                numerator = _number(event.get("numerator"), positive=True)
                denominator = _number(event.get("denominator"), positive=True)
                if numerator is None or denominator is None:
                    raise MarketDataError("Invalid split ratio")
                action["ratio"] = numerator / denominator
            else:
                amount = _number(event.get("amount"))
                if amount is None or amount < 0:
                    raise MarketDataError("Invalid dividend amount")
                action["amount"] = amount
            actions.append(action)

    indicators = result.get("indicators") or {}
    quotes = indicators.get("quote") if isinstance(indicators, dict) else None
    if not isinstance(quotes, list) or len(quotes) != 1 or not isinstance(quotes[0], dict):
        raise MarketDataError("Daily quote data is absent")
    prices = quotes[0]
    adjusted = indicators.get("adjclose") or []
    adjusted_values = adjusted[0].get("adjclose", []) if adjusted and isinstance(adjusted[0], dict) else []
    timestamps = result.get("timestamp") or []
    if not isinstance(timestamps, list):
        raise MarketDataError("Invalid daily timestamps")
    by_session: dict[str, dict[str, Any]] = {}
    invalid: list[str] = []
    for index, timestamp in enumerate(timestamps):
        session = _event_session(timestamp)
        if session not in completed:
            continue
        if session in by_session or session in invalid:
            raise MarketDataError(f"Duplicate daily bar for {session}")
        values = {}
        for key in ("open", "close"):
            series = prices.get(key) or []
            values[key] = _number(series[index], positive=True) if isinstance(series, list) and index < len(series) else None
        if any(value is None for value in values.values()):
            invalid.append(session)
            continue
        day_actions = [action for action in actions if action["session"] == session]
        by_session[session] = {
            "session": session, **values,
            "adjusted_close": _number(adjusted_values[index], positive=True) if isinstance(adjusted_values, list) and index < len(adjusted_values) else None,
            "split": math.prod(action["ratio"] for action in day_actions if action["type"] == "split"),
            "dividend": sum(action["amount"] for action in day_actions if action["type"] == "dividend"),
        }
    return {
        "symbol": symbol, "source": SOURCE, "request_url": url,
        "fetched_at": fetched.isoformat(), "as_of": cutoff.isoformat(),
        "effective_as_of": effective_cutoff.isoformat(),
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "calendar": "XNYS", "calendar_version": xcals.__version__,
        "bars": [by_session[key] for key in sorted(by_session)],
        "corporate_actions": sorted(actions, key=lambda action: (action["session"], action["type"])),
        "missing_sessions": sorted(completed - by_session.keys()),
        "invalid_sessions": sorted(invalid),
        "price_basis": "provider_open_close; price_only; dividends_not_reinvested",
        "raw_response": raw,
    }


def evaluate_horizon(
    symbol_snapshot: dict[str, Any], benchmark_snapshot: dict[str, Any],
    entry_date: str | date, exit_date: str | date,
) -> dict[str, Any]:
    """Compare a hypothetical long instrument return with SPY, without fills.

    No forward fill, adjusted-close/open mixing, or split adjustment is done.
    Any corporate action in either series yields an unresolved result. The
    same-session action check is deliberately conservative even if ex-date
    adjustment occurred before the entry open. Drawdown uses entry open and
    daily closes only; it is NOT an intraday maximum-drawdown measurement.

    Snapshot vintage identifiers accompany the result. The caller must retain
    the first settlement and report provider revisions as separate correction
    records; recomputation is not permission to overwrite a stored result.
    """
    entry, exit_day = _date(entry_date), _date(exit_date)
    cal, sessions = _sessions(entry, exit_day)
    expected = [session.date().isoformat() for session in sessions]
    if not expected or expected[0] != entry.isoformat() or expected[-1] != exit_day.isoformat():
        raise ValueError("Entry and exit must both be US equity sessions")
    if benchmark_snapshot.get("symbol") != "SPY":
        raise ValueError("The broad-market comparator must be SPY")
    common = {
        "symbol": symbol_snapshot.get("symbol"), "benchmark": "SPY",
        "entry": entry.isoformat(), "exit": exit_day.isoformat(),
        "return_basis": "entry_open_to_exit_close_price_only",
        "dividends_included": False,
        "drawdown_basis": MARKET_CONTRACT["drawdown_basis"],
        "data_vintages": [
            {
                "symbol": snapshot.get("symbol"),
                "source": snapshot.get("source"),
                "fetched_at": snapshot.get("fetched_at"),
                "as_of": snapshot.get("as_of"),
                "snapshot_sha256": hashlib.sha256(json.dumps(
                    snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                ).encode("utf-8")).hexdigest(),
            }
            for snapshot in (symbol_snapshot, benchmark_snapshot)
        ],
    }

    def unresolved(reason: str, **details: Any) -> dict[str, Any]:
        return {**common, "status": "unresolved", "reason": reason, **details}

    series = []
    drawdowns = []
    worst_returns = []
    for snapshot in (symbol_snapshot, benchmark_snapshot):
        name = snapshot.get("symbol")
        try:
            cutoff = _utc(datetime.fromisoformat(snapshot.get("effective_as_of") or snapshot["as_of"]))
        except (KeyError, TypeError, ValueError):
            return unresolved("missing-or-invalid-as-of", affected_symbol=name)
        if cal.session_close(exit_day.isoformat()).to_pydatetime() > cutoff:
            return unresolved("horizon-not-complete", affected_symbol=name)
        bars = snapshot.get("bars") or []
        if not isinstance(bars, list) or any(not isinstance(bar, dict) for bar in bars):
            return unresolved("malformed-bars", affected_symbol=name)
        index = {bar.get("session"): bar for bar in bars}
        if len(index) != len(bars):
            return unresolved("duplicate-bars", affected_symbol=name)
        missing = [session for session in expected if session not in index]
        if missing:
            return unresolved("missing-bars", affected_symbol=name, missing_sessions=missing)
        reported_actions = snapshot.get("corporate_actions") or []
        if not isinstance(reported_actions, list) or any(not isinstance(action, dict) for action in reported_actions):
            return unresolved("malformed-corporate-actions", affected_symbol=name)
        actions = [action for action in reported_actions if action.get("session") in expected]
        for session in expected:
            bar = index[session]
            if _number(bar.get("open"), positive=True) is None or _number(bar.get("close"), positive=True) is None:
                return unresolved("invalid-prices", affected_symbol=name, session=session)
            if bar.get("split", 1.0) not in (None, 1, 1.0) or bar.get("dividend", 0.0) not in (None, 0, 0.0):
                actions.append({"session": session, "split": bar.get("split"), "dividend": bar.get("dividend")})
        if actions:
            return unresolved("action-adjustment-required", affected_symbol=name, corporate_actions=actions)
        opening = float(index[entry.isoformat()]["open"])
        path = [opening] + [float(index[session]["close"]) for session in expected]
        peak = opening
        drawdown = 0.0
        for value in path:
            peak = max(peak, value)
            drawdown = max(drawdown, 1.0 - value / peak)
        series.append(path[-1] / opening - 1.0)
        drawdowns.append(drawdown)
        worst_returns.append(min(value / opening - 1.0 for value in path))
    return {
        **common, "status": "resolved", "symbol_return": series[0],
        "benchmark_return": series[1], "excess_return": series[0] - series[1],
        "max_close_drawdown": drawdowns[0], "worst_close_return": worst_returns[0],
        "benchmark_max_close_drawdown": drawdowns[1],
        "benchmark_worst_close_return": worst_returns[1],
    }
