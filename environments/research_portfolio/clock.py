"""Deterministic event time for isolated source replay, independent of wall time."""
from datetime import timedelta

from .scenario import timestamp


PERIODS = {"mid": 30, "strong": 40}


def event_timeline(manifest):
    start, end = map(timestamp, (manifest["timeline"][0], manifest["timeline"][-1]))
    if end - start > timedelta(days=93):
        raise ValueError("scheduled episodes are limited to 93 days")
    events = {timestamp(item) for item in manifest["timeline"]}
    for minutes in PERIODS.values():
        tick = start
        while tick < end:
            events.add(tick)
            tick += timedelta(minutes=minutes)
    for source in manifest["sources"]:
        events.add(max(timestamp(source["published_at"]), timestamp(source["available_at"])))
    for bar in manifest["bars"]:
        events.update((timestamp(bar["open_at"]), timestamp(bar["close_at"])))
    return [item.isoformat() for item in sorted(events) if start <= item <= end]


def is_tick(now, start, role):
    seconds = (timestamp(now) - timestamp(start)).total_seconds()
    return seconds >= 0 and seconds % (PERIODS[role] * 60) == 0


def next_tick(now, start, role):
    origin, current = timestamp(start), timestamp(now)
    interval = PERIODS[role] * 60
    index = int((current - origin).total_seconds() // interval) + 1
    return (origin + timedelta(seconds=index * interval)).isoformat()
