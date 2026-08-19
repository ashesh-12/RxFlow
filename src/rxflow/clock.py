"""Clocks and duration parsing.

Operators never call ``datetime.now()`` directly. Tests inject a virtual clock;
production uses wall time. Duration strings match the blueprint: ``'30s'``, ``'2m'``.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Protocol

_DURATION = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|h|d)$")

_UNITS = {
    "ms": lambda n: timedelta(milliseconds=n),
    "s": lambda n: timedelta(seconds=n),
    "m": lambda n: timedelta(minutes=n),
    "h": lambda n: timedelta(hours=n),
    "d": lambda n: timedelta(days=n),
}


class Clock(Protocol):
    def now(self) -> datetime: ...


class WallClock:
    """Real UTC time."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class VirtualClock:
    """Deterministic clock for tests. Advance explicitly; never sleeps."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2020, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def set(self, when: datetime) -> None:
        self._now = when if when.tzinfo else when.replace(tzinfo=timezone.utc)

    def advance(self, delta: timedelta | str | float) -> datetime:
        self._now = self._now + parse_duration(delta)
        return self._now


def parse_duration(value: str | timedelta | float | int) -> timedelta:
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    match = _DURATION.fullmatch(value.strip())
    if not match:
        raise ValueError(
            f"Cannot parse duration {value!r}; expected e.g. '30s', '2m', '1h'"
        )
    amount, unit = match.groups()
    return _UNITS[unit](float(amount))
