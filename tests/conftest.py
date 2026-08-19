from datetime import datetime, timedelta, timezone

from rxflow import Envelope

UTC = timezone.utc
T0 = datetime(2024, 1, 1, tzinfo=UTC)


def env(payload, *, key=None, event_time=None, offset=None, partition=0, **headers):
    return Envelope(
        payload=payload,
        key=key,
        event_time=event_time,
        offset=offset,
        partition=partition,
        headers=headers,
    )


def at(seconds: float, payload, *, key=None, offset=None):
    return env(
        payload,
        key=key,
        event_time=T0 + timedelta(seconds=seconds),
        offset=offset,
    )
