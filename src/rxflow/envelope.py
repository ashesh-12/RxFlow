"""Event model: every item that crosses an operator boundary is an Envelope.

Domain meaning lives only in ``payload``. The engine uses key, timestamps,
offset, and headers for partitioning, windows, replay, and tracing.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Hashable, Mapping


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _freeze_headers(headers: Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(headers, MappingProxyType):
        return headers
    return MappingProxyType(dict(headers))


@dataclass(frozen=True, slots=True)
class Envelope:
    """One event as the engine sees it. Domain data lives in payload only."""

    payload: Any
    key: Hashable | None = None
    event_time: datetime | None = None
    ingest_time: datetime = field(default_factory=_utc_now)
    offset: int | None = None
    partition: int | str | None = None
    headers: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "headers", _freeze_headers(self.headers))

    def with_payload(self, payload: Any) -> Envelope:
        return replace(self, payload=payload)

    def with_key(self, key: Hashable | None) -> Envelope:
        return replace(self, key=key)

    def with_event_time(self, event_time: datetime | None) -> Envelope:
        return replace(self, event_time=event_time)

    def with_offset(
        self, offset: int | None, partition: int | str | None = None
    ) -> Envelope:
        if partition is None:
            return replace(self, offset=offset)
        return replace(self, offset=offset, partition=partition)

    def with_headers(self, headers: Mapping[str, Any]) -> Envelope:
        return replace(self, headers=headers)

    def with_header(self, name: str, value: Any) -> Envelope:
        return replace(self, headers={**self.headers, name: value})


def as_envelope(item: Any) -> Envelope:
    """Lift a raw payload into an envelope. Envelopes pass through unchanged."""
    if isinstance(item, Envelope):
        return item
    return Envelope(payload=item)
