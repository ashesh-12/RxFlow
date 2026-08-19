"""Stdout, file, callback, and in-memory sinks. Circuit breaker wraps any sink."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, TextIO

from rxflow.clock import Clock, WallClock, parse_duration
from rxflow.context import send_dlq
from rxflow.envelope import Envelope


class CallbackSink:
    def __init__(self, fn: Callable[[Envelope], Any]) -> None:
        self.fn = fn

    def write(self, envelope: Envelope) -> None:
        self.fn(envelope)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class MemorySink:
    """Collects envelopes for tests and in-process dashboards."""

    def __init__(self) -> None:
        self.items: list[Envelope] = []

    def write(self, envelope: Envelope) -> None:
        self.items.append(envelope)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None

    @property
    def payloads(self) -> list[Any]:
        return [e.payload for e in self.items]


class FileSink:
    def __init__(self, path: str | Path, *, encoding: str = "utf-8") -> None:
        self.path = Path(path)
        self._fh: TextIO = self.path.open("a", encoding=encoding)

    def write(self, envelope: Envelope) -> None:
        self._fh.write(f"{envelope.payload!r}\n")

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class StdoutSink:
    def write(self, envelope: Envelope) -> None:
        print(envelope.payload)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class CircuitBreaker:
    """Open after ``failure_threshold`` sink failures; try again after ``reset_after``."""

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_after: str | timedelta = "30s",
    ) -> None:
        self.failure_threshold = failure_threshold
        self.reset_after = parse_duration(reset_after)
        self.failures = 0
        self.state = "closed"
        self._opened_at: datetime | None = None

    def allow(self, clock: Clock) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            if self._opened_at is not None and clock.now() - self._opened_at >= self.reset_after:
                self.state = "half_open"
                return True
            return False
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.state = "closed"
        self._opened_at = None

    def record_failure(self, clock: Clock) -> None:
        self.failures += 1
        if self.state == "half_open" or self.failures >= self.failure_threshold:
            self.state = "open"
            self._opened_at = clock.now()


class ProtectedSink:
    """Retry + circuit breaker around a sink. Failures go to DLQ, job keeps running."""

    def __init__(
        self,
        inner: Any,
        *,
        retries: int = 2,
        breaker: CircuitBreaker | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.inner = inner
        self.retries = retries
        self.breaker = breaker or CircuitBreaker()
        self.clock = clock or WallClock()

    def write(self, envelope: Envelope) -> None:
        if not self.breaker.allow(self.clock):
            send_dlq(envelope, "sink", "circuit_open")
            return
        last: BaseException | None = None
        for _ in range(self.retries + 1):
            try:
                self.inner.write(envelope)
                self.breaker.record_success()
                return
            except Exception as exc:
                last = exc
        self.breaker.record_failure(self.clock)
        send_dlq(envelope, "sink", last)

    def flush(self) -> None:
        flush = getattr(self.inner, "flush", None)
        if flush is not None:
            flush()

    def close(self) -> None:
        close = getattr(self.inner, "close", None)
        if close is not None:
            close()
