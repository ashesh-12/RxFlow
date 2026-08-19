"""Runtime context installed for the duration of subscribe()/run().

Operators stay ``stream -> stream`` functions. Clock, metrics, DLQ, watermarks,
and the state store are read from here so ``pipe()`` stays declarative.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator

from rxflow.clock import Clock, WallClock
from rxflow.envelope import Envelope
from rxflow.metrics import Metrics
from rxflow.state import KeyValueStore


@dataclass
class Subscription:
    """Handle returned by subscribe/run. cancel() stops; drain() finishes in-flight."""

    cancelled: bool = False
    draining: bool = False
    _on_drain: Any = None

    def cancel(self) -> None:
        self.cancelled = True
        self.draining = False

    def drain(self) -> None:
        """Close the source and let operators flush, then stop."""
        self.draining = True
        self.cancelled = True
        if self._on_drain is not None:
            self._on_drain()


@dataclass
class Runtime:
    clock: Clock = field(default_factory=WallClock)
    metrics: Metrics = field(default_factory=Metrics)
    state: KeyValueStore = field(default_factory=KeyValueStore)
    dlq: Any = None
    side_outputs: dict[str, Any] = field(default_factory=dict)
    watermark: datetime | None = None
    delivery: str = "at_most_once"
    subscription: Subscription | None = None
    last_offsets: dict[Any, int] = field(default_factory=dict)

    @property
    def stop_now(self) -> bool:
        sub = self.subscription
        return bool(sub and sub.cancelled and not sub.draining)

    @property
    def is_draining(self) -> bool:
        return bool(self.subscription and self.subscription.draining)


_RUNTIME: ContextVar[Runtime | None] = ContextVar("rxflow_runtime", default=None)


def get_runtime() -> Runtime | None:
    return _RUNTIME.get()


def current_clock() -> Clock:
    rt = get_runtime()
    return rt.clock if rt is not None else WallClock()


@contextmanager
def use_runtime(runtime: Runtime) -> Iterator[Runtime]:
    token = _RUNTIME.set(runtime)
    try:
        yield runtime
    finally:
        _RUNTIME.reset(token)


def emit_side(name: str, env: Envelope) -> None:
    rt = get_runtime()
    if rt is None:
        return
    sink = rt.side_outputs.get(name)
    if sink is not None:
        sink.write(env)
    if name == "late":
        rt.metrics.record_late()
    else:
        rt.metrics.record_dropped(name)


def send_dlq(env: Envelope, stage: str, error: BaseException | str | None = None) -> None:
    rt = get_runtime()
    marked = env.with_header("dlq_stage", stage)
    if error is not None:
        marked = marked.with_header("dlq_error", str(error))
    if rt is None:
        return
    rt.metrics.record_dlq(stage)
    if rt.dlq is not None:
        rt.dlq.write(marked)
