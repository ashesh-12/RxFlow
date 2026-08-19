"""Observable: cold source + lazy operator graph + subscribe/run.

``pipe()`` is the linear case. ``run()`` is the job: sinks, DLQ, delivery, checkpoints.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Iterator

from rxflow.clock import Clock, WallClock
from rxflow.compose import compose
from rxflow.connectors.sinks import ProtectedSink
from rxflow.context import Runtime, Subscription, use_runtime
from rxflow.envelope import Envelope, as_envelope
from rxflow.metrics import Metrics
from rxflow.state import KeyValueStore, load_checkpoint, save_checkpoint


class Observable:
    """Wraps a stream source and attaches a lazy operator pipeline."""

    def __init__(
        self,
        source_fn: Callable[[], Iterator[Any]],
        *,
        source: Any | None = None,
    ) -> None:
        self._source_fn = source_fn
        self._source = source

    @classmethod
    def from_iterable(cls, iterable: Iterable[Any]) -> Observable:
        snapshot = list(iterable)
        return cls(lambda: (as_envelope(x) for x in snapshot))

    @classmethod
    def from_source(cls, source: Any) -> Observable:
        return cls(lambda: iter(source), source=source)

    def pipe(self, *operators: Callable) -> Observable:
        pipeline = compose(*operators)
        return Observable(lambda: pipeline(self._source_fn()), source=self._source)

    def share(self) -> Observable:
        """Cache a finite run so a second sequential subscribe does not re-run a one-shot source.

        Two sinks in one job should use ``run(sinks=[a, b])`` instead — that is O(1) fan-out.
        """
        cache: list[Envelope] | None = None

        def gen():
            nonlocal cache
            if cache is None:
                cache = [as_envelope(x) for x in self._source_fn()]
            yield from cache

        return Observable(gen, source=self._source)

    def subscribe(
        self,
        on_next: Callable[[Envelope], Any],
        on_error: Callable[[BaseException], Any] | None = None,
        on_complete: Callable[[], Any] | None = None,
        *,
        runtime: Runtime | None = None,
        clock: Clock | None = None,
    ) -> Subscription:
        """Blocking pull until the source ends, errors, or the subscription is cancelled."""
        rt = runtime or Runtime(clock=clock or WallClock())
        if clock is not None:
            rt.clock = clock
        sub = rt.subscription or Subscription()
        rt.subscription = sub
        if self._source is not None:
            sub._on_drain = getattr(self._source, "close", None)
        with use_runtime(rt):
            try:
                for item in self._source_fn():
                    if sub.cancelled and not sub.draining:
                        break
                    on_next(as_envelope(item))
            except Exception as exc:
                if on_error:
                    on_error(exc)
                else:
                    raise
            else:
                if on_complete:
                    on_complete()
        return sub

    def collect(self, **kwargs: Any) -> list[Envelope]:
        out: list[Envelope] = []
        self.subscribe(out.append, **kwargs)
        return out

    def run(
        self,
        sinks: Iterable[Any] = (),
        *,
        dlq: Any | None = None,
        side_outputs: dict[str, Any] | None = None,
        delivery: str = "at_most_once",
        clock: Clock | None = None,
        state: KeyValueStore | None = None,
        checkpoint_dir: str | None = None,
        checkpoint_every: int = 100,
        protect_sinks: bool = True,
        on_error: Callable[[BaseException], Any] | None = None,
        on_complete: Callable[[], Any] | None = None,
    ) -> Subscription:
        """Execute the topology: write sinks, honor delivery, checkpoint, drain."""
        sink_list = [
            ProtectedSink(s) if protect_sinks and not isinstance(s, ProtectedSink) else s
            for s in sinks
        ]
        sub = Subscription()
        metrics = Metrics()
        store = state or KeyValueStore()
        source = self._source

        if checkpoint_dir is not None:
            saved = load_checkpoint(checkpoint_dir)
            if saved is not None:
                store.load(saved["state"])
                if source is not None:
                    for part, offset in (saved.get("offsets") or {}).items():
                        source.seek(part, int(offset) + 1)

        rt = Runtime(
            clock=clock or WallClock(),
            metrics=metrics,
            state=store,
            dlq=dlq,
            side_outputs=side_outputs or {},
            delivery=delivery,
            subscription=sub,
        )

        processed = 0

        def commit_progress(env: Envelope) -> None:
            if env.partition is None or env.offset is None:
                return
            rt.last_offsets[env.partition] = env.offset
            if delivery == "at_most_once":
                return
            if source is not None and hasattr(source, "commit"):
                source.commit(env.partition, env.offset)

        def maybe_checkpoint() -> None:
            if checkpoint_dir is None:
                return
            save_checkpoint(checkpoint_dir, store, rt.last_offsets)

        def on_next(env: Envelope) -> None:
            nonlocal processed
            if delivery == "at_most_once" and source is not None and hasattr(source, "commit"):
                if env.partition is not None and env.offset is not None:
                    source.commit(env.partition, env.offset)
            for sink in sink_list:
                sink.write(env)
            for sink in sink_list:
                sink.flush()
            if delivery in ("at_least_once", "effectively_once"):
                commit_progress(env)
            elif delivery == "at_most_once":
                if env.partition is not None and env.offset is not None:
                    rt.last_offsets[env.partition] = env.offset
            processed += 1
            if delivery == "effectively_once" or (
                checkpoint_dir is not None and processed % checkpoint_every == 0
            ):
                maybe_checkpoint()

        try:
            self.subscribe(
                on_next,
                on_error=on_error,
                on_complete=on_complete,
                runtime=rt,
                clock=rt.clock,
            )
            maybe_checkpoint()
        finally:
            for sink in sink_list:
                close = getattr(sink, "close", None)
                if close is not None:
                    close()
            if source is not None:
                close = getattr(source, "close", None)
                if close is not None:
                    close()
        sub.metrics = metrics  # type: ignore[attr-defined]
        sub.runtime = rt  # type: ignore[attr-defined]
        return sub
