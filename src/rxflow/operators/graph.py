"""Topology as a DAG: merge, fan-out, multicast share."""

from __future__ import annotations

from heapq import heappop, heappush
from itertools import count, tee
from typing import Any, Iterable, Iterator

from rxflow.compose import operator
from rxflow.envelope import Envelope, as_envelope


def _ts(env: Envelope):
    return env.event_time or env.ingest_time


def merge(*sources) -> "Any":
    """Merge several observables/iterables by event time (then ingest time)."""
    from rxflow.observable import Observable

    def gen() -> Iterator[Envelope]:
        seq = count()
        heap: list[tuple] = []
        peeks: list[tuple] = []
        for i, src in enumerate(sources):
            it = iter(src._source_fn() if hasattr(src, "_source_fn") else src)
            peeks.append((i, it))
            try:
                item = as_envelope(next(it))
            except StopIteration:
                continue
            heappush(heap, (_ts(item), next(seq), i, item, it))
        while heap:
            _, _, _, item, it = heappop(heap)
            yield item
            try:
                nxt = as_envelope(next(it))
            except StopIteration:
                continue
            heappush(heap, (_ts(nxt), next(seq), id(it), nxt, it))

    return Observable(lambda: gen())


@operator
def fan_out(stream, *sinks):
    """Write each item to extra sinks (alerts + audit + metrics) and yield it through."""
    for item in stream:
        env = as_envelope(item)
        for sink in sinks:
            sink.write(env)
        yield env


def share_iter(stream: Iterable) -> tuple[Iterator, Iterator]:
    """Two consumers of one pull without double-reading the source (buffers if they diverge)."""
    return tee(stream, 2)
