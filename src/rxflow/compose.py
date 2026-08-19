"""Composition, the shared operator factory, and per-item error policy.

This is the non-redundancy layer: laziness, stage-tagged errors, and metrics
hooks live here. New operators reuse this instead of copying try/except.
"""

from __future__ import annotations

from functools import reduce, wraps
from typing import Any, Callable, Iterable, Iterator, Literal, TypeVar

from rxflow.context import get_runtime, send_dlq
from rxflow.errors import StreamProcessingError

SKIP = object()

ErrorPolicy = Literal["fail_job", "skip", "retry"]

T = TypeVar("T")
Operator = Callable[[Iterable[T]], Iterator[T]]


def compose(*operators: Operator) -> Operator:
    """Combine operators left-to-right into a single pipeline."""
    if not operators:
        return lambda source: iter(source)
    return lambda source: reduce(lambda stream, op: op(stream), operators, source)


def apply_policy(
    stage: str,
    fn: Callable[[T], Any],
    item: T,
    *,
    on_error: ErrorPolicy = "fail_job",
    retries: int = 0,
) -> Any:
    """Run ``fn(item)``. ``retries`` extra attempts apply to every policy.

    After the budget is spent: ``fail_job`` raises; ``skip`` / ``retry`` DLQ and drop.
    """
    last: BaseException | None = None
    for _ in range(max(1, retries + 1)):
        try:
            return fn(item)
        except StreamProcessingError:
            raise
        except Exception as exc:
            last = exc
    assert last is not None
    rt = get_runtime()
    if rt is not None:
        rt.metrics.record_error(stage)
    if on_error == "fail_job":
        raise StreamProcessingError(stage, last) from last
    send_dlq(item, stage, last)  # type: ignore[arg-type]
    if rt is not None:
        rt.metrics.record_dropped(stage)
    return SKIP


def operator(fn: Callable[..., Iterator[Any]]) -> Callable[..., Operator]:
    """Turn a generator function ``(stream, *args) -> yields items`` into a lazy,
    error-safe operator factory. Does not re-wrap StreamProcessingError.
    """

    @wraps(fn)
    def factory(*args: Any, **kwargs: Any) -> Operator:
        stage = fn.__name__

        def apply(stream: Iterable[Any]) -> Iterator[Any]:
            rt = get_runtime()
            counted = _count_in(stream, stage, rt)
            gen = fn(counted, *args, **kwargs)
            while True:
                if rt is not None and rt.stop_now:
                    return
                try:
                    item = next(gen)
                except StopIteration:
                    return
                except StreamProcessingError:
                    raise
                except Exception as exc:
                    if rt is not None:
                        rt.metrics.record_error(stage)
                    raise StreamProcessingError(stage, exc) from exc
                if rt is not None:
                    rt.metrics.record_out(stage)
                yield item

        apply.__name__ = stage
        apply.__qualname__ = stage
        return apply

    return factory


def _count_in(stream: Iterable[Any], stage: str, rt: Any) -> Iterator[Any]:
    for item in stream:
        if rt is not None:
            rt.metrics.record_in(stage)
        yield item
