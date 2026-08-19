"""Phase 4: DLQ, retry, delivery, circuit breaker."""

from rxflow import (
    CircuitBreaker,
    MemorySink,
    Observable,
    PartitionedLog,
    ProtectedSink,
    validate,
)
from rxflow.clock import VirtualClock
from tests.conftest import env


def test_validate_sends_poison_to_dlq_and_keeps_going():
    events = [env({"ok": True}), env("bad"), env({"ok": True})]
    dlq = MemorySink()
    sink = MemorySink()
    Observable.from_iterable(events).pipe(
        validate(lambda p: isinstance(p, dict) and p.get("ok"))
    ).run(sinks=[sink], dlq=dlq, protect_sinks=False)
    assert sink.payloads == [{"ok": True}, {"ok": True}]
    assert len(dlq.items) == 1
    assert dlq.items[0].headers["dlq_stage"] == "validate"


def test_at_least_once_commits_offset_after_sink():
    log = PartitionedLog()
    log.append("a")
    log.append("b")
    consumer = log.consumer()
    sink = MemorySink()
    Observable.from_source(consumer).run(
        sinks=[sink], delivery="at_least_once", protect_sinks=False
    )
    assert consumer.committed()[0] == 1  # last processed offset


def test_circuit_breaker_opens_and_sends_to_dlq():
    class Boom:
        def write(self, env):
            raise RuntimeError("down")

        def flush(self):
            return None

        def close(self):
            return None

    clock = VirtualClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_after="30s")
    dlq = MemorySink()
    sink = ProtectedSink(Boom(), retries=0, breaker=breaker, clock=clock)
    from rxflow.context import Runtime, use_runtime

    with use_runtime(Runtime(dlq=dlq, clock=clock)):
        sink.write(env("x"))
        sink.write(env("y"))  # circuit open
    assert breaker.state == "open"
    assert len(dlq.items) == 2
