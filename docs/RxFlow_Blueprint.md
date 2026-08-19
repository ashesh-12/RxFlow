# RxFlow
*A generic, robust, efficient functional stream-processing engine*

**Design blueprint**

---

## 1. Purpose

RxFlow is a lazy, composable stream processing **runtime** built from functional Python (closures, higher-order functions, generators, and function composition — no external dependencies in the core). The engine itself is domain-agnostic: it knows nothing about fraud, logs, IoT, or any other subject. Every real application is created by configuring the same engine with a different topology and a different set of small functions.

This is not a single happy-path `map` / `filter` chain. It is a **mini stream processor** with the same *shape* as systems such as Kafka Streams or Flink DataStream:

- connectors at the edges (sources and sinks)
- events with keys, timestamps, and offsets
- a composable operator graph (a topology, not only a linear pipe)
- per-key state, time windows, and failure paths
- restart, replay, and observability

The ceiling is **advanced streaming semantics in one process**. The engine is not a distributed cluster. Domain meaning still lives only in callables supplied at configuration time.

Code samples in this document are **reference sketches** of contracts and shape, not a finished implementation.

This document is the blueprint for that engine: architecture, requirements, how functional-programming concepts are used deliberately (not decoratively), how the same core is reused across unrelated use cases, and which real-world streaming semantics are in scope.

---

## 2. Design Requirements

Four original requirements still drive the operator layer. Four more raise the engine from a pipeline tutorial to a processor.

| Requirement | What it means here | How it's satisfied |
|---|---|---|
| **Generic** | The engine must not encode any business rules. | Core (`Observable`, `compose`, operators, state, windows, connectors) contains zero domain logic. Domain behavior only enters as callables supplied at configuration time. |
| **Robust** | Failures in one stage must not crash the whole job silently or untraceably. | Every stage is wrapped uniformly. Failures become `StreamProcessingError` tagged with the stage name. Poison events, retries, and sink outages follow named policies and a dead-letter path — not only a traceback at subscribe time. |
| **Efficient** | Must handle unbounded streams without loading everything into memory. | Stages are lazy. Memory is O(1) per in-flight item except where an operator *explicitly* keeps a bounded window, a bounded queue, or per-key state. Backpressure prevents unbounded buffering when a sink is slow. |
| **Non-redundant** | No repeated boilerplate across operators; one bug fix should not need to be repeated five times. | Cross-cutting concerns (laziness, error wrapping, metrics hooks) live in shared factories. New operators and connectors reuse those, they do not copy them. |
| **Time-aware** | Aggregations and alerts are defined in event time, not “every N items.” | Window operators, watermarks, and late-data policy are first-class. Count buffers remain available; they are not the primary windowing model. |
| **Keyed** | Work is per entity (user, device, service), not one global accumulator. | `key_by` partitions the stream. Distinct, scan, and windows run per key. |
| **Restartable** | A crash must not silently forget counts or replay from nowhere. | Per-key state is explicit, checkpointed, and restored. Sources replay from committed offsets. |
| **Operable** | An operator must be able to answer why an alert did or did not fire. | Per-stage counters, lag, trace ids, drain-on-shutdown, and a deterministic virtual clock for tests. |

---

## 3. Scope

### In scope

A single-process stream processor that implements production *semantics*:

- event envelopes, connectors, keyed state
- event-time windows and watermarks
- stream–stream and stream–table joins
- checkpoints, offset commit, delivery guarantees
- dead-letter / side outputs, backpressure, fan-out
- metrics, graceful drain, virtual-time tests

Kafka, webhooks, and Slack appear as **adapter contracts**. A simulated partitioned log (offsets + replay) is the default “Kafka-like” source so the core stays dependency-free. Real client libraries may be added later as optional connectors; they must not leak into the engine.

### Out of scope

These would turn the project into a product, not a processor:

- cluster membership, network shuffle, consumer rebalance
- exactly-once across multiple machines
- embedding Kafka / Flink / Spark as the engine
- a SQL layer (a different project, optional later)

---

## 4. Architecture Overview

The runtime has six layers. Data still flows one item at a time; no stage waits for the whole stream to finish. The job is a **topology** (a DAG), not only a linear `.pipe()`.

```
Sources (connectors)
  → Event envelopes (key, event_time, offset, headers)
    → Topology (key_by, operators, windows, joins, side outputs)
      → State store + checkpoints
        → Sinks (connectors, offset commit)
          → Metrics / drain / DLQ
```

| Layer | Responsibility |
|---|---|
| **Source connector** | Turns a file tail, queue, webhook, or simulated log into a stream of envelopes. Owns offsets. |
| **Event model** | Every item is an envelope. Operators read payload; windows and joins read time and key. |
| **Topology** | Composed operators: map, filter, key_by, windows, join, tap, fan-out, merge. Assembled with `compose()` / `.pipe()`, plus graph edges for side outputs. |
| **State** | Per-key stores for scan, windows, and joins. Periodic snapshots. Restore on restart. |
| **Sink connector** | Pushes results to a dashboard, file, HTTP, or alerting callback. Acks success so offsets can commit. |
| **Control plane** | Error policy, backpressure, metrics, watermarks, subscription/cancellation, drain. |

Pull vs push: the operator graph is **pull-based** (generators). Live push sources (webhook, WebSocket) adapt at the edge into a bounded queue that the topology pulls from. The engine is not rewritten as a callback bus.

Cold vs hot: a topology is **cold** until a source is connected and run. A live connector is **hot**. `share()` / multicast exists so two sinks do not double-consume one source. Second subscribe on a one-shot generator is empty; that is specified, not accidental.

---

## 5. Event Model

Raw dicts are domain payloads, not the unit the engine processes. Every item that crosses an operator boundary is an envelope:

| Field | Role |
|---|---|
| `payload` | Domain body (transaction, log line, sensor reading). |
| `key` | Partition key: user id, device id, service name. May be assigned by `key_by`. |
| `event_time` | When the event *happened* (windowing and joins use this). |
| `ingest_time` | When RxFlow *received* it. |
| `offset` / `partition` | Progress token from the source (even a file has a line offset). |
| `headers` | Trace id, retry count, source name. |

Without this model, windowing, joins, replay, and tracing are pretend. Domain lambdas usually see `payload`; time/key operators see the envelope.

---

## 6. Core Engine (operator layer)

The engine has no knowledge of any specific application. The operator layer exposes generic operators built on a single shared decorator that handles laziness and error wrapping consistently. This layer is the FP core; windows, state, and connectors sit on top of it.

Reference sketch of that core (shape only):

```python
"""
RxFlow -- Generic, Robust, Efficient Functional Stream Processing Engine
Domain-agnostic core. No business logic lives here.
"""
from functools import reduce, wraps
from typing import Callable, Iterable, Any, Generator


class StreamProcessingError(Exception):
    """Wraps any exception raised inside an operator with stage context."""
    def __init__(self, stage: str, original: Exception):
        super().__init__(f"Stage '{stage}' failed: {original}")
        self.stage = stage
        self.original = original


def compose(*operators: Callable) -> Callable:
    """Combine operators into a single pipeline via functional reduction."""
    return lambda source: reduce(lambda stream, op: op(stream), operators, source)


def operator(fn: Callable) -> Callable:
    """
    Turns a plain generator function (stream, *args -> yields items) into a
    lazy, uniformly error-safe operator factory. Every operator reuses this
    instead of re-implementing try/except and generator wiring.
    Must not re-wrap an existing StreamProcessingError (preserve the failing stage).
    """
    @wraps(fn)
    def factory(*args, **kwargs) -> Callable:
        def apply(stream: Iterable) -> Generator:
            gen = fn(stream, *args, **kwargs)
            while True:
                try:
                    item = next(gen)
                except StopIteration:
                    return
                except StreamProcessingError:
                    raise
                except Exception as exc:
                    raise StreamProcessingError(fn.__name__, exc) from exc
                yield item
        return apply
    return factory


class Observable:
    """Wraps a stream source and attaches a lazy operator pipeline."""

    def __init__(self, source_fn: Callable[[], Generator[Any, None, None]]):
        self._source_fn = source_fn

    @classmethod
    def from_iterable(cls, iterable: Iterable) -> "Observable":
        return cls(lambda: (x for x in iterable))

    def pipe(self, *operators: Callable) -> "Observable":
        pipeline = compose(*operators)
        return Observable(lambda: pipeline(self._source_fn()))

    def subscribe(self, on_next: Callable, on_error: Callable = None,
                  on_complete: Callable = None):
        """
        Blocking pull until the source ends, errors, or the subscription is cancelled.
        Returns a handle with .cancel() / drain. Skip/retry of individual events is
        not implemented here — that is an operator-level error policy (Section 11).
        """
        try:
            for item in self._source_fn():
                on_next(item)
        except Exception as exc:
            if on_error:
                on_error(exc)
            else:
                raise
        else:
            if on_complete:
                on_complete()
```

### 6.1 Stateless / light-state operators

| Operator | Role |
|---|---|
| `map_op` | Transform payload (or envelope). Does not mutate in place; domain functions return new values. |
| `filter_op` | Keep or drop by predicate. |
| `distinct_until_changed` | Drop consecutive duplicates. After `key_by`, this is per key. |
| `scan_op` | Running accumulator. After `key_by`, this is per key. Seed is not emitted on an empty stream. |
| `buffer_count` | Count-based batch (still useful; not a substitute for time windows). |
| `take` / `skip` | Bound or offset a stream. |
| `flat_map` | One event in, zero or more out (expand, split). |
| `tap` | Side effect without changing the item. I/O in domain predicates is forbidden; I/O belongs in `tap` or a sink. |

Reference bodies for the original five:

```python
@operator
def map_op(stream, transform_fn):
    for item in stream:
        yield transform_fn(item)

@operator
def filter_op(stream, predicate_fn):
    for item in stream:
        if predicate_fn(item):
            yield item

@operator
def distinct_until_changed(stream, key_fn=lambda x: x):
    has_value, last_key = False, None
    for item in stream:
        key = key_fn(item)
        if not has_value or key != last_key:
            has_value, last_key = True, key
            yield item

@operator
def scan_op(stream, accumulator_fn, seed):
    state = seed
    for item in stream:
        state = accumulator_fn(state, item)
        yield state

@operator
def buffer_count(stream, size):
    buffer = []
    for item in stream:
        buffer.append(item)
        if len(buffer) == size:
            yield list(buffer)
            buffer = []
    if buffer:
        yield buffer
```

`distinct_until_changed` and `scan_op` are **stateful, not pure**. Closures avoid globals; they do not avoid mutation. Purity is required of *domain* lambdas (predicates, transforms), not of the operators that hold per-stream state.

---

## 7. Functional Programming Concepts

| Concept | Where it appears | Why it matters here |
|---|---|---|
| **Higher-order functions** | `compose()`, `operator()`, every `*_op` factory, window/join configs | The topology is assembled from functions, not hardcoded control flow. |
| **Closures** | `distinct_until_changed`, `scan_op`, per-key stores | State is scoped to a stream/key, not a global. |
| **Function composition** | `compose()` via `functools.reduce` | Arbitrary-length operator chains collapse into one callable, left to right. |
| **Generators / lazy evaluation** | Every operator body | Nothing is computed until consumed. Enables constant-memory processing of unbounded streams. |
| **Decorators** | `operator()` | Laziness, error wrapping, and metrics hooks apply uniformly. |
| **Pure functions** | Domain lambdas only | Easy to test in isolation. No hidden I/O in predicates. |
| **Immutability** | Envelope and payload updates | Stages produce new values; they do not mutate records in place. The engine does not magically enforce this — domain functions must return new objects. |

---

## 8. Connectors

`from_iterable` is a test helper, not the production source story. Sources and sinks are adapters with a small interface.

**Sources** must emit envelopes and expose:

- `read()` / iterate envelopes
- current `partition` + `offset`
- `seek(offset)` for replay
- `close()`

**Sinks** must:

- `write(envelope)` (or batch)
- `flush()`
- signal success/failure so the runtime can commit or retry
- `close()`

Default connectors (no third-party deps):

| Connector | Role |
|---|---|
| Simulated partitioned log | Kafka-like append-only log with offsets; primary teaching/production-shaped source. |
| File tail / line reader | Log monitoring demos. |
| In-memory queue | Tests and webhook adapter buffer. |
| HTTP / webhook adapter | Push at the edge → bounded queue → pull topology. |
| Stdout / file / callback sink | Dashboards, Slack-shaped callbacks, audit logs. |

Real Kafka or HTTP client libraries, if added later, implement the same interface. They do not change operators.

---

## 9. Keyed Streams

Production jobs are per entity. `key_by(key_fn)` partitions the stream. After that:

- `distinct_until_changed` is per user, not global
- `scan` is per device
- windows are per service

A global running total is a demo. **Partitioned state** is the job. Keys live on the envelope; domain lambdas still only supply `key_fn`.

---

## 10. Event-Time Windowing and Watermarks

Count-based `buffer_count` is batching. Real monitoring is “errors in 60 seconds,” not “every 50 events.”

### Windows

| Kind | Meaning |
|---|---|
| **Tumbling** | Fixed, non-overlapping (`1m`, `5m`). |
| **Sliding** | Fixed size, advancing by a step (e.g. 5m window every 1m). |
| **Session** | Closes after a gap of inactivity per key. |

Windows are keyed and driven by **event time**.

### Watermarks and late data

A watermark is the engine’s belief that “event time has moved past T.” Events with `event_time` behind the watermark are **late**. Policy (configured, not hardcoded):

- drop
- send to a **side output**
- allowed lateness (keep the window open a little longer, then close)

Out-of-order arrival is the default (mobile, IoT, HTTP retries), not an edge case.

---

## 11. Joins

Almost no production job is one stream.

| Join | Meaning | Example |
|---|---|---|
| **Stream–stream** | Match two streams in a time window | Transaction joined with a device-login event in ±2 minutes |
| **Stream–table** | Enrich a stream with a slowly changing table | Transaction joined with a user-risk profile |

Join key and window are engine concerns. Match logic beyond equality can be a domain callable.

---

## 12. State, Checkpoints, and Replay

Operator closures are not enough to survive a crash.

- Explicit **state store** per key (scan accumulators, window contents, join buffers).
- Periodic **checkpoint** (snapshot to disk).
- **Restore** from the latest complete checkpoint on restart.
- Source **replay from offset** after restore.

This is what makes delivery guarantees real. The project does not need distributed exactly-once. It does need: *crash, restore, do not silently double-count or forget.*

---

## 13. Failure as a Data Path

A traceback at `subscribe()` is necessary but not sufficient. `on_error` stopping the generator cannot “skip and continue”; once a generator raises, it is done. Skip / retry live **inside** operators as named policies.

| Mechanism | Role |
|---|---|
| **Dead-letter sink / side output** | Poison payload (bad JSON, failed schema) leaves the main path with context. |
| **Retry with cap** | Transient transform/sink errors retry N times, then DLQ. |
| **Event policy** | `fail_job` / `skip` / `retry` — chosen per topology, not hardcoded. |
| **Sink circuit breaker** | Downstream Slack/HTTP down: open the breaker, do not kill the whole job. |
| **Schema / validate operator** | `validate(schema_fn)` fails at a named stage instead of inside a random lambda. |

`StreamProcessingError` still identifies the failing stage and chains the original exception. It must not be wrapped again by downstream stages.

---

## 14. Delivery Guarantees

The topology declares one contract. Sinks and offset commit must honor it.

| Guarantee | Meaning in this engine |
|---|---|
| **At-most-once** | Emit, then move on. Crash may lose in-flight events. |
| **At-least-once** | Replay from last committed offset. Sinks must be idempotent or accept duplicates. |
| **Effectively-once (single process)** | Checkpoint state, write sink, **then** commit offset. Crash restores both together. |

This is the question every real streaming job has to answer. RxFlow makes the answer explicit.

---

## 15. Backpressure and Flow Control

A webhook can outrun a dashboard writer. Unbounded queues contradict “efficient.”

Between source adapter and topology (and before slow sinks): **bounded buffers** with a configured policy:

- **block** the source (apply backpressure)
- **drop oldest**
- **sample**

Memory stays bounded even when `on_next` / sink I/O is slow.

---

## 16. Topology (Not Only a Linear Pipe)

A job is a DAG.

| Pattern | Role |
|---|---|
| **Fan-out** | One stream, several sinks (alerts + audit table + metrics). |
| **Merge** | Several sources into one downstream. |
| **Side outputs** | Late events, DLQ, validation failures — parallel to the main path. |
| **Multicast / `share()`** | Multiple subscribers without double-consuming a hot source. |

`.pipe().subscribe()` remains the simple case. Fraud that writes Slack *and* a table *and* metrics is a graph.

`subscribe()` is blocking and must return a **subscription handle** (`cancel`, drain). There is no “fire and forget forever” without a way to stop.

---

## 17. Time-Based Operators

Alert fatigue is a real requirement. Page when a condition **holds**, not on every matching event.

| Operator | Role |
|---|---|
| `debounce` | Emit after a quiet period. |
| `throttle` | Emit at most once per interval. |
| `timeout` | Signal if no event arrives in time. |
| `sample` | Periodic latest value. |

These use the same clock as windows (virtual clock in tests, wall clock in production).

---

## 18. Async I/O at the Edges

The pipeline CPU is cheap; the sink is the bottleneck. Blocking HTTP/DB inside `map_op` stalls the whole job.

- Domain predicates and transforms are synchronous and pure.
- Source/sink adapters may be async or run on a worker pool.
- Optional `map_async` / async subscribe is an edge for I/O-bound enrichment, not the default operator model.

---

## 19. Observability and Shutdown

If you cannot answer “why didn’t this alert fire?”, it is not a processor.

| Signal | Role |
|---|---|
| Per-stage counters | In, out, dropped, late, DLQ. |
| Lag | Watermark / event time vs now. |
| Trace id | Travels in envelope headers. |
| Drain | On shutdown: finish in-flight windows that can close, flush sinks, commit offsets, then stop. |

---

## 20. Testing with a Virtual Clock

`time.sleep` cannot unit-test “3 events in 2 minutes.” Tests inject envelopes at fake timestamps, advance watermarks, restore a checkpoint, and assert window output.

Cover at least:

- operator laws (map/filter identity, compose associativity)
- error tagging (correct stage name; no double-wrap)
- empty stream + `scan_op` (seed not emitted)
- leftover `buffer_count` window
- late events to side output
- crash / restore / replay (no silent loss or double count)
- cold generator: second subscribe is empty; list source can replay

---

## 21. Reusability — Same Engine, Different Domains

Switching domains means a new topology and new lambdas, not a fork of the engine. Two examples below use the same RxFlow runtime.

### 21.1 Fraud detection

Keyed by user. Sliding window in event time. Enrich from a user-risk table. Malformed payloads go to DLQ. Checkpoint so a crash does not reset counts. Alerts are throttled.

Conceptual shape (reference, not finished code):

```python
# Configuration only -- the engine never changes.
tx_source = log_source("transactions")  # envelopes: key, event_time, offset

fraud_topology = (
    Observable.from_source(tx_source)
    .pipe(
        validate(transaction_schema),          # bad events -> DLQ side output
        key_by(lambda env: env.payload["user"]),
        distinct_until_changed(
            key_fn=lambda env: (
                env.payload["user"],
                env.payload["amount"],
                env.payload["type"],
            )
        ),
        filter_op(lambda env: env.payload["type"] == "BUY"
                              and env.payload["amount"] > 5000.0),
        join_table(user_risk_profile),         # stream-table enrich
        sliding_window(size="2m", slide="30s"),
        map_op(lambda window: score_window(window)),  # domain: risk_score
        throttle(interval="1m"),
    )
)

fraud_topology.run(
    sinks=[dashboard_sink, audit_table_sink],
    dlq=dead_letter_sink,
    delivery="at_least_once",
)
```

### 21.2 Server error-rate monitoring

Keyed by **service** (never attribute a mixed batch to `batch[0]["service"]`). Tumbling window on event time. Late logs to a side stream. Alerts throttled. Sink uses retry + circuit breaker.

```python
log_source = file_tail_source("app.jsonl")

error_rate_topology = (
    Observable.from_source(log_source)
    .pipe(
        filter_op(lambda env: env.payload["status"] >= 500),
        key_by(lambda env: env.payload["service"]),
        tumbling_window(size="1m"),
        map_op(lambda window: {
            "service": window.key,
            "errors_in_window": len(window.items),
        }),
        throttle(interval="1m"),
    )
)

error_rate_topology.run(
    sinks=[on_call_sink],
    side_outputs={"late": late_log_sink},
    delivery="at_least_once",
)
```

Other drop-in domains that need no engine changes: IoT sensor anomaly detection, e-commerce cart-abandonment tracking, stock price alerting, API rate-abuse detection. Each is a different topology and set of lambdas on the same operators, windows, and connectors.

---

## 22. Input / Processing / Output Model

| Stage | Example (fraud) | Example (logs) |
|---|---|---|
| **Input** | Transaction envelopes from a simulated log or webhook adapter (offsets, event time) | Structured log lines tailed from a file (line offset = progress) |
| **Processing** | Validate → key by user → dedupe → filter large BUY → join risk table → 2m sliding window → score → throttle | Filter 5xx → key by service → 1m tumbling window → summary → throttle; late events to side output |
| **Output** | Alerts to dashboard + audit table; poison events to DLQ | Error-rate summaries to on-call; late logs to a side sink |

---

## 23. Robustness Details

- Every operator raises `StreamProcessingError` with the stage name and `raise ... from exc`. A failure deep in the graph is traceable.
- Downstream operators must not re-wrap that error (or the tagged stage is a lie).
- Skip / retry / fail-job are **operator policies**, because `on_error` on subscribe cannot resume a dead generator.
- Sinks fail independently of transforms: circuit breaker + DLQ keep the job alive when Slack is down.
- One malformed event must not take down log monitoring.

---

## 24. Efficiency Notes

- O(1) memory per in-flight item, plus **bounded** window state, per-key store, and backpressure queues.
- Windows and joins are the legitimate bounded-memory exceptions; they must evict on watermark close.
- Filtering and validation happen as early as possible so enrichment, joins, and windows never run on discarded items.
- I/O stays at connectors; the operator graph stays CPU-cheap and lazy.

---

## 25. Non-Redundancy Notes

- A single `operator()` factory owns generator wiring and error handling. Adding an operator does not copy that boilerplate.
- Connectors share one source/sink interface. Adding file tail or a fake log does not fork the topology runtime.
- Domain logic never touches engine modules — one core, many topologies.

---

## 26. Mapping to Real Systems

RxFlow stays one process. The *concepts* match production processors:

| Real system | RxFlow |
|---|---|
| Kafka topic | Append-only partitioned log + offsets (in-memory and/or file) |
| Consumer commit | Sink ack, then commit offset |
| Kafka Streams `groupByKey` | `key_by` |
| Flink windows + watermarks | Tumbling / sliding / session + late side output |
| RocksDB state | Disk-backed per-key store + checkpoint |
| DLQ topic | Dead-letter sink |
| Kafka Connect | Source / sink adapters |
| Job metrics | Stage counters + lag |
| Job restart | Restore checkpoint, replay from offset |

---

## 27. Implementation Phases

Build in this order so each phase is a working processor, not a pile of features.

| Phase | What lands | Difficulty |
|---|---|---|
| **1. Operator core** | Envelope, `compose`, `operator()`, map/filter/scan/distinct/buffer, subscribe handle, tests | Intermediate |
| **2. Connectors + keys** | Source/sink interface, simulated log, `key_by`, per-key scan/distinct | Intermediate |
| **3. Time** | Virtual clock, tumbling/sliding windows, watermarks, late side output | High-intermediate |
| **4. Failure + delivery** | DLQ, retry policy, at-least-once offset commit, circuit breaker | High-intermediate |
| **5. State + topology** | Checkpoints, restore, fan-out, merge, `share()`, drain | Advanced |
| **6. Joins + ops polish** | Stream–table, stream–stream, debounce/throttle, metrics | Advanced |

---

## 28. Deferred / Optional Later

Not required for the advanced bar above; useful after it:

- True callback-style push into the source queue (WebSocket) — adapter only.
- Schedulers (`subscribe_on` / `observe_on`) if work ever leaves one thread.
- Optional real Kafka / HTTP client connectors behind the same interface.
- Async operator variants for mid-pipeline I/O enrichment.
