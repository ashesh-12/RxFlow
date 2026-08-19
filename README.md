# RxFlow

A tiny, one-process stream processor in pure Python.

No Kafka. No cluster. No third-party dependencies. The same *ideas* as Kafka Streams and Flink — keyed state, event-time windows, crash recovery, failure isolation — small enough to read end to end.

```
events in  →  clean → group by entity → time window → decide  →  alerts / summaries out
                                                     ↘ poison → dead letter
                                                     ↘ late   → side output
```

---

## The problem this solves

Imagine data that never stops arriving: credit card swipes, app clicks, server logs, sensor readings, delivery-driver locations. You cannot wait for it to “finish” and then process it — there is no end. You need to react as it happens, continuously, forever, without the program running out of memory or falling over the first time something goes wrong.

That is **stream processing**. The real-world tools are Kafka, Flink, Spark Streaming — companies like Uber, Netflix, and banks run these. They are genuinely hard, heavyweight, cluster-based systems.

**RxFlow is a one-computer, no-dependencies version of that idea** — built to teach and practice the concepts those systems use, without installing Kafka or running a cluster.

The engine is domain-agnostic. It does not know what fraud, a 500 error, or a temperature spike is. You plug in small functions. Tomorrow’s job is a different topology, not a fork of the runtime.

---

## A concrete example: fraud detection

Say you work at a bank. Every card swipe comes in as an event: `{user: "alice", amount: 6000, type: "BUY"}`. You want to page someone the moment a user makes large purchases in a short window. You cannot:

- Wait for “all the data” — there is no “all,” it is continuous
- Loop over the whole history every time a new swipe arrives — too slow, memory explodes
- Write one giant tangled script — nobody can maintain that, and you will want the same machinery for server monitoring or IoT later

So you write a pipeline of small, plain functions, and RxFlow runs them continuously over the stream:

```python
Observable.from_source(transaction_log).pipe(
    validate(transaction_schema),              # bad JSON → dead letter, job keeps going
    key_by(lambda env: env.payload["user"]),   # Alice’s spending ≠ Bob’s
    filter_op(lambda env: env.payload["amount"] > 5000),
    sliding_window(size="2m", slide="30s"),    # rolling 2-minute windows in event time
    map_payload(score_window),                 # your scoring logic
    throttle(interval="1m"),                   # don’t spam on-call
).run(sinks=[dashboard], dlq=dead_letter, delivery="at_least_once")
```

Every line is a reusable building block. The engine (`key_by`, `sliding_window`, `throttle`, checkpoints, retries) never changes. Only the small functions in the middle change if tomorrow you want to monitor server errors instead of fraud — **same engine, different lambdas**.

That second job looks like this:

```python
Observable.from_source(log_tail).pipe(
    filter_op(lambda env: env.payload["status"] >= 500),
    key_by(lambda env: env.payload["service"]),
    tumbling_window(size="1m"),
    map_payload(lambda w: {"service": w.key, "errors": len(w.items)}),
).run(sinks=[on_call], side_outputs={"late": late_sink})
```

---

## Why the “advanced” stuff matters

Each piece maps to a real pain a naive script hits:

| Feature | The real problem it solves |
|---|---|
| **Windows** (“2 minutes”) | “Alert if purchases happen close together.” You cannot count every 50 events — volume on Black Friday is not volume at 3am. |
| **Watermarks / late events** | Someone’s phone was offline and the swipe arrives 10 minutes late. Do you drop it or handle it on a side path? |
| **Per-key state** (`key_by`) | Alice’s spending must not mix with Bob’s in one global counter. |
| **Checkpoint / restore** | The process crashes at 3am. On restart, does it forget everything and double-count, or resume where it left off? |
| **Dead-letter queue** | One malformed record must not take down monitoring for every other server. |
| **Circuit breaker** | If Slack is down, fraud detection should keep running and retry the sink later. |

---

## Who this is for

The use case is **not** “run this in production at a real bank.”

It is a learning / portfolio project that shows how real streaming systems work under the hood — keyed state, event-time windows, crash recovery, failure isolation — built small enough to read in an afternoon, without a cluster.

| This is | This is not |
|---|---|
| A single-process stream processor with production *semantics* | Kafka, Flink, or a distributed product |
| Domain-agnostic engine + topologies you configure | A fraud app, a log product, or a dashboard |
| Unbounded events, bounded memory | “Load the CSV into pandas” |
| Restart, replay, and “why didn’t this alert fire?” | A `map` / `filter` homework |

Full design: [`docs/RxFlow_Blueprint.md`](docs/RxFlow_Blueprint.md).

---

## Quick start

Python 3.11+. Nothing else to install for the core.

```bash
git clone https://github.com/ashesh-12/RxFlow.git
cd RxFlow

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

python examples/simulate.py      # fraud + error-rate, one run
python examples/simulate.py --live
python examples/fraud.py
python examples/error_rate.py
pytest
```

`PartitionedLog` is the Kafka stand-in: an in-process append-only log with partitions, offsets, `seek`, and `commit`. Replay is `consumer(start_offsets=...)`.

---

## How a job is shaped

```
Sources (file tail, partitioned log, queue, webhook)
  → Envelope (payload, key, event_time, offset, headers)
    → Topology (operators, windows, joins, side outputs)
      → State store + checkpoints
        → Sinks (stdout, file, callback) + offset commit
          → Metrics / DLQ / drain
```

**Input** — events that never finish: a swipe, a log line, a sensor reading. Each one is wrapped in an **envelope** so the engine can see *who* it belongs to, *when* it happened, and *how far* the source has been read.

**Processing** — a lazy pipeline (a topology, not only a straight pipe). Typical stations: validate, key by entity, window in event time, score, throttle. Domain lambdas see `payload`; time and keys live on the envelope.

**Output** — alerts, summaries, audit rows. Poison events leave on a dead-letter sink. Late events leave on a side output. The main job keeps running.

The operator graph is **pull-based** (generators). Live push sources (webhook, a producer thread) adapt at the edge into a bounded queue that the topology pulls from.

---

## Technical overview

### Event model

Raw dicts are domain payloads, not the unit the engine processes. Every item that crosses an operator boundary is an `Envelope`:

| Field | Role |
|---|---|
| `payload` | Domain body (transaction, log line, reading) |
| `key` | Partition key: user, device, service. Set by `key_by`. |
| `event_time` | When the event *happened* (windows and joins) |
| `ingest_time` | When RxFlow received it |
| `offset` / `partition` | Progress token from the source |
| `headers` | Trace id, retry count, source name |

`Envelope` is frozen. Updates go through `with_payload` / `with_key` / … — stages do not mutate records in place.

### Operators

Built on one shared `@operator` factory: laziness, stage-tagged errors, metrics. New operators reuse that; they do not copy try/except.

| Kind | Operators |
|---|---|
| Stateless | `map_op`, `map_payload`, `filter_op`, `flat_map`, `tap`, `take`, `skip`, `validate`, `assign_trace_id` |
| Keyed / stateful | `key_by`, `scan_op`, `distinct_until_changed`, `buffer_count` |
| Time windows | `tumbling_window`, `sliding_window`, `session_window`, `assign_watermarks` |
| Time ops | `debounce`, `throttle`, `timeout`, `sample` |
| Joins | `join_table` (stream–table), `join_stream` (stream–stream) |
| Graph | `fan_out`, `merge`, `Observable.share()` |

Windows are **keyed** and driven by **event time**, not “every N items.” A watermark is the engine’s belief that event time has moved past `T`. Events behind it are late: drop, side output, or allowed lateness — configured, not hardcoded.

`distinct_until_changed` and `scan_op` are stateful, not pure. Closures scope that state; they do not make it immutable. Purity is required of *domain* lambdas (predicates, transforms), not of operators that hold per-key state.

### Connectors

Sources expose iterate / `seek` / `commit` / `close`. Sinks expose `write` / `flush` / `close`. Real Kafka or HTTP clients would implement the same protocols; they must not leak into operators.

| Included (zero deps) | Role |
|---|---|
| `PartitionedLog` | Kafka-like append-only log with offsets |
| `FileSource` / `FileSink` | Line / JSONL tail and write |
| `QueueSource` | Tests and live push → pull adapter; bounded, with block / drop-oldest / sample |
| `WebhookSource` | HTTP POST at the edge → bounded queue |
| `StdoutSink`, `CallbackSink`, `MemorySink` | Dashboards, alerts, tests |
| `ProtectedSink` + `CircuitBreaker` | Sink outages do not kill the job |

### Failure, delivery, restart

A traceback at `subscribe()` is not enough: once a generator raises, it is done. Skip / retry live **inside** operators as named policies (`fail_job` / `skip` / `retry`), with a dead-letter path.

| Guarantee | Meaning here |
|---|---|
| `at_most_once` | Emit, then move on. A crash may lose in-flight events. |
| `at_least_once` | Replay from last committed offset. Sinks should be idempotent or accept duplicates. |
| `effectively_once` | Checkpoint state, write sink, **then** commit offset. Single process, not distributed exactly-once. |

On `run(..., checkpoint_dir=...)`, state and offsets snapshot to disk. Restart restores the store and `seek`s the source. That is the difference between a demo and a job that can crash at 3am.

### Observability and tests

Per-stage counters (in / out / dropped / errors), watermark lag, trace ids on envelopes. `subscribe()` / `run()` return a handle with `cancel` and drain — there is no fire-and-forget without a way to stop.

Time is injectable. Tests use `VirtualClock` and fake timestamps; they do not `time.sleep` to prove “3 events in 2 minutes.” Covering tests live under `tests/` (operator laws, error tagging, late side output, crash / restore / replay).

### Mapping to real systems

| Real system | RxFlow |
|---|---|
| Kafka topic | `PartitionedLog` (offsets + replay) |
| Consumer commit | Sink ack, then commit offset |
| Kafka Streams `groupByKey` | `key_by` |
| Flink windows + watermarks | Tumbling / sliding / session + late side output |
| RocksDB state | Disk-backed `KeyValueStore` + checkpoint |
| DLQ topic | Dead-letter sink |
| Kafka Connect | Source / sink adapters |
| Job restart | Restore checkpoint, replay from offset |

Ceiling: **advanced streaming semantics in one process**. Out of scope on purpose: cluster membership, network shuffle, consumer rebalance, exactly-once across machines, a SQL layer.

---

## Functional core (deliberate, not decorative)

| Concept | Where | Why |
|---|---|---|
| Higher-order functions | `compose()`, `@operator`, every `*_op` factory | The topology is assembled from functions, not hardcoded control flow |
| Function composition | `compose()` via `functools.reduce` | Arbitrary-length chains collapse into one callable, left to right |
| Generators / laziness | Every operator body | Nothing runs until consumed; unbounded streams stay O(1) per in-flight item |
| Closures | scan, distinct, per-key stores | State is scoped to a stream/key, not a global |
| Pure functions | Domain lambdas only | Easy to test; no hidden I/O in predicates |
| Decorators | `@operator` | Laziness, error wrapping, and metrics apply uniformly |

---

## Repository layout

```
src/rxflow/           engine
  envelope.py         event model
  compose.py          compose() + @operator + error policy
  observable.py       pipe / subscribe / run
  operators/          map, windows, joins, graph, time ops
  connectors/         sources and sinks
  state.py            keyed store + checkpoints
  clock.py            wall clock + virtual clock
examples/             fraud, error-rate, end-to-end simulate
tests/                phase tests + virtual-time scenarios
docs/RxFlow_Blueprint.md
```
