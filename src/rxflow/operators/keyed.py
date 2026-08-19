"""Keyed streams: partition key lives on the envelope."""

from __future__ import annotations

from rxflow.compose import operator
from rxflow.envelope import as_envelope


@operator
def key_by(stream, key_fn):
    for item in stream:
        env = as_envelope(item)
        yield env.with_key(key_fn(env))
