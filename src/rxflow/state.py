"""Per-key state store and atomic checkpoints.

Operator closures do not survive a crash. Scan, windows, and joins write here
so restore + replay can continue without silently dropping or double-counting.
"""

from __future__ import annotations

import pickle
from collections import defaultdict
from pathlib import Path
from typing import Any, Hashable, Iterator


class KeyValueStore:
    """In-memory namespaced key-value store. Cheap to snapshot."""

    def __init__(self) -> None:
        self._data: dict[str, dict[Hashable, Any]] = defaultdict(dict)

    def get(self, namespace: str, key: Hashable, default: Any = None) -> Any:
        return self._data[namespace].get(key, default)

    def put(self, namespace: str, key: Hashable, value: Any) -> None:
        self._data[namespace][key] = value

    def pop(self, namespace: str, key: Hashable, default: Any = None) -> Any:
        return self._data[namespace].pop(key, default)

    def items(self, namespace: str) -> Iterator[tuple[Hashable, Any]]:
        return iter(self._data[namespace].items())

    def namespace(self, name: str) -> dict[Hashable, Any]:
        return self._data[name]

    def dump(self) -> dict[str, dict[Hashable, Any]]:
        return {ns: dict(items) for ns, items in self._data.items()}

    def load(self, data: dict[str, dict[Hashable, Any]]) -> None:
        self._data = defaultdict(dict, {ns: dict(items) for ns, items in data.items()})

    def clear(self) -> None:
        self._data.clear()


def save_checkpoint(
    directory: str | Path,
    store: KeyValueStore,
    offsets: dict[Any, int],
) -> Path:
    """Atomic checkpoint: write temp file, then replace."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    final = directory / "ckpt.pkl"
    tmp = directory / "ckpt.pkl.tmp"
    payload = {"state": store.dump(), "offsets": dict(offsets)}
    with tmp.open("wb") as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(final)
    return final


def load_checkpoint(directory: str | Path) -> dict[str, Any] | None:
    path = Path(directory) / "ckpt.pkl"
    if not path.exists():
        return None
    with path.open("rb") as fh:
        return pickle.load(fh)
