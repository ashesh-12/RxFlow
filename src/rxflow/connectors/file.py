"""File tail / line reader. Line number is the progress offset."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from rxflow.envelope import Envelope


class FileSource:
    def __init__(self, path: str | Path, *, jsonl: bool = True, encoding: str = "utf-8") -> None:
        self.path = Path(path)
        self.jsonl = jsonl
        self.encoding = encoding
        self._offset = 0
        self._committed: dict[str, int] = {str(self.path): -1}
        self._closed = False

    def __iter__(self) -> Iterator[Envelope]:
        with self.path.open(encoding=self.encoding) as fh:
            for i, line in enumerate(fh):
                if self._closed:
                    return
                if i < self._offset:
                    continue
                text = line.rstrip("\n")
                if not text:
                    self._offset = i + 1
                    continue
                payload: object = text
                if self.jsonl:
                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError:
                        payload = text
                yield Envelope(payload=payload, offset=i, partition=str(self.path))
                self._offset = i + 1

    def seek(self, partition: int | str, offset: int) -> None:
        self._offset = offset

    def commit(self, partition: int | str, offset: int) -> None:
        self._committed[str(partition)] = offset

    def committed(self) -> dict:
        return dict(self._committed)

    def close(self) -> None:
        self._closed = True
