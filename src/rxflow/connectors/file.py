"""File line reader. ``follow=True`` tails new lines until close()/drain()."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterator

from rxflow.envelope import Envelope


class FileSource:
    def __init__(
        self,
        path: str | Path,
        *,
        jsonl: bool = True,
        encoding: str = "utf-8",
        follow: bool = False,
        poll_interval: float = 0.05,
    ) -> None:
        self.path = Path(path)
        self.jsonl = jsonl
        self.encoding = encoding
        self.follow = follow
        self.poll_interval = poll_interval
        self._offset = 0
        self._committed: dict[str, int] = {str(self.path): -1}
        self._closed = False

    def __iter__(self) -> Iterator[Envelope]:
        with self.path.open(encoding=self.encoding) as fh:
            index = -1
            while True:
                line = fh.readline()
                if line:
                    index += 1
                    if index < self._offset:
                        continue
                    text = line.rstrip("\n")
                    if not text:
                        self._offset = index + 1
                        continue
                    payload: object = text
                    if self.jsonl:
                        try:
                            payload = json.loads(text)
                        except json.JSONDecodeError:
                            payload = text
                    yield Envelope(
                        payload=payload, offset=index, partition=str(self.path)
                    )
                    self._offset = index + 1
                    continue
                if self._closed or not self.follow:
                    return
                time.sleep(self.poll_interval)

    def seek(self, partition: int | str, offset: int) -> None:
        self._offset = offset

    def commit(self, partition: int | str, offset: int) -> None:
        self._committed[str(partition)] = offset

    def committed(self) -> dict:
        return dict(self._committed)

    def close(self) -> None:
        self._closed = True
