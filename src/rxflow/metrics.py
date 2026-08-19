"""Per-stage counters plus watermark lag."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta


@dataclass
class StageMetrics:
    in_count: int = 0
    out_count: int = 0
    dropped: int = 0
    errors: int = 0
    late: int = 0
    dlq: int = 0


class Metrics:
    def __init__(self) -> None:
        self._stages: dict[str, StageMetrics] = defaultdict(StageMetrics)
        self.watermark: datetime | None = None
        self.lag: timedelta | None = None

    def stage(self, name: str) -> StageMetrics:
        return self._stages[name]

    def record_in(self, stage: str) -> None:
        self._stages[stage].in_count += 1

    def record_out(self, stage: str) -> None:
        self._stages[stage].out_count += 1

    def record_dropped(self, stage: str) -> None:
        self._stages[stage].dropped += 1

    def record_error(self, stage: str) -> None:
        self._stages[stage].errors += 1

    def record_late(self, stage: str = "late") -> None:
        self._stages[stage].late += 1

    def record_dlq(self, stage: str) -> None:
        self._stages[stage].dlq += 1

    def record_watermark(self, watermark: datetime, now: datetime) -> None:
        self.watermark = watermark
        self.lag = now - watermark

    @property
    def lag_seconds(self) -> float | None:
        if self.lag is None:
            return None
        return self.lag.total_seconds()

    def snapshot(self) -> dict[str, dict[str, int]]:
        return {name: asdict(stats) for name, stats in self._stages.items()}

    def summary(self) -> dict:
        return {
            "stages": self.snapshot(),
            "watermark": self.watermark.isoformat() if self.watermark else None,
            "lag_seconds": self.lag_seconds,
        }
