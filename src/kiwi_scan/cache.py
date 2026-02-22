from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class TTLCache:
    ttl_s: float
    _seen: dict[int, float]

    def __init__(self, ttl_s: float) -> None:
        self.ttl_s = float(ttl_s)
        self._seen = {}

    def allow(self, key: int, *, now: float | None = None) -> bool:
        if now is None:
            now = time.time()
        last = self._seen.get(key)
        if last is not None and (now - last) < self.ttl_s:
            return False
        self._seen[key] = now
        return True

    def gc(self, *, now: float | None = None) -> None:
        if now is None:
            now = time.time()
        cutoff = now - self.ttl_s
        self._seen = {k: t for (k, t) in self._seen.items() if t >= cutoff}
