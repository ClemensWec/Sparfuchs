from __future__ import annotations

from cachetools import TTLCache


def ttl_cache(maxsize: int = 256, ttl_seconds: int = 3600) -> TTLCache:
    return TTLCache(maxsize=maxsize, ttl=ttl_seconds)

