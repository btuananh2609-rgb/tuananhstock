"""
cache.py — In-memory cache đơn giản với TTL
Dùng để tránh gọi API quá nhiều và tăng tốc response.
"""

import time
from typing import Any, Optional


class SimpleCache:
    def __init__(self):
        self._store: dict = {}
        self._ttl: dict = {}

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        Lưu giá trị vào cache.
        ttl: thời gian sống tính bằng giây (None = vĩnh viễn)
        """
        self._store[key] = value
        if ttl is not None:
            self._ttl[key] = time.time() + ttl
        elif key in self._ttl:
            del self._ttl[key]

    def get(self, key: str) -> Optional[Any]:
        """Lấy giá trị từ cache, None nếu hết hạn hoặc không tồn tại."""
        if key not in self._store:
            return None
        if key in self._ttl and time.time() > self._ttl[key]:
            del self._store[key]
            del self._ttl[key]
            return None
        return self._store[key]

    def delete(self, key: str):
        self._store.pop(key, None)
        self._ttl.pop(key, None)

    def clear(self):
        self._store.clear()
        self._ttl.clear()

    def keys(self):
        return list(self._store.keys())


# Singleton dùng trong toàn bộ app
cache = SimpleCache()
