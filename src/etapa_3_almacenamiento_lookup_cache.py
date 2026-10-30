"""Eventos persistentes, tabla hash reciente y caché LRU coherente."""

from collections import OrderedDict
import json
from pathlib import Path
import sqlite3


class AlmacenamientoLookupCache:
    def __init__(self, capacity=1000, path=None, memory_limit=10000):
        if capacity < 1 or memory_limit < 1:
            raise ValueError("Las capacidades deben ser positivas.")
        self.capacity, self.memory_limit = capacity, memory_limit
        self.storage, self.cache = OrderedDict(), OrderedDict()
        self.db = None
        if path:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self.db = sqlite3.connect(str(path), check_same_thread=False)
            self.db.execute("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, data TEXT)")

    def put_storage(self, event_id, event_data):
        self.storage[event_id] = event_data.copy()
        self.storage.move_to_end(event_id)
        self.cache.pop(event_id, None)  # Nunca devolver una copia anterior.
        if self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO events VALUES (?,?)",
                (str(event_id), json.dumps(event_data, default=str, ensure_ascii=False)),
            )
            if len(self.storage) > self.memory_limit:
                self.storage.popitem(last=False)

    def get_storage(self, event_id):
        if event_id in self.storage:
            return self.storage[event_id].copy()
        if self.db:
            row = self.db.execute("SELECT data FROM events WHERE id=?", (str(event_id),)).fetchone()
            if row:
                return json.loads(row[0])
        return None

    def get_from_cache_or_storage(self, event_id):
        if event_id in self.cache:
            self.cache.move_to_end(event_id)
            return self.cache[event_id].copy(), "CACHE_HIT"
        value = self.get_storage(event_id)
        if value is None:
            return None, "NOT_FOUND"
        self.cache[event_id] = value.copy()
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
        return value, "CACHE_MISS"

    def flush(self):
        if self.db:
            self.db.commit()

    def close(self):
        if self.db:
            self.flush()
            self.db.close()
