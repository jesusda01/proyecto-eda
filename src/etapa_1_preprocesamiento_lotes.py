"""Validación, ordenación y búsqueda binaria entre lotes."""

from bisect import bisect_left
from datetime import datetime, timezone
import math
from pathlib import Path
import sqlite3


def parse_time(value):
    if isinstance(value, datetime):
        return (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
    result = datetime.fromisoformat(str(value).replace(" UTC", "+00:00").replace("Z", "+00:00"))
    return (
        result.replace(tzinfo=timezone.utc)
        if result.tzinfo is None
        else result.astimezone(timezone.utc)
    )


class PreprocesamientoLotes:
    """Subconjunto de IDs ordenado y acotado; SQLite recuerda todo el historial.

    El lote se ordena en O(b log b). La consulta al historial usa búsqueda
    binaria; actualizar el historial y escribir en disco tiene costos aparte.
    """

    def __init__(self, history_path=None, history_limit=100000):
        if history_limit < 1:
            raise ValueError("history_limit debe ser positivo.")
        if history_path:
            Path(history_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(
            str(history_path) if history_path else ":memory:", check_same_thread=False
        )
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY)")
        self.processed_ids, self.history_limit = [], history_limit
        self.rejected, self.duplicates = [], 0

    def validate_batch(self, batch):
        valid, self.rejected = [], []
        for raw in batch:
            try:
                if not isinstance(raw, dict):
                    raise ValueError("El evento no es un diccionario.")
                event = raw.copy()
                for key in ("event_id", "product_id", "user_id", "event_type", "event_time"):
                    if event.get(key) is None or str(event[key]).strip() == "":
                        raise ValueError(f"Falta {key}.")
                for key in ("event_id", "product_id", "user_id"):
                    event[key] = str(event[key])
                event["price"] = float(event["price"])
                if not math.isfinite(event["price"]) or event["price"] < 0:
                    raise ValueError("Precio inválido.")
                event["event_time"] = parse_time(event["event_time"]).isoformat()
                for key in ("brand", "category_code"):
                    value = event.get(key)
                    event[key] = (
                        ""
                        if value is None or (isinstance(value, float) and math.isnan(value))
                        else str(value)
                    )
                valid.append(event)
            except (ValueError, KeyError, TypeError, OverflowError) as error:
                self.rejected.append(
                    {
                        "event_id": raw.get("event_id") if isinstance(raw, dict) else None,
                        "reason": str(error),
                    }
                )
        return valid

    def deduplicate_batch(self, batch):
        unique, self.duplicates = {}, 0
        for event in sorted(batch, key=lambda e: str(e["event_id"])):
            identifier = str(event["event_id"])
            i = bisect_left(self.processed_ids, identifier)
            seen = i < len(self.processed_ids) and self.processed_ids[i] == identifier
            if (
                identifier in unique
                or seen
                or self.db.execute("SELECT 1 FROM seen WHERE id=?", (identifier,)).fetchone()
            ):
                self.duplicates += 1
            else:
                unique[identifier] = event
        self.db.executemany("INSERT INTO seen VALUES (?)", ((key,) for key in unique))
        self.db.commit()
        self.processed_ids = sorted(set(self.processed_ids).union(unique))[-self.history_limit :]
        return list(unique.values())

    @staticmethod
    def sort_batch(batch, key="event_time", reverse=False):
        return sorted(batch, key=lambda event: event[key], reverse=reverse)

    def close(self):
        self.db.close()
