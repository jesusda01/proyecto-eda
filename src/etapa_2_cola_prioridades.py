"""Urgencia de negocio, fecha y desempate estable por orden de llegada."""

import heapq
import itertools


def event_priority(event):
    if event.get("alert_type") == "payment_alert" or event.get("event_type") == "payment_alert":
        return 0
    if event.get("event_type") in ("purchase", "refund"):
        return {"express": 1, "standard": 2, "scheduled": 3}.get(event.get("order_urgency"), 2)
    return {"cart": 4, "search": 5, "review": 5, "view": 6}.get(event.get("event_type"), 6)


class ColaPrioridadesHeap:
    def __init__(self):
        self.heap, self.counter = [], itertools.count()

    def push(self, event):
        priority = event_priority(event)
        event["priority"] = priority
        heapq.heappush(
            self.heap, (priority, str(event.get("event_time", "")), next(self.counter), event)
        )

    def pop(self):
        return heapq.heappop(self.heap)[3] if self.heap else None

    def is_empty(self):
        return not self.heap

    def size(self):
        return len(self.heap)
