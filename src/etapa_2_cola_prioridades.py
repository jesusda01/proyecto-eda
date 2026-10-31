import heapq
from itertools import count

# Contador global para asegurar estabilidad estricta
tie_breaker = count()


class EventoPriorizado:
  """Clase auxiliar para definir la jerarquía de prioridad en la MinHeap

  con clave compuesta: (prioridad, timestamp, contador).
  """

  def __init__(self, priority, timestamp, count_id, event):
    self.priority = priority
    self.timestamp = timestamp
    self.count_id = count_id
    self.event = event

  def __lt__(self, other):
    # 1. Compara por prioridad (menor número = mayor prioridad)
    if self.priority != other.priority:
      return self.priority < other.priority

    # 2. Si empatan en prioridad, compara por marca de tiempo (FIFO cronológico)
    if str(self.timestamp) != str(other.timestamp):
      return str(self.timestamp) < str(other.timestamp)

    # 3. Si empatan en prioridad y timestamp, desempata con el contador de llegada
    return self.count_id < other.count_id


class ColaPrioridadesHeap:
  """Etapa 2: Cola con prioridades

  Estructura: MinHeap (Binary Heap) con costo O(log N) para inserción y extracción.
  """

  def __init__(self):
    self.heap = []

  def push(self, event):
    priority = event.get('priority', 5)
    timestamp = event.get('event_time', '')
    # Se añade el contador único para garantizar estabilidad absoluta
    item = EventoPriorizado(priority, timestamp, next(tie_breaker), event)
    heapq.heappush(self.heap, item)

  def pop(self):
    if not self.is_empty():
      item = heapq.heappop(self.heap)
      return item.event
    return None

  def is_empty(self):
    return len(self.heap) == 0

  def size(self):
    return len(self.heap)