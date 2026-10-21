import heapq


class EventoPriorizado:
  """Clase auxiliar para definir la jerarquía de prioridad en la MinHeap."""

  def __init__(self, priority, timestamp, event):
    self.priority = priority
    self.timestamp = timestamp
    self.event = event

  def __lt__(self, other):
    # Si las prioridades son distintas, menor número indica mayor prioridad (1 > 2 > 3...)
    if self.priority != other.priority:
      return self.priority < other.priority
    # En caso de empate en prioridad, se aplica política FIFO basada en el timestamp más antiguo
    return str(self.timestamp) < str(other.timestamp)


class ColaPrioridadesHeap:
  """Etapa 2: Cola con prioridades

  Estructura: MinHeap (Binary Heap) con costo O(log N) para inserción y extracción.
  """

  def __init__(self):
    self.heap = []  # Estructura de MinHeap subyacente basada en listas de Python

  def push(self, event):
    priority = event.get('priority', 5)
    timestamp = event.get('event_time', '')
    item = EventoPriorizado(priority, timestamp, event)
    # Inserta el elemento manteniendo la propiedad de ordenamiento del MinHeap -> O(log N)
    heapq.heappush(self.heap, item)

  def pop(self):
    if not self.is_empty():
      # Extrae y retorna el elemento con mayor prioridad (raíz del MinHeap) -> O(log N)
      item = heapq.heappop(self.heap)
      return item.event
    return None

  def is_empty(self):
    return len(self.heap) == 0

  def size(self):
    return len(self.heap)