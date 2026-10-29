from collections import deque


class IngestaBufferFIFO:
  """Etapa 0: Ingesta y buffer.

  Estructura: Cola FIFO (First-In, First-Out) para la ingesta continua del stream.

  Complejidad:
      - Enqueue (push): O(1)
      - Dequeue (pop): O(1)
      - Espacio: O(N)
  """

  def __init__(self, capacity=1_000_000):
    # Inicializa el buffer con una capacidad máxima fija para evitar desbordamiento de RAM
    self.capacity = capacity
    self.buffer = deque(maxlen=capacity)

  def enqueue(self, event):
    # Inserta el evento entrante al final de la cola en tiempo O(1)
    if len(self.buffer) >= self.capacity:
      raise OverflowError("Capacidad máxima del buffer alcanzada.")
    self.buffer.append(event)

  def dequeue(self):
    # Extrae y remueve el evento más antiguo desde el inicio de la cola en tiempo O(1)
    return self.buffer.popleft() if not self.is_empty() else None

  def is_empty(self):
    return len(self.buffer) == 0

  def size(self):
    # Retorna la cantidad actual de elementos en el buffer
    return len(self.buffer)