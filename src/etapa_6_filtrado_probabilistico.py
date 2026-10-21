import math
import mmh3


class BloomFilter:
  """Filtro de Bloom para verificación probabilística de membresía en O(1)."""

  def __init__(self, capacity, error_rate=0.01):
    self.capacity = capacity
    self.error_rate = error_rate
    # Calcula el tamaño óptimo del arreglo de bits en función de la capacidad y tasa de error permitida
    self.size = int(
        -(capacity * math.log(error_rate)) / (math.log(2) ** 2)
    )
    # Calcula el número óptimo de funciones hash a aplicar
    self.hash_count = int((self.size / capacity) * math.log(2))
    self.bit_array = [0] * self.size  # Inicializa el vector de bits en 0

  def add(self, item):
    # Aplica múltiples funciones hash (variando la semilla) y marca los bits en 1
    for seed in range(self.hash_count):
      index = mmh3.hash(str(item), seed) % self.size
      self.bit_array[index] = 1

  def __contains__(self, item):
    # Verifica si todos los bits correspondientes al elemento están encendidos
    for seed in range(self.hash_count):
      index = mmh3.hash(str(item), seed) % self.size
      if self.bit_array[index] == 0:
        return False  # Si algún bit es 0, el elemento definitivamente NO existe
    return True  # Probablemente existe (con tasa de falso positivo controlada)


class CountMinSketch:
  """Count-Min Sketch para estimación eficiente de frecuencias en streaming."""

  def __init__(self, width=2000, depth=5):
    self.width = width  # Ancho de la matriz hash (controla colisiones)
    self.depth = depth  # Profundidad de la matriz (controla la precisión)
    self.table = [[0] * width for _ in range(depth)]  # Matriz de contadores

  def add(self, item):
    # Incrementa el contador en cada fila utilizando funciones hash independientes
    for i in range(self.depth):
      col = abs(mmh3.hash(str(item), i)) % self.width
      self.table[i][col] += 1

  def estimate_frequency(self, item):
    min_val = float('inf')
    # Retorna el valor mínimo estimado entre todas las filas para acotar el error por colisión
    for i in range(self.depth):
      col = abs(mmh3.hash(str(item), i)) % self.width
      min_val = min(min_val, self.table[i][col])
    return min_val