from collections import OrderedDict


class AlmacenamientoLookupCache:
  """Etapa 3: Almacenamiento, Lookup y Caché

  Estructuras: Tabla Hash primaria O(1) + Caché LRU (Least Recently Used) con OrderedDict.
  """

  def __init__(self, capacity=1000):
    self.storage = {}  # Tabla Hash primaria para almacenamiento persistente en memoria -> O(1)
    self.cache = OrderedDict()  # Caché LRU para consultas frecuentes
    self.capacity = capacity  # Capacidad máxima permitida en caché

  def put_storage(self, event_id, event_data):
    # Almacena el evento directamente en la Hash Table primaria -> O(1)
    self.storage[event_id] = event_data

  def get_storage(self, event_id):
    # Consulta directa por ID en la Hash Table primaria -> O(1)
    return self.storage.get(event_id, None)

  def get_from_cache_or_storage(self, event_id):
    # 1. Revisa si el elemento se encuentra en el caché LRU -> O(1)
    if event_id in self.cache:
      # Mueve el elemento consultado al final para marcarlo como recientemente usado
      self.cache.move_to_end(event_id)
      return self.cache[event_id], 'CACHE_HIT'

    # 2. Si no está en caché, busca en el almacenamiento principal -> O(1)
    data = self.get_storage(event_id)
    if data:
      # Si excede la capacidad del caché, desaloja el elemento menos recientemente usado (el primero)
      if len(self.cache) >= self.capacity:
        self.cache.popitem(last=False)  # Desalojo LRU O(1)
      # Inserta el nuevo elemento consultado en el caché como el más reciente
      self.cache[event_id] = data
      return data, 'CACHE_MISS'

    return None, 'NOT_FOUND'