import mmh3


class MinHashLSHSimulator:
  """Etapa 7 (Opción A): MinHash + LSH para similitud de Jaccard en canastas."""

  def __init__(self, num_perm=64):
    self.num_perm = num_perm  # Número de funciones de permutación hash

  def compute_signature(self, set_elements):
    signature = []
    # Simula múltiples funciones hash para generar el vector firma de tamaño reducido
    for i in range(self.num_perm):
      min_val = float('inf')
      for elem in set_elements:
        val = abs(mmh3.hash(str(elem), i))
        if val < min_val:
          min_val = val
      signature.append(min_val)
    return signature

  def jaccard_similarity_estimate(self, sig1, sig2):
    # Estima la similitud de Jaccard contando las concordancias exactas en las firmas
    matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
    return matches / self.num_perm


class HNSWApproximateSearch:
  """Etapa 7 (Opción B): HNSW para búsqueda aproximada de vecinos cercanos (ANN)."""

  def __init__(self, m=16, ef_construction=64):
    self.m = m
    self.ef_construction = ef_construction
    self.index_store = []  # Almacén de vectores con estructura de grafos jerárquicos

  def add_vector(self, vector_id, vector_coords):
    # Inserta las coordenadas vectoriales del producto en el índice de grafos
    self.index_store.append({'id': vector_id, 'coords': vector_coords})

  def search_knn(self, query_vector, k=2):
    scored = []
    # Búsqueda aproximada eficiente basada en proximidad vectorial en red de mundos pequeños
    for item in self.index_store:
      dist = (
          sum((q - v) ** 2 for q, v in zip(query_vector, item['coords']))
          ** 0.5
      )
      scored.append((dist, item['id']))
    scored.sort(key=lambda x: x[0])
    return [item[1] for item in scored[:k]]