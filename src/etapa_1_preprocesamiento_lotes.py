import numpy as np


class PreprocesamientoLotes:
  """Etapa 1: Preprocesamiento por lotes

  Estructuras: Timsort O(N log N) + Búsqueda Binaria en C O(log N) (NumPy).
  """

  def __init__(self):
    # Almacén histórico de IDs procesados en un array contiguo de NumPy para eficiencia en RAM
    self.processed_ids = np.array([], dtype=object)

  def sort_batch(self, batch, key='event_time', reverse=False):
    # Ordena el lote usando Timsort (algoritmo híbrido MergeSort + InsertionSort nativo de Python) O(N log N)
    return sorted(batch, key=lambda x: x[key], reverse=reverse)

  def deduplicate_batch(self, batch):
    if not batch:
      return []

    # Extrae los IDs del lote actual para validación masiva
    batch_ids = np.array([e['event_id'] for e in batch], dtype=object)
    unique_batch = []
    new_ids_to_add = []

    if len(self.processed_ids) == 0:
      # Si es el primer lote, registra todos los elementos únicos iniciales
      for event in batch:
        e_id = event['event_id']
        if e_id not in new_ids_to_add:
          unique_batch.append(event)
          new_ids_to_add.append(e_id)
      self.processed_ids = np.array(sorted(new_ids_to_add), dtype=object)
      return unique_batch

    # Búsqueda Binaria vectorizada en C usando np.searchsorted -> O(log N)
    idxs = np.searchsorted(self.processed_ids, batch_ids)
    idxs = np.clip(idxs, 0, len(self.processed_ids) - 1)
    is_duplicate = self.processed_ids[idxs] == batch_ids

    seen_in_batch = set()
    for event, dup in zip(batch, is_duplicate):
      e_id = event['event_id']
      # Filtra duplicados históricos y duplicados internos del mismo lote
      if not dup and e_id not in seen_in_batch:
        unique_batch.append(event)
        seen_in_batch.add(e_id)
        new_ids_to_add.append(e_id)

    # Actualiza el histórico ordenado combinando nuevos registros únicos
    if new_ids_to_add:
      self.processed_ids = np.unique(
          np.concatenate([self.processed_ids, np.array(new_ids_to_add)])
      )
    return unique_batch