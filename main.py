import os
import pandas as pd
from src.convert_to_parquet import process_csv_to_parquet
from src.etapa_0_ingesta_buffer import IngestaBufferFIFO
from src.etapa_1_preprocesamiento_lotes import PreprocesamientoLotes
from src.etapa_2_cola_prioridades import ColaPrioridadesHeap
from src.etapa_3_almacenamiento_lookup_cache import AlmacenamientoLookupCache
from src.etapa_4_indexacion_principal import IndexacionPrincipalBPlusTree
from src.etapa_5_indice_multidimensional import IndiceMultidimensionalKDTree
from src.etapa_6_filtrado_probabilistico import BloomFilter, CountMinSketch
from src.etapa_7_similitud_aproximada import (
    HNSWApproximateSearch,
    MinHashLSHSimulator,
)
from src.etapa_8_grafo_dominio import GrafoDominioRed
from src.etapa_9_servicio_visualizacion import TopKAnalytics, TrieAutocompletado

CSV_PATH = '2019-Nov.csv'


def run_pipeline_tests():
  print('========================================================')
  print('--- PASO 1: VERIFICACIÓN DE DATASETS PARQUET ---')
  print('========================================================')
  target_file = 'data/ecommerce_1K.parquet'
  if not os.path.exists(target_file) or os.path.getsize(target_file) == 0:
    process_csv_to_parquet(csv_input_path=CSV_PATH)
  else:
    print("✅ Datasets Parquet válidos listos en la carpeta 'data/'.")

  df_1k = pd.read_parquet(target_file)

  # ETAPA 0
  print('\n--- ETAPA 0: Ingesta y Buffer FIFO ---')
  buffer = IngestaBufferFIFO(capacity=5000)
  for _, row in df_1k.iterrows():
    buffer.enqueue(row.to_dict())
  print(f'✅ Eventos en Buffer: {buffer.size()}')
  lote = [buffer.dequeue() for _ in range(50)]

  # ETAPA 1
  print('\n--- ETAPA 1: Preprocesamiento (Timsort + Búsqueda Binaria) ---')
  preproc = PreprocesamientoLotes()
  lote_dedup = preproc.deduplicate_batch(lote)
  lote_ordenado = preproc.sort_batch(lote_dedup, key='event_time')
  print(f'✅ Lote procesado y ordenado: {len(lote_ordenado)} eventos.')

  # ETAPA 2
  print('\n--- ETAPA 2: Cola de Prioridades (MinHeap) ---')
  cola = ColaPrioridadesHeap()
  for ev in lote_ordenado:
    cola.push(ev)
  print(f'✅ Eventos en MinHeap de prioridades: {cola.size()}')

  # ETAPA 3
  print('\n--- ETAPA 3: Almacenamiento, Lookup y Caché LRU ---')
  storage_cache = AlmacenamientoLookupCache(capacity=10)
  for ev in lote_ordenado:
    storage_cache.put_storage(ev['event_id'], ev)
  sample_id = lote_ordenado[0]['event_id']
  data_res, status = storage_cache.get_from_cache_or_storage(sample_id)
  print(f'✅ Lookup ID {sample_id} -> Estado: {status}')

  # ETAPA 4
  print('\n--- ETAPA 4: Indexación Principal (Árbol B+) ---')
  b_plus = IndexacionPrincipalBPlusTree()
  b_plus.insert(10050, {'desc': 'Indice de prueba disco'})
  print('✅ Clave insertada en Árbol B+ con éxito.')

  # ETAPA 5
  print('\n--- ETAPA 5: Índice Multidimensional & KNN ---')
  points = [(row['price'], row['user_id']) for _, row in df_1k.head(20).iterrows()]
  data_pts = list(df_1k.head(20)['event_id'])
  kd_tree = IndiceMultidimensionalKDTree(points, data_pts)
  knn_res = kd_tree.knn_exact_baseline([100.0, 50000], points, data_pts, k_neighbors=2)
  print(f'✅ KNN Baseline ejecutado. Vecinos encontrados: {knn_res}')

  # ETAPA 6 [FIJA]
  print('\n--- ETAPA 6 [FIJA]: Filtrado Probabilístico ---')
  bloom = BloomFilter(capacity=1000)
  cms = CountMinSketch()
  bloom.add('prod_12345')
  cms.add('prod_12345')
  print(f'✅ ¿"prod_12345" en Bloom Filter?: {"prod_12345" in bloom}')
  print(f'✅ Frecuencia estimada (CMS): {cms.estimate_frequency("prod_12345")}')

  # ETAPA 7 [FIJA]
  print(
      '\n--- ETAPA 7 [FIJA]: Similitud Aproximada (MinHash+LSH y HNSW) ---'
  )
  lsh_sim = MinHashLSHSimulator()
  sig1 = lsh_sim.compute_signature([101, 102, 103])
  sig2 = lsh_sim.compute_signature([101, 102, 999])
  jaccard_est = lsh_sim.jaccard_similarity_estimate(sig1, sig2)
  print(f'✅ Similitud de Jaccard estimada (MinHash): {jaccard_est:.2f}')

  hnsw = HNSWApproximateSearch()
  hnsw.add_vector('prod_A', [0.12, 0.45])
  hnsw.add_vector('prod_B', [0.15, 0.42])
  neighbors = hnsw.search_knn([0.13, 0.44], k=1)
  print(f'✅ Vecino más cercano por HNSW: {neighbors}')

  # ETAPA 8 [FIJA]
  print('\n--- ETAPA 8 [FIJA]: Grafo de Dominio & PageRank ---')
  grafo = GrafoDominioRed()
  grafo.add_edge('user_1', 'prod_A', weight=2.0)
  grafo.add_edge('prod_A', 'prod_B', weight=1.5)
  pr = grafo.compute_pagerank()
  print(f'✅ PageRank calculado para nodos: {list(pr.keys())}')

  # ETAPA 9
  print('\n--- ETAPA 9: Servicio y Visualización (Trie & Top-K) ---')
  trie = TrieAutocompletado()
  trie.insert('laptop gamer')
  trie.insert('laptop lenovo')
  print(f'✅ Autocompletado para "lap": {trie.search_prefix("lap")}')

  print(
      '\n🎉 ¡PIPELINE INTEGRADAMENTE EJECUTADO DESDE LA ETAPA 0 HASTA LA 9'
      ' CON EXITO Y RIGOR TEÓRICO! 🎉'
  )


if __name__ == '__main__':
  run_pipeline_tests()