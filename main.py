
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


def run_integrated_pipeline():
  print('========================================================')
  print('--- PIPELINE INTEGRADO DE 10 ETAPAS (FLUJO SECUENCIAL) ---')
  print('========================================================')

  target_file = 'data/ecommerce_1K.parquet'
  if not os.path.exists(target_file) or os.path.getsize(target_file) == 0:
    process_csv_to_parquet(csv_input_path=CSV_PATH)
  else:
    print("✅ Dataset Parquet base listo en la carpeta 'data/'.")

  df_1k = pd.read_parquet(target_file)

  # -------------------------------------------------------------------------
  # ETAPA 0: Ingesta y Buffer (Stream de entrada)
  # -------------------------------------------------------------------------
  print('\n[ETAPA 0] Inyectando stream de eventos al Buffer FIFO...')
  buffer = IngestaBufferFIFO(capacity=5000)
  for _, row in df_1k.iterrows():
    buffer.enqueue(row.to_dict())
  print(f'✅ Eventos totales en Buffer FIFO: {buffer.size()}')

  # Extraemos un lote masivo del buffer para que inicie su viaje por el pipeline
  lote_crudo = [buffer.dequeue() for _ in range(100)]

  # -------------------------------------------------------------------------
  # ETAPA 1: Preprocesamiento por lotes (Limpieza y Deduplicación)
  # -------------------------------------------------------------------------
  print(
      '\n[ETAPA 1] Limpiando y deduplicando lote con Timsort + Búsqueda'
      ' Binaria...'
  )
  preproc = PreprocesamientoLotes()
  lote_dedup = preproc.deduplicate_batch(lote_crudo)
  lote_ordenado = preproc.sort_batch(lote_dedup, key='event_time')
  print(
      f'✅ Lote limpio y ordenado: {len(lote_ordenado)} eventos listos para'
      ' priorizar.'
  )

  # -------------------------------------------------------------------------
  # ETAPA 2: Cola con Prioridades (MinHeap)
  # -------------------------------------------------------------------------
  print('\n[ETAPA 2] Encolando eventos limpios en la MinHeap de prioridades...')
  cola_prioridad = ColaPrioridadesHeap()
  for ev in lote_ordenado:
    # Asignamos una prioridad sintética basada en el tipo de evento para la demo
    ev['priority'] = 1 if ev.get('event_type') == 'purchase' else 3
    cola_prioridad.push(ev)
  print(f'✅ Eventos en Cola de Prioridades: {cola_prioridad.size()}')

  # Extraemos los eventos ordenados por su prioridad de negocio
  lote_priorizado = [cola_prioridad.pop() for _ in range(len(lote_ordenado))]

  # -------------------------------------------------------------------------
  # ETAPA 3: Almacenamiento, Lookup y Caché LRU
  # -------------------------------------------------------------------------
  print(
      '\n[ETAPA 3] Almacenando eventos priorizados y evaluando Caché LRU de'
      ' consultas...'
  )
  storage_cache = AlmacenamientoLookupCache(capacity=20)
  for ev in lote_priorizado:
    storage_cache.put_storage(ev['event_id'], ev)

  # Probamos el flujo de caché con un evento real que acaba de pasar por el pipeline
  sample_event = lote_priorizado[0]
  sample_id = sample_event['event_id']
  _, status1 = storage_cache.get_from_cache_or_storage(
      sample_id
  )  # Primer acceso (MISS)
  _, status2 = storage_cache.get_from_cache_or_storage(
      sample_id
  )  # Segundo acceso (HIT)
  print(
      f'✅ Evento real ID {sample_id} consultado -> 1er acceso: {status1}, 2do'
      f' acceso (Caché): {status2}'
  )

  # -------------------------------------------------------------------------
  # ETAPA 4: Indexación Principal (Árbol B+ en disco)
  # -------------------------------------------------------------------------
  print('\n[ETAPA 4] Indexando IDs de los eventos procesados en Árbol B+...')
  b_plus = IndexacionPrincipalBPlusTree(t=2)
  for ev in lote_priorizado[:30]:
    # Indexamos usando el event_id numérico o hash como clave de disco
    numeric_key = abs(hash(ev['event_id'])) % 100000
    b_plus.insert(numeric_key, {'event_type': ev['event_type']})
  print(
      '✅ Índices de eventos en disco actualizados correctamente en el Árbol'
      ' B+.'
  )

  # -------------------------------------------------------------------------
  # ETAPA 5: Índice Multidimensional y KNN Exacto (KD-Tree)
  # -------------------------------------------------------------------------
  print(
      '\n[ETAPA 5] Alimentando KD-Tree con atributos numéricos (precio, user_id)'
      ' de los eventos...'
  )
  points = [
      (float(ev['price']), int(ev['user_id']))
      for ev in lote_priorizado
      if ev.get('price') is not None
  ]
  data_ids = [ev['event_id'] for ev in lote_priorizado if ev.get('price')]

  if points:
    kd_tree = IndiceMultidimensionalKDTree(points, data_ids)
    query_pt = points[0]  # Tomamos el primer punto del stream como consulta
    knn_res = kd_tree.knn_exact_baseline(
        query_pt, points, data_ids, k_neighbors=2
    )
    print(
        f'✅ Vecinos más cercanos (KNN Baseline) basados en el stream real:'
        f' {knn_res}'
    )

  # -------------------------------------------------------------------------
  # ETAPA 6 [FIJA]: Filtrado Probabilístico (Bloom Filter & Count-Min Sketch)
  # -------------------------------------------------------------------------
  print(
      '\n[ETAPA 6] Filtrando duplicados y estimando frecuencias de productos'
      ' con estructuras probabilísticas...'
  )
  bloom = BloomFilter(capacity=1000)
  cms = CountMinSketch()

  for ev in lote_priorizado:
    prod_id = str(ev.get('product_id'))
    bloom.add(prod_id)
    cms.add(prod_id)

  test_prod = str(lote_priorizado[0].get('product_id'))
  print(
      f'✅ ¿Producto {test_prod} detectado en Bloom Filter?:'
      f' {test_prod in bloom}'
  )
  print(
      f'✅ Frecuencia estimada en streaming (Count-Min Sketch) para'
      f' {test_prod}: {cms.estimate_frequency(test_prod)}'
  )

  # -------------------------------------------------------------------------
  # ETAPA 7 [FIJA]: Similitud Aproximada (MinHash+LSH y HNSW)
  # -------------------------------------------------------------------------
  print(
      '\n[ETAPA 7] Calculando similitud de canastas (MinHash) y vectores'
      ' (HNSW) sobre los datos del stream...'
  )
  lsh_sim = MinHashLSHSimulator(num_perm=32)
  # Agrupamos productos por usuario simulando canastas reales del stream
  user_baskets = {}
  for ev in lote_priorizado:
    uid = ev['user_id']
    user_baskets.setdefault(uid, set()).add(ev['product_id'])

  u_ids = list(user_baskets.keys())
  if len(u_ids) >= 2:
    sig1 = lsh_sim.compute_signature(user_baskets[u_ids[0]])
    sig2 = lsh_sim.compute_signature(user_baskets[u_ids[1]])
    jaccard_est = lsh_sim.jaccard_similarity_estimate(sig1, sig2)
    print(
        f'✅ Similitud de Jaccard estimada entre canastas de usuarios'
        f' {u_ids[0]} y {u_ids[1]}: {jaccard_est:.2f}'
    )

  hnsw = HNSWApproximateSearch()
  for i, ev in enumerate(lote_priorizado[:15]):
    # Simulamos embeddings vectoriales a partir del precio y categoría del evento real
    vec = [float(ev.get('price', 0)) / 100.0, float(i) / 15.0]
    hnsw.add_vector(ev['event_id'], vec)

  if lote_priorizado:
    query_vec = [float(lote_priorizado[0].get('price', 0)) / 100.0, 0.0]
    neighbors_hnsw = hnsw.search_knn(query_vec, k=1)
    print(
        f'✅ Búsqueda ANN (HNSW) sobre vectores del catálogo real:adores'
        f' {neighbors_hnsw}'
    )

  # -------------------------------------------------------------------------
  # ETAPA 8 [FIJA]: Grafo de Dominio (PageRank y Comunidades)
  # -------------------------------------------------------------------------
  print(
      '\n[ETAPA 8] Construyendo grafo de dominio (Usuario -> Producto) y'
      ' ejecutando PageRank / Louvain...'
  )
  grafo = GrafoDominioRed()
  for ev in lote_priorizado:
    u_node = f"user_{ev['user_id']}"
    p_node = f"prod_{ev['product_id']}"
    grafo.add_edge(u_node, p_node, weight=1.0)

  pagerank_res = grafo.compute_pagerank()
  top_nodes = sorted(pagerank_res.items(), key=lambda x: x[1], reverse=True)[:3]
  print(f'✅ Top 3 nodos más influyentes de la red (PageRank): {top_nodes}')

  communities = grafo.detect_communities()
  print(
      f'✅ Comunidades detectadas en el grafo transaccional:'
      f' {len(communities)} clústeres'
  )

  # -------------------------------------------------------------------------
  # ETAPA 9: Servicio y Visualización (Trie & Top-K Analytics)
  # -------------------------------------------------------------------------
  print(
      '\n[ETAPA 9] Alimentando motor de búsqueda (Trie) y analíticas de'
      ' popularidad (Top-K Heap)...'
  )
  trie = TrieAutocompletado()
  top_k = TopKAnalytics(k=3)

  for ev in lote_priorizado:
    # Alimentamos el Top-K con los productos más populares del stream
    prod_id_str = str(ev['product_id'])
    top_k.add_item(prod_id_str)
    # Simulamos inserción de términos de búsqueda en el Trie
    trie.insert(f"producto {prod_id_str}")

  print(
      f'✅ Productos más populares del stream (Top-K Heap):'
      f' {top_k.get_top_k()}'
  )
  print(
      f'✅ Sugerencias de autocompletado (Trie) para prefijo "pro":'
      f' {trie.search_prefix("pro")[:3]}'
  )

  print(
      '\n🎉 ¡PIPELINE DE 10 ETAPAS EJECUTADO DE PUNTA A PUNTA CON DATOS'
      ' REALES DEL STREAM! 🎉'
  )


if __name__ == '__main__':
  run_integrated_pipeline()