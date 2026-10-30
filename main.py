import os
import time
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

# ⚙️ CONFIGURACIÓN DEL DATASET A EVALUAR (Cambiar aquí según se requiera: '1K', '10K', '100K', '1M', '10M')
DATASET_SIZE_STR = '1K'


def etapa_0_ingesta(target_file):
    start_time = time.perf_counter()
    print('\n[ETAPA 0] Inyectando stream de eventos al Buffer FIFO...')
    df_data = pd.read_parquet(target_file)
    buffer = IngestaBufferFIFO(capacity=len(df_data))
    for _, row in df_data.iterrows():
        buffer.enqueue(row.to_dict())
    print(f'✅ Eventos totales en Buffer FIFO: {buffer.size()}')
    
    lote_crudo = [buffer.dequeue() for _ in range(buffer.size())]
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 0: {elapsed:.2f} ms')
    return lote_crudo


def etapa_1_preprocesamiento(lote_crudo):
    start_time = time.perf_counter()
    print('\n[ETAPA 1] Limpiando y deduplicando lote con Timsort + Búsqueda Binaria...')
    preproc = PreprocesamientoLotes()
    lote_dedup = preproc.deduplicate_batch(lote_crudo)
    lote_ordenado = preproc.sort_batch(lote_dedup, key='event_time')
    print(f'✅ Lote limpio y ordenado: {len(lote_ordenado)} eventos.')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 1: {elapsed:.2f} ms')
    return lote_ordenado


def etapa_2_priorizacion(lote_ordenado):
    start_time = time.perf_counter()
    print('\n[ETAPA 2] Encolando eventos limpios en la MinHeap de prioridades...')
    cola_prioridad = ColaPrioridadesHeap()
    for ev in lote_ordenado:
        ev['priority'] = 1 if ev.get('event_type') == 'purchase' else 3
        cola_prioridad.push(ev)
    lote_priorizado = [cola_prioridad.pop() for _ in range(len(lote_ordenado))]
    print(f'✅ Eventos priorizados listos para almacenamiento: {len(lote_priorizado)}')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 2: {elapsed:.2f} ms')
    return lote_priorizado


def etapa_3_almacenamiento(lote_priorizado):
    start_time = time.perf_counter()
    print('\n[ETAPA 3] Almacenando eventos priorizados y evaluando Caché LRU...')
    storage_cache = AlmacenamientoLookupCache(capacity=5000)
    for ev in lote_priorizado:
        storage_cache.put_storage(ev['event_id'], ev)
    
    if lote_priorizado:
        sample_id = lote_priorizado[0]['event_id']
        _, status1 = storage_cache.get_from_cache_or_storage(sample_id)
        _, status2 = storage_cache.get_from_cache_or_storage(sample_id)
        print(f'✅ Caché LRU validada -> 1er acceso: {status1}, 2do acceso: {status2}')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 3: {elapsed:.2f} ms')
    return lote_priorizado


def etapa_4_indexacion(lote_priorizado):
    start_time = time.perf_counter()
    print('\n[ETAPA 4] Alimentando Árbol B+ con las claves primarias del lote...')
    b_plus = IndexacionPrincipalBPlusTree(t=2)
    for ev in lote_priorizado[:min(30, len(lote_priorizado))]:
        numeric_key = abs(hash(ev['event_id'])) % 100000
        b_plus.insert(numeric_key, {'event_type': ev['event_type']})
    print('✅ Índices de disco actualizados en Árbol B+.')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 4: {elapsed:.2f} ms')
    return lote_priorizado


def etapa_5_multidimensional(lote_priorizado):
    start_time = time.perf_counter()
    print('\n[ETAPA 5] Construyendo KD-Tree con atributos espaciales del lote...')
    points = [(float(ev['price']), int(ev['user_id'])) for ev in lote_priorizado if ev.get('price') is not None]
    data_ids = [ev['event_id'] for ev in lote_priorizado if ev.get('price')]
    if points:
        kd_tree = IndiceMultidimensionalKDTree(points, data_ids)
        knn_res = kd_tree.knn_exact_baseline(points[0], points, data_ids, k_neighbors=2)
        print(f'✅ Vecinos más cercanos (KNN Baseline) desde el KD-Tree: {knn_res}')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 5: {elapsed:.2f} ms')
    return lote_priorizado


def etapa_6_filtrado(lote_priorizado):
    start_time = time.perf_counter()
    print('\n[ETAPA 6] Aplicando filtrado probabilístico (Bloom Filter & CMS)...')
    bloom = BloomFilter(capacity=max(len(lote_priorizado), 1000))
    cms = CountMinSketch()

    lote_filtrado = []
    for ev in lote_priorizado:
        prod_id = str(ev.get('product_id'))
        is_duplicate = prod_id in bloom
        bloom.add(prod_id)
        cms.add(prod_id)
        if not is_duplicate:
            lote_filtrado.append(ev)

    print(f'✅ Eventos antes del filtro: {len(lote_priorizado)} -> Tras filtrado: {len(lote_filtrado)}')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 6: {elapsed:.2f} ms')
    return lote_filtrado


def etapa_7_similitud(lote_filtrado):
    start_time = time.perf_counter()
    print('\n[ETAPA 7] Procesando similitud sobre el flujo filtrado (LSH y HNSW)...')
    lsh_sim = MinHashLSHSimulator(num_perm=32)
    user_baskets = {}
    for ev in lote_filtrado:
        uid = ev['user_id']
        user_baskets.setdefault(uid, set()).add(ev['product_id'])

    u_ids = list(user_baskets.keys())
    if len(u_ids) >= 2:
        sig1 = lsh_sim.compute_signature(user_baskets[u_ids[0]])
        sig2 = lsh_sim.compute_signature(user_baskets[u_ids[1]])
        print(f'✅ Similitud Jaccard estimada entre usuarios: {lsh_sim.jaccard_similarity_estimate(sig1, sig2):.2f}')

    hnsw = HNSWApproximateSearch()
    for i, ev in enumerate(lote_filtrado[:min(15, len(lote_filtrado))]):
        hnsw.add_vector(ev['event_id'], [float(ev.get('price', 0)) / 100.0, float(i) / max(15, len(lote_filtrado))])
    
    if lote_filtrado:
        print(f'✅ Búsqueda ANN (HNSW) en lote filtrado: {hnsw.search_knn([float(lote_filtrado[0].get("price", 0)) / 100.0, 0.0], k=1)}')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 7: {elapsed:.2f} ms')
    return lote_filtrado


def etapa_8_grafo(lote_filtrado):
    start_time = time.perf_counter()
    print('\n[ETAPA 8] Construyendo grafo de dominio con el flujo depurado...')
    grafo = GrafoDominioRed()
    for ev in lote_filtrado:
        grafo.add_edge(f"user_{ev['user_id']}", f"prod_{ev['product_id']}", weight=1.0)

    top_nodes = sorted(grafo.compute_pagerank().items(), key=lambda x: x[1], reverse=True)[:3]
    print(f'✅ Top 3 nodos influyentes (PageRank): {top_nodes}')
    print(f'✅ Comunidades detectadas en red transaccional: {len(grafo.detect_communities())} clústeres')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 8: {elapsed:.2f} ms')
    return lote_filtrado


def etapa_9_visualizacion(lote_filtrado):
    start_time = time.perf_counter()
    print('\n[ETAPA 9] Alimentando motor de búsqueda (Trie) y analíticas (Top-K)...')
    trie = TrieAutocompletado()
    top_k = TopKAnalytics(k=3)

    for ev in lote_filtrado:
        prod_id_str = str(ev['product_id'])
        top_k.add_item(prod_id_str)
        trie.insert(f"producto {prod_id_str}")

    print(f'✅ Productos populares del stream procesado (Top-K): {top_k.get_top_k()}')
    print(f'✅ Autocompletado (Trie) para prefijo "pro": {trie.search_prefix("pro")[:3]}')
    
    elapsed = (time.perf_counter() - start_time) * 1000
    print(f'⏱️ Tiempo Etapa 9: {elapsed:.2f} ms')


def run_integrated_pipeline():
    total_start_time = time.perf_counter()
    
    print('========================================================')
    print(f'--- PIPELINE INTEGRADO DE 10 ETAPAS ({DATASET_SIZE_STR}) ---')
    print('========================================================')

    target_file = f'data/ecommerce_{DATASET_SIZE_STR}.parquet'
    if not os.path.exists(target_file) or os.path.getsize(target_file) == 0:
        print(f"⚠️ El archivo {target_file} no existe. Generando desde CSV...")
        process_csv_to_parquet(csv_input_path=CSV_PATH)
    else:
        print(f"✅ Dataset Parquet ({DATASET_SIZE_STR}) listo en la carpeta 'data/'.")

    # Obtener número total de registros en crudo
    df_temp = pd.read_parquet(target_file)
    total_registros = len(df_temp)

    # FLUJO SECUENCIAL DE PUNTA A PUNTA (Data-driven pipeline)
    lote_crudo = etapa_0_ingesta(target_file)
    lote_ordenado = etapa_1_preprocesamiento(lote_crudo)
    lote_priorizado = etapa_2_priorizacion(lote_ordenado)
    
    lote_almacenado = etapa_3_almacenamiento(lote_priorizado)
    lote_indexado = etapa_4_indexacion(lote_almacenado)
    lote_espacial = etapa_5_multidimensional(lote_indexado)
    
    lote_filtrado = etapa_6_filtrado(lote_espacial)          
    lote_similar = etapa_7_similitud(lote_filtrado)          
    lote_grafo = etapa_8_grafo(lote_similar)                 
    
    etapa_9_visualizacion(lote_grafo)                        

    total_elapsed_ms = (time.perf_counter() - total_start_time) * 1000
    total_elapsed_sec = total_elapsed_ms / 1000

    print('\n========================================================')
    print(f'🎉 ¡PIPELINE EJECUTADO EXITOSAMENTE!')
    print(f'📊 Dataset Base Evaluado: {DATASET_SIZE_STR} ({total_registros:,} registros en crudo)')
    print(f'🔍 Registros Únicos post-filtrado (Etapa 6): {len(lote_filtrado):,} registros pasaron limpios')
    print(f'⏱️ Tiempo Total de Ejecución: {total_elapsed_ms:.2f} ms ({total_elapsed_sec:.2f} segundos)')
    print('========================================================')


if __name__ == '__main__':
    run_integrated_pipeline()