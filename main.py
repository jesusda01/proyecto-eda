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


def etapa_0_ingesta(target_file):
    print('\n[ETAPA 0] Inyectando stream de eventos al Buffer FIFO...')
    df_1k = pd.read_parquet(target_file)
    buffer = IngestaBufferFIFO(capacity=5000)
    for _, row in df_1k.iterrows():
        buffer.enqueue(row.to_dict())
    print(f'✅ Eventos totales en Buffer FIFO: {buffer.size()}')
    # Retorna lote crudo inicial
    return [buffer.dequeue() for _ in range(100)]


def etapa_1_preprocesamiento(lote_crudo):
    print('\n[ETAPA 1] Limpiando y deduplicando lote con Timsort + Búsqueda Binaria...')
    preproc = PreprocesamientoLotes()
    lote_dedup = preproc.deduplicate_batch(lote_crudo)
    lote_ordenado = preproc.sort_batch(lote_dedup, key='event_time')
    print(f'✅ Lote limpio y ordenado: {len(lote_ordenado)} eventos.')
    return lote_ordenado


def etapa_2_priorizacion(lote_ordenado):
    print('\n[ETAPA 2] Encolando eventos limpios en la MinHeap de prioridades...')
    cola_prioridad = ColaPrioridadesHeap()
    for ev in lote_ordenado:
        ev['priority'] = 1 if ev.get('event_type') == 'purchase' else 3
        cola_prioridad.push(ev)
    lote_priorizado = [cola_prioridad.pop() for _ in range(len(lote_ordenado))]
    print(f'✅ Eventos priorizados listos para almacenamiento: {len(lote_priorizado)}')
    return lote_priorizado


def etapa_3_almacenamiento(lote_priorizado):
    print('\n[ETAPA 3] Almacenando eventos priorizados y evaluando Caché LRU...')
    storage_cache = AlmacenamientoLookupCache(capacity=50)
    for ev in lote_priorizado:
        storage_cache.put_storage(ev['event_id'], ev)
    
    sample_id = lote_priorizado[0]['event_id']
    _, status1 = storage_cache.get_from_cache_or_storage(sample_id)
    _, status2 = storage_cache.get_from_cache_or_storage(sample_id)
    print(f'✅ Caché LRU validada -> 1er acceso: {status1}, 2do acceso: {status2}')
    # Retorna el lote intacto para la siguiente etapa
    return lote_priorizado


def etapa_4_indexacion(lote_priorizado):
    print('\n[ETAPA 4] Alimentando Árbol B+ con las claves primarias del lote...')
    b_plus = IndexacionPrincipalBPlusTree(t=2)
    for ev in lote_priorizado[:30]:
        numeric_key = abs(hash(ev['event_id'])) % 100000
        b_plus.insert(numeric_key, {'event_type': ev['event_type']})
    print('✅ Índices de disco actualizados en Árbol B+.')
    return lote_priorizado


def etapa_5_multidimensional(lote_priorizado):
    print('\n[ETAPA 5] Construyendo KD-Tree con atributos espaciales del lote...')
    points = [(float(ev['price']), int(ev['user_id'])) for ev in lote_priorizado if ev.get('price') is not None]
    data_ids = [ev['event_id'] for ev in lote_priorizado if ev.get('price')]
    if points:
        kd_tree = IndiceMultidimensionalKDTree(points, data_ids)
        knn_res = kd_tree.knn_exact_baseline(points[0], points, data_ids, k_neighbors=2)
        print(f'✅ Vecinos más cercanos (KNN Baseline) desde el KD-Tree: {knn_res}')
    return lote_priorizado


def etapa_6_filtrado(lote_priorizado):
    print('\n[ETAPA 6] Aplicando filtrado probabilístico (Bloom Filter & CMS)...')
    bloom = BloomFilter(capacity=1000)
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
    # Retorna el lote filtrado que servirá de entrada real para las etapas 7 y 8
    return lote_filtrado


def etapa_7_similitud(lote_filtrado):
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
    for i, ev in enumerate(lote_filtrado[:15]):
        hnsw.add_vector(ev['event_id'], [float(ev.get('price', 0)) / 100.0, float(i) / 15.0])
    
    if lote_filtrado:
        print(f'✅ Búsqueda ANN (HNSW) en lote filtrado: {hnsw.search_knn([float(lote_filtrado[0].get("price", 0)) / 100.0, 0.0], k=1)}')
    
    return lote_filtrado


def etapa_8_grafo(lote_filtrado):
    print('\n[ETAPA 8] Construyendo grafo de dominio con el flujo depurado...')
    grafo = GrafoDominioRed()
    for ev in lote_filtrado:
        grafo.add_edge(f"user_{ev['user_id']}", f"prod_{ev['product_id']}", weight=1.0)

    top_nodes = sorted(grafo.compute_pagerank().items(), key=lambda x: x[1], reverse=True)[:3]
    print(f'✅ Top 3 nodos influyentes (PageRank): {top_nodes}')
    print(f'✅ Comunidades detectadas en red transaccional: {len(grafo.detect_communities())} clústeres')
    
    return lote_filtrado


def etapa_9_visualizacion(lote_filtrado):
    print('\n[ETAPA 9] Alimentando motor de búsqueda (Trie) y analíticas (Top-K)...')
    trie = TrieAutocompletado()
    top_k = TopKAnalytics(k=3)

    for ev in lote_filtrado:
        prod_id_str = str(ev['product_id'])
        top_k.add_item(prod_id_str)
        trie.insert(f"producto {prod_id_str}")

    print(f'✅ Productos populares del stream procesado (Top-K): {top_k.get_top_k()}')
    print(f'✅ Autocompletado (Trie) para prefijo "pro": {trie.search_prefix("pro")[:3]}')


def run_integrated_pipeline():
    print('========================================================')
    print('--- PIPELINE INTEGRADO DE 10 ETAPAS (SECUENCIAL ESTRICTO) ---')
    print('========================================================')

    target_file = 'data/ecommerce_1K.parquet'
    if not os.path.exists(target_file) or os.path.getsize(target_file) == 0:
        process_csv_to_parquet(csv_input_path=CSV_PATH)
    else:
        print("✅ Dataset Parquet base listo en la carpeta 'data/'.")

    # FLUJO SECUENCIAL DE PUNTA A PUNTA (Data-driven pipeline)
    lote_crudo = etapa_0_ingesta(target_file)
    lote_ordenado = etapa_1_preprocesamiento(lote_crudo)
    lote_priorizado = etapa_2_priorizacion(lote_ordenado)
    
    lote_almacenado = etapa_3_almacenamiento(lote_priorizado)
    lote_indexado = etapa_4_indexacion(lote_almacenado)
    lote_espacial = etapa_5_multidimensional(lote_indexado)
    
    lote_filtrado = etapa_6_filtrado(lote_espacial)         # Puntos que pasan el filtro
    lote_similar = etapa_7_similitud(lote_filtrado)         # Usa el flujo filtrado
    lote_grafo = etapa_8_grafo(lote_similar)                # Alimenta el grafo de dominio
    
    etapa_9_visualizacion(lote_grafo)                       # Cierra el pipeline con analíticas

    print('\n🎉 ¡PIPELINE DE 10 ETAPAS EJECUTADO DE PUNTA A PUNTA EXITOSAMENTE! 🎉')


if __name__ == '__main__':
    run_integrated_pipeline()