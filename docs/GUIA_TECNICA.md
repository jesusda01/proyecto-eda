# Guía técnica del pipeline

## Datos y ejecución

El histórico registra vistas, carritos y compras; no contiene fraude confirmado ni identificadores de órdenes. `src/convert_to_parquet.py` lee el CSV por bloques de 50.000 filas y escribe los cortes Parquet. `src/stream_reader.py` entrega microlotes al mismo objeto `EcommercePipeline`, que conserva su estado entre lotes. Se reproduce incrementalmente un histórico finito.

El escenario controlado añade ocho eventos con órdenes explícitas y una cuenta señalada. Se contabiliza por separado y se desactiva con `--sin-escenario`. Una sesión no se interpreta como una orden: las compras históricas sin `order_id` no producen aristas de co-compra.

Los identificadores antiguos de ocho caracteres se conservan en `event_id_original` y se normalizan a SHA-256 de fuente y posición. Los identificadores modernos se conservan para detectar reenvíos. Las etiquetas históricas de fraude generadas al azar no se utilizan como verdad de referencia.

## Contratos entre etapas

| Etapa | Operación y salida | Coste principal |
| --- | --- | --- |
| 0 · FIFO | Recibe y desencola un microlote para validar | O(b) por lote; O(1) por operación |
| 1 · Preprocesamiento | Valida, deduplica y ordena para alimentar el heap | O(b log b) al ordenar; búsqueda binaria O(log n), más persistencia |
| 2 · Prioridad | Atiende por urgencia, fecha y llegada; entrega eventos al almacenamiento | O(b log b) |
| 3 · Hash y LRU | Persiste y recupera eventos para formar catálogo | O(1) esperado en hash/LRU; E/S adicional |
| 4 · B+ | Indexa `(precio, producto)`; devuelve candidatos por rango | O(log_f n + r) visitas a páginas |
| 5 · KD-Tree | Recupera vecinos y contrasta con búsqueda exacta para recomendaciones | La consulta puede degradar a O(nd) |
| 6 · Bloom y CMS | Verifica riesgo; entrega aceptados a similitud/grafo y genera alertas | O(k) Bloom y O(h) CMS; reconstrucción temporal adicional |
| 7 · LSH y HNSW | Actualiza canastas y vectores; entrega candidatos de similitud | Depende de firmas, candidatos, M y ef |
| 8 · Grafo | Agrega co-compras por orden, PageRank y Louvain; produce influencia y comunidades | PageRank O(i(V+E)); Louvain depende del grafo e iteraciones |
| 9 · Servicio | Combina señales; ofrece Top-K, recomendaciones y autocompletado | Trie O(longitud + salida); ranking depende de candidatos |

`b`: tamaño de lote; `n`: población; `d`: dimensiones; `f`: fanout; `r`: resultados; `k/h`: funciones hash; `i`: iteraciones. Las complejidades describen operaciones, no tiempos medidos.

Las colas se vacían al atender el lote. Eventos, identificadores vistos, páginas B+ y órdenes quedan en SQLite, en `data/runtime/<id>/`; catálogo, KD-Tree, HNSW, grafo y Trie permanecen en RAM. Los lotes evitan cargar todo el histórico de entrada, pero no garantizan memoria total constante.

El catálogo conserva el primer precio observado. El vector tiene 17 dimensiones: precio logarítmico normalizado y dos grupos de ocho buckets hash para categoría y marca. Puede presentar colisiones y no representa semántica de lenguaje.

## Familias avanzadas

**Bloom y Count-Min Sketch.** Bloom comparte bits y puede dar falsos positivos. CMS toma el mínimo entre varias filas de contadores y puede sobreestimar frecuencias. La clave es `user_id`, la ventana es de 300 segundos y el umbral es tres intentos. La cuenta señalada y el conteo se verifican con estado exacto antes de emitir una alerta de revisión. Una marca de agua y un heap temporal tratan eventos fuera de orden; al caducar registros se reconstruye el CMS. La confirmación exacta también consume memoria. Se comparan con `set` y `Counter`.

**MinHash y LSH.** Se calculan 64 mínimos hash por conjunto, en 16 bandas de cuatro filas. Las coincidencias de bandas producen candidatos y las firmas estiman Jaccard, con umbral 0,3. Reduce búsquedas a cambio de posibles omisiones. El sistema usa canastas por usuario; el benchmark usa conjuntos de interacciones por sesión. Se compara con Jaccard exhaustivo sobre las mismas consultas.

**HNSW.** Un grafo multicapa realiza descenso voraz y búsqueda de candidatos. Se utiliza M=8, ef de construcción=48 y ef de búsqueda=32. Construcción y búsqueda tienen costes distintos; el recall depende de parámetros y datos. Se compara con KNN exhaustivo. Es una implementación educativa propia, sin garantía universal de coste logarítmico.

**PageRank.** Propaga influencia mediante enlaces ponderados con amortiguación hasta convergencia. Se compara con grado mediante tiempo, memoria y coincidencia del Top-20. El resultado depende de la definición del grafo.

**Louvain.** Optimiza modularidad mediante movimientos locales y agregación. Depende de semilla, resolución y estructura; no garantiza un óptimo global. Se compara con propagación de etiquetas sobre el mismo grafo, registrando modularidad, tiempo, tamaños y estabilidad ARI respecto de la primera partición.

## Evaluación experimental

`benchmarks.py` registra corridas y resúmenes: media, desviación estándar, memoria Python actual/pico con `tracemalloc`, RSS antes/después, parámetros, plataforma, semilla y hashes del código medido. La preparación de entradas queda fuera de la región medida. RSS antes/después no equivale a pico nativo.

`--max-entities` limita conjuntos, vectores y estructuras base; Bloom/CMS y sus controles reciben todas las claves de eventos. El JSON distingue eventos, productos únicos, vectores, conjuntos, nodos, aristas y registros del grafo. El grafo experimental utiliza órdenes simuladas de tres productos consecutivos.

| Evidencia incluida | Repeticiones | Alcance |
| --- | --- | --- |
| `output/benchmarks/report.json` | 100 en 1K | 19 algoritmos; parámetros y población en el JSON |
| `output/escalabilidad/report.json` | 1 por escala | 10K, 100K, 1M y 10M; 300 vectores/conjuntos y grafo de 900 eventos |

Una sola escala se representa con barras comparativas. Las curvas log-log se interpretan junto con su población efectiva: 10M eventos no equivalen a 10M vectores. Para cubrir completamente la experimentación de la rúbrica se necesitan mediciones adicionales con las poblaciones exigidas y suficientes repeticiones en escalas mayores, según recursos disponibles.

Los tiempos del visor y el notebook incluyen instrumentación y persistencia; no sustituyen benchmarks por algoritmo. Las animaciones de los popovers explican algoritmos; el estado de cada etapa proviene de observaciones reales del motor.

## Fuentes

- [Datos de REES46](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
- Bloom (1970), [Space/time trade-offs in hash coding](https://doi.org/10.1145/362686.362692).
- Cormode y Muthukrishnan (2005), [The Count-Min sketch](https://doi.org/10.1016/j.jalgor.2003.12.001).
- Broder (1997), [On the resemblance and containment of documents](https://doi.org/10.1109/SEQUEN.1997.666900).
- Indyk y Motwani (1998), [Approximate nearest neighbors](https://doi.org/10.1145/276698.276876).
- Malkov y Yashunin (2020), [Hierarchical navigable small world graphs](https://doi.org/10.1109/TPAMI.2018.2889473).
- Brin y Page (1998), [The anatomy of a large-scale hypertextual Web search engine](https://doi.org/10.1016/S0169-7552(98)00110-X).
- Blondel et al. (2008), [Fast unfolding of communities](https://doi.org/10.1088/1742-5468/2008/10/P10008).
- Raghavan et al. (2007), [Near linear time algorithm to detect community structures](https://doi.org/10.1103/PhysRevE.76.036106).
- Implementaciones de referencia: [NetworkX](https://networkx.org/documentation/stable/), [scikit-learn](https://scikit-learn.org/stable/modules/neighbors.html), [hnswlib](https://github.com/nmslib/hnswlib) y [datasketch](https://github.com/ekzhu/datasketch). LSH y HNSW se implementan directamente en el proyecto.
