# Pipeline de comercio electrónico

Proyecto de Estructuras de Datos y Algoritmos, UTEC 2026-2.
Procesa un histórico como un stream finito: lee microlotes, mantiene estado
entre lotes y produce recomendaciones explicadas, alertas verificadas y rankings.
El visor ejecuta exactamente ese pipeline; sus animaciones internas son didácticas.

## Empezar

Desde la raíz del proyecto, en PowerShell:

El entorno fijado requiere **Python 3.12 o superior** (excepto 3.14.1 por una
restricción de NetworkX). Se verificó con Python 3.14.4.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe main.py
```

Abre http://127.0.0.1:8765. La ejecución automática no tiene pausas. Para observar
cada paso, selecciona 2 s/etapa y pulsa **Ejecutar pipeline**. **Detener** conserva
los lotes ya guardados. La terminal permanece abierta para servir la página;
`Ctrl+C` cierra el servidor. Un visor viejo debe cerrarse antes de iniciar esta versión.

Para procesar sin web ni pausas:

```powershell
.\.venv\Scripts\python.exe main.py --sin-visor
.\.venv\Scripts\python.exe main.py --sin-visor --dataset data/ecommerce_10K.parquet --batch-size 1000 --sin-escenario
```

El dataset predeterminado tiene 1.000 eventos. Se procesan **todos**, no solo 100.
Por defecto se añaden **ocho eventos simulados separados** para mostrar dos órdenes
de co-compra y una cuenta con intentos repetidos. Se registran como `is_simulated=true`;
la demo recibe 1.008 eventos y genera dos alertas de revisión. No modifica las filas
originales. `--sin-escenario` desactiva esos ocho eventos. `--limit N` hace un recorte
explícito: no debe confundirse con evaluar el archivo completo.

## Datos y preparación

El CSV original contiene actividad histórica (vistas, carrito, compras), no fraude
confirmado ni órdenes explícitas. Sus columnas mínimas son `event_time`, `event_type`,
`product_id`, `user_id`, `price`. Categoría, marca y sesión son opcionales para preparar.

```powershell
.\.venv\Scripts\python.exe -m src.convert_to_parquet --csv 2019-Nov.csv --rows 1000
.\.venv\Scripts\python.exe -m src.convert_to_parquet --csv 2019-Nov.csv --rows 10000000
```

La preparación lee y escribe bloques de 50.000 filas. No acumula el CSV completo,
no marca fraude al azar y no genera millones de filas si falta la ruta. Escribe los
cortes completos que correspondan al objetivo: 1K, 10K, 100K, 1M, 10M. Si el CSV
termina antes, el nombre del archivo identifica el corte incompleto.

Los Parquet antiguos siguen siendo legibles. Al leer un ID antiguo de ocho caracteres,
se conserva en `event_id_original` y se deriva un ID SHA-256 de fuente/posición. Esto
evita sus colisiones sin reescribir 10 millones de filas. Los IDs modernos se conservan:
un reenvío con el mismo ID se deduplica. No se presenta la antigua etiqueta aleatoria
`is_fraud` como fraude confirmado. Para experimentos nuevos, conviene regenerar los
cortes con el preparador corregido.

Los CSV, Parquet y bases de ejecución no se suben a Git. Una clonación debe preparar
sus datos o recibir una muestra por separado. Noviembre es la fuente predeterminada;
octubre no se concatena silenciosamente. Fuente pública de referencia:
[eCommerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).

## Flujo y memoria

```text
Parquet por lotes
  -> 0 FIFO -> 1 validar/deduplicar -> 2 heap
  -> 3 guardar -> 4 indexar y consultar rango -> 5 KD-Tree
  -> 6 verificar riesgo -> 7 LSH/HNSW -> 8 co-compra/PageRank/Louvain
  -> 9 recomendaciones, consultas, alertas y popularidad
```

La salida de cada etapa se consume después. Por ejemplo, los IDs guardados alimentan
el catálogo; el rango y el KD generan candidatos; la verificación decide qué eventos
pueden alimentar similitud y grafo; el servicio combina esas señales en un ranking.

| Etapa | Estructura y propósito | Estado conservado |
| --- | --- | --- |
| 0 | `deque`: entrada pendiente | Solo un lote; se vacía al consumirlo |
| 1 | Ordenación, búsqueda binaria e historial exacto | IDs en SQLite y subconjunto ordenado acotado |
| 2 | Minheap estable: urgencia, fecha, llegada | Se vacía al atender el lote |
| 3 | Tabla hash reciente, LRU y eventos en SQLite | Eventos persistentes; cachés acotadas |
| 4 | B+ propio; SQLite guarda páginas por ID | Claves `(precio, producto)`, hojas enlazadas y metadatos |
| 5 | KD-Tree con poda y referencia exhaustiva | Catálogo de productos en memoria |
| 6 | Bloom empaquetado, CMS y ventana exacta | Últimos 300 s de intentos y cuentas señaladas |
| 7 | MinHash con bandas LSH y HNSW multicapa real | Vectores del catálogo y últimas 10.000 canastas de usuarios |
| 8 | Grafo ponderado producto-producto | Grafo en RAM; membresía de órdenes en SQLite |
| 9 | Trie, frecuencias y ranking combinado | Compras aceptadas, candidatos y señales |

Las ejecuciones escriben bases distintas en `data/runtime/<id>/`. No se borran
automáticamente: sirven para inspección y persistencia. Los resultados de la última
ejecución están en `output/results/last_pipeline.json`.

El catálogo conserva el **primer precio observado como referencia**, no inventario
actual de una tienda. Los precios siguientes permanecen en los eventos. El vector
tiene 17 dimensiones: precio logarítmico normalizado, ocho buckets para categoría
y ocho para marca; el hashing puede colisionar y no equivale a semántica de lenguaje.

## Riesgo y co-compra sin confundir simulación con realidad

La clave de riesgo es `user_id`, la ventana es de 300 segundos y el umbral es tres
intentos. Bloom pregunta si la cuenta podría estar señalada; un conjunto exacto
confirma esa condición. CMS estima la intensidad y el conteo exacto verifica el umbral.
Una alerta significa **revisar**, no acusar a una persona ni bloquear automáticamente.
Los eventos marcados para revisión quedan guardados, pero no actualizan recomendaciones.

La ventana admite eventos fuera de orden: usa una marca de agua (máxima fecha vista)
y un heap temporal. Un evento anterior al inicio de ventana se marca tardío. Cuando
caducan eventos, se reconstruye CMS a partir del conteo exacto; esa reconstrucción
tiene costo y no se declara O(1). La verificación exacta también consume memoria.

El grafo añade una pareja una vez por orden; nuevas órdenes aumentan su peso.
Solo se usan compras con `order_id` explícito. `user_session` **no se supone una orden**.
Por eso las compras históricas sin orden se cuentan pero no se convierten en co-compra.
El escenario separado suministra órdenes sintéticas declaradas para demostrarlo.

## Benchmarks y resultados

```powershell
# Diagnóstico rápido; no es la evaluación estadística final.
.\.venv\Scripts\python.exe benchmarks.py --sizes 1K --repetitions 3

# 100 repeticiones cuando son viables; 10M utiliza 5 por defecto.
.\.venv\Scripts\python.exe benchmarks.py --sizes 1K 10K 100K 1M 10M --max-entities 2000

# Sin recorte analítico: requiere evaluar tiempo y memoria del equipo.
.\.venv\Scripts\python.exe benchmarks.py --sizes 1K 10K --max-entities 0
```

Se comparan Bloom/set, CMS/Counter, MinHash+LSH/Jaccard, HNSW/KNN exacto,
PageRank/grado y Louvain/propagación de etiquetas, además de las estructuras base.
Cada corrida registra tiempo, memoria Python actual/pico, RSS antes/después,
calidad, parámetros, plataforma, semilla y población realmente usada. RSS antes/después
no se presenta como pico nativo. La preparación de entradas se excluye de los tiempos.

`--max-entities` limita conjuntos, vectores y estructuras base; el contador probabilístico
sí recibe todos los eventos del archivo. El JSON distingue eventos, vectores, conjuntos,
nodos, aristas y recortes. Un experimento recortado **no demuestra HNSW con 10M vectores**.
El grafo experimental utiliza órdenes simuladas de tres productos consecutivos, no
órdenes reales. La estabilidad compara las particiones con la primera repetición mediante
ARI; las semillas de comunidades se registran como 42 + repetición. Los controles exactos
usan las mismas consultas. No se inventan NDCG, aceleraciones ni cifras de bibliografía.

`output/benchmarks/` contiene JSON por dataset, corridas crudas, resúmenes y gráficos.
Las escalas grandes que aún no se hayan ejecutado deben declararse pendientes en la entrega.
No usar las pausas ni tiempos del visor como resultados de rendimiento.

## Pruebas

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Cubren B+ aleatorio y reapertura, rangos, LRU actualizada, deduplicación entre lotes,
prioridades, KD contra exacto, Bloom/CMS, caducidad y verificación, LSH, recall de
HNSW, co-compra y una salida integrada, además de pausa/parada/reinicio por HTTP.

## Notebooks

Instala el soporte de notebooks dentro del mismo entorno:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-notebooks.txt
```

En VS Code, abre `notebooks/01_pipeline_demo.ipynb` y selecciona `.venv` como
kernel. Ejecuta las celdas de arriba hacia abajo. El segundo notebook consulta
los resultados guardados; no lanza experimentos largos salvo que lo habilites.

## Organización

`main.py` inicia la ejecución; `src/pipeline.py` coordina; `src/stream_reader.py`
entrega lotes; `src/etapa_*.py` contiene las estructuras; `benchmarks.py` evalúa;
`dashboard.*` y `dashboard-popover.js` presentan; `notebooks/` explica y grafica;
`docs/` conserva metodología, informe y fuentes. El servidor es local y compartido,
no un servicio público multiusuario listo para producción.

## Responsabilidades declaradas por el equipo

| Integrante | Etapas asignadas | Responsabilidad que debe sustentar |
| --- | --- | --- |
| Jesús Delgado | 0, 1, 2 | Lectura incremental, validación, deduplicación y prioridad |
| Erik Poma | 3, 4 | Almacenamiento/caché e índice B+ persistente y consultas |
| Mentor Naquiche | 5, 6 | KD-Tree, exacto, filtros, ventana y verificación |
| Guillermo Calderón | 7, 8, 9 | LSH, HNSW, grafo, comunidades y servicio |

Esta tabla expresa la asignación del README original, no certifica autoría de cambios
hechos posteriormente con asistencia. Registrar y explicar las contribuciones reales;
no atribuir retrospectivamente commits o código a personas sin evidencia.
