# Pipeline de comercio electrónico

Proyecto de Estructuras de Datos y Algoritmos · Maestría en Ciencia de Datos e Inteligencia Artificial · UTEC 2026-2.

Procesa eventos de navegación y compra en microlotes a través de diez etapas. Mantiene índices entre lotes y produce recomendaciones, alertas verificadas, autocompletado y rankings. El visor web muestra las entradas y salidas de cada etapa durante la ejecución.

## Instalación y ejecución

Requiere Python 3.12 o superior, salvo 3.14.1 por una restricción de NetworkX. En PowerShell, desde la raíz:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Descarga el CSV de [eCommerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store), guárdalo en `data/raw/` y prepara los datos. También puedes indicar otra ruta con `--csv`:

```powershell
python -m src.convert_to_parquet --csv data/raw/2019-Nov.csv --rows 1000
python main.py
```

Abre **http://127.0.0.1:8765**. Selecciona **0,8 s/etapa** y pulsa **Ejecutar pipeline** para seguir el flujo. **Detener** interrumpe el procesamiento; `Ctrl+C` cierra el servidor. Cada ejecución guarda su salida en `output/results/last_pipeline.json`.

Por defecto se leen los 1.000 eventos completos en lotes de 500 y se añaden ocho eventos simulados identificados con `is_simulated`, para demostrar co-compra y alertas. Para procesar solo el histórico, sin web:

```powershell
python main.py --sin-visor --sin-escenario
```

## Notebooks

En VS Code selecciona el kernel **Python de `.venv`** y ejecuta las celdas en orden.

| Notebook | Para qué sirve |
| --- | --- |
| [01 · Demostración del pipeline](notebooks/01_pipeline_demo.ipynb) | Ejecuta el sistema; permite elegir lote y etapa, inspeccionar datos y consultar recomendaciones y alertas. |
| [02 · Evaluación experimental](notebooks/02_benchmarks.ipynb) | Presenta tiempos, memoria, calidad y escalabilidad de los resultados guardados, con tablas y gráficos comparativos. |

## Experimentos

La preparación de 10M genera también los cortes de 1K, 10K, 100K y 1M. Los CSV y Parquet permanecen fuera de Git.

```powershell
python -m src.convert_to_parquet --csv data/raw/2019-Nov.csv --rows 10000000
python benchmarks.py --sizes 1K 10K 100K 1M 10M --max-entities 2000
python -m unittest discover -s tests -v
```

Los resultados incluidos contienen **100 repeticiones en 1K** y una exploración de las escalas mayores con **una repetición y poblaciones analíticas acotadas**. Esta última no acredita una evaluación completa de todas las estructuras con 10M elementos. El notebook muestra los tamaños efectivos y las limitaciones. El benchmark mide sin las pausas del visor.

## Estructura y entregables

```text
main.py                 Entrada del sistema y del visor
benchmarks.py           Experimentos por estructura y familia
requirements.txt        Dependencias del sistema y los notebooks
src/                    Orquestación, lectura y diez etapas
web/                    Servidor local y frontend en static/
notebooks/              Demostración y análisis experimental
tests/                  Pruebas de estructuras, integración y servidor
docs/                   Informe PDF, presentación, diagrama y guía técnica
output/benchmarks/      Mediciones de 1K y gráficos
output/escalabilidad/    Exploración de las escalas mayores
data/                   Datos y bases locales, excluidos de Git
```

Entregables: [informe PDF](docs/Proyecto_Final_Grupo4.pdf), [presentación](docs/Presentacion_Grupo4.pptx) y [diagrama del pipeline](docs/pipeline_architecture.png). La [guía técnica](docs/GUIA_TECNICA.md) explica el flujo, las estructuras, el método experimental y las fuentes.

## Responsabilidades del equipo

| Integrante | Etapas | Aporte asignado |
| --- | --- | --- |
| Jesús Ricardo Delgado Alvarado | 0, 1, 2 | Ingesta, validación, deduplicación y prioridades |
| Erik Rogers Poma García | 3, 4 | Almacenamiento, caché LRU e índice B+ persistente |
| Mentor Alexis Naquiche Ventura | 5, 6 | KD-Tree, referencia exacta y filtrado probabilístico |
| Guillermo Diego Calderón Canchanya | 7, 8, 9 | Similitud aproximada, grafo y servicio de resultados |

Docente: Heider Sánchez Enríquez.
