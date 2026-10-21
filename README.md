# Proyecto Final - Estructuras de Datos y Algoritmos (EDA)

Pipeline de procesamiento, indexación y análisis de alto rendimiento para datos masivos e-commerce optimizado en memoria RAM mediante algoritmos avanzados y estructuras de datos eficientes.

---

## 🛠️ Tecnologías y Herramientas Utilizadas

- **Lenguaje:** Python 3.x
- **Formato de Almacenamiento:** Apache Parquet / CSV
- **Procesamiento y Álgebra Lineal:** NumPy, pandas
- **Búsqueda Vectorial y Grafos:** NetworkX, HNSWlib, Datasketch
- **Visualización y Cuadernos:** Jupyter Notebooks, Matplotlib

---

## 📋 Arquitectura del Pipeline y Responsables por Etapa

A continuación se detalla la distribución de responsabilidades por integrante, de acuerdo a la arquitectura del pipeline de 10 etapas (Etapas 0 a 9):

| Etapa | Nombre de la Etapa | Estructuras de Datos / Algoritmos Aplicados | Responsable Principal |
| :---: | :--- | :--- | :--- |
| **0** | **Ingesta & Buffer de Memoria** | Circular Queue / Ring Buffer | Jesús Delgado |
| **1** | **Preprocesamiento por Lotes** | Timsort / MergeSort ($O(N \log N)$), Búsqueda Binaria en C | Jesús Delgado |
| **2** | **Cola con Prioridades** | MinHeap / Binary Heap ($O(\log N)$) | Jesús Delgado |
| **3** | **Almacenamiento & Lookup Caché** | Tabla Hash $O(1)$ + Caché LRU (`OrderedDict`) | Erick Poma |
| **4** | **Indexación Principal** | Árbol B+ (B+ Tree) | Erick Poma |
| **5** | **Índice Multidimensional** | Árbol k-d (k-d Tree) | Mentor Naquiche |
| **6** | **Filtrado Probabilístico** | Filtro de Bloom & Count-Min Sketch | Mentor Naquiche |
| **7** | **Similitud Aproximada** | HNSW (Hierarchical Navigable Small World) & MinHash LSH | Guillermo Calderón |
| **8** | **Grafo de Dominio** | Redes de Recomendación (Grafos con NetworkX / Dijkstra / PageRank) | Guillermo Calderón |
| **9** | **Servicio & Visualización** | Árbol Trie (Autocompletado) & Top-K Analytics | Guillermo Calderón |

---

## 👥 Tabla de Roles y Contribución Individual

| Integrante | Rol / Etapas Asignadas | Aporte Concreto en el Proyecto |
| :--- | :--- | :--- |
| **Jesús Delgado** | Responsable Etapas 0, 1 y 2 | Implementación del Buffer Circular de Ingesta, optimización de limpieza de duplicados por lotes con Timsort/Búsqueda Binaria en C/NumPy, y la Cola de Prioridades basada en MinHeap para la criticidad de eventos. |
| **Erick Poma** | Responsable Etapas 3 y 4 | Desarrollo del almacenamiento primario en Tabla Hash acoplado a un Caché LRU, e implementación de la indexación de búsquedas por rango utilizando Árboles B+. |
| **Mentor Naquiche** | Responsable Etapas 5 y 6 | Construcción del índice multidimensional k-d Tree para consultas espaciales/características y de los filtros probabilísticos (Filtro de Bloom y Count-Min Sketch) para detección de frecuencia y pertenencia. |
| **Guillermo Calderón** | Responsable Etapas 7, 8 y 9 | Implementación de algoritmos de similitud aproximada (HNSW y MinHash LSH), modelado de la red de recomendación basada en Grafos, y desarrollo del Árbol Trie para autocompletado y métricas Top-K. |

---

## 🚀 Instrucciones de Ejecución

### 1. Requisitos Previos e Instalación de Dependencias

Asegúrate de contar con Python 3.10+ instalado. Activa tu entorno virtual y ejecuta:

```bash
pip install -r requirements.txt