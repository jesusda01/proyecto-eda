"""Experimentos reproducibles por familia, fuera del visor y sus pausas.

Ejemplo rápido: python benchmarks.py --sizes 1K --repetitions 3
La evaluación final usa 100 repeticiones cuando es viable. --max-entities 0
desactiva el recorte analítico. El JSON siempre declara la población real:
eventos, conjuntos, vectores, nodos y aristas no significan lo mismo.
"""

import argparse
from collections import Counter, defaultdict
import json
import platform
from pathlib import Path
import random
import statistics
import time
import tracemalloc
import tempfile
import itertools
from datetime import datetime
from zoneinfo import ZoneInfo
import importlib.metadata
from hashlib import sha256

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pyarrow.parquet as pq
from sklearn.metrics import adjusted_rand_score

from src.etapa_6_filtrado_probabilistico import BloomFilter, CountMinSketch
from src.etapa_7_similitud_aproximada import HNSWApproximateSearch, MinHashLSHSimulator
from src.etapa_5_indice_multidimensional import IndiceMultidimensionalKDTree
from src.stream_reader import product_vector
from src.etapa_0_ingesta_buffer import IngestaBufferFIFO
from src.etapa_1_preprocesamiento_lotes import PreprocesamientoLotes
from src.etapa_2_cola_prioridades import ColaPrioridadesHeap
from src.etapa_3_almacenamiento_lookup_cache import AlmacenamientoLookupCache
from src.etapa_4_indexacion_principal import IndexacionPrincipalBPlusTree
from src.etapa_9_servicio_visualizacion import TrieAutocompletado, TopKAnalytics

SEED = 42


def measured(function):
    """Mide asignaciones Python; RSS queda registrado aparte, no sumado."""
    import psutil

    process = psutil.Process()
    rss_before = process.memory_info().rss
    tracemalloc.start()
    start = time.perf_counter()
    try:
        result = function()
        elapsed = time.perf_counter() - start
        current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    rss_after = process.memory_info().rss
    result.pop("_retained", None)
    return {
        "seconds": elapsed,
        "current_bytes": current,
        "peak_bytes": peak,
        "rss_before": rss_before,
        "rss_after": rss_after,
        **result,
    }


def load_workload(path, max_entities):
    """Lee solo columnas necesarias; el recorte de entidades queda explícito."""
    keys = pq.read_table(path, columns=["product_id"])["product_id"].to_numpy()
    catalog, baskets = {}, defaultdict(set)
    graph = nx.Graph()
    offset = 0
    cap = max_entities or len(keys)
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=5000, columns=["product_id", "price", "brand", "category_code", "user_session"]
    ):
        for event in batch.to_pylist():
            pid = str(event["product_id"])
            if len(catalog) < cap or pid in catalog:
                if (
                    event["price"] is not None
                    and np.isfinite(event["price"])
                    and event["price"] >= 0
                ):
                    catalog.setdefault(pid, event)
            session = str(event.get("user_session") or f"unknown-{offset}")
            if len(baskets) < cap or session in baskets:
                baskets[session].add(pid)
            offset += 1
        if max_entities and len(catalog) >= cap and len(baskets) >= cap:
            break
    ids = list(catalog)
    vectors = [product_vector(catalog[i]) for i in ids]
    sets = dict(baskets)
    # El CSV no contiene order_id: este grafo de benchmark usa órdenes
    # SIMULADAS de tres productos. No se presenta como co-compra real.
    graph_events = len(keys) if not max_entities else min(len(keys), max_entities * 3)
    for i in range(0, graph_events - 2, 3):
        products = list(dict.fromkeys(str(v) for v in keys[i : i + 3]))
        graph.add_nodes_from(products)
        for a in range(len(products)):
            for b in range(a + 1, len(products)):
                u, v = products[a], products[b]
                graph.add_edge(u, v, weight=graph.get_edge_data(u, v, {}).get("weight", 0) + 1)
    return keys, ids, vectors, sets, graph, offset, graph_events


def evaluate(path, repetitions, max_entities=2000, queries=10):
    keys, ids, vectors, sets, graph, rows_read, graph_events = load_workload(path, max_entities)
    unique = np.unique(keys)
    query_ids = ids[:queries]
    query_vectors = vectors[:queries]
    absent = [f"absent:{i}" for i in range(max(1000, len(unique)))]
    raw = []
    for repetition in range(repetitions):
        if repetition % 10 == 0:
            print(f"  Corrida {repetition+1}/{repetitions}", flush=True)

        # Misma carga de trabajo y semilla por repetición; cambia solo la semilla
        # de comunidades para medir estabilidad, que se registra por separado.
        def bloom_run():
            bloom = BloomFilter(max(1, len(unique)))
            for item in keys:
                bloom.add(item)
            start = time.perf_counter()
            fp = sum(item in bloom for item in absent)
            fn = sum(item not in bloom for item in unique)
            return {
                "query_seconds": time.perf_counter() - start,
                "fpr": fp / len(absent),
                "false_negatives": fn,
                "_retained": bloom,
                "m_bits": bloom.size,
                "k_hashes": bloom.hash_count,
            }

        def set_run():
            exact = set(keys)
            start = time.perf_counter()
            for item in itertools.chain(unique, absent):
                _ = item in exact
            return {
                "query_seconds": time.perf_counter() - start,
                "unique": len(exact),
                "_retained": exact,
            }

        exact_counts = Counter(keys)

        def cms_run():
            cms = CountMinSketch(width=max(2000, len(unique) * 2))
            for item in keys:
                cms.add(item)
            errors = [cms.estimate_frequency(item) - count for item, count in exact_counts.items()]
            return {
                "mean_absolute_error": float(np.mean(errors)) if errors else 0.0,
                "max_overestimate": max(errors, default=0),
                "width": cms.width,
                "depth": cms.depth,
                "_retained": cms,
            }

        def counter_run():
            counter = Counter()
            for item in keys:
                counter[item] += 1
            return {"unique": len(counter), "_retained": counter}

        for name, fn in [
            ("bloom", bloom_run),
            ("set", set_run),
            ("cms", cms_run),
            ("counter", counter_run),
        ]:
            raw.append(
                dict(family="probabilistic", algorithm=name, repetition=repetition, **measured(fn))
            )

        set_queries = list(sets.items())[:queries]

        def exact_sets():
            results = []
            for qid, basket in set_queries:
                matches = sorted(
                    (
                        (MinHashLSHSimulator.exact_jaccard(basket, other), identifier)
                        for identifier, other in sets.items()
                        if identifier != qid
                    ),
                    reverse=True,
                )
                results.append({identifier for score, identifier in matches if score >= 0.3})
            return results

        ground_sets = exact_sets()

        def lsh_run():
            index = MinHashLSHSimulator()
            for identifier, basket in sets.items():
                index.insert(identifier, basket)
            build_done = time.perf_counter()
            recalls, precisions, errors = [], [], []
            for (qid, basket), truth in zip(set_queries, ground_sets):
                found = {i for i, _ in index.query(basket, k=len(sets), exclude_id=qid)}
                recalls.append(len(found & truth) / len(truth) if truth else 1.0)
                precisions.append(
                    len(found & truth) / len(found) if found else (1.0 if not truth else 0.0)
                )
                for i in list(sets)[:10]:
                    errors.append(
                        abs(
                            index.jaccard_similarity_estimate(
                                index.compute_signature(basket), index.signatures[i]
                            )
                            - index.exact_jaccard(basket, sets[i])
                        )
                    )
            return {
                "query_seconds": time.perf_counter() - build_done,
                "recall": float(np.mean(recalls)) if recalls else 1.0,
                "precision": float(np.mean(precisions)) if precisions else 1.0,
                "signature_mae": float(np.mean(errors)) if errors else 0.0,
                "bands": index.bands,
                "num_perm": index.num_perm,
                "_retained": index,
            }

        def brute_sets():
            start = time.perf_counter()
            exact_sets()
            return {"query_seconds": time.perf_counter() - start}

        raw.append(
            dict(family="sets", algorithm="minhash_lsh", repetition=repetition, **measured(lsh_run))
        )
        raw.append(
            dict(
                family="sets",
                algorithm="jaccard_exact",
                repetition=repetition,
                **measured(brute_sets),
            )
        )

        truths = [
            IndiceMultidimensionalKDTree.knn_exact_baseline(q, vectors, ids, 5, qid)
            for qid, q in zip(query_ids, query_vectors)
        ]

        def ann_run():
            building = time.perf_counter()
            index = HNSWApproximateSearch(seed=SEED)
            for identifier, vector in zip(ids, vectors):
                index.add_vector(identifier, vector)
            build_seconds = time.perf_counter() - building
            start = time.perf_counter()
            found = [index.search_knn(q, 5, qid) for qid, q in zip(query_ids, query_vectors)]
            elapsed = time.perf_counter() - start
            recall = [len(set(a) & set(b)) / len(b) if b else 1.0 for a, b in zip(found, truths)]
            tie_recall = []
            by_id = dict(zip(ids, vectors))
            for q, found_ids, true_ids in zip(query_vectors, found, truths):
                distance = lambda identifier: sum(
                    (a - b) ** 2 for a, b in zip(q, by_id[identifier])
                )
                radius = max((distance(i) for i in true_ids), default=0.0)
                tie_recall.append(
                    sum(distance(i) <= radius + 1e-12 for i in found_ids) / len(true_ids)
                    if true_ids
                    else 1.0
                )
            return {
                "query_seconds": elapsed,
                "recall_at_5": float(np.mean(recall)) if recall else 1.0,
                "tie_aware_recall_at_5": float(np.mean(tie_recall)) if tie_recall else 1.0,
                "build_seconds": build_seconds,
                "layers": len(index.layers),
                "m": index.m,
                "ef_search": index.ef_search,
                "ef_construction": index.ef_construction,
                "_retained": index,
            }

        def exact_vectors():
            start = time.perf_counter()
            for qid, q in zip(query_ids, query_vectors):
                IndiceMultidimensionalKDTree.knn_exact_baseline(q, vectors, ids, 5, qid)
            return {"query_seconds": time.perf_counter() - start}

        raw.append(
            dict(family="vectors", algorithm="hnsw", repetition=repetition, **measured(ann_run))
        )
        raw.append(
            dict(
                family="vectors",
                algorithm="knn_exact",
                repetition=repetition,
                **measured(exact_vectors),
            )
        )

        rankings = {}

        def rank(method):
            scores = (
                nx.pagerank(graph, weight="weight")
                if method == "pagerank"
                else dict(graph.degree(weight="weight"))
            )
            rankings[method] = sorted(scores, key=lambda i: (-scores[i], str(i)))[:20]
            return {"top_20": rankings[method]}

        for name in ("pagerank", "degree"):
            raw.append(
                dict(
                    family="ranking",
                    algorithm=name,
                    repetition=repetition,
                    **measured(lambda n=name: rank(n)),
                )
            )
        overlap = len(set(rankings["pagerank"]) & set(rankings["degree"])) / max(
            1, len(rankings["pagerank"])
        )
        raw[-2]["overlap_with_degree_at_20"] = overlap
        partitions = {}

        def community(method):
            if not len(graph):
                groups = []
            elif not graph.number_of_edges():
                groups = [{i} for i in graph]
            elif method == "louvain":
                groups = nx.community.louvain_communities(
                    graph, seed=SEED + repetition, weight="weight"
                )
            else:
                groups = list(
                    nx.community.asyn_lpa_communities(
                        graph, seed=SEED + repetition, weight="weight"
                    )
                )
            mapping = {node: i for i, group in enumerate(groups) for node in group}
            labels = [mapping[i] for i in graph]
            partitions[method] = labels
            reference = previous.get(method, labels)
            return {
                "modularity": (
                    nx.community.modularity(graph, groups, weight="weight")
                    if graph.number_of_edges()
                    else 0.0
                ),
                "community_count": len(groups),
                "community_sizes": sorted(map(len, groups), reverse=True),
                "stability_ari": float(adjusted_rand_score(reference, labels)) if labels else 1.0,
            }

        if repetition == 0:
            previous = {}
        for name in ("louvain", "label_propagation"):
            raw.append(
                dict(
                    family="communities",
                    algorithm=name,
                    repetition=repetition,
                    **measured(lambda n=name: community(n)),
                )
            )
        if repetition == 0:
            previous = partitions.copy()
        # Estructuras base: población declarada por separado del stream completo.
        base_n = len(keys) if not max_entities else min(len(keys), max_entities)
        events = [
            {
                "event_id": f"bench-{i}",
                "event_type": "purchase" if i % 3 == 0 else "view",
                "event_time": f"{i:012d}",
            }
            for i in range(base_n)
        ]

        def base_run(name):
            if name == "fifo":
                obj = IngestaBufferFIFO(max(1, base_n))
                for event in events:
                    obj.enqueue(event)
                while not obj.is_empty():
                    obj.dequeue()
            elif name == "preprocess":
                obj = PreprocesamientoLotes()
                obj.sort_batch(obj.deduplicate_batch(events))
                obj.close()
            elif name == "heap":
                obj = ColaPrioridadesHeap()
                for event in events:
                    obj.push(event.copy())
                while not obj.is_empty():
                    obj.pop()
            elif name == "cache":
                obj = AlmacenamientoLookupCache(capacity=100)
                for event in events:
                    obj.put_storage(event["event_id"], event)
                for event in events[:100]:
                    obj.get_from_cache_or_storage(event["event_id"])
                    obj.get_from_cache_or_storage(event["event_id"])
            elif name == "bplus_disk":
                with tempfile.TemporaryDirectory() as directory:
                    obj = IndexacionPrincipalBPlusTree(path=Path(directory) / "index.sqlite")
                    for i in range(base_n):
                        obj.insert(i, i)
                    obj.flush()
                    obj.range_search(0, min(100, base_n))
                    obj.close()
            elif name == "kd_tree":
                obj = IndiceMultidimensionalKDTree(vectors, ids)
                for qid, q in zip(query_ids, query_vectors):
                    obj.query(q, 5, qid)
            else:
                obj = TrieAutocompletado()
                top = TopKAnalytics()
                for event in events:
                    obj.insert(event["event_id"])
                    top.add_item(event["event_id"])
                obj.search_prefix("bench")
                top.get_top_k()
            return {"input_entities": len(ids) if name == "kd_tree" else base_n, "_retained": obj}

        for name in ("fifo", "preprocess", "heap", "cache", "bplus_disk", "kd_tree", "trie_topk"):
            raw.append(
                dict(
                    family="base",
                    algorithm=name,
                    repetition=repetition,
                    **measured(lambda n=name: base_run(n)),
                )
            )
    summary = {}
    for algorithm in {r["algorithm"] for r in raw}:
        rows = [r for r in raw if r["algorithm"] == algorithm]
        numeric = {
            k
            for r in rows
            for k, v in r.items()
            if isinstance(v, (int, float)) and k != "repetition"
        }
        summary[algorithm] = {
            key: {
                "mean": statistics.mean(r[key] for r in rows if key in r),
                "stdev": (
                    statistics.stdev([r[key] for r in rows if key in r]) if len(rows) > 1 else 0.0
                ),
            }
            for key in numeric
        }
    return {
        "path": str(path),
        "events": len(keys),
        "distinct_keys": len(unique),
        "vectors": len(ids),
        "dimensions": len(vectors[0]) if vectors else 0,
        "sets": len(sets),
        "nodes": len(graph),
        "edges": graph.number_of_edges(),
        "graph_events": graph_events,
        "rows_for_entities": rows_read,
        "entity_cap": max_entities,
        "queries": min(queries, len(ids)),
        "graph_provenance": "Órdenes SIMULADAS: grupos consecutivos de tres product_id; no órdenes reales.",
        "repetitions": repetitions,
        "entity_sampled": rows_read < len(keys),
        "graph_sampled": graph_events < len(keys),
        "raw": raw,
        "summary": summary,
    }


def run_benchmarks(
    sizes=("1K",), repetitions=None, max_entities=2000, queries=10, output="output/benchmarks"
):
    if repetitions is not None and repetitions < 1 or max_entities < 0 or queries < 1:
        raise ValueError("Configuración experimental inválida.")
    target = Path(output)
    target.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(ZoneInfo("America/Bogota")).isoformat(),
        "versions": {
            name: importlib.metadata.version(name)
            for name in ("numpy", "pandas", "pyarrow", "networkx", "scikit-learn", "mmh3", "psutil")
        },
        "source_sha256": {
            str(path): sha256(path.read_bytes()).hexdigest()
            for path in [Path("benchmarks.py"), *sorted(Path("src").glob("*.py"))]
        },
        "seed": SEED,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "memory_scope": "tracemalloc: asignaciones Python actuales y pico. RSS antes/después: proceso, no pico nativo.",
        "limitation": "El recorte de entidades se informa y NO acredita evaluación analítica completa a 10M.",
        "datasets": [],
    }
    for size in sizes:
        n = repetitions if repetitions is not None else (5 if size == "10M" else 100)
        print(
            f'Experimento {size}: {n} repeticiones; máximo de entidades={max_entities or "sin recorte"}.',
            flush=True,
        )
        result = evaluate(Path("data") / f"ecommerce_{size}.parquet", n, max_entities, queries)
        report["datasets"].append(result)
        (target / f"{size}.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        (target / "report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    fig, ax = plt.subplots(figsize=(8, 5))
    for algorithm in ("bloom", "set", "cms", "counter"):
        x = [d["events"] for d in report["datasets"]]
        y = [d["summary"][algorithm]["seconds"]["mean"] for d in report["datasets"]]
        ax.loglog(x, y, "o-", label=algorithm)
    ax.set(
        xlabel="Eventos realmente insertados",
        ylabel="Construcción + consultas (s)",
        title="Escalabilidad de estructuras probabilísticas y controles",
    )
    ax.legend()
    ax.grid(True, alpha=0.2)
    fig.tight_layout()
    fig.savefig(target / "scalability.png", dpi=150)
    plt.close(fig)
    for family, algorithms, population in [
        ("sets", ("minhash_lsh", "jaccard_exact"), "sets"),
        ("vectors", ("hnsw", "knn_exact"), "vectors"),
        ("ranking", ("pagerank", "degree"), "nodes"),
        ("communities", ("louvain", "label_propagation"), "nodes"),
    ]:
        fig, ax = plt.subplots(figsize=(8, 5))
        for algorithm in algorithms:
            ax.loglog(
                [d[population] for d in report["datasets"]],
                [max(1e-9, d["summary"][algorithm]["seconds"]["mean"]) for d in report["datasets"]],
                "o-",
                label=algorithm,
            )
        ax.set(
            xlabel=f"Población analítica real: {population}",
            ylabel="Carga experimental total (s)",
            title=f"Comparación de {family}",
        )
        ax.legend()
        ax.grid(True, alpha=0.2)
        fig.tight_layout()
        fig.savefig(target / f"scalability_{family}.png", dpi=150)
        plt.close(fig)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sizes", nargs="+", default=["1K"], choices=["1K", "10K", "100K", "1M", "10M"]
    )
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--max-entities", type=int, default=2000)
    parser.add_argument("--queries", type=int, default=10)
    parser.add_argument("--output", default="output/benchmarks")
    args = parser.parse_args()
    run_benchmarks(args.sizes, args.repetitions, args.max_entities, args.queries, args.output)
