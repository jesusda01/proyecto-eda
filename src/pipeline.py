"""Orquestación de las diez etapas con contratos y estado entre microlotes.

Los observadores solo reciben muestras acotadas. No modifican decisiones del
negocio. El pipeline funciona sin servidor, animaciones ni pausas visuales.
"""

from collections import Counter, OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
import time
import uuid

from .etapa_0_ingesta_buffer import IngestaBufferFIFO
from .etapa_1_preprocesamiento_lotes import PreprocesamientoLotes, parse_time
from .etapa_2_cola_prioridades import ColaPrioridadesHeap
from .etapa_3_almacenamiento_lookup_cache import AlmacenamientoLookupCache
from .etapa_4_indexacion_principal import IndexacionPrincipalBPlusTree
from .etapa_5_indice_multidimensional import IndiceMultidimensionalKDTree
from .etapa_6_filtrado_probabilistico import RiskWindow
from .etapa_7_similitud_aproximada import HNSWApproximateSearch, MinHashLSHSimulator
from .etapa_8_grafo_dominio import GrafoDominioRed
from .etapa_9_servicio_visualizacion import TopKAnalytics, TrieAutocompletado
from .stream_reader import product_vector


class PipelineCancelled(Exception):
    """Parada cooperativa: conserva la salida de los lotes terminados."""


def sample(events):
    return {"cantidad": len(events), "muestra": events[:3]}


@dataclass
class PipelineConfig:
    batch_size: int = 500
    seed: int = 42
    window_seconds: int = 300
    alert_threshold: int = 3
    top_k: int = 5
    basket_limit: int = 10000
    runtime_dir: str = "data/runtime"


class EcommercePipeline:
    """Los índices guardan catálogo; las colas solo mantienen trabajo pendiente.

    El catálogo usa la primera observación como precio de referencia y sus
    características originales. No pretende representar inventario comercial
    actualizado. Los cambios de precio quedan en el histórico de eventos.
    """

    def __init__(self, config=None, observer=None, cancel=None):
        self.config = config or PipelineConfig()
        if self.config.batch_size < 1 or self.config.basket_limit < 1:
            raise ValueError("Tamaños positivos requeridos.")
        self.observer, self.cancel = observer, self.cancel_callback(cancel)
        self.directory = Path(self.config.runtime_dir) / uuid.uuid4().hex
        self.directory.mkdir(parents=True, exist_ok=True)
        self.buffer = IngestaBufferFIFO(self.config.batch_size)
        self.preproc = PreprocesamientoLotes(self.directory / "seen.sqlite")
        self.queue = ColaPrioridadesHeap()
        self.storage = AlmacenamientoLookupCache(100, path=self.directory / "events.sqlite")
        self.price_index = IndexacionPrincipalBPlusTree(path=self.directory / "prices.sqlite")
        self.risk = RiskWindow(self.config.window_seconds, self.config.alert_threshold)
        self.ann = HNSWApproximateSearch(seed=self.config.seed)
        self.lsh = MinHashLSHSimulator()
        self.graph = GrafoDominioRed(path=self.directory / "orders.sqlite")
        self.trie, self.top = TrieAutocompletado(), TopKAnalytics(self.config.top_k)
        self.catalog, self.baskets = {}, OrderedDict()
        self.counts = Counter(
            {
                key: 0
                for key in (
                    "received",
                    "simulated",
                    "rejected",
                    "duplicates",
                    "processed",
                    "alerts",
                    "batches",
                    "accepted_purchases",
                    "purchases_without_order",
                )
            }
        )
        self.last_event_time = "2019-11-01T00:00:00+00:00"
        self.alerts, self.trace_events, self.recommendations = [], [], []
        self.pagerank, self.communities = {}, []
        self.last = {}
        self.closed = False

    @staticmethod
    def cancel_callback(callback):
        return callback or (lambda: False)

    def check_cancel(self):
        if self.cancel():
            raise PipelineCancelled()

    @contextmanager
    def stage(self, number, data):
        self.check_cancel()
        if self.observer:
            self.observer(number, "start", dict(data, lote=self.counts["batches"] + 1))
        started = time.perf_counter()
        result = {}
        yield result
        self.check_cancel()
        elapsed = time.perf_counter() - started
        self.last[number] = result
        if self.observer:
            self.observer(number, "end", dict(result, seconds=elapsed))

    def process_batch(self, incoming):
        if len(incoming) > self.config.batch_size:
            raise ValueError("El lote supera la capacidad configurada.")
        with self.stage(0, {"entrada": sample(incoming)}) as output:
            for event in incoming:
                self.check_cancel()
                self.buffer.enqueue(event)
            raw = []
            while not self.buffer.is_empty():
                raw.append(self.buffer.dequeue())
            self.counts["received"] += len(raw)
            self.counts["simulated"] += sum(bool(e.get("is_simulated")) for e in raw)
            output.update(lote_crudo=sample(raw), pendientes_buffer=self.buffer.size())
        with self.stage(1, output.copy()) as output:
            valid = self.preproc.validate_batch(raw)
            clean = self.preproc.deduplicate_batch(valid)
            ordered = self.preproc.sort_batch(clean)
            if ordered:
                self.last_event_time = ordered[-1]["event_time"]
            self.counts["rejected"] += len(self.preproc.rejected)
            self.counts["duplicates"] += self.preproc.duplicates
            output.update(
                lote_ordenado=sample(ordered),
                rechazados=self.preproc.rejected[:3],
                duplicados=self.preproc.duplicates,
            )
        with self.stage(2, output.copy()) as output:
            for event in ordered:
                self.queue.push(event)
            prioritized = []
            while not self.queue.is_empty():
                prioritized.append(self.queue.pop())
            self.counts["processed"] += len(prioritized)
            self.trace_events = [e["event_id"] for e in prioritized[:3]]
            output.update(
                lote_priorizado=sample(prioritized),
                heap_pending=self.queue.size(),
                orden_prioridades=[
                    {"id": e["event_id"], "priority": e["priority"]} for e in prioritized[:8]
                ],
            )
        with self.stage(3, output.copy()) as output:
            for event in prioritized:
                self.check_cancel()
                self.storage.put_storage(event["event_id"], event)
            self.storage.flush()
            status1 = status2 = "SIN_EVENTOS"
            if prioritized:
                identifier = prioritized[0]["event_id"]
                _, status1 = self.storage.get_from_cache_or_storage(identifier)
                _, status2 = self.storage.get_from_cache_or_storage(identifier)
            output.update(
                almacenados=self.counts["processed"],
                status1=status1,
                status2=status2,
                archivo_eventos=str(self.directory / "events.sqlite"),
            )
        with self.stage(4, output.copy()) as output:
            new_products = []
            for event in prioritized:
                # El índice se alimenta de eventos que ya se pueden recuperar.
                stored = self.storage.get_storage(event["event_id"])
                pid = stored["product_id"]
                if pid not in self.catalog:
                    self.catalog[pid] = {
                        k: stored.get(k) for k in ("product_id", "price", "brand", "category_code")
                    }
                    self.catalog[pid]["source_event_id"] = stored["event_id"]
                    self.price_index.insert((stored["price"], pid), pid)
                    new_products.append(pid)
            self.price_index.flush()
            query_id = next(iter(self.catalog), None)
            ranged = self.price_candidates(query_id) if query_id else []
            output.update(
                productos_catalogo=len(self.catalog),
                nuevos_productos=new_products[:5],
                consulta_rango=ranged[:8],
                indice_disco=str(self.directory / "prices.sqlite"),
            )
        with self.stage(5, output.copy()) as output:
            ids = list(self.catalog)
            vectors = [product_vector(self.catalog[i]) for i in ids]
            self.kd = IndiceMultidimensionalKDTree(vectors, ids)
            nearest = (
                self.kd.query(product_vector(self.catalog[query_id]), self.config.top_k, query_id)
                if query_id
                else []
            )
            exact = (
                self.kd.knn_exact_baseline(
                    product_vector(self.catalog[query_id]),
                    vectors,
                    ids,
                    self.config.top_k,
                    query_id,
                )
                if query_id
                else []
            )
            output.update(
                knn_res=nearest,
                referencia_exacta=exact,
                kd_coincide=nearest == exact,
                candidatos_rango=ranged[:8],
                dimensiones=len(vectors[0]) if vectors else 0,
            )
        with self.stage(6, output.copy()) as output:
            accepted, decisions = [], []
            for event in prioritized:
                self.check_cancel()
                decision = self.risk.observe(
                    event["user_id"],
                    parse_time(event["event_time"]).timestamp(),
                    bool(event.get("account_flagged")),
                )
                enriched = dict(event, risk=decision)
                self.storage.put_storage(event["event_id"], enriched)
                if decision["requires_review"]:
                    self.counts["alerts"] += 1
                    self.alerts.append(
                        {
                            "event_id": event["event_id"],
                            **decision,
                            "is_simulated": bool(event.get("is_simulated")),
                        }
                    )
                    self.alerts = self.alerts[-100:]
                else:
                    accepted.append(enriched)
                decisions.append({"event_id": event["event_id"], **decision})
            self.storage.flush()
            output.update(
                aceptados=sample(accepted),
                alertas=self.alerts[-5:],
                decisiones=decisions[-5:],
                ventana_segundos=self.config.window_seconds,
            )
        with self.stage(7, output.copy()) as output:
            for event in accepted:
                pid, uid = event["product_id"], event["user_id"]
                self.ann.add_vector(pid, product_vector(self.catalog[pid]))
                if event["event_type"] == "purchase":
                    basket = self.baskets.setdefault(uid, set())
                    basket.add(pid)
                    self.baskets.move_to_end(uid)
                    self.lsh.insert(uid, basket)
                    if len(self.baskets) > self.config.basket_limit:
                        old, _ = self.baskets.popitem(last=False)
                        self.lsh.remove(old)
            ann = (
                self.ann.search_knn(
                    product_vector(self.catalog[query_id]), self.config.top_k, query_id
                )
                if query_id
                else []
            )
            similar = self.lsh.query(
                self.baskets.get(next(iter(self.baskets), None), set()),
                exclude_id=next(iter(self.baskets), None),
            )
            exact_ann = (
                IndiceMultidimensionalKDTree.knn_exact_baseline(
                    product_vector(self.catalog[query_id]),
                    [v.tolist() for v in self.ann.vectors.values()],
                    list(self.ann.vectors),
                    self.config.top_k,
                    query_id,
                )
                if query_id
                else []
            )
            recall = len(set(ann) & set(exact_ann)) / len(exact_ann) if exact_ann else 1.0
            output.update(
                neighbors_hnsw=ann,
                referencia_exacta=exact_ann,
                recall_at_k=recall,
                usuarios_similares=similar,
                capas_hnsw=len(self.ann.layers),
                vectores=len(self.ann.vectors),
            )
        with self.stage(8, output.copy()) as output:
            for event in accepted:
                if event["event_type"] == "purchase" and event.get("order_id"):
                    self.graph.add_purchase(event["order_id"], event["product_id"])
                elif event["event_type"] == "purchase":
                    self.counts["purchases_without_order"] += 1
            self.pagerank = self.graph.compute_pagerank()
            self.graph.flush()
            self.communities = self.graph.detect_communities(seed=self.config.seed)
            output.update(
                grafo={
                    "nodos": len(self.graph.graph),
                    "aristas": self.graph.graph.number_of_edges(),
                    "muestra_aristas": list(self.graph.graph.edges(data=True))[:8],
                },
                top_nodes=sorted(self.pagerank.items(), key=lambda x: -x[1])[:5],
                comunidades=self.graph.last_metrics,
            )
        with self.stage(9, output.copy()) as output:
            for event in accepted:
                pid = event["product_id"]
                self.trie.insert(f"producto {pid}")
                if event["event_type"] == "purchase":
                    self.top.add_item(pid)
                    self.counts["accepted_purchases"] += 1
            self.recommendations = (
                self.recommend(query_id, ann, ranged, nearest, similar) if query_id else []
            )
            self.counts["batches"] += 1
            output.update(
                top_k=self.top.get_top_k(),
                sugerencias=self.trie.search_prefix("pro")[:5],
                recomendaciones=self.recommendations,
                alertas=self.alerts[-5:],
                contadores=dict(self.counts),
            )
        return self.result()

    def price_candidates(self, product_id, lower=None, upper=None):
        product = self.catalog.get(product_id)
        if not product:
            return []
        price = product["price"]
        low, high = (max(0, price * 0.5) if lower is None else lower), (
            price * 1.5 + 1 if upper is None else upper
        )
        if self.closed:
            index = IndexacionPrincipalBPlusTree(path=self.directory / "prices.sqlite")
            try:
                return [value for _, value in index.range_search((low, ""), (high, "\uffff"), 100)]
            finally:
                index.close()
        return [
            value for _, value in self.price_index.range_search((low, ""), (high, "\uffff"), 100)
        ]

    def recommend(self, product_id, ann=None, ranged=None, nearest=None, similar=None):
        if product_id not in self.catalog:
            return []
        ann = (
            ann
            if ann is not None
            else self.ann.search_knn(
                product_vector(self.catalog[product_id]), self.config.top_k, product_id
            )
        )
        ranged = ranged if ranged is not None else self.price_candidates(product_id)
        complements = self.graph.complementary(product_id, self.config.top_k)
        basket_products = set()
        for user, _ in similar or []:
            basket_products.update(self.baskets.get(user, set()))
        pool = set(ann) | set(complements) | set(nearest or []) | basket_products
        recommendations = []
        for pid in pool - {product_id}:
            signals = {
                "ann": pid in ann,
                "price_range": pid in ranged,
                "kd": pid in (nearest or []),
                "cobuy": pid in complements,
                "similar_basket": pid in basket_products,
                "pagerank": self.pagerank.get(pid, 0),
            }
            score = (
                signals["ann"]
                + signals["price_range"] * 0.2
                + signals["kd"] * 0.2
                + signals["cobuy"] * 2
                + signals["similar_basket"]
                + signals["pagerank"]
            )
            recommendations.append(
                {"product_id": pid, "score": round(score, 6), "signals": signals}
            )
        return sorted(recommendations, key=lambda x: (-x["score"], x["product_id"]))[
            : self.config.top_k
        ]

    def result(self):
        return {
            "counts": dict(self.counts),
            "recommendations": self.recommendations,
            "alerts": self.alerts,
            "top_k": self.top.get_top_k(),
            "runtime_dir": str(self.directory),
            "catalog_size": len(self.catalog),
            "query_product": next(iter(self.catalog), None),
            "products_sample": list(self.catalog)[:30],
        }

    def close(self):
        if not self.closed:
            self.storage.close()
            self.preproc.close()
            self.price_index.close()
            self.graph.close()
            self.closed = True
