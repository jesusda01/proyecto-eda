"""Grafo de co-compra: productos unidos dentro de órdenes explícitas."""

from collections import Counter
from collections import OrderedDict
import json
import sqlite3
from itertools import combinations
import networkx as nx


class GrafoDominioRed:
    def __init__(self, path=None, order_cache_size=128):
        self.graph = nx.Graph()
        self.orders = OrderedDict()
        self.order_cache_size = order_cache_size
        self.db = sqlite3.connect(str(path) if path else ":memory:", check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, products TEXT)")
        self.last_metrics = {}

    def add_edge(self, source, target, weight=1.0):
        if source == target:
            return
        old = self.graph.get_edge_data(source, target, {}).get("weight", 0)
        self.graph.add_edge(source, target, weight=old + weight)

    def add_purchase(self, order_id, product_id):
        """Añadir un producto nuevo a una orden cuenta cada pareja una vez."""
        order_id = str(order_id)
        if order_id not in self.orders:
            row = self.db.execute("SELECT products FROM orders WHERE id=?", (order_id,)).fetchone()
            self.orders[order_id] = set(json.loads(row[0])) if row else set()
        self.orders.move_to_end(order_id)
        products = self.orders[order_id]
        product_id = str(product_id)
        self.graph.add_node(product_id)
        if product_id not in products:
            for other in products:
                self.add_edge(other, product_id)
            products.add(product_id)
            self.db.execute(
                "INSERT OR REPLACE INTO orders VALUES (?,?)",
                (order_id, json.dumps(sorted(products))),
            )
        if len(self.orders) > self.order_cache_size:
            self.orders.popitem(last=False)

    def flush(self):
        self.db.commit()

    def close(self):
        self.flush()
        self.db.close()

    def compute_pagerank(self, alpha=0.85):
        return nx.pagerank(self.graph, alpha=alpha, weight="weight") if len(self.graph) else {}

    def detect_communities(self, seed=42, method="louvain"):
        if not len(self.graph):
            self.last_metrics = {"modularity": 0.0, "count": 0, "sizes": []}
            return []
        if self.graph.number_of_edges() == 0:
            self.last_metrics = {
                "modularity": 0.0,
                "count": len(self.graph),
                "sizes": [1] * len(self.graph),
            }
            return [[node] for node in self.graph]
        if method == "louvain":
            groups = nx.community.louvain_communities(self.graph, seed=seed, weight="weight")
        elif method == "label_propagation":
            groups = list(nx.community.asyn_lpa_communities(self.graph, seed=seed, weight="weight"))
        elif method == "components":
            groups = list(nx.connected_components(self.graph))
        else:
            raise ValueError("Método de comunidades desconocido.")
        self.last_metrics = {
            "modularity": nx.community.modularity(self.graph, groups, weight="weight"),
            "count": len(groups),
            "sizes": sorted(map(len, groups), reverse=True),
        }
        return [sorted(group) for group in groups]

    def complementary(self, product_id, k=5):
        if product_id not in self.graph:
            return []
        return sorted(
            self.graph[product_id], key=lambda p: (-self.graph[product_id][p]["weight"], str(p))
        )[:k]
