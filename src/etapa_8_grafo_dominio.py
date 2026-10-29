import networkx as nx
from networkx.algorithms.community import greedy_modularity_communities


class GrafoDominioRed:
    """Etapa 8: Grafo de Dominio, PageRank y Detección de Comunidades."""

    def __init__(self):
        self.graph = nx.DiGraph()  # Inicializa un grafo dirigido

    def add_edge(self, source, target, weight=1.0):
        # Añade aristas ponderadas entre entidades del ecosistema
        self.graph.add_edge(source, target, weight=weight)

    def add_edges_from_stream(self, stream_data, limit=300):
        """⚡ PROTECCIÓN: Limita la inserción masiva a un umbral seguro para evitar saturar RAM/CPU."""
        for i, ev in enumerate(stream_data):
            if i >= limit:
                break
            self.add_edge(f"user_{ev.get('user_id')}", f"prod_{ev.get('product_id')}", weight=1.0)

    def compute_pagerank(self, alpha=0.85):
        # Calcula la centralidad PageRank para determinar la influencia de cada nodo
        if len(self.graph) == 0:
            return {}
        return nx.pagerank(self.graph, alpha=alpha)

    def detect_communities(self):
        # Convierte el grafo dirigido a no dirigido para aplicar algoritmos de modularidad
        if len(self.graph) == 0:
            return []
        undirected = self.graph.to_undirected()
        communities = list(greedy_modularity_communities(undirected))
        return [list(c) for c in communities]