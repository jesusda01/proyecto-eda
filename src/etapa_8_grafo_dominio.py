import networkx as nx


class GrafoDominioRed:
  """Etapa 8: Grafo de Dominio, PageRank y Detección de Comunidades."""

  def __init__(self):
    self.graph = nx.DiGraph()  # Inicializa un grafo dirigido

  def add_edge(self, source, target, weight=1.0):
    # Añade aristas ponderadas entre entidades del ecosistema (usuario, producto, IP)
    self.graph.add_edge(source, target, weight=weight)

  def compute_pagerank(self, alpha=0.85):
    # Calcula la centralidad PageRank para determinar la influencia de cada nodo en la red
    return nx.pagerank(self.graph, alpha=alpha)

  def detect_communities(self):
    # Convierte el grafo dirigido a no dirigido para aplicar algoritmos de modularidad
    undirected = self.graph.to_undirected()
    from networkx.algorithms.community import greedy_modularity_communities

    communities = list(greedy_modularity_communities(undirected))
    return [list(c) for c in communities]