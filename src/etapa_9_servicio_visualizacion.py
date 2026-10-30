import heapq


class TrieNode:
    """Nodo constitutivo del Árbol Trie para autocompletado."""

    def __init__(self):
        self.children = {}  # Diccionario de caracteres hijos
        self.is_end_of_word = False  # Marca el fin de una palabra válida


class TrieAutocompletado:
    """Estructura Trie para sugerencias de búsqueda de texto en tiempo real."""

    def __init__(self):
        self.root = TrieNode()

    def insert(self, word):
        node = self.root
        # Inserta carácter por carácter en el árbol de prefijos
        for char in word.lower():
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True

    def search_prefix(self, prefix):
        node = self.root
        # Navega hasta el nodo correspondiente al prefijo ingresado
        for char in prefix.lower():
            if char not in node.children:
                return []
            node = node.children[char]
        results = []
        self._dfs(node, prefix.lower(), results)
        return results

    def _dfs(self, node, current_word, results):
        # Recorrido en profundidad (DFS) para recolectar todas las palabras completas bajo el prefijo
        if node.is_end_of_word:
            results.append(current_word)
        for char, next_node in node.children.items():
            self._dfs(next_node, current_word + char, results)


class TopKAnalytics:
    """Estructura basada en Heap para extraer los K elementos más populares."""

    def __init__(self, k=5):
        if k < 1:
            raise ValueError("k debe ser positivo.")
        self.k = k
        self.min_heap = []
        self.freq_map = {}

    def add_item(self, item):
        # Incrementa el conteo de frecuencia de cada elemento en la Hash Table de soporte
        self.freq_map[item] = self.freq_map.get(item, 0) + 1

    def get_top_k(self):
        # Extrae los K elementos con mayor frecuencia acumulada usando heapq.nlargest
        return heapq.nlargest(self.k, self.freq_map.items(), key=lambda x: x[1])
