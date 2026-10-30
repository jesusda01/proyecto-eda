"""MinHash con bandas LSH y HNSW multicapa implementados en Python.

No son envoltorios de fuerza bruta: LSH recupera candidatos de buckets y HNSW
navega un grafo de proximidad. Las referencias exactas se evalúan por separado.
"""

from collections import defaultdict
import heapq
import itertools
import math
import random
import mmh3
import numpy as np


class MinHashLSHSimulator:
    """Nombre conservado por compatibilidad; ahora incluye índice LSH real."""

    def __init__(self, num_perm=64, bands=16, threshold=0.3):
        if num_perm < 1 or bands < 1 or num_perm % bands or not 0 <= threshold <= 1:
            raise ValueError("num_perm debe ser múltiplo de bands; umbral entre 0 y 1.")
        self.num_perm, self.bands, self.threshold = num_perm, bands, threshold
        self.rows = num_perm // bands
        self.buckets = defaultdict(set)
        self.signatures = {}
        self.sets = {}

    def compute_signature(self, set_elements):
        elements = set(map(str, set_elements))
        return tuple(
            min((mmh3.hash(v, i, signed=False) for v in elements), default=2**32 - 1)
            for i in range(self.num_perm)
        )

    def jaccard_similarity_estimate(self, sig1, sig2):
        if len(sig1) != self.num_perm or len(sig2) != self.num_perm:
            raise ValueError("Longitud de firma incorrecta.")
        return sum(a == b for a, b in zip(sig1, sig2)) / self.num_perm

    def _keys(self, signature):
        return [
            (band, signature[band * self.rows : (band + 1) * self.rows])
            for band in range(self.bands)
        ]

    def insert(self, identifier, elements):
        if identifier in self.signatures:
            for key in self._keys(self.signatures[identifier]):
                self.buckets[key].discard(identifier)
                if not self.buckets[key]:
                    del self.buckets[key]
        values = set(map(str, elements))
        signature = self.compute_signature(values)
        self.sets[identifier], self.signatures[identifier] = values, signature
        if values:
            for key in self._keys(signature):
                self.buckets[key].add(identifier)

    def query(self, elements, k=5, exclude_id=None):
        if not elements:
            return []
        signature = self.compute_signature(elements)
        candidates = set().union(*(self.buckets.get(key, set()) for key in self._keys(signature)))
        scored = [
            (self.jaccard_similarity_estimate(signature, self.signatures[i]), str(i), i)
            for i in candidates
            if i != exclude_id
        ]
        return [
            (identifier, score)
            for score, _, identifier in sorted(scored, key=lambda r: (-r[0], r[1]))[:k]
            if score >= self.threshold
        ]

    def remove(self, identifier):
        if identifier in self.signatures:
            for key in self._keys(self.signatures.pop(identifier)):
                self.buckets[key].discard(identifier)
                if not self.buckets[key]:
                    del self.buckets[key]
            self.sets.pop(identifier, None)

    @staticmethod
    def exact_jaccard(a, b):
        a, b = set(map(str, a)), set(map(str, b))
        return len(a & b) / len(a | b) if a | b else 1.0


class HNSWApproximateSearch:
    """Grafo jerárquico con descenso voraz y búsqueda de candidatos por capa.

    M limita vecinos; ef_construction/ef_search controlan amplitud. Las aristas
    son dirigidas después de la poda local, como listas de vecinos por nodo.
    No se garantiza costo logarítmico en todo dataset ni recall perfecto.
    """

    def __init__(self, m=8, ef_construction=48, ef_search=32, seed=42):
        if m < 2 or ef_construction < m or ef_search < 1:
            raise ValueError("Se requiere m>=2, ef_construction>=m y ef_search>=1.")
        self.m, self.ef_construction, self.ef_search = m, ef_construction, ef_search
        self.rng = random.Random(seed)
        self.vectors, self.layers = {}, []
        self.entry = None
        self.levels = {}
        self.dim = None
        self.last_visited = 0
        self._order, self._sequence = {}, itertools.count()

    @property
    def index_store(self):
        return [{"id": i, "coords": v.tolist()} for i, v in self.vectors.items()]

    def _dist(self, q, identifier):
        d = q - self.vectors[identifier]
        return float(d @ d)

    def _select_neighbors(self, query, candidates, limit):
        """Prefiere enlaces diversos para no aislar grupos de puntos próximos.

        Un candidato se acepta si no queda tapado por uno ya seleccionado.
        Si faltan enlaces, se completan con los restantes más cercanos.
        """
        ordered = sorted(set(candidates), key=lambda i: (self._dist(query, i), self._order[i]))
        selected, remaining = [], []
        for candidate in ordered:
            distance = self._dist(query, candidate)
            if all(self._dist(self.vectors[candidate], other) > distance for other in selected):
                selected.append(candidate)
                if len(selected) == limit:
                    return selected
            else:
                remaining.append(candidate)
        return (selected + remaining)[:limit]

    def _search_layer(self, q, entries, ef, level):
        visited = set(entries)
        candidates = [(self._dist(q, i), self._order[i], i) for i in entries]
        heapq.heapify(candidates)
        best = [(-d, -o, i) for d, o, i in candidates]
        heapq.heapify(best)
        while candidates:
            distance, _, node = heapq.heappop(candidates)
            if len(best) >= ef and distance > -best[0][0]:
                break
            for neighbor in self.layers[level].get(node, ()):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                d = self._dist(q, neighbor)
                order = self._order[neighbor]
                if len(best) < ef or (d, order) < (-best[0][0], -best[0][1]):
                    heapq.heappush(candidates, (d, order, neighbor))
                    heapq.heappush(best, (-d, -order, neighbor))
                    if len(best) > ef:
                        heapq.heappop(best)
        self.last_visited += len(visited)
        return [(i, -d) for d, _, i in sorted(best, key=lambda v: (-v[0], -v[1]))]

    def add_vector(self, vector_id, vector_coords):
        vector = np.asarray(vector_coords, dtype=np.float64)
        if vector.ndim != 1 or not len(vector) or not np.isfinite(vector).all():
            raise ValueError("El vector debe ser un arreglo finito de una dimensión.")
        if self.dim is not None and len(vector) != self.dim:
            raise ValueError("Dimensión de vector incompatible.")
        if vector_id in self.vectors:
            if np.array_equal(vector, self.vectors[vector_id]):
                return
            # Reinsertar el producto para no conservar enlaces de coordenadas antiguas.
            for layer in self.layers:
                layer.pop(vector_id, None)
                for neighbors in layer.values():
                    neighbors.discard(vector_id)
            del self.vectors[vector_id]
            self.levels.pop(vector_id)
            if self.entry == vector_id:
                self.entry = max(self.levels, key=self.levels.get) if self.levels else None
        self.dim = len(vector)
        level = min(12, int(-math.log(max(self.rng.random(), 1e-12)) / math.log(self.m)))
        old_top = self.levels[self.entry] if self.entry is not None else -1
        self.vectors[vector_id] = vector
        self._order[vector_id] = next(self._sequence)
        self.levels[vector_id] = level
        while len(self.layers) <= max(level, old_top):
            self.layers.append({})
        for l in range(level + 1):
            self.layers[l][vector_id] = set()
        if self.entry is None:
            self.entry = vector_id
            return
        entry = self.entry
        for l in range(old_top, level, -1):
            entry = self._search_layer(vector, [entry], 1, l)[0][0]
        for l in range(min(level, old_top), -1, -1):
            candidates = self._search_layer(vector, [entry], self.ef_construction, l)
            neighbors = self._select_neighbors(
                vector, [i for i, _ in candidates if i != vector_id], self.m
            )
            self.layers[l][vector_id].update(neighbors)
            for neighbor in neighbors:
                links = self.layers[l][neighbor]
                links.add(vector_id)
                limit = self.m * 2 if l == 0 else self.m
                if len(links) > limit:
                    nearest = self._select_neighbors(self.vectors[neighbor], links, limit)
                    self.layers[l][neighbor] = set(nearest)
            if candidates:
                entry = candidates[0][0]
        if level > old_top:
            self.entry = vector_id

    def search_knn(self, query_vector, k=3, exclude_id=None, ef_search=None):
        if self.entry is None or k < 1:
            return []
        q = np.asarray(query_vector, dtype=np.float64)
        if q.shape != (self.dim,) or not np.isfinite(q).all():
            raise ValueError("Consulta vectorial inválida.")
        self.last_visited = 0
        entry = self.entry
        for level in range(self.levels[entry], 0, -1):
            entry = self._search_layer(q, [entry], 1, level)[0][0]
        result = self._search_layer(q, [entry], max(k + 1, ef_search or self.ef_search), 0)
        return [identifier for identifier, _ in result if identifier != exclude_id][:k]
