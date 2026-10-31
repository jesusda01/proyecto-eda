"""KD-Tree con poda y fuerza bruta exacta para comparar resultados."""

from dataclasses import dataclass
import heapq
import numpy as np


@dataclass
class KDTreeNode:
    point: tuple
    data: object
    ordinal: int
    axis: int
    left: object = None
    right: object = None


class IndiceMultidimensionalKDTree:
    def __init__(self, points, data_list):
        if len(points) != len(data_list):
            raise ValueError("Cada punto debe tener un identificador.")
        self.dim = len(points[0]) if len(points) else 0
        items = []
        for i, (pt, identifier) in enumerate(zip(points, data_list)):
            if len(pt) != self.dim or not np.isfinite(pt).all() or not self.dim:
                raise ValueError("Coordenadas incompatibles o no finitas.")
            items.append((tuple(pt), identifier, i))
        self.root = self._build(items, 0)

    def _build(self, items, depth):
        if not items:
            return None
        axis = depth % self.dim
        items.sort(key=lambda item: (item[0][axis], item[2]))
        mid = len(items) // 2
        pt, identifier, ordinal = items[mid]
        return KDTreeNode(
            pt,
            identifier,
            ordinal,
            axis,
            self._build(items[:mid], depth + 1),
            self._build(items[mid + 1 :], depth + 1),
        )

    def query(self, point, k_neighbors=3, exclude_id=None):
        """Exacto; la poda pierde eficacia en dimensiones elevadas."""
        if k_neighbors < 1 or self.root is None:
            return []
        if len(point) != self.dim or not np.isfinite(point).all():
            raise ValueError("Consulta inválida.")
        heap = []

        def visit(node):
            if node is None:
                return
            distance = sum((a - b) ** 2 for a, b in zip(point, node.point))
            if node.data != exclude_id:
                item = (-distance, -node.ordinal, node.data)
                if len(heap) < k_neighbors:
                    heapq.heappush(heap, item)
                elif item[:2] > heap[0][:2]:
                    heapq.heapreplace(heap, item)
            delta = point[node.axis] - node.point[node.axis]
            near, far = (node.left, node.right) if delta < 0 else (node.right, node.left)
            visit(near)
            if len(heap) < k_neighbors or delta**2 <= -heap[0][0]:
                visit(far)

        visit(self.root)
        return [item[2] for item in sorted(heap, key=lambda x: (-x[0], -x[1]))]

    @staticmethod
    def knn_exact_baseline(query_point, points, data_list, k_neighbors=3, exclude_id=None):
        if len(points) != len(data_list):
            raise ValueError("Puntos e IDs desalineados.")
        scored = [
            (sum((a - b) ** 2 for a, b in zip(query_point, pt)), i, identifier)
            for i, (pt, identifier) in enumerate(zip(points, data_list))
            if identifier != exclude_id
        ]
        return [identifier for _, _, identifier in sorted(scored)[: max(0, k_neighbors)]]
