import numpy as np


class KDTreeNode:
  """Nodo constitutivo del KD-Tree para particionamiento espacial."""

  def __init__(self, point, data, left=None, right=None):
    self.point = point  # Coordenadas multidimensionales (ej. [precio, cantidad])
    self.data = data  # Datos asociados al punto
    self.left = left  # Subárbol izquierdo (menores en el eje actual)
    self.right = right  # Subárbol derecho (mayores en el eje actual)


class IndiceMultidimensionalKDTree:
  """Etapa 5: Índice Multidimensional y KNN Exacto

  Estructuras: KD-Tree (O(log N)) + KNN Exacto (Línea base O(N)).
  """

  def __init__(self, points, data_list):
    k = len(points[0]) if points else 2
    # Construye el árbol particionando recursivamente por ejes alternos
    self.root = self._build_tree(
        list(zip(points, data_list)), depth=0, k=k
    )

  def _build_tree(self, points_data, depth, k):
    if not points_data:
      return None
    # Selecciona el eje actual en función de la profundidad del árbol (rotación de dimensiones)
    axis = depth % k
    points_data.sort(key=lambda x: x[0][axis])
    mid = len(points_data) // 2
    return KDTreeNode(
        point=points_data[mid][0],
        data=points_data[mid][1],
        left=self._build_tree(points_data[:mid], depth + 1, k),
        right=self._build_tree(points_data[mid + 1 :], depth + 1, k),
    )

  def knn_exact_baseline(self, query_point, points, data_list, k_neighbors=3):
    distances = []
    # Línea base: Calcula la distancia euclidiana contra todos los registros -> O(N)
    for pt, data in zip(points, data_list):
      dist = np.linalg.norm(np.array(pt) - np.array(query_point))
      distances.append((dist, data))
    # Ordena por distancia ascendente para extraer los K vecinos más cercanos
    distances.sort(key=lambda x: x[0])
    return [item[1] for item in distances[:k_neighbors]]