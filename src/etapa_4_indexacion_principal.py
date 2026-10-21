class BPlusTreeNode:
  """Nodo constitutivo de un Árbol B+ para indexación en disco."""

  def __init__(self, leaf=False):
    self.leaf = leaf  # Booleano: True si es nodo hoja, False si es nodo interno
    self.keys = []  # Lista de claves indexadas
    self.children = []  # Lista de punteros a hijos o registros de datos
    self.next = (
        None  # Puntero al nodo hermano siguiente (optimiza escaneos por rango)
    )


class IndexacionPrincipalBPlusTree:
  """Etapa 4: Indexación principal

  Estructura: Árbol B+ balanceado optimizado para almacenamiento secundario.
  """

  def __init__(self, t=3):
    self.root = BPlusTreeNode(leaf=True)  # Raíz inicial del árbol (hoja)
    self.t = t  # Grado mínimo del árbol B+

  def insert(self, key, value):
    root = self.root
    # Si la raíz se llena, se divide (split) incrementando la altura del árbol
    if len(root.keys) >= (2 * self.t - 1):
      temp = BPlusTreeNode()
      self.root = temp
      temp.children.insert(0, root)
      self._split_child(temp, 0, root)
      self._insert_non_full(temp, key, value)
    else:
      self._insert_non_full(root, key, value)

  def _insert_non_full(self, node, key, value):
    i = len(node.keys) - 1
    if node.leaf:
      node.keys.append(None)
      node.children.append(None)
      # Desplaza las claves mayores para ubicar la nueva clave en orden ascendente
      while i >= 0 and key < node.keys[i][0]:
        node.keys[i + 1] = node.keys[i]
        node.children[i + 1] = node.children[i]
        i -= 1
      node.keys[i + 1] = (key, value)
    else:
      while i >= 0 and key < node.keys[i]:
        i -= 1
      i += 1
      if len(node.children[i].keys) >= (2 * self.t - 1):
        self._split_child(node, i, node.children[i])
        if key > node.keys[i]:
          i += 1
      self._insert_non_full(node.children[i], key, value)

  def _split_child(self, parent, i, child):
    t = self.t
    z = BPlusTreeNode(leaf=child.leaf)
    midt = t - 1
    parent.children.insert(i + 1, z)
    parent.keys.insert(i, child.keys[midt])
    z.keys = child.keys[midt + 1 :]
    child.keys = child.keys[: midt + 1]
    if not child.leaf:
      z.children = child.children[t:]
      child.children = child.children[:t]