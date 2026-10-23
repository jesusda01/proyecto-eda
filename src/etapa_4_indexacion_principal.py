class BPlusTreeNode:
  """Nodo constitutivo de un Árbol B+ para indexación en disco."""

  def __init__(self, leaf=False):
    self.leaf = leaf  # True si es nodo hoja, False si es nodo interno
    self.keys = []  # Lista de claves indexadas
    self.children = (
        []
    )  # Punteros a hijos (interno) o valores asociados (hoja)
    self.next = None  # Puntero al siguiente nodo hoja (para escaneos por rango)


class IndexacionPrincipalBPlusTree:
  """Etapa 4: Indexación principal

  Estructura: Árbol B+ balanceado optimizado para almacenamiento secundario.
  """

  def __init__(self, t=3):
    self.root = BPlusTreeNode(leaf=True)  # Raíz inicial configurada como hoja
    self.t = t  # Grado mínimo del árbol B+

  def insert(self, key, value):
    root = self.root
    # Si la raíz se llena, se divide incrementando la altura del árbol
    if len(root.keys) >= (2 * self.t - 1):
      temp = BPlusTreeNode(leaf=False)
      self.root = temp
      temp.children.insert(0, root)
      self._split_child(temp, 0, root)
      self._insert_non_full(temp, key, value)
    else:
      self._insert_non_full(root, key, value)

  def _insert_non_full(self, node, key, value):
    i = len(node.keys) - 1
    if node.leaf:
      # Búsqueda de la posición correcta para mantener el orden ascendente
      while i >= 0:
        k_val = node.keys[i][0] if isinstance(node.keys[i], tuple) else node.keys[i]
        if key < k_val:
          i -= 1
        else:
          break
      # Inserción segura usando .insert() en lugar de asignación directa por índice
      node.keys.insert(i + 1, (key, value))
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

    # Extrae de forma segura la clave media para ascenderla al nodo padre
    promoted_key = (
        child.keys[midt][0] if isinstance(child.keys[midt], tuple) else child.keys[midt]
    )

    parent.children.insert(i + 1, z)
    parent.keys.insert(i, promoted_key)

    # Divide las claves entre el nodo actual y el nuevo hermano (z)
    z.keys = child.keys[midt + 1 :]
    child.keys = child.keys[: midt + 1]

    if not child.leaf:
      z.children = child.children[t:]
      child.children = child.children[:t]
    else:
      # Enlaza los nodos hoja para optimizar futuros escaneos por rango
      z.next = child.next
      child.next = z