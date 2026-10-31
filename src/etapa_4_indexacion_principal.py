"""B+ propio con páginas en SQLite. SQLite solo almacena páginas por ID.

Las divisiones, separadores y búsquedas las implementa este árbol. El caché
acotado evita cargar todo el índice. Se confirma cada lote con ``flush``.
"""

from bisect import bisect_left, bisect_right
from collections import OrderedDict
from dataclasses import dataclass, field
import json
from pathlib import Path
import sqlite3


def _key(value):
    return tuple(_key(v) for v in value) if isinstance(value, list) else value


@dataclass
class BPlusTreeNode:
    id: int
    leaf: bool = True
    keys: list = field(default_factory=list)
    values: list = field(default_factory=list)
    children: list = field(default_factory=list)
    next: int | None = None


class IndexacionPrincipalBPlusTree:
    """Claves comparables; repetir una clave reemplaza su valor.

    El pipeline usa (precio, producto) para conservar productos de igual precio.
    Un nodo interno tiene siempre una cantidad de hijos igual a claves + 1.
    """

    def __init__(self, t=32, path=None, cache_size=128):
        if t < 2 or cache_size < 1:
            raise ValueError("t >= 2 y cache_size >= 1 son obligatorios.")
        if path:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path) if path else ":memory:", check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS pages (id INTEGER PRIMARY KEY, data TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER)")
        meta = dict(self.db.execute("SELECT key,value FROM meta"))
        self.t, self.root_id, self.next_id = (
            meta.get("t", t),
            meta.get("root", 1),
            meta.get("next", 2),
        )
        self.cache_size, self.cache = cache_size, OrderedDict()
        if not meta:
            self._save(BPlusTreeNode(1))
            self.flush()

    @property
    def root(self):
        return self._load(self.root_id)

    def _remember(self, node):
        self.cache[node.id] = node
        self.cache.move_to_end(node.id)
        if len(self.cache) > self.cache_size:
            self.cache.popitem(last=False)

    def _load(self, identifier):
        if identifier in self.cache:
            self.cache.move_to_end(identifier)
            return self.cache[identifier]
        row = self.db.execute("SELECT data FROM pages WHERE id=?", (identifier,)).fetchone()
        if row is None:
            raise ValueError(f"Página inexistente: {identifier}")
        data = json.loads(row[0])
        data["keys"] = [_key(k) for k in data["keys"]]
        node = BPlusTreeNode(**data)
        self._remember(node)
        return node

    def _save(self, node):
        self.db.execute(
            "INSERT OR REPLACE INTO pages VALUES (?,?)", (node.id, json.dumps(vars(node)))
        )
        self._remember(node)

    def _new(self, leaf):
        node = BPlusTreeNode(self.next_id, leaf=leaf)
        self.next_id += 1
        return node

    def insert(self, key, value):
        result = self._insert(self.root_id, key, value)
        if result:
            separator, right_id = result
            root = self._new(False)
            root.keys, root.children = [separator], [self.root_id, right_id]
            self.root_id = root.id
            self._save(root)

    def _insert(self, identifier, key, value):
        node = self._load(identifier)
        if node.leaf:
            i = bisect_left(node.keys, key)
            if i < len(node.keys) and node.keys[i] == key:
                node.values[i] = value
            else:
                node.keys.insert(i, key)
                node.values.insert(i, value)
        else:
            i = bisect_right(node.keys, key)
            result = self._insert(node.children[i], key, value)
            if result:
                separator, right_id = result
                node.keys.insert(i, separator)
                node.children.insert(i + 1, right_id)
        if len(node.keys) <= 2 * self.t - 1:
            self._save(node)
            return None
        mid = len(node.keys) // 2
        right = self._new(node.leaf)
        if node.leaf:
            right.keys, node.keys = node.keys[mid:], node.keys[:mid]
            right.values, node.values = node.values[mid:], node.values[:mid]
            right.next, node.next = node.next, right.id
            separator = right.keys[0]
        else:
            separator = node.keys[mid]
            right.keys, node.keys = node.keys[mid + 1 :], node.keys[:mid]
            right.children, node.children = node.children[mid + 1 :], node.children[: mid + 1]
        self._save(node)
        self._save(right)
        return separator, right.id

    def search(self, key, default=None):
        node = self.root
        while not node.leaf:
            node = self._load(node.children[bisect_right(node.keys, key)])
        i = bisect_left(node.keys, key)
        return node.values[i] if i < len(node.keys) and node.keys[i] == key else default

    def range_search(self, lower, upper, limit=None):
        """Recorre hojas enlazadas. Incluye ambos extremos del rango."""
        if lower > upper or limit == 0:
            return []
        node = self.root
        while not node.leaf:
            node = self._load(node.children[bisect_right(node.keys, lower)])
        output = []
        while True:
            for key, value in zip(node.keys, node.values):
                if key > upper:
                    return output
                if key >= lower:
                    output.append((key, value))
                    if limit is not None and len(output) >= limit:
                        return output
            if node.next is None:
                return output
            node = self._load(node.next)

    def validate(self):
        """Prueba invariantes de orden, separadores, altura y enlaces."""
        leaves = []

        def visit(identifier, depth):
            n = self._load(identifier)
            assert n.keys == sorted(n.keys) and len(set(n.keys)) == len(n.keys)
            assert len(n.keys) <= 2 * self.t - 1
            if n.leaf:
                assert len(n.values) == len(n.keys)
                leaves.append((n, depth))
                return (n.keys[0], n.keys[-1]) if n.keys else (None, None)
            assert len(n.children) == len(n.keys) + 1
            bounds = [visit(child, depth + 1) for child in n.children]
            for i, key in enumerate(n.keys):
                assert bounds[i][1] < key == bounds[i + 1][0]
            return bounds[0][0], bounds[-1][1]

        visit(self.root_id, 0)
        assert len({d for _, d in leaves}) == 1
        for i, (leaf, _) in enumerate(leaves):
            assert leaf.next == (leaves[i + 1][0].id if i + 1 < len(leaves) else None)
        return True

    def flush(self):
        self.db.executemany(
            "INSERT OR REPLACE INTO meta VALUES (?,?)",
            [("t", self.t), ("root", self.root_id), ("next", self.next_id)],
        )
        self.db.commit()

    def close(self):
        self.flush()
        self.db.close()
