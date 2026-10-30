"""Resúmenes probabilísticos compactos y alerta temporal con verificación."""

from collections import deque, Counter
import heapq
import itertools
import math
import mmh3
import numpy as np


class BloomFilter:
    """Sin falsos negativos para elementos insertados; admite falsos positivos."""

    def __init__(self, capacity, error_rate=0.01):
        if capacity < 1 or not 0 < error_rate < 1:
            raise ValueError("Capacidad positiva y 0 < error_rate < 1 requeridos.")
        self.capacity, self.error_rate = capacity, error_rate
        self.size = max(8, math.ceil(-capacity * math.log(error_rate) / math.log(2) ** 2))
        self.hash_count = max(1, round(self.size / capacity * math.log(2)))
        self.bit_array = bytearray((self.size + 7) // 8)

    def add(self, item):
        text = str(item)
        for seed in range(self.hash_count):
            bit = mmh3.hash(text, seed, signed=False) % self.size
            self.bit_array[bit // 8] |= 1 << (bit % 8)

    def __contains__(self, item):
        text = str(item)
        for seed in range(self.hash_count):
            bit = mmh3.hash(text, seed, signed=False) % self.size
            if not self.bit_array[bit // 8] & (1 << (bit % 8)):
                return False
        return True


class CountMinSketch:
    """Contadores uint64: el mínimo no subestima mientras no haya overflow."""

    def __init__(self, width=2000, depth=5):
        if width < 1 or depth < 1:
            raise ValueError("width y depth deben ser positivos.")
        self.width, self.depth = width, depth
        self.table = np.zeros((depth, width), dtype=np.uint64)

    def add(self, item, count=1):
        if count < 0:
            raise ValueError("Este sketch no admite restas.")
        text = str(item)
        for seed in range(self.depth):
            col = mmh3.hash(text, seed, signed=False) % self.width
            if int(self.table[seed, col]) + count > 2**64 - 1:
                raise OverflowError("Contador Count-Min agotado.")
            self.table[seed, col] += count

    def estimate_frequency(self, item):
        return min(
            int(self.table[i, mmh3.hash(str(item), i, signed=False) % self.width])
            for i in range(self.depth)
        )


class RiskWindow:
    """Ventana deslizante exacta y CMS por intervalo de tiempo.

    La clave es la cuenta. Bloom contiene cuentas señaladas; un positivo se
    verifica con el conjunto exacto. CMS se reconstruye al avanzar la ventana;
    el conteo exacto evita confirmar alertas debidas a colisiones. Los eventos
    tardíos fuera de la ventana no se usan como intentos actuales.
    """

    def __init__(self, seconds=300, threshold=3, flagged_accounts=()):
        if seconds < 1 or threshold < 1:
            raise ValueError("Ventana y umbral positivos requeridos.")
        self.seconds, self.threshold = seconds, threshold
        self.flagged = set(map(str, flagged_accounts))
        self.bloom = BloomFilter(max(100, len(self.flagged) * 2))
        for account in self.flagged:
            self.bloom.add(account)
        self.events, self.exact = [], Counter()
        self.sequence = itertools.count()
        self.cms = CountMinSketch()
        self.watermark = float("-inf")

    def observe(self, account, timestamp, flagged=False):
        account = str(account)
        if flagged:
            self.flagged.add(account)
            self.bloom.add(account)
        self.watermark = max(self.watermark, timestamp)
        cutoff = self.watermark - self.seconds
        # El heap de prioridades puede alterar el orden temporal del lote.
        expired = False
        while self.events and self.events[0][0] <= cutoff:
            _, _, old = heapq.heappop(self.events)
            self.exact[old] -= 1
            if not self.exact[old]:
                del self.exact[old]
            expired = True
        if expired:
            self.cms = CountMinSketch()
            for a, n in self.exact.items():
                self.cms.add(a, n)
        late = timestamp <= cutoff
        if not late:
            heapq.heappush(self.events, (timestamp, next(self.sequence), account))
            self.exact[account] += 1
            self.cms.add(account)
        possible = account in self.bloom
        estimated = self.cms.estimate_frequency(account)
        verified = possible and account in self.flagged
        alert = (
            not late
            and verified
            and estimated >= self.threshold
            and self.exact[account] >= self.threshold
        )
        return {
            "account": account,
            "window_seconds": self.seconds,
            "threshold": self.threshold,
            "possibly_flagged": possible,
            "verified_flagged": verified,
            "estimated_count": estimated,
            "exact_count": self.exact[account],
            "late": late,
            "requires_review": alert,
        }
