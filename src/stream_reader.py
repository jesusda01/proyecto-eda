"""Lectura incremental y escenarios explícitamente simulados.

Los Parquet antiguos se leen sin sobrescribirlos: sus IDs truncados se guardan
como event_id_original y se sustituyen por un ID estable de fuente + posición.
Los eventos modernos conservan su ID para poder reconocer reenvíos.
"""

from hashlib import sha256
from pathlib import Path
import math
import pyarrow.parquet as pq
from .etapa_1_preprocesamiento_lotes import parse_time


def stable_id(source, row):
    return sha256(f"{source}:{row}".encode()).hexdigest()


def clean_record(raw, source, row):
    record = {
        k: (None if isinstance(v, float) and not math.isfinite(v) else v) for k, v in raw.items()
    }
    identifier = record.get("event_id")
    if not identifier or len(str(identifier)) == 8:
        record["event_id_original"] = identifier
        record["event_id"] = stable_id(source, row)
    record["source"] = record.get("source") or source
    record["source_row"] = row
    record["is_simulated"] = bool(record.get("is_simulated", False))
    record["event_type_original"] = record.get("event_type_original", record.get("event_type"))
    # Un indicador aleatorio antiguo no se presenta como fraude confirmado.
    record["is_fraud_simulated"] = bool(
        record.get("is_fraud_simulated", record.get("is_fraud", False))
    )
    record.setdefault("account_flagged", False)
    record.setdefault("order_id", None)
    record.setdefault("order_urgency", "standard")
    return record


def read_parquet_batches(path, batch_size=500, limit=None):
    if batch_size < 1 or (limit is not None and limit < 0):
        raise ValueError("Tamaño de lote positivo y límite no negativo requeridos.")
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"No existe {path}. Ejecuta la preparación indicada en el README.")
    offset = 0
    for block in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
        records = []
        for raw in block.to_pylist():
            if limit is not None and offset >= limit:
                break
            records.append(
                clean_record(
                    raw, "legacy:ecommerce" if "ecommerce_" in path.name else path.name, offset
                )
            )
            offset += 1
        if records:
            yield records
        if limit is not None and offset >= limit:
            return


def demo_scenario(products, event_time):
    """Ocho eventos nuevos, no modificaciones encubiertas de filas reales.

    Dos órdenes normales de dos productos muestran co-compra. Cuatro intentos
    desde una cuenta señalada muestran la verificación exacta y el umbral.
    """
    values = list(products.values())[:2]
    if len(values) < 2:
        return []
    stamp = parse_time(event_time).isoformat()
    events = []
    for i in range(8):
        product = values[i % 2]
        event = {k: product.get(k) for k in ("product_id", "price", "brand", "category_code")}
        event.update(
            event_id=stable_id("escenario-controlado", i),
            event_time=stamp,
            user_id=f"sim-user-{i//2 if i<4 else 99}",
            event_type="purchase",
            event_type_original=None,
            order_id=f"sim-order-{i//2 if i<4 else 99}",
            order_urgency="express" if i < 2 else "standard",
            source="escenario-controlado",
            is_simulated=True,
            is_fraud_simulated=i >= 4,
            account_flagged=i >= 4,
            alert_type="payment_alert" if i >= 4 else None,
        )
        events.append(event)
    return events


def product_vector(product, buckets=8):
    """Precio logarítmico normalizado + categorías/marcas por hashing estable.

    Son atributos de catálogo, no embeddings semánticos. Hashing puede unir
    categorías distintas; ese límite se declara en el informe y benchmarks.
    """
    vector = [min(math.log1p(float(product["price"])) / math.log1p(100000), 1.0)] + [0.0] * (
        buckets * 2
    )
    for offset, key in ((1, "category_code"), (1 + buckets, "brand")):
        value = str(product.get(key) or "desconocido")
        position = int.from_bytes(sha256(value.encode()).digest()[:4], "big") % buckets
        vector[offset + position] = 0.5
    return vector
