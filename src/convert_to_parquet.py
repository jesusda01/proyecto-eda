"""Preparación incremental reproducible, sin acumular el CSV completo."""

import argparse
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from .stream_reader import stable_id


def process_csv_to_parquet(
    csv_input_path="data/raw/2019-Nov.csv", output_dir="data", target_10m=10_000_000, chunk_size=50000
):
    """Escribe cortes con tamaños honestos; nunca inventa un CSV ausente.

    No se modifica event_type ni se etiqueta fraude al azar. El escenario
    sintético demostrativo se añade por separado al ejecutar el pipeline.
    """
    path, output = Path(csv_input_path), Path(output_dir)
    if not path.is_file():
        raise FileNotFoundError(
            f"CSV no encontrado: {path}. No se generarán millones de filas automáticamente."
        )
    if target_10m < 1 or chunk_size < 1:
        raise ValueError("Objetivo y tamaño de bloque deben ser positivos.")
    output.mkdir(parents=True, exist_ok=True)
    sizes = {
        name: n
        for name, n in [
            ("1K", 1000),
            ("10K", 10000),
            ("100K", 100000),
            ("1M", 1000000),
            ("10M", 10000000),
        ]
        if n <= target_10m
    }
    if target_10m not in sizes.values():
        sizes[str(target_10m)] = target_10m
    writers, counts = {}, {name: 0 for name in sizes}
    total = 0
    reader = pd.read_csv(
        path,
        chunksize=chunk_size,
        dtype={
            "user_id": "string",
            "product_id": "string",
            "category_id": "string",
            "price": "float64",
        },
    )
    try:
        for block in reader:
            required = {"event_time", "event_type", "product_id", "user_id", "price"}
            if not required <= set(block):
                raise ValueError(f"Faltan columnas: {sorted(required-set(block))}")
            block = block.iloc[: max(0, target_10m - total)].copy()
            if block.empty:
                break
            block["event_id"] = [stable_id(path.name, total + i) for i in range(len(block))]
            block["source"] = path.name
            block["source_row"] = range(total, total + len(block))
            block["is_simulated"] = False
            block["event_type_original"] = block["event_type"]
            # Campos opcionales con tipos constantes entre row groups.
            for field in ("category_code", "brand", "user_session"):
                if field not in block:
                    block[field] = ""
                block[field] = block[field].fillna("").astype(str)
            block["event_time"] = pd.to_datetime(
                block["event_time"], utc=True, errors="coerce"
            ).astype(str)
            table = pa.Table.from_pandas(block, preserve_index=False)
            for name, maximum in sizes.items():
                take = min(len(block), maximum - counts[name])
                if take <= 0:
                    continue
                if name not in writers:
                    writers[name] = pq.ParquetWriter(
                        output / f"ecommerce_{name}.parquet", table.schema, compression="snappy"
                    )
                writers[name].write_table(table.slice(0, take))
                counts[name] += take
            total += len(block)
            print(
                f"Preparadas {total:,} / {target_10m:,} filas; sin acumular bloques en RAM.",
                flush=True,
            )
            if total >= target_10m:
                break
    finally:
        reader.close()
        for writer in writers.values():
            writer.close()
    for name, count in counts.items():
        if count and count != sizes[name]:
            # El nombre no debe prometer una cardinalidad que el CSV no contiene.
            original = output / f"ecommerce_{name}.parquet"
            honest = output / f"ecommerce_{name}_incompleto_{count}.parquet"
            original.replace(honest)
    if total < target_10m:
        print(
            f"El CSV terminó en {total:,} filas. Los cortes incompletos se identifican en su nombre."
        )
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="data/raw/2019-Nov.csv")
    parser.add_argument("--output", default="data")
    parser.add_argument("--rows", type=int, default=1000)
    args = parser.parse_args()
    process_csv_to_parquet(args.csv, args.output, args.rows)
