"""Preparación pequeña, nombres honestos y compatibilidad de fuentes antiguas."""

import tempfile
from pathlib import Path
import unittest
import pandas as pd
import pyarrow.parquet as pq
from src.convert_to_parquet import process_csv_to_parquet
from src.stream_reader import read_parquet_batches


class PreparationTest(unittest.TestCase):
    def test_incremental_csv_no_silent_fraud(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = [
                {
                    "event_time": "2019-11-01 00:00:00 UTC",
                    "event_type": "view",
                    "product_id": 1,
                    "user_id": 2,
                    "price": 0,
                }
                for _ in range(12)
            ]
            csv = root / "input.csv"
            pd.DataFrame(records).to_csv(csv, index=False)
            counts = process_csv_to_parquet(csv, root / "out", target_10m=12, chunk_size=5)
            self.assertEqual(counts["12"], 12)
            target = root / "out/ecommerce_12.parquet"
            table = pq.read_table(target)
            self.assertEqual(len(set(table["event_id"].to_pylist())), 12)
            self.assertEqual(set(table["event_type"].to_pylist()), {"view"})
            self.assertNotIn("is_fraud", table.column_names)
            self.assertEqual(sum(len(b) for b in read_parquet_batches(target, 5)), 12)
            with self.assertRaises(FileNotFoundError):
                process_csv_to_parquet(root / "missing.csv", root / "out", 12)

    def test_truncated_legacy_ids_migrate_without_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ecommerce_1K.parquet"
            pd.DataFrame(
                [{"event_id": "deadbeef", "price": 1}, {"event_id": "deadbeef", "price": 2}]
            ).to_parquet(path)
            batches = list(read_parquet_batches(path, 1))
            a, b = batches[0][0], batches[1][0]
            self.assertNotEqual(a["event_id"], b["event_id"])
            self.assertEqual(a["event_id_original"], "deadbeef")
            self.assertEqual(pq.read_table(path)["event_id"].to_pylist(), ["deadbeef", "deadbeef"])
