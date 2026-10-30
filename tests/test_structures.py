"""Pruebas de contratos e invariantes; no comparan detalles privados triviales."""

import random
from pathlib import Path
import tempfile
import unittest
import numpy as np

from src.etapa_1_preprocesamiento_lotes import PreprocesamientoLotes
from src.etapa_2_cola_prioridades import ColaPrioridadesHeap
from src.etapa_3_almacenamiento_lookup_cache import AlmacenamientoLookupCache
from src.etapa_4_indexacion_principal import IndexacionPrincipalBPlusTree
from src.etapa_5_indice_multidimensional import IndiceMultidimensionalKDTree
from src.etapa_6_filtrado_probabilistico import BloomFilter, CountMinSketch, RiskWindow
from src.etapa_7_similitud_aproximada import HNSWApproximateSearch, MinHashLSHSimulator
from src.etapa_8_grafo_dominio import GrafoDominioRed
from src.pipeline import EcommercePipeline, PipelineConfig
from src.stream_reader import demo_scenario, read_parquet_batches


class StructuresTest(unittest.TestCase):
    def test_bplus_random_ranges_and_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "index.sqlite"
            tree = IndexacionPrincipalBPlusTree(t=2, path=path, cache_size=3)
            keys = list(range(200))
            random.Random(42).shuffle(keys)
            for key in keys:
                tree.insert((key // 5, str(key)), key)
                tree.validate()
            tree.insert((0, "0"), "updated")
            expected = [((key // 5, str(key)), key if key else "updated") for key in range(25, 50)]
            self.assertEqual(tree.range_search((5, ""), (9, "z")), sorted(expected))
            tree.close()
            reopened = IndexacionPrincipalBPlusTree(path=path, cache_size=2)
            self.assertTrue(reopened.validate())
            self.assertEqual(reopened.search((0, "0")), "updated")
            self.assertEqual(reopened.search((999, "x")), None)
            reopened.close()

    def test_cache_update_eviction_and_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = AlmacenamientoLookupCache(1, Path(directory) / "events.sqlite", 1)
            cache.put_storage("a", {"price": 1})
            cache.get_from_cache_or_storage("a")
            cache.put_storage("a", {"price": 2})
            self.assertEqual(cache.get_from_cache_or_storage("a")[0]["price"], 2)
            cache.put_storage("b", {})
            self.assertEqual(cache.get_from_cache_or_storage("b")[0], {})
            self.assertNotIn("a", cache.cache)
            self.assertEqual(cache.get_storage("a")["price"], 2)
            cache.close()

    def test_dedup_validation_and_priority(self):
        pre = PreprocesamientoLotes(history_limit=1)
        base = {
            "event_id": "a",
            "event_time": "2019-11-01 00:00:00 UTC",
            "event_type": "view",
            "product_id": 1,
            "user_id": "user_1",
            "price": 0,
        }
        valid = pre.validate_batch(
            [base, dict(base, event_id="b"), None, dict(base, price=float("nan"))]
        )
        self.assertEqual(len(valid), 2)
        self.assertEqual(len(pre.rejected), 2)
        self.assertEqual(len(pre.deduplicate_batch(valid + [valid[0]])), 2)
        self.assertEqual(pre.deduplicate_batch(valid), [])
        pre.close()
        queue = ColaPrioridadesHeap()
        for event in [
            base,
            dict(base, event_id="c", event_type="purchase"),
            dict(base, event_id="alert", alert_type="payment_alert"),
        ]:
            queue.push(event.copy())
        self.assertEqual([queue.pop()["event_id"] for _ in range(3)], ["alert", "c", "a"])

    def test_kd_matches_exact_and_rejects_misalignment(self):
        points = np.random.default_rng(42).normal(size=(150, 3)).tolist()
        ids = list(range(150))
        kd = IndiceMultidimensionalKDTree(points, ids)
        for qid in (0, 17, 90):
            self.assertEqual(
                kd.query(points[qid], 7, qid),
                kd.knn_exact_baseline(points[qid], points, ids, 7, qid),
            )
        with self.assertRaises(ValueError):
            IndiceMultidimensionalKDTree([(0, 1), (1, 2)], ["only_one"])

    def test_bloom_cms_and_window_verification(self):
        bloom = BloomFilter(100)
        cms = CountMinSketch()
        counts = {str(i): i + 1 for i in range(30)}
        for key, n in counts.items():
            bloom.add(key)
            cms.add(key, n)
        self.assertTrue(all(key in bloom for key in counts))
        self.assertTrue(all(cms.estimate_frequency(k) >= n for k, n in counts.items()))
        risk = RiskWindow(10, 3, flagged_accounts=["bad"])
        self.assertFalse(risk.observe("bad", 1)["requires_review"])
        risk.observe("bad", 2)
        self.assertTrue(risk.observe("bad", 3)["requires_review"])
        self.assertFalse(risk.observe("bad", 20)["requires_review"])
        risk.bloom.add("false_positive_candidate")
        for t in (21, 22, 23):
            result = risk.observe("false_positive_candidate", t)
        self.assertFalse(result["requires_review"])

    def test_lsh_updates_and_removal(self):
        lsh = MinHashLSHSimulator()
        lsh.insert("a", {"1", "2", "3"})
        lsh.insert("b", {"1", "2", "3"})
        lsh.insert("c", {"9"})
        self.assertIn("b", [i for i, _ in lsh.query({"1", "2", "3"}, exclude_id="a")])
        lsh.insert("b", {"9"})
        self.assertNotIn("b", [i for i, _ in lsh.query({"1", "2", "3"}, exclude_id="a")])
        lsh.remove("c")
        self.assertNotIn("c", lsh.signatures)

    def test_hnsw_recall_dimensions_and_update(self):
        vectors = np.random.default_rng(42).normal(size=(250, 6)).tolist()
        ids = list(range(250))
        index = HNSWApproximateSearch(ef_search=80)
        for identifier, vector in zip(ids, vectors):
            index.add_vector(identifier, vector)
        recall = []
        for qid in range(15):
            found = index.search_knn(vectors[qid], 5, qid)
            exact = IndiceMultidimensionalKDTree.knn_exact_baseline(
                vectors[qid], vectors, ids, 5, qid
            )
            recall.append(len(set(found) & set(exact)) / 5)
        self.assertGreaterEqual(float(np.mean(recall)), 0.9)
        self.assertGreater(len(index.layers), 1)
        index.add_vector(0, [100.0] * 6)
        self.assertEqual(index.search_knn([100.0] * 6, 1), [0])
        with self.assertRaises(ValueError):
            index.search_knn([1, 2])

    def test_cobuy_and_integrated_output(self):
        graph = GrafoDominioRed()
        for order in ("a", "b"):
            graph.add_purchase(order, "phone")
            graph.add_purchase(order, "case")
            graph.add_purchase(order, "case")
        self.assertEqual(graph.graph["phone"]["case"]["weight"], 2)
        self.assertEqual(graph.complementary("phone"), ["case"])
        with tempfile.TemporaryDirectory() as directory:
            pipeline = EcommercePipeline(PipelineConfig(batch_size=20, runtime_dir=directory))
            products = {
                "phone": {
                    "product_id": "phone",
                    "price": 300,
                    "brand": "a",
                    "category_code": "phone",
                },
                "case": {"product_id": "case", "price": 30, "brand": "a", "category_code": "case"},
            }
            scenario = demo_scenario(products, "2019-11-01T00:00:00+00:00")
            try:
                result = pipeline.process_batch(scenario)
                self.assertEqual(result["counts"]["processed"], 8)
                self.assertEqual(result["counts"]["alerts"], 2)
                self.assertTrue(result["recommendations"])
                self.assertTrue(any(r["signals"]["cobuy"] for r in result["recommendations"]))
                self.assertEqual(len(pipeline.graph.graph), 2)
                self.assertTrue(pipeline.price_index.validate())
                second = pipeline.process_batch(scenario)
                self.assertEqual(second["counts"]["duplicates"], 8)
                self.assertEqual(second["counts"]["processed"], 8)
            finally:
                pipeline.close()
            self.assertTrue(pipeline.price_candidates("phone"))


if __name__ == "__main__":
    unittest.main()
