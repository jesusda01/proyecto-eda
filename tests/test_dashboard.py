"""Prueba HTTP de pausa, parada y reinicio con observaciones explícitas."""

import json
import threading
import time
import unittest
from urllib.request import Request, urlopen
from tempfile import TemporaryDirectory
import dashboard
from src.pipeline import EcommercePipeline, PipelineConfig


class DashboardTest(unittest.TestCase):
    def test_stop_and_restart(self):
        with TemporaryDirectory() as directory:

            def run(observer, cancel, log):
                engine = EcommercePipeline(PipelineConfig(runtime_dir=directory), observer, cancel)
                try:
                    return engine.process_batch([])
                finally:
                    engine.close()

            server = dashboard.ViewerServer(("127.0.0.1", 0), dashboard.Handler)
            server.run_function = run
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            url = f"http://127.0.0.1:{server.server_port}"

            def request(path, payload=None):
                req = Request(
                    url + path,
                    data=None if payload is None else json.dumps(payload).encode(),
                    headers={"X-Pipeline-Viewer": "1"},
                )
                with urlopen(req, timeout=3) as response:
                    return json.load(response)

            def wait(predicate):
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    value = request("/api/state")
                    if predicate(value):
                        return value
                    time.sleep(0.02)
                self.fail("No se alcanzó el estado esperado.")

            try:
                request("/api/run", {"delay": 2})
                wait(lambda s: s["active"] == 0)
                time.sleep(0.1)
                self.assertEqual(request("/api/state")["status"], "running")
                request("/api/stop", {})
                state = wait(lambda s: s["status"] == "stopped")
                self.assertEqual(state["stages"]["0"]["status"], "stopped")
                request("/api/run", {"delay": 0})
                state = wait(lambda s: s["status"] in ("done", "error"))
                self.assertEqual(state["status"], "done", state.get("error"))
                self.assertEqual(len(state["stages"]), 10)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)
