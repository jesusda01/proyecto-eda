"""Prueba HTTP de pausa, parada y reinicio con observaciones explícitas."""

import json
import time
import unittest
from urllib.request import Request, urlopen
from tempfile import TemporaryDirectory
from web import server as dashboard
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

            server, thread = dashboard.start_verified_server(0, run)
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
                health = request("/api/health")
                self.assertEqual(health["service"], dashboard.SERVICE_ID)
                self.assertEqual(health["version"], dashboard.VIEWER_VERSION)
                for path, kind in (("/", "text/html"), ("/dashboard.css", "text/css"), ("/dashboard.js", "text/javascript"), ("/dashboard-popover.js", "text/javascript")):
                    with urlopen(url + path, timeout=3) as response:
                        self.assertEqual(response.status, 200)
                        self.assertIn(kind, response.headers["Content-Type"])
                        self.assertTrue(response.read())
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
                self.assertEqual(state["version"], dashboard.VIEWER_VERSION)
                self.assertEqual(request("/api/health")["version"], dashboard.VIEWER_VERSION)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)
