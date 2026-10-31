"""Diagnóstico de conexiones, versiones, archivos y fallos de ejecución."""

from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from web import diagnostics
from web import server as dashboard


class DiagnosticsTest(unittest.TestCase):
    def test_no_listener_reports_connection_error(self):
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            url = f"http://127.0.0.1:{reserved.getsockname()[1]}"
            with self.assertRaises(diagnostics.ViewerDiagnosticError) as caught:
                diagnostics.probe_dashboard(url)
            self.assertEqual(caught.exception.code, "SIN_CONEXION")

    def test_unrelated_service_and_incompatible_version(self):
        payload = {"version": diagnostics.VIEWER_VERSION}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}"
            with self.assertRaises(diagnostics.ViewerDiagnosticError) as caught:
                diagnostics.probe_dashboard(url)
            self.assertEqual(caught.exception.code, "SERVICIO_AJENO")
            payload.update(service=diagnostics.SERVICE_ID, version=-1)
            with self.assertRaises(diagnostics.ViewerDiagnosticError) as caught:
                diagnostics.probe_dashboard(url)
            self.assertEqual(caught.exception.code, "VERSION_INCOMPATIBLE")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

    def test_incomplete_frontend_does_not_announce_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(dashboard, "STATIC_DIR", Path(directory)):
                with self.assertRaises(diagnostics.ViewerDiagnosticError) as caught:
                    dashboard.start_verified_server(0)
                self.assertEqual(caught.exception.code, "FRONTEND_INCOMPLETO")

    def test_diagnostic_checks_real_http_ignoring_environment_proxy(self):
        server, thread = dashboard.start_verified_server(0)
        try:
            with patch.dict("os.environ", {"http_proxy": "http://127.0.0.1:1", "no_proxy": ""}):
                health = diagnostics.probe_dashboard(f"http://127.0.0.1:{server.server_port}", verify_assets=True)
            self.assertEqual(health["service"], diagnostics.SERVICE_ID)
            with patch.object(diagnostics, "configure_logging"), patch.object(diagnostics, "LOGGER"), redirect_stdout(StringIO()) as output:
                self.assertEqual(diagnostics.diagnose_dashboard(server.server_port), 0)
            self.assertIn("archivos del frontend responden", output.getvalue())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

    def test_pipeline_error_is_recorded_without_killing_http(self):
        def fail(**kwargs):
            raise ValueError("Fallo controlado de prueba")

        with dashboard.LOCK:
            dashboard.STATE.update(status="idle", active=None)
        server, thread = dashboard.start_verified_server(0, fail)
        try:
            with patch.object(dashboard, "LOGGER") as logger, redirect_stdout(StringIO()):
                dashboard.run_pipeline(run_function=fail)
            logger.error.assert_called_once()
            self.assertIn("Fallo controlado", str(logger.error.call_args))
            health = diagnostics.probe_dashboard(f"http://127.0.0.1:{server.server_port}")
            self.assertEqual(health["pipeline_status"], "error")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
            with dashboard.LOCK:
                dashboard.STATE.update(status="idle", active=None)
