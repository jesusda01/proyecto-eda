"""Visor local basado en observaciones explícitas, sin rastrear líneas Python.

Una ejecución compartida a la vez. Las pausas solo afectan a la demostración;
los benchmarks invocan directamente las estructuras sin este servidor.
"""

import copy
import json
import math
import os
from pathlib import Path
import socket
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from src.pipeline import PipelineCancelled
from .diagnostics import (
    ASSETS, LOGGER, LOG_FILE, SERVICE_ID, VIEWER_VERSION,
    ViewerDiagnosticError, configure_logging, local_request, probe_dashboard,
)

ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = Path(__file__).resolve().parent / "static"
LOCK = threading.Lock()
STOP = threading.Event()
STATE = {"status": "idle", "active": None, "stages": {}, "logs": [], "version": VIEWER_VERSION}


def safe(value):
    """JSON estándar: los campos no finitos nunca se envían como NaN."""
    if isinstance(value, dict):
        return {str(k): safe(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [safe(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def run_pipeline(delay=0, run_function=None, allow_prepare=False):
    """Publica entrada y salida de cada etapa; cancelar no cierra la página."""

    def log(message):
        with LOCK:
            STATE["logs"].append(str(message))
            STATE["logs"] = STATE["logs"][-150:]
        print(message, flush=True)
        LOGGER.info("%s", message)

    def observe(stage, phase, data):
        if STOP.is_set():
            raise PipelineCancelled()
        with LOCK:
            key = str(stage)
            if phase == "start":
                if stage == 0:
                    STATE["stages"] = {}
                STATE["batch"] = data.get("lote", 1)
                STATE["active"] = stage
                STATE["stages"][key] = {"status": "running", "input": safe(data)}
            else:
                STATE["stages"][key].update(
                    status="done", data=safe(data), seconds=round(data["seconds"], 4)
                )
        if phase == "start" and delay and STOP.wait(delay):
            raise PipelineCancelled()

    try:
        if run_function is None:
            from main import run_integrated_pipeline

            run_function = run_integrated_pipeline
        result = run_function(observer=observe, cancel=STOP.is_set, log=log)
        with LOCK:
            STATE.update(status="done", active=None, result=safe(result))
    except PipelineCancelled:
        with LOCK:
            active = STATE["active"]
            if active is not None:
                STATE["stages"][str(active)]["status"] = "stopped"
            STATE.update(status="stopped", active=None)
        log("Ejecución detenida. Los lotes terminados permanecen en disco.")
    except Exception:
        error = traceback.format_exc()
        with LOCK:
            active = STATE["active"]
            if active is not None:
                STATE["stages"][str(active)]["status"] = "error"
            STATE.update(status="error", active=None, error=error)
        log(error)
        LOGGER.error("El pipeline terminó con error:\n%s", error)


class ViewerServer(ThreadingHTTPServer):
    allow_reuse_address = False

    def server_bind(self):
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()

    def handle_error(self, request, client_address):
        LOGGER.exception("Error al atender una petición del visor")
        print(f"Error HTTP del visor. Revisa el registro: {LOG_FILE}", flush=True)


def start_run(server, delay, run_function=None):
    with LOCK:
        if STATE["status"] in ("running", "stopping"):
            return False
        STOP.clear()
        STATE.clear()
        STATE.update(status="running", active=None, stages={}, logs=[], version=VIEWER_VERSION)
    threading.Thread(
        target=run_pipeline,
        args=(delay, run_function or getattr(server, "run_function", None)),
        daemon=True,
    ).start()
    return True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, status, value, kind="application/json; charset=utf-8"):
        body = (
            value
            if isinstance(value, bytes)
            else json.dumps(safe(value), ensure_ascii=False, allow_nan=False).encode()
        )
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        url = urlparse(self.path)
        files = ASSETS
        if url.path in files:
            name, kind = files[url.path]
            self.respond(200, (STATIC_DIR / name).read_bytes(), kind + "; charset=utf-8")
        elif url.path == "/api/health":
            with LOCK:
                status = STATE["status"]
            self.respond(200, {
                "service": SERVICE_ID, "version": VIEWER_VERSION,
                "pid": os.getpid(), "pipeline_status": status,
            })
        elif url.path == "/api/state":
            with LOCK:
                result = copy.deepcopy(STATE)
            self.respond(200, result)
        elif url.path == "/api/query":
            import main

            engine = main.LAST_PIPELINE
            with LOCK:
                busy = STATE["status"] in ("running", "stopping")
            if busy or engine is None:
                self.respond(409, {"error": "Espera a que termine una ejecución."})
                return
            params = parse_qs(url.query)
            product = params.get("product", [""])[0]
            prefix = params.get("prefix", ["pro"])[0][:100]
            self.respond(
                200,
                {
                    "recommendations": engine.recommend(product),
                    "suggestions": engine.trie.search_prefix(prefix)[:10],
                    "price_candidates": engine.price_candidates(product),
                    "alerts": engine.alerts[-5:],
                    "products": list(engine.catalog)[:30],
                },
            )
        else:
            self.respond(404, {"error": "Ruta desconocida."})

    def do_POST(self):
        if self.headers.get("X-Pipeline-Viewer") != "1":
            self.respond(403, {"error": "Origen no autorizado."})
            return
        if self.path == "/api/stop":
            with LOCK:
                if STATE["status"] == "running":
                    STATE["status"] = "stopping"
                    STOP.set()
            self.respond(200, {"requested": True})
        elif self.path == "/api/run":
            try:
                size = int(self.headers.get("Content-Length", 0))
                if not 0 <= size <= 1024:
                    raise ValueError()
                payload = json.loads(self.rfile.read(size) or b"{}")
                delay = float(payload.get("delay", 0))
                if not math.isfinite(delay) or not 0 <= delay <= 5:
                    raise ValueError()
            except (ValueError, TypeError):
                self.respond(400, {"error": "Pausa inválida."})
                return
            runner = None
            options = payload.get("config")
            if options is not None:
                from functools import partial
                from main import run_integrated_pipeline

                allowed = {"input_path", "batch_size", "simulate", "limit"}
                if not isinstance(options, dict) or not set(options) <= allowed:
                    self.respond(400, {"error": "Configuración inválida."})
                    return
                runner = partial(run_integrated_pipeline, **options)
            started = start_run(self.server, delay, runner)
            self.respond(202 if started else 409, {"started": started})
        else:
            self.respond(404, {"error": "Ruta desconocida."})


def start_verified_server(port, run_function=None):
    """Arranca HTTP y comprueba sus rutas antes de iniciar cualquier lote."""
    missing = [name for name, _ in ASSETS.values() if not (STATIC_DIR / name).is_file()]
    if missing:
        raise ViewerDiagnosticError("FRONTEND_INCOMPLETO", f"Faltan archivos en {STATIC_DIR}: {', '.join(missing)}.")
    server = ViewerServer(("127.0.0.1", port), Handler)
    server.run_function = run_function
    thread = threading.Thread(target=server.serve_forever, name="visor-http", daemon=True)
    try:
        thread.start()
        probe_dashboard(f"http://127.0.0.1:{server.server_port}", verify_assets=True)
    except BaseException:
        if thread.is_alive():
            server.shutdown()
        server.server_close()
        if thread.ident is not None:
            thread.join(timeout=3)
        raise
    return server, thread


def launch_dashboard(run_function=None, auto_run=False, port=8765):
    """Anuncia el visor solo después de verificar HTTP; conserva el servidor vivo."""
    import errno
    from urllib.error import HTTPError

    configure_logging()
    url = f"http://127.0.0.1:{port}"
    LOGGER.info("Inicio solicitado en %s", url)
    try:
        server, thread = start_verified_server(port, run_function)
    except OSError as error:
        LOGGER.error("No se pudo abrir %s", url, exc_info=True)
        if error.errno == errno.EADDRINUSE or getattr(error, "winerror", None) == 10048:
            try:
                remote = probe_dashboard(url, verify_assets=True)
                if not auto_run:
                    raise ViewerDiagnosticError("VISOR_ABIERTO", f"Ya existe un visor en {url}, PID {remote.get('pid')}.")
                configuration = getattr(run_function, "keywords", {})
                with local_request(url + "/api/run", {"delay": 0, "config": configuration}):
                    pass
            except HTTPError as request_error:
                detail = "El visor ya está procesando. Usa su página o espera a que termine." if request_error.code == 409 else str(request_error)
                raise SystemExit(f"[VISOR_OCUPADO] {detail}\nRegistro: {LOG_FILE}") from request_error
            except (ViewerDiagnosticError, OSError) as probe_error:
                LOGGER.error("Puerto ocupado: %s", probe_error)
                raise SystemExit(
                    f"[PUERTO_OCUPADO] No se puede reutilizar {url}. {probe_error}\n"
                    f"Cierra el visor anterior en su terminal o usa python main.py --port {port + 1 if port < 65535 else 8765}.\n"
                    f"Registro: {LOG_FILE}"
                ) from probe_error
            print(f"Visor existente verificado: {url}. PID: {remote.get('pid')}.\nEjecución solicitada; el servidor sigue en su terminal original.", flush=True)
            return
        raise SystemExit(
            f"[PUERTO_NO_DISPONIBLE] No se pudo abrir {url}: {error}.\n"
            f"Prueba python main.py --port {port + 1 if port < 65535 else 8765}. Registro: {LOG_FILE}"
        ) from error
    except ViewerDiagnosticError as error:
        LOGGER.error("No se pudo preparar el visor: %s", error, exc_info=True)
        raise SystemExit(f"ERROR {error}\nRegistro: {LOG_FILE}") from error
    print(
        f"Visor listo: {url}\n"
        "Comprobación HTTP: OK (servidor, página, CSS y JavaScript).\n"
        f"PID del servidor: {os.getpid()}. Registro: {LOG_FILE}\n"
        "Abre la URL en el navegador de esta computadora.\n"
        "Mantén esta terminal abierta; Ctrl+C cierra el servidor.", flush=True,
    )
    LOGGER.info("Servidor listo y verificado en %s", url)
    try:
        if auto_run:
            start_run(server, 0)
        while thread.is_alive():
            thread.join(timeout=0.5)
        raise RuntimeError("El hilo del servidor terminó inesperadamente.")
    except KeyboardInterrupt:
        print("\nCerrando el visor...", flush=True)
    except Exception:
        LOGGER.exception("Fallo del servidor")
        raise
    finally:
        STOP.set()
        if thread.is_alive():
            server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        LOGGER.info("Servidor cerrado en %s", url)
        print("Visor cerrado. La URL dejará de responder hasta ejecutar main.py otra vez.", flush=True)


if __name__ == "__main__":
    launch_dashboard()
