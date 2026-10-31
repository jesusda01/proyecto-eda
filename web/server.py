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
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from src.pipeline import PipelineCancelled

ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = Path(__file__).resolve().parent / "static"
LOCK = threading.Lock()
STOP = threading.Event()
STATE = {"status": "idle", "active": None, "stages": {}, "logs": [], "version": 4}


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


class ViewerServer(ThreadingHTTPServer):
    allow_reuse_address = False

    def server_bind(self):
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def start_run(server, delay, run_function=None):
    with LOCK:
        if STATE["status"] in ("running", "stopping"):
            return False
        STOP.clear()
        STATE.clear()
        STATE.update(status="running", active=None, stages={}, logs=[], version=3)
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
        files = {
            "/": ("index.html", "text/html"),
            "/dashboard.css": ("dashboard.css", "text/css"),
            "/dashboard.js": ("dashboard.js", "text/javascript"),
            "/dashboard-popover.js": ("dashboard-popover.js", "text/javascript"),
        }
        if url.path in files:
            name, kind = files[url.path]
            self.respond(200, (STATIC_DIR / name).read_bytes(), kind + "; charset=utf-8")
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


def launch_dashboard(run_function=None, auto_run=False):
    import urllib.request
    import urllib.error

    os.chdir(ROOT)
    url = "http://127.0.0.1:8765"
    try:
        server = ViewerServer(("127.0.0.1", 8765), Handler)
    except OSError:
        if not auto_run:
            raise SystemExit(f"Ya existe un visor o servicio en {url}.")
        with urllib.request.urlopen(url + "/api/state", timeout=5) as response:
            remote = json.load(response)
        if remote.get("version") != 4:
            raise SystemExit("Cierra el visor anterior con Ctrl+C y vuelve a ejecutar main.py.")
        configuration = getattr(run_function, "keywords", {})
        body = json.dumps({"delay": 0, "config": configuration}).encode()
        request = urllib.request.Request(
            url + "/api/run", data=body, headers={"X-Pipeline-Viewer": "1"}
        )
        try:
            with urllib.request.urlopen(request, timeout=5):
                print(f"Ejecución solicitada en {url}.")
        except urllib.error.HTTPError as error:
            raise SystemExit(f"El visor está ocupado: {error}") from error
        return
    server.run_function = run_function
    print(f"Visor: {url}. Mantén la terminal abierta; Ctrl+C cierra el servidor.", flush=True)
    if auto_run:
        start_run(server, 0)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        STOP.set()
    finally:
        server.server_close()


if __name__ == "__main__":
    launch_dashboard()
