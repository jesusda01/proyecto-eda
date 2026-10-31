"""Comprobación HTTP local y registro de problemas del visor."""

import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

SERVICE_ID = "proyecto-eda-visor"
VIEWER_VERSION = 4
LOG_FILE = Path(__file__).resolve().parents[1] / "output/logs/visor.log"
LOGGER = logging.getLogger("proyecto_eda.visor")
ASSETS = {
    "/": ("index.html", "text/html"),
    "/dashboard.css": ("dashboard.css", "text/css"),
    "/dashboard.js": ("dashboard.js", "text/javascript"),
    "/dashboard-popover.js": ("dashboard-popover.js", "text/javascript"),
}


class ViewerDiagnosticError(RuntimeError):
    """Error con código, explicación y acción sugerida para la terminal."""

    def __init__(self, code, message):
        self.code = code
        super().__init__(f"[{code}] {message}")


def configure_logging():
    """Conserva hasta tres archivos de 2 MB, sin duplicar manejadores."""
    if LOGGER.handlers:
        return
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=2, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s pid=%(process)d %(message)s"))
        LOGGER.addHandler(handler)
    except OSError as error:
        # Un fallo al guardar el log no impide mostrar el diagnóstico en terminal.
        LOGGER.addHandler(logging.NullHandler())
        print(f"Aviso: no se puede escribir {LOG_FILE}: {error}", file=sys.stderr)


def local_request(url, payload=None):
    """Accede al servidor local sin usar los proxies del entorno del equipo."""
    request = Request(
        url,
        data=None if payload is None else json.dumps(payload).encode("utf-8"),
        headers={"X-Pipeline-Viewer": "1", "Content-Type": "application/json"},
    )
    return build_opener(ProxyHandler({})).open(request, timeout=3)


def probe_dashboard(url, verify_assets=False):
    """Comprueba identidad, versión y, opcionalmente, archivos por HTTP real.

    La salud del servidor se distingue del estado del pipeline: una ejecución
    fallida puede seguir consultándose desde un servidor que responde bien.
    """
    try:
        with local_request(url + "/api/health") as response:
            data = json.loads(response.read(65536))
    except HTTPError as error:
        raise ViewerDiagnosticError(
            "HTTP_INVALIDO",
            f"{url}/api/health respondió HTTP {error.code}. Puede ser otro servicio "
            "o un visor anterior. Revisa la terminal que ocupa ese puerto.",
        ) from error
    except (URLError, OSError) as error:
        raise ViewerDiagnosticError(
            "SIN_CONEXION",
            f"No se pudo conectar con {url}. Comprueba que main.py siga abierto "
            f"y que uses el puerto correcto. Detalle: {error}",
        ) from error
    except (ValueError, UnicodeError) as error:
        raise ViewerDiagnosticError("RESPUESTA_INVALIDA", f"{url} no devolvió un diagnóstico JSON válido.") from error
    if not isinstance(data, dict) or data.get("service") != SERVICE_ID:
        raise ViewerDiagnosticError("SERVICIO_AJENO", f"El servicio de {url} no se identifica como el visor de este proyecto.")
    if data.get("version") != VIEWER_VERSION:
        raise ViewerDiagnosticError(
            "VERSION_INCOMPATIBLE", "El visor abierto pertenece a otra versión. "
            "Ciérralo con Ctrl+C en su terminal y vuelve a ejecutar main.py.",
        )
    if verify_assets:
        for route, (_, kind) in ASSETS.items():
            try:
                with local_request(url + route) as response:
                    if response.headers.get_content_type() != kind or not response.read(1):
                        raise ValueError("contenido vacío o tipo incorrecto")
            except (URLError, OSError, ValueError) as error:
                raise ViewerDiagnosticError(
                    "FRONTEND_NO_DISPONIBLE", f"No se pudo cargar {url}{route}: {error}. Revisa los archivos de web/static/.",
                ) from error
    return data


def diagnose_dashboard(port=8765):
    """Imprime una comprobación reproducible; devuelve 0 si responde o 1 si falla."""
    configure_logging()
    url = f"http://127.0.0.1:{port}"
    print(f"Diagnóstico del visor: {url}\nPython: {sys.executable}\nRegistro: {LOG_FILE}", flush=True)
    try:
        data = probe_dashboard(url, verify_assets=True)
    except ViewerDiagnosticError as error:
        LOGGER.error("Diagnóstico fallido: %s", error, exc_info=True)
        print(f"ERROR {error}\nEjecuta python main.py en otra terminal y repite esta comprobación.", flush=True)
        return 1
    print(
        f"OK: servidor y archivos del frontend responden por HTTP. PID: {data.get('pid')}.\n"
        f"Estado del pipeline: {data.get('pipeline_status')}.\n"
        "Si la página no abre, prueba esta misma URL en un navegador externo de esta computadora.\n"
        "Esta comprobación no evalúa el navegador ni ejecuta JavaScript.", flush=True,
    )
    LOGGER.info("Diagnóstico correcto en %s", url)
    return 0
