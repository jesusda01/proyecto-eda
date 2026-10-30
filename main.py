"""Entrada del proyecto: procesa todos los lotes y ofrece un visor opcional.

Ejemplo: python main.py --sin-visor --dataset data/ecommerce_1K.parquet
La preparación del CSV es explícita: python -m src.convert_to_parquet --help.
"""

import argparse
from functools import partial
import json
from pathlib import Path

from src.pipeline import EcommercePipeline, PipelineConfig
from src.stream_reader import read_parquet_batches, demo_scenario

ROOT = Path(__file__).resolve().parent
LAST_PIPELINE = None


def run_integrated_pipeline(
    input_path=None,
    batch_size=500,
    simulate=True,
    observer=None,
    cancel=None,
    log=print,
    limit=None,
):
    """Ejecuta un histórico como stream finito, sin reiniciar índices por lote.

    Los ocho eventos del escenario didáctico se contabilizan por separado.
    Se pueden desactivar con --sin-escenario. Nunca se generan millones de
    eventos por no encontrar una ruta ni se sobrescriben los CSV originales.
    """
    global LAST_PIPELINE
    path = Path(input_path) if input_path else ROOT / "data/ecommerce_1K.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"No existe {path}. Prepara el CSV según el README.")
    config = PipelineConfig(batch_size=batch_size, runtime_dir=str(ROOT / "data/runtime"))

    def observe(stage, phase, data):
        if phase == "start":
            log(f'[ETAPA {stage}] Lote {engine.counts["batches"]+1}: inicio')
        if observer:
            observer(stage, phase, data)

    engine = EcommercePipeline(config, observe, cancel)
    LAST_PIPELINE = engine
    log(f"Dataset: {path.name}. Lotes de {batch_size}. Escenario adicional: {simulate}.")
    last_time = "2019-11-01T00:00:00+00:00"
    try:
        for batch in read_parquet_batches(path, batch_size, limit):
            engine.process_batch(batch)
            last_time = engine.last_event_time
        if simulate:
            extra = demo_scenario(engine.catalog, last_time)
            log(f"Escenario controlado: {len(extra)} eventos SIMULADOS adicionales.")
            for offset in range(0, len(extra), batch_size):
                engine.process_batch(extra[offset : offset + batch_size])
        result = engine.result()
        target = ROOT / "output/results"
        target.mkdir(parents=True, exist_ok=True)
        (target / "last_pipeline.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        log(f'Finalizado: {result["counts"]}. Resultados: output/results/last_pipeline.json')
        return result
    finally:
        engine.close()


def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default=str(ROOT / "data/ecommerce_1K.parquet"))
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--limit", type=int, help="Recorte explícito, registrado por los contadores."
    )
    parser.add_argument("--sin-visor", action="store_true")
    parser.add_argument("--sin-escenario", action="store_true")
    args = parser.parse_args()
    run = partial(
        run_integrated_pipeline,
        input_path=args.dataset,
        batch_size=args.batch_size,
        simulate=not args.sin_escenario,
        limit=args.limit,
    )
    if args.sin_visor:
        run()
    else:
        from dashboard import launch_dashboard

        launch_dashboard(run_function=run, auto_run=True)


if __name__ == "__main__":
    # Una sola instancia del módulo, también al atender consultas desde la web.
    import sys

    sys.modules.setdefault("main", sys.modules[__name__])
    cli()
