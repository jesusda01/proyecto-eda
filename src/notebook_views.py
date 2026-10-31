"""Presentación de observaciones y mediciones para los notebooks del proyecto.

Estas funciones no procesan eventos ni alteran los resultados experimentales.
"""

import json

import matplotlib.pyplot as plt
import pandas as pd
from IPython.display import Markdown, display

STAGES = (
    "Ingesta y buffer", "Validar y deduplicar", "Priorizar eventos",
    "Almacenar y consultar", "Indexar precios", "Buscar vecinos exactos",
    "Verificar riesgo", "Comparar similitud", "Analizar co-compra", "Servir resultados",
)

FAMILIES = {
    "Bloom y pertenencia exacta": ("bloom", "set"),
    "CMS y frecuencia exacta": ("cms", "counter"),
    "Similitud de conjuntos": ("minhash_lsh", "jaccard_exact"),
    "Vecinos vectoriales": ("hnsw", "knn_exact"),
    "Influencia": ("pagerank", "degree"),
    "Comunidades": ("louvain", "label_propagation"),
}


def observation_table(observations):
    """Empareja inicio y fin, manteniendo el lote al que pertenece cada etapa."""
    rows, starts = [], {}
    for item in observations:
        stage, data = item["etapa"], item["datos"]
        if item["fase"] == "start":
            starts[stage] = data
        else:
            rows.append({
                "Lote": starts[stage]["lote"], "Etapa": stage,
                "Operación": STAGES[stage], "Tiempo observado (ms)": data["seconds"] * 1000,
                "entrada": starts[stage], "salida": data,
            })
    return pd.DataFrame(rows)


def show_fields(data):
    """Despliega muestras como tablas y metadatos como pares de campo y valor."""
    scalars = []
    for key, value in data.items():
        if key == "seconds":
            continue
        if isinstance(value, dict) and "muestra" in value:
            display(Markdown(f"**{key}** · {value['cantidad']} registros; muestra de hasta tres."))
            if value["muestra"]:
                display(pd.json_normalize(value["muestra"]))
        elif isinstance(value, list) and value:
            display(Markdown(f"**{key}**"))
            if isinstance(value[0], dict):
                display(pd.json_normalize(value))
            else:
                display(pd.DataFrame({"Valor": [json.dumps(v, ensure_ascii=False) for v in value]}))
        else:
            scalars.append({"Campo": key, "Valor": json.dumps(value, ensure_ascii=False)})
    if scalars:
        display(pd.DataFrame(scalars))


def inspect_stage(table, batch=1, stage=0):
    row = table.loc[(table["Lote"] == batch) & (table["Etapa"] == stage)].iloc[0]
    display(Markdown(f"### Lote {batch} · {stage} · {STAGES[stage]}"))
    display(Markdown("**Entrada**"))
    show_fields(row["entrada"])
    display(Markdown("**Salida**"))
    show_fields(row["salida"])


def explore_stages(table):
    """Selectores de lote y etapa; usa el mismo registro que la vista estática."""
    import ipywidgets as widgets

    return widgets.interactive(
        lambda lote, etapa: inspect_stage(table, lote, etapa),
        lote=widgets.Dropdown(options=sorted(table["Lote"].unique()), description="Lote:"),
        etapa=widgets.Dropdown(options=[(f"{i} · {name}", i) for i, name in enumerate(STAGES)], description="Etapa:"),
    )


def plot_stage_times(table):
    totals = table.groupby("Etapa")["Tiempo observado (ms)"].sum().reindex(range(10))
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.barh([f"{i} · {name}" for i, name in enumerate(STAGES)], totals, color="#087e8b")
    ax.invert_yaxis()
    ax.set(xlabel="Tiempo acumulado observado (ms)", title="Trabajo observado por etapa en esta ejecución")
    ax.grid(axis="x", alpha=0.2)
    fig.tight_layout()
    return fig


def population_table(report):
    columns = {
        "events": "Eventos", "repetitions": "Repeticiones", "vectors": "Vectores",
        "sets": "Conjuntos", "nodes": "Nodos", "edges": "Aristas",
        "graph_events": "Eventos del grafo", "entity_cap": "Límite de entidades",
    }
    return pd.DataFrame([
        {label: dataset[key] for key, label in columns.items()}
        for dataset in report["datasets"]
    ])


def benchmark_table(dataset):
    rows = []
    for algorithm, metrics in dataset["summary"].items():
        rows.append({
            "Algoritmo": algorithm,
            "Tiempo medio (ms)": metrics["seconds"]["mean"] * 1000,
            "Desviación (ms)": metrics["seconds"]["stdev"] * 1000,
            "Memoria actual (KiB)": metrics["current_bytes"]["mean"] / 1024,
            "Memoria pico (KiB)": metrics["peak_bytes"]["mean"] / 1024,
        })
    return pd.DataFrame(rows).set_index("Algoritmo")


def quality_table(dataset):
    metrics = {
        "bloom": ("fpr", "false_negatives"),
        "cms": ("mean_absolute_error", "max_overestimate"),
        "minhash_lsh": ("recall", "precision", "signature_mae"),
        "hnsw": ("recall_at_5", "tie_aware_recall_at_5"),
        "pagerank": ("overlap_with_degree_at_20",),
        "louvain": ("modularity", "stability_ari", "community_count"),
        "label_propagation": ("modularity", "stability_ari", "community_count"),
    }
    rows = []
    for algorithm, names in metrics.items():
        for name in names:
            value = dataset["summary"][algorithm].get(name)
            if value is not None:
                rows.append({"Algoritmo": algorithm, "Métrica": name, "Media": value["mean"], "Desviación": value["stdev"]})
    return pd.DataFrame(rows)


def plot_comparison(dataset, family, metric="seconds"):
    algorithms = FAMILIES[family]
    scale = 1000 if metric == "seconds" else 1 / 1024
    unit = "Tiempo total (ms)" if metric == "seconds" else "Memoria Python pico (KiB)"
    means = [dataset["summary"][a][metric]["mean"] * scale for a in algorithms]
    errors = [dataset["summary"][a][metric]["stdev"] * scale for a in algorithms]
    fig, ax = plt.subplots(figsize=(8, 3.5))
    bars = ax.barh(algorithms, means, xerr=errors, capsize=4, color=["#087e8b", "#66758c"])
    ax.bar_label(bars, fmt="%.2f", padding=6)
    ax.set(xlabel=unit, title=f"{family} · {dataset['events']:,} eventos · {dataset['repetitions']} repeticiones")
    ax.margins(x=0.25)
    ax.grid(axis="x", alpha=0.2)
    fig.tight_layout()
    return fig


def plot_scaling(report):
    """Solo traza curvas donde crece la población; no une tamaños constantes."""
    datasets = sorted(report["datasets"], key=lambda d: d["events"])
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for ax, algorithms, population, title in (
        (axes[0], ("bloom", "set", "cms", "counter"), "events", "Todos los eventos"),
        (axes[1], ("hnsw", "knn_exact"), "vectors", "Población vectorial efectiva"),
    ):
        x = [d[population] for d in datasets]
        for algorithm in algorithms:
            y = [d["summary"][algorithm]["seconds"]["mean"] for d in datasets]
            if len(set(x)) > 1:
                ax.loglog(x, y, "o-", label=algorithm)
            else:
                ax.scatter(x, y, label=algorithm, alpha=0.7)
                ax.set_yscale("log")
        ax.set(xlabel="Eventos" if population == "events" else "Vectores", ylabel="Tiempo total (s)", title=title)
        ax.legend()
        ax.grid(True, alpha=0.2)
    if len({d["vectors"] for d in datasets}) == 1:
        axes[1].text(0.5, -0.28, "Tamaño constante: no permite inferir escalabilidad ANN.", ha="center", transform=axes[1].transAxes)
    fig.tight_layout()
    return fig
