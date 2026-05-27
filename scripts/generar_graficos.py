import json
import os
import glob
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use("Agg")
except ImportError:
    print("ERROR: instala matplotlib con: pip install matplotlib")
    exit(1)

CARPETA_RESULTADOS = "resultados"
CARPETA_GRAFICOS   = "resultados/graficos"
os.makedirs(CARPETA_GRAFICOS, exist_ok=True)

# Carga de resultados
def cargamosResultados():

    resultados = {}
    for archivo in sorted(glob.glob(f"{CARPETA_RESULTADOS}/escenario*.json")):
        nombre = Path(archivo).stem
        if "serie_temporal" in nombre:
            continue
        try:
            with open(archivo) as f:
                resultados[nombre] = json.load(f)
        except Exception as e:
            print(f"  Aviso: no se pudo leer {archivo}: {e}")
    return resultados

def cargamos_Series_Temporales():
    """Lee los archivos *_serie_temporal.json (si existen)."""
    series = {}
    for archivo in sorted(glob.glob(f"{CARPETA_RESULTADOS}/*_serie_temporal.json")):
        nombre = Path(archivo).stem.replace("_serie_temporal", "")
        try:
            with open(archivo) as f:
                series[nombre] = json.load(f)
        except Exception:
            pass
    return series

# FUncion para generar graficos
def grafico_throughput_comparativo(resultados):
    nombres = list(resultados.keys())
    valores = [r.get("throughput_promedio_seg", r.get("throughput_qps", r.get("throughput_consultas_seg", 0))) for r in resultados.values()]

    plt.figure(figsize=(10, 5))
    barras = plt.bar(range(len(nombres)), valores, color="steelblue")
    plt.xticks(range(len(nombres)), nombres, rotation=30, ha="right", fontsize=8)
    plt.ylabel("Throughput (consultas/s)")
    plt.title("Comparacion de Throughput entre escenarios")
    for barra, valor in zip(barras, valores):
        plt.text(barra.get_x() + barra.get_width() / 2, barra.get_height(),
                 f"{valor:.1f}", ha="center", va="bottom", fontsize=8)
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/01_throughput_comparativo.png", dpi=120)
    plt.close()
    print("  → 01_throughput_comparativo.png")

def grafico_DE_latencias_comparativo(resultados):
    nombres = list(resultados.keys())
    p50_valores = [r.get("latencia_procesamiento", {}).get("p50", 0) for r in resultados.values()]
    p95_valores = [r.get("latencia_procesamiento", {}).get("p95", 0) for r in resultados.values()]

    indices = range(len(nombres))
    ancho_barra = 0.35
    plt.figure(figsize=(10, 5))
    plt.bar([i - ancho_barra/2 for i in indices], p50_valores, ancho_barra, label="p50", color="steelblue")
    plt.bar([i + ancho_barra/2 for i in indices], p95_valores, ancho_barra, label="p95", color="coral")
    plt.xticks(indices, nombres, rotation=30, ha="right", fontsize=8)
    plt.ylabel("Latencia (ms)")
    plt.title("Latencia p50 vs p95 por escenario")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/02_latencias_p50_p95.png", dpi=120)
    plt.close()
    print("  → 02_latencias_p50_p95.png")

def graficoConsumers_vs_throughput(resultados):
    """Para escenario 3: como escala con N consumidores."""
    datos_consumidores = []
    for nombre, datos in resultados.items():
        n = None
        if "1consumidor" in nombre or "_1con" in nombre:
            n = 1
        elif "2consumidores" in nombre:
            n = 2
        elif "4consumidores" in nombre:
            n = 4
        if n is not None:
            datos_consumidores.append((n, datos.get("throughput_promedio_seg", datos.get("throughput_consultas_seg", 0)),
                                          datos.get("latencia_procesamiento", {}).get("p95", 0)))

    if len(datos_consumidores) < 2:
        return
    datos_consumidores.sort()
    cantidades_n = [d[0] for d in datos_consumidores]
    throughputs  = [d[1] for d in datos_consumidores]
    latencias_p95 = [d[2] for d in datos_consumidores]

    figura, eje1 = plt.subplots(figsize=(8, 5))
    color_throughput = "steelblue"
    eje1.set_xlabel("Cantidad de consumidores")
    eje1.set_ylabel("Throughput (qps)", color=color_throughput)
    eje1.plot(cantidades_n, throughputs, "o-", color=color_throughput,
              linewidth=2, markersize=8, label="Throughput")
    eje1.tick_params(axis="y", labelcolor=color_throughput)

    eje2 = eje1.twinx()
    color_latencia = "coral"
    eje2.set_ylabel("Latencia p95 (ms)", color=color_latencia)
    eje2.plot(cantidades_n, latencias_p95, "s--", color=color_latencia,
              linewidth=2, markersize=8, label="Latencia p95")
    eje2.tick_params(axis="y", labelcolor=color_latencia)

    plt.title("Impacto de la cantidad de consumidores")
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/03_escalado_consumidores.png", dpi=120)
    plt.close()
    print("  → 03_escalado_consumidores.png")

def graficoCONretries_DLQ(resultados):
    """Comparacion de reintentos y DLQ entre escenarios."""
    nombres = list(resultados.keys())
    reintentos = [r.get("reintentos", 0) for r in resultados.values()]
    dlq        = [r.get("cantidad_dlq", 0) for r in resultados.values()]

    indices = range(len(nombres))
    ancho_barra = 0.35
    plt.figure(figsize=(10, 5))
    plt.bar([i - ancho_barra/2 for i in indices], reintentos, ancho_barra,
            label="Reintentos", color="goldenrod")
    plt.bar([i + ancho_barra/2 for i in indices], dlq, ancho_barra,
            label="DLQ", color="firebrick")
    plt.xticks(indices, nombres, rotation=30, ha="right", fontsize=8)
    plt.ylabel("Cantidad de consultas")
    plt.title("Reintentos y DLQ por escenario")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/04_reintentos_dlq.png", dpi=120)
    plt.close()
    print("  → 04_reintentos_dlq.png")

def grafico_de_LA_recuperacion(resultados):
    """Recovery rate y recovery time por escenario."""
    nombres = list(resultados.keys())
    tasas_recuperacion = [r.get("tasa_recuperacion_pct", 0) for r in resultados.values()]
    tiempos_promedio   = [r.get("tiempos_recuperacion_seg", {}).get("promedio", 0)
                          for r in resultados.values()]

    figura, (eje_arriba, eje_abajo) = plt.subplots(2, 1, figsize=(10, 8))
    eje_arriba.bar(range(len(nombres)), tasas_recuperacion, color="seagreen")
    eje_arriba.set_xticks(range(len(nombres)))
    eje_arriba.set_xticklabels(nombres, rotation=30, ha="right", fontsize=8)
    eje_arriba.set_ylabel("Recovery rate (%)")
    eje_arriba.set_title("Tasa de recuperacion por escenario")

    eje_abajo.bar(range(len(nombres)), tiempos_promedio, color="darkorange")
    eje_abajo.set_xticks(range(len(nombres)))
    eje_abajo.set_xticklabels(nombres, rotation=30, ha="right", fontsize=8)
    eje_abajo.set_ylabel("Tiempo de recuperacion promedio (s)")
    eje_abajo.set_title("Tiempo de recuperacion por escenario")
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/05_recuperacion.png", dpi=120)
    plt.close()
    print("  → 05_recuperacion.png")

def grafico_backlog_temporal(series_temporales):
    """Grafica la evolucion temporal del backlog para cada experimento."""
    if not series_temporales:
        print("  (no hay series temporales — corre backlog_snapshotter.py durante experimentos)")
        return

    plt.figure(figsize=(12, 6))
    for nombre, serie in series_temporales.items():
        tiempos = [punto["t_relativo_seg"] for punto in serie]
        # Sumar backlog total (queries + retry + dlq)
        backlogs = []
        for punto in serie:
            valor_backlog = punto.get("backlog", {})
            total_backlog = sum(v for v in valor_backlog.values() if isinstance(v, int) and v >= 0)
            backlogs.append(total_backlog)
        plt.plot(tiempos, backlogs, label=nombre, linewidth=1.5)

    plt.xlabel("Tiempo (s)")
    plt.ylabel("Backlog total (mensajes pendientes)")
    plt.title("Evolucion temporal del backlog")
    plt.legend(fontsize=8)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/06_backlog_temporal.png", dpi=120)
    plt.close()
    print("  → 06_backlog_temporal.png")

def grafico_throughput_temporal(series_temporales):
    if not series_temporales:
        return
    plt.figure(figsize=(12, 6))
    for nombre, serie in series_temporales.items():
        tiempos       = [punto["t_relativo_seg"] for punto in serie]
        throughputs   = [punto["throughput_qps"] for punto in serie]
        plt.plot(tiempos, throughputs, label=nombre, linewidth=1.5)
    plt.xlabel("Tiempo (s)")
    plt.ylabel("Throughput (qps)")
    plt.title("Evolucion temporal del throughput")
    plt.legend(fontsize=8)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/07_throughput_temporal.png", dpi=120)
    plt.close()
    print("  → 07_throughput_temporal.png")

def grafico_sincrono_vs_kafka(resultados):
    """Comparacion especifica: sistema base (Tarea 1) vs Kafka."""
    base_data = None
    kafka_data = None
    for nombre, datos in resultados.items():
        if "base" in nombre.lower():
            base_data = datos
        elif "kafka_1consumidor" in nombre or ("recuperacion" in nombre and "kafka" in nombre):
            kafka_data = datos

    if not (base_data and kafka_data):
        print("  (no hay datos suficientes para comparacion sincrono vs Kafka)")
        return

    categorias = ["Throughput\n(qps)", "Latencia p50\n(ms)", "Latencia p95\n(ms)"]
    valores_base = [
        base_data.get("throughput_qps", base_data.get("throughput_consultas_seg", 0)),
        base_data.get("latencias_ms", {}).get("zipf", {}).get("p50_ms",
            base_data.get("latencia_procesamiento", {}).get("p50", 0)),
        base_data.get("latencias_ms", {}).get("zipf", {}).get("p95_ms",
            base_data.get("latencia_procesamiento", {}).get("p95", 0)),
    ]
    valores_kafka = [
        kafka_data.get("throughput_promedio_seg", kafka_data.get("throughput_consultas_seg", 0)),
        kafka_data.get("latencia_procesamiento", {}).get("p50", 0),
        kafka_data.get("latencia_procesamiento", {}).get("p95", 0),
    ]

    indices = range(len(categorias))
    ancho_barra = 0.35
    plt.figure(figsize=(9, 5))
    plt.bar([i - ancho_barra/2 for i in indices], valores_base, ancho_barra,
            label="Sistema Base (sincrono)", color="gray")
    plt.bar([i + ancho_barra/2 for i in indices], valores_kafka, ancho_barra,
            label="Sistema con Kafka", color="steelblue")
    plt.xticks(indices, categorias)
    plt.ylabel("Valor")
    plt.title("Sistema sincrono vs Sistema con Kafka")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/08_sincrono_vs_kafka.png", dpi=120)
    plt.close()
    print("  → 08_sincrono_vs_kafka.png")

def main():
    print(f"Leyendo resultados de '{CARPETA_RESULTADOS}/' ...")
    resultados        = cargamosResultados()
    series_temporales = cargamos_Series_Temporales()

    if not resultados:
        print("ERROR: no se encontraron archivos JSON. Corre experimentos.sh primero.")
        return

    print(f"Encontrados {len(resultados)} escenarios y {len(series_temporales)} series temporales.")
    print(f"Generando graficos en '{CARPETA_GRAFICOS}/' ...")

    grafico_throughput_comparativo(resultados)
    grafico_DE_latencias_comparativo(resultados)
    graficoConsumers_vs_throughput(resultados)
    graficoCONretries_DLQ(resultados)
    grafico_de_LA_recuperacion(resultados)
    grafico_backlog_temporal(series_temporales)
    grafico_throughput_temporal(series_temporales)
    grafico_sincrono_vs_kafka(resultados)

    print(f"\nListo. Graficos en {CARPETA_GRAFICOS}/")

if __name__ == "__main__":
    main()
