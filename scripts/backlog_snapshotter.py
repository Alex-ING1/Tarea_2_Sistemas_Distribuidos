import sys
import time
import json
import os
import urllib.request

URL_METRICAS = os.getenv("URL_METRICAS", "http://localhost:8080/metrics/snapshot")

def tomarSnapshot():
    try:
        with urllib.request.urlopen(URL_METRICAS, timeout=3) as respuesta:
            return json.loads(respuesta.read().decode("utf-8"))
    except Exception as error:
        print(f"  [snapshot] error: {error}")
        return None

def main():
    if len(sys.argv) < 2:
        print("Uso: python3 backlog_snapshotter.py <nombre_experimento> [duracion_seg] [intervalo_seg]")
        sys.exit(1)

    nombreExperimento = sys.argv[1]
    duracionTOotal     = int(sys.argv[2]) if len(sys.argv) > 2 else 120
    intervalo_de_la_muestra  = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0

    serie_tempo = []
    momentoInicio = time.time()
    print(f"[snapshotter] '{nombreExperimento}' duracion={duracionTOotal}s intervalo={intervalo_de_la_muestra}s")

    while (time.time() - momentoInicio) < duracionTOotal:
        snapshot = tomarSnapshot()
        if snapshot:
            registro = {
                "t_relativo_seg":       round(time.time() - momentoInicio, 2),
                "throughput_qps":       snapshot.get("throughput_consultas_seg", 0),
                "total_procesadas":     snapshot.get("total_procesadas", 0),
                "reintentos":           snapshot.get("reintentos", 0),
                "cantidad_dlq":         snapshot.get("cantidad_dlq", 0),
                "backlog":              snapshot.get("backlog", {}),
                "latencia_procesamiento": snapshot.get("latencia_procesamiento", {}),
            }
            serie_tempo.append(registro)
            print(f"  t={registro['t_relativo_seg']:6.1f}s  "
                  f"throughput={registro['throughput_qps']:5.1f}  "
                  f"backlog={registro['backlog']}  "
                  f"dlq={registro['cantidad_dlq']}")
        time.sleep(intervalo_de_la_muestra)

    os.makedirs("resultados", exist_ok=True)
    archivoDeSalida = f"resultados/{nombreExperimento}_serie_temporal.json"
    with open(archivoDeSalida, "w") as f:
        json.dump(serie_tempo, f, indent=2)
    print(f"[snapshotter] Guardado en {archivoDeSalida}")

if __name__ == "__main__":
    main()
