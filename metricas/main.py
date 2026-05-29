import os
import json
import time
import threading
import numpy as np
import redis
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_MAIN   = os.getenv("TOPIC_MAIN", "queries")
TOPIC_RETRY  = os.getenv("TOPIC_RETRY", "queries-retry")
TOPIC_DLQ    = os.getenv("TOPIC_DLQ", "queries-dlq")
GROUP_ID     = os.getenv("KAFKA_GROUP_ID", "grupo-consumidores")
REDIS_HOST   = os.getenv("REDIS_HOST", "redis")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
OUTPUT_FILE  = os.getenv("OUTPUT_FILE", "/metricas/metricas.json")

lock = threading.Lock()

estado = {
    "latencias": [],

    "total_main":  0,
    "total_retry": 0,
    "total_dlq":   0,

    "recovered": 0,

    "inicio": None,
    "fin":    None,

    "backlog_snapshots": [],
}


def conectar_consumer(topics):
    print(f"[Métricas] Esperando Kafka para tópicos {topics}...")
    while True:
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BROKER,
                group_id="metricas-observer",   
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=30000,       
            )
            print("[Métricas] Conectado a Kafka ✓")
            return consumer
        except NoBrokersAvailable:
            time.sleep(3)


def conectar_redis():
    cliente = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    cliente.ping()
    print("[Métricas] Conectado a Redis ✓")
    return cliente


# ── Hilo de backlog ──────────────────────────────────────────────────────────
def monitorear_backlog(cliente_redis, intervalo=5):
    """
    Cada `intervalo` segundos consulta el lag del consumer group en Kafka
    usando kafka-consumer-groups via redis como proxy de tiempo.
    Como alternativa simple: mide el largo de la cola de retry en Redis si se usara
    una lista, pero aquí usamos el conteo acumulado de mensajes en retry.
    """
    while True:
        with lock:
            snapshot = {
                "timestamp": time.time(),
                "retry_acumulado": estado["total_retry"],
                "dlq_acumulado":   estado["total_dlq"],
            }
            estado["backlog_snapshots"].append(snapshot)
        time.sleep(intervalo)


def escuchar():
    consumer = conectar_consumer([TOPIC_MAIN, TOPIC_RETRY, TOPIC_DLQ])

    for mensaje in consumer:
        topico   = mensaje.topic
        consulta = mensaje.value
        ahora    = time.time()

        with lock:
            if estado["inicio"] is None:
                estado["inicio"] = ahora
            estado["fin"] = ahora

            if topico == TOPIC_MAIN:
                estado["total_main"] += 1

                created_at = consulta.get("created_at")
                if created_at:
                    latencia_ms = (ahora - created_at) * 1000
                    estado["latencias"].append(latencia_ms)

                if consulta.get("retry_count", 0) > 0:
                    estado["recovered"] += 1

            elif topico == TOPIC_RETRY:
                estado["total_retry"] += 1

            elif topico == TOPIC_DLQ:
                estado["total_dlq"] += 1

    print("[Métricas] Consumer timeout — generando resumen final.")


def calcular_y_guardar(cliente_redis):
    with lock:
        latencias    = estado["latencias"]
        total_main   = estado["total_main"]
        total_retry  = estado["total_retry"]
        total_dlq    = estado["total_dlq"]
        recovered    = estado["recovered"]
        inicio       = estado["inicio"]
        fin          = estado["fin"]
        snapshots    = estado["backlog_snapshots"]

    duracion_s   = (fin - inicio) if (fin and inicio and fin > inicio) else 1
    throughput   = round(total_main / duracion_s, 2)

    latencias_arr = np.array(latencias) if latencias else np.array([0])
    p50  = round(float(np.percentile(latencias_arr, 50)), 2)
    p95  = round(float(np.percentile(latencias_arr, 95)), 2)
    avg  = round(float(np.mean(latencias_arr)), 2)
    pmax = round(float(np.max(latencias_arr)), 2)
    pmin = round(float(np.min(latencias_arr)), 2)

    total_procesadas = total_main + total_dlq
    retry_rate    = round(total_retry  / total_procesadas * 100, 2) if total_procesadas else 0
    dlq_rate      = round(total_dlq    / total_procesadas * 100, 2) if total_procesadas else 0
    recovery_rate = round(recovered    / total_retry      * 100, 2) if total_retry      else 0

    recovery_time = round(duracion_s, 2)

    info_redis = cliente_redis.info("stats")
    evictions  = info_redis.get("evicted_keys", 0)
    hits       = info_redis.get("keyspace_hits", 0)
    misses     = info_redis.get("keyspace_misses", 0)
    hit_rate   = round(hits / (hits + misses) * 100, 2) if (hits + misses) else 0

    resumen = {
        "throughput_qps":   throughput,
        "latencia_p50_ms":  p50,
        "latencia_p95_ms":  p95,
        "latencia_avg_ms":  avg,
        "latencia_max_ms":  pmax,
        "latencia_min_ms":  pmin,
        "total_consultas":  total_main,
        "total_reintentos": total_retry,
        "total_dlq":        total_dlq,
        "consultas_recuperadas": recovered,
        "retry_rate_pct":   retry_rate,
        "recovery_rate_pct": recovery_rate,
        "dlq_rate_pct":     dlq_rate,
        "recovery_time_s":  recovery_time,
        "cache_hit_rate_pct": hit_rate,
        "cache_evictions":  evictions,
        "duracion_total_s": round(duracion_s, 2),
        "backlog_snapshots": snapshots,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(resumen, f, indent=2)

    print("\n RESUMEN DE METRICAS :")
    for k, v in resumen.items():
        if k != "backlog_snapshots":
            print(f"  {k}: {v}")
    print(f"\n[Métricas] Guardado en {OUTPUT_FILE}")


if __name__ == "__main__":
    time.sleep(15)
    cliente_redis = conectar_redis()

    hilo_backlog = threading.Thread(
        target=monitorear_backlog, args=(cliente_redis,), daemon=True
    )
    hilo_backlog.start()

    escuchar()

    calcular_y_guardar(cliente_redis)
