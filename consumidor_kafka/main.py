import json, time, os, socket, requests, redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

direccionKafka         = os.getenv("DIRECCION_KAFKA", "kafka:9092")
consultasPrinncipales  = os.getenv("TOPICO_PRINCIPAL", "queries")
IdGrupo                = os.getenv("ID_GRUPO", "consumer_group_1")
direccionServidorRedis = os.getenv("HOST_CACHE", "redis_db")
PuertoRedis            = int(os.getenv("PUERTO_CACHE", "6379"))
ttlDelCaache           = int(os.getenv("TTL_CACHE", "300"))
DireccionCalRespuestas = os.getenv("URL_GENERADOR_RESPUESTAS", "http://generador_respuestas:8000")
DireccionMetricas      = os.getenv("URL_METRICAS", "http://servicio_metricas:8080/event")
idInstanciaConsumidor  = os.getenv("ID_CONSUMIDOR", socket.gethostname())
cacheDelCliente = redis.Redis(host=direccionServidorRedis, port=PuertoRedis, decode_responses=True)

def esperandoKafka():
    while True:
        try:
            c = KafkaConsumer(bootstrap_servers=direccionKafka, group_id=IdGrupo,
                              value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                              auto_offset_reset="earliest", enable_auto_commit=True,
                              session_timeout_ms=30000, heartbeat_interval_ms=10000)
            p = KafkaProducer(bootstrap_servers=direccionKafka,
                              value_serializer=lambda m: json.dumps(m).encode("utf-8"))
            return c, p
        except NoBrokersAvailable:
            time.sleep(3)

def generarLlaveCache(q):
    t, p = q["tipo"], q["params"]
    if t == "q1": return f"count:{p['zone_id']}:conf={float(p.get('confidence_min',0.0)):.2f}"
    if t == "q2": return f"area:{p['zone_id']}:conf={float(p.get('confidence_min',0.0)):.2f}"
    if t == "q3": return f"density:{p['zone_id']}:conf={float(p.get('confidence_min',0.0)):.2f}"
    if t == "q4": return f"compare:density:{p['zone_a']}:{p['zone_b']}:conf={float(p.get('confidence_min',0.0)):.2f}"
    if t == "q5": return f"confidence_dist:{p['zone_id']}:bins={p.get('bins',5)}"
    return f"{t}:{json.dumps(p, sort_keys=True)}"

def registroMetrica(d):
    try: requests.post(DireccionMetricas, json=d, timeout=1)
    except: pass

def escucharMensajes():
    consumidor, _ = esperandoKafka()
    consumidor.subscribe([consultasPrinncipales])
    while True:
        for _, msgs in consumidor.poll(timeout_ms=1000).items():
            for msg in msgs:
                q = msg.value; inicio = time.time(); llave = generarLlaveCache(q)
                if cacheDelCliente.get(llave):
                    registroMetrica({"event": "cache_hit", "query_id": q.get("query_id"),
                                     "tipo": q.get("tipo"), "latencia_ms": (time.time()-inicio)*1000, "ts": time.time()})
                else:
                    try:
                        r = requests.get(f"{DireccionCalRespuestas}/{q['tipo']}", params=q["params"], timeout=5)
                        r.raise_for_status()
                        cacheDelCliente.setex(llave, ttlDelCaache, json.dumps(r.json()))
                        registroMetrica({"event": "cache_miss_processed", "query_id": q.get("query_id"),
                                         "tipo": q.get("tipo"), "latencia_ms": (time.time()-inicio)*1000, "ts": time.time()})
                    except Exception as e:
                        print(f"[{idInstanciaConsumidor}] Error: {e}")

if __name__ == "__main__":
    time.sleep(20)
    escucharMensajes()
