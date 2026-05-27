import json
import time
import os
import socket
import random
import requests
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# variables que utilizamos
direccionKafka    = os.getenv("DIRECCION_KAFKA", "kafka:9092")
consultasPrinncipales = os.getenv("TOPICO_PRINCIPAL", "queries")
consultasReintento = os.getenv("TOPICO_REINTENTO", "queries_retry")
consultasFalloDemasiado = os.getenv("TOPICO_DLQ", "queries_dlq")
IdGrupo = os.getenv("ID_GRUPO", "consumer_group_1")
MaxNumReintentos = int(os.getenv("MAX_REINTENTOS", "3"))
SegAntesReintento = float(os.getenv("ESPERA_REINTENTO", "2.0"))   # segundos entre reintentos

direccionServidorRedis = os.getenv("HOST_CACHE", "redis_db")
PuertoRedis            = int(os.getenv("PUERTO_CACHE", "6379"))
ttlDelCaache               = int(os.getenv("TTL_CACHE", "300"))       # segundos de vida en cache

DireccionCalRespuestas = os.getenv("URL_GENERADOR_RESPUESTAS", "http://generador_respuestas:8000")
DireccionMetricas            = os.getenv("URL_METRICAS", "http://servicio_metricas:8080/event")

idInstanciaConsumidor           = os.getenv("ID_CONSUMIDOR", socket.gethostname())

porcentajeFallos       = float(os.getenv("TASA_FALLOS", "0"))
cacheDelCliente = redis.Redis(host=direccionServidorRedis, port=PuertoRedis, decode_responses=True)


def esperandoKafka():
    """Reintenta la conexion a Kafka hasta que broker y topicos esten disponibles"""
    print(f"[{idInstanciaConsumidor}] Esperando Kafka...")
    while True:
        try:
            consumidor = KafkaConsumer(
                bootstrap_servers=direccionKafka,
                group_id=IdGrupo,
                value_deserializer=lambda mensaje: json.loads(mensaje.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            pReintentos = KafkaProducer(
                bootstrap_servers=direccionKafka,
                value_serializer=lambda mensaje: json.dumps(mensaje).encode("utf-8"),
            )
            print(f"[{idInstanciaConsumidor}] Kafka disponible.")
            return consumidor, pReintentos
        except NoBrokersAvailable:
            print(f"[{idInstanciaConsumidor}] Kafka no disponible, reintentando en 3s...")
            time.sleep(3)


def generarLlaveCache(consulta):
    """Genera la clave de cache segun el tipo de consulta (igual que Tarea 1)."""
    tipo   = consulta["tipo"]
    parametros = consulta["params"]

    if tipo == "q1":
        return f"count:{parametros['zone_id']}:conf={float(parametros.get('confidence_min', 0.0)):.2f}"
    elif tipo == "q2":
        return f"area:{parametros['zone_id']}:conf={float(parametros.get('confidence_min', 0.0)):.2f}"
    elif tipo == "q3":
        return f"density:{parametros['zone_id']}:conf={float(parametros.get('confidence_min', 0.0)):.2f}"
    elif tipo == "q4":
        return (f"compare:density:{parametros['zone_a']}:{parametros['zone_b']}"
                f":conf={float(parametros.get('confidence_min', 0.0)):.2f}")
    elif tipo == "q5":
        return f"confidence_dist:{parametros['zone_id']}:bins={parametros.get('bins', 5)}"

    return f"{tipo}:{json.dumps(parametros, sort_keys=True)}"


def llamarCalculoConsulta(consulta):
    """Llama al servicio de generacion de respuestas. Lanza excepcion si falla."""
    tipo   = consulta["tipo"]
    parametros = consulta["params"]
    url    = f"{DireccionCalRespuestas}/{tipo}"
    respuestaHTTP = requests.get(url, params=parametros, timeout=5)
    respuestaHTTP.raise_for_status()
    return respuestaHTTP.json()


def registroMetrica(datosEvento):
    """Envia un evento al servicio de metricas (no bloquea si falla)."""
    try:
        requests.post(DireccionMetricas, json=datosEvento, timeout=1)
    except Exception:
        pass


def procesarLaConsulta(consulta, productor_reintento):
    """
    Flujo principal:
      1. Verificar cache → retornar si hay hit.
      2. En miss → llamar al generador de respuestas y guardar en cache.
    Lanza excepcion si ocurre un fallo (real o artificial).
    """
    identificador      = consulta.get("query_id", "?")
    TIPOconsulta      = consulta.get("tipo")
    cantidadReintentos = consulta.get("retry_count", 0)
    creacionDeConsulta = consulta.get("created_at", time.time())
    inicioDeConsulta   = time.time()

    # Fallo artificial para simular escenarios de prueba
    if porcentajeFallos > 0 and random.random() * 100 < porcentajeFallos:
        raise RuntimeError(f"Fallo artificial simulado ({porcentajeFallos}% configurado)")


    llaveCache    = generarLlaveCache(consulta)
    resultadoCache = cacheDelCliente.get(llaveCache)

    if resultadoCache:
        latencia_ms       = (time.time() - inicioDeConsulta) * 1000
        latencia_total_ms = (time.time() - creacionDeConsulta) * 1000
        registroMetrica({
            "event":            "cache_hit",
            "query_id":         identificador,
            "tipo":             TIPOconsulta,
            "consumer_id":      idInstanciaConsumidor,
            "latencia_ms":      latencia_ms,
            "total_latencia_ms": latencia_total_ms,
            "retry_count":      cantidadReintentos,
            "distribucion":     consulta.get("distribucion", "?"),
            "ts":               time.time()
        })
        return True


    resultado_calculado = llamarCalculoConsulta(consulta)
    cacheDelCliente.setex(llaveCache, ttlDelCaache, json.dumps(resultado_calculado))

    latencia_ms       = (time.time() - inicioDeConsulta) * 1000
    latencia_total_ms = (time.time() - creacionDeConsulta) * 1000
    registroMetrica({
        "event":            "cache_miss_processed",
        "query_id":         identificador,
        "tipo":             TIPOconsulta,
        "consumer_id":      idInstanciaConsumidor,
        "latencia_ms":      latencia_ms,
        "total_latencia_ms": latencia_total_ms,
        "retry_count":      cantidadReintentos,
        "distribucion":     consulta.get("distribucion", "?"),
        "ts":               time.time()
    })
    return True


def manejarFallo(consulta, productor_reintento, error):
    """
    Gestiona una consulta fallida:
      - Si no alcanzo el maximo de reintentos → enviar al topico de reintento.
      - Si lo alcanzo → enviar a la Dead Letter Queue (DLQ).
    """
    identificador        = consulta.get("query_id", "?")
    contador         = consulta.get("retry_count", 0) + 1
    consulta["retry_count"] = contador

    if contador >= MaxNumReintentos:
        # Reintentos agotados → DLQ
        productor_reintento.send(consultasFalloDemasiado, value=consulta)
        registroMetrica({
            "event":       "dlq",
            "query_id":    identificador,
            "tipo":        consulta.get("tipo"),
            "consumer_id": idInstanciaConsumidor,
            "retry_count": contador,
            "error":       str(error),
            "ts":          time.time()
        })
        print(f"[{idInstanciaConsumidor}] DLQ: query_id={identificador} "
              f"tras {contador} reintentos. Error: {error}")
    else:
        productor_reintento.send(consultasReintento, value=consulta)
        registroMetrica({
            "event":       "retry",
            "query_id":    identificador,
            "tipo":        consulta.get("tipo"),
            "consumer_id": idInstanciaConsumidor,
            "retry_count": contador,
            "error":       str(error),
            "ts":          time.time()
        })
        print(f"[{idInstanciaConsumidor}] REINTENTO ({contador}/{MaxNumReintentos}): "
              f"query_id={identificador}. Error: {error}")


def escucharMensajes():
    """Bucle principal: suscripcion a topicos y procesamiento de mensajes."""
    consumidor, productor_reintento = esperandoKafka()
    consumidor.subscribe([consultasPrinncipales, consultasReintento])
    print(f"[{idInstanciaConsumidor}] Suscrito a '{consultasPrinncipales}' y '{consultasReintento}'. "
          f"Esperando mensajes...")

    consultas_procesadas = 0

    while True:
        lote_mensajes = consumidor.poll(timeout_ms=1000)

        for particion_topico, mensajes in lote_mensajes.items():
            for mensaje in mensajes:
                consulta     = mensaje.value
                topico_origen = particion_topico.topic

                try:
                    # Esperar antes de reintentar para no saturar el sistema
                    if topico_origen == consultasReintento:
                        time.sleep(SegAntesReintento)

                    procesarLaConsulta(consulta, productor_reintento)
                    consultas_procesadas += 1

                    if consultas_procesadas % 50 == 0:
                        print(f"[{idInstanciaConsumidor}] Procesadas: {consultas_procesadas}")

                except Exception as error:
                    manejarFallo(consulta, productor_reintento, error)


if __name__ == "__main__":
    time.sleep(20)
    escucharMensajes()
