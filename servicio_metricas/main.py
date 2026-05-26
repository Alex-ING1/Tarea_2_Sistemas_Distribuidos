import json
import time
import os
import threading
from collections import defaultdict, deque
from typing import Optional

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

app = FastAPI()
direccionKafka     = os.getenv("DIRECCION_KAFKA", "kafka:9092")
consultasPrinncipales = os.getenv("TOPICO_PRINCIPAL", "queries")
consultasReintento = os.getenv("TOPICO_REINTENTO", "queries_retry")
consultasFalloDemasiado       = os.getenv("TOPICO_DLQ", "queries_dlq")


candado = threading.Lock() 

estado = {
    "aciertos_cache":          0,
    "fallos_cache":            0,
    "reintentos":              0,
    "enviados_hacia_dlq":            0,
    "total_consultas_procesadas":        0,
    "latencias_procesamiento": deque(maxlen=10000),
    "latencias_extremo_a_extremo": deque(maxlen=10000),  
    "eventos_por_consumidor":  defaultdict(int),
    "eventos_por_tipo_consulta": defaultdict(int),
    "eventos_por_distribucion": defaultdict(int),
    "linea_de_tiempo":         deque(maxlen=5000),   
    "histograma_de_reintentos":   defaultdict(int),     
    "momento_inicio":          time.time(),
    "tiempos_recuperacion_tras_falla":    [],                   
    "inicio_falla_activa":     None,                 
    "consultas_con_fallo":     set(),                
    "consultas_recuperadas_con_fallo_y_solucion":   set(),                
    "consultas_perdidas_dlq":  set(),                
    "primer_evento_exitoso":   None,                 
    "ultimo_evento_exitoso":   None,                 
}


class EventoMetrica(BaseModel):
    event:            str
    query_id:         Optional[str]   = None
    tipo:             Optional[str]   = None
    consumer_id:      Optional[str]   = None
    latencia_ms:      Optional[float] = None
    total_latencia_ms: Optional[float] = None
    retry_count:      Optional[int]   = 0
    distribucion:     Optional[str]   = None
    error:            Optional[str]   = None
    ts:               Optional[float] = None



@app.post("/event")
def recibirElEvento(evento: EventoMetrica):
    with candado:
        marcaTiempo = evento.ts or time.time()
        estado["linea_de_tiempo"].append((marcaTiempo, evento.event))

        if evento.event in ("cache_hit", "cache_miss_processed"):
            if estado["primer_evento_exitoso"] is None:
                estado["primer_evento_exitoso"] = marcaTiempo
            estado["ultimo_evento_exitoso"] = marcaTiempo

        if evento.event == "cache_hit":
            estado["aciertos_cache"]   += 1
            estado["total_consultas_procesadas"] += 1
            
            if evento.query_id and evento.query_id in estado["consultas_con_fallo"]:
                estado["consultas_recuperadas_con_fallo_y_solucion"].add(evento.query_id)

        elif evento.event == "cache_miss_processed":
            estado["fallos_cache"]     += 1
            estado["total_consultas_procesadas"] += 1
            
            if evento.query_id and evento.query_id in estado["consultas_con_fallo"]:
                estado["consultas_recuperadas_con_fallo_y_solucion"].add(evento.query_id)
            
            if estado["inicio_falla_activa"] is not None:
                tiempo_recuperacion = marcaTiempo - estado["inicio_falla_activa"]
                estado["tiempos_recuperacion_tras_falla"].append(tiempo_recuperacion)
                estado["inicio_falla_activa"] = None

        elif evento.event == "retry":
            estado["reintentos"] += 1
            if evento.query_id:
                estado["consultas_con_fallo"].add(evento.query_id)
            if estado["inicio_falla_activa"] is None:
                estado["inicio_falla_activa"] = marcaTiempo

        elif evento.event == "dlq":
            estado["enviados_hacia_dlq"] += 1
            if evento.query_id:
                estado["consultas_perdidas_dlq"].add(evento.query_id)

        
        if evento.latencia_ms is not None:
            estado["latencias_procesamiento"].append(evento.latencia_ms)
        if evento.total_latencia_ms is not None:
            estado["latencias_extremo_a_extremo"].append(evento.total_latencia_ms)

    
        if evento.consumer_id:
            estado["eventos_por_consumidor"][evento.consumer_id] += 1
        if evento.tipo:
            estado["eventos_por_tipo_consulta"][evento.tipo] += 1
        if evento.distribucion:
            estado["eventos_por_distribucion"][evento.distribucion] += 1
        if evento.retry_count is not None:
            estado["histograma_de_reintentos"][str(evento.retry_count)] += 1

    return {"ok": True}

def calcularThroughput(VENTANAsegCONSULTAS=10):
    ahora  = time.time()
    limite = ahora - VENTANAsegCONSULTAS
    with candado:
        cantidadEventoExitoso = sum(
            1 for (ts, tipoEvento) in estado["linea_de_tiempo"]
            if ts >= limite and tipoEvento in ("cache_hit", "cache_miss_processed")
        )
    return cantidadEventoExitoso / VENTANAsegCONSULTAS


def calcularThroughputPromedio():
    with candado:
        ini   = estado["primer_evento_exitoso"]
        fin   = estado["ultimo_evento_exitoso"]
        total = estado["total_consultas_procesadas"]
    if ini is None or fin is None or fin <= ini:
        return 0.0
    return total / (fin - ini)


def calculandoPercentiles(listaLatencias):
    if not listaLatencias:
        return {"p50": 0, "p95": 0, "avg": 0, "min": 0, "max": 0, "n": 0}
    arreglo = list(listaLatencias)
    return {
        "p50": float(np.percentile(arreglo, 50)),
        "p95": float(np.percentile(arreglo, 95)),
        "avg": float(np.mean(arreglo)),
        "min": float(np.min(arreglo)),
        "max": float(np.max(arreglo)),
        "n":   len(arreglo)
    }


def obteneMensajesPendientes():
    consultarGrupoConsumidores = os.getenv("ID_GRUPO", "consumer_group_1")
    try:
        admin = KafkaAdminClient(bootstrap_servers=direccionKafka, request_timeout_ms=3000)
        try:
            offsetsComprometidos = admin.list_consumer_group_offsets(consultarGrupoConsumidores)
        except Exception:
            offsetsComprometidos = {}
        mensajesSinProcesarPorTopico = {}
        for NombreTopico in [consultasPrinncipales, consultasReintento, consultasFalloDemasiado]:
            try:
                sonda = KafkaConsumer(
                    bootstrap_servers=direccionKafka,
                    group_id="sonda_metricas",
                    enable_auto_commit=False
                )
                particiones = sonda.partitions_for_topic(NombreTopico) or set()
                LAGtotal = 0
                for NumParticion in particiones:
                    t = TopicPartition(NombreTopico, NumParticion)
                    offsetFinal  = sonda.end_offsets([t]).get(t, 0)
                    metadataOffsetComprometido = offsetsComprometidos.get(t)
                    offsetLEIDO = metadataOffsetComprometido.offset if metadataOffsetComprometido else 0
                    LAGparticion = max(0, offsetFinal - offsetLEIDO)
                    LAGtotal    += LAGparticion
                mensajesSinProcesarPorTopico[NombreTopico] = LAGtotal
                sonda.close()
            except Exception:
                mensajesSinProcesarPorTopico[NombreTopico] = -1

        admin.close()
        return mensajesSinProcesarPorTopico

    except Exception:
        return {consultasPrinncipales: -1, consultasReintento: -1, consultasFalloDemasiado: -1}

@app.get("/metrics")
def obtener_metricas():
    with candado:
        total          = estado["total_consultas_procesadas"]
        aciertos       = estado["aciertos_cache"]
        fallos         = estado["fallos_cache"]
        reintentos     = estado["reintentos"]
        totalConsultasDQL         = estado["enviados_hacia_dlq"]
        estatisticasLatenciaProcesamiento       = calculandoPercentiles(estado["latencias_procesamiento"])
        latenciaTotalPuntaEnPunta        = calculandoPercentiles(estado["latencias_extremo_a_extremo"])
        tiemposRecuperacionFalla  = list(estado["tiempos_recuperacion_tras_falla"])
        tiempoActivoSistema  = time.time() - estado["momento_inicio"]
        cantidadConsultasConFallo   = len(estado["consultas_con_fallo"])
        cantidadConsultasRecuperadas = len(estado["consultas_recuperadas_con_fallo_y_solucion"])
        cantidadConsultasPerdidas    = len(estado["consultas_perdidas_dlq"])

    throughput          = calcularThroughput(VENTANAsegCONSULTAS=10)
    throughputPromedio  = calcularThroughputPromedio()
    tasaConsultasReingresadas = (reintentos / total * 100) if total > 0 else 0
    tasaConsultasTerminadasDQL        = (totalConsultasDQL / total * 100)    if total > 0 else 0
    TASAaciertosCache   = (aciertos / total * 100)  if total > 0 else 0

    if cantidadConsultasConFallo > 0:
        tasa_recuperacion_pct = (cantidadConsultasRecuperadas / cantidadConsultasConFallo) * 100
    else:
        tasa_recuperacion_pct = 100.0  
    return {
        "tiempo_activo_seg":          round(tiempoActivoSistema, 1),
        "total_procesadas":           total,
        "aciertos_cache":             aciertos,
        "fallos_cache":               fallos,
        "tasa_aciertos_pct":          round(TASAaciertosCache, 2),
        "throughput_consultas_seg":   round(throughput, 2),
        "throughput_promedio_seg":    round(throughputPromedio, 2),
        "latencia_procesamiento":     estatisticasLatenciaProcesamiento,
        "latencia_extremo_a_extremo": latenciaTotalPuntaEnPunta,
        "reintentos":                 reintentos,
        "tasa_reintentos_pct":        round(tasaConsultasReingresadas, 2),
        "cantidad_dlq":               totalConsultasDQL,
        "tasa_dlq_pct":               round(tasaConsultasTerminadasDQL, 2),
        "tasa_recuperacion_pct":      round(tasa_recuperacion_pct, 2),
        "consultas_que_fallaron":     cantidadConsultasConFallo,
        "consultas_recuperadas":      cantidadConsultasRecuperadas,
        "consultas_perdidas_dlq":     cantidadConsultasPerdidas,
        "tiempos_recuperacion_seg": {
            "cantidad": len(tiemposRecuperacionFalla),
            "promedio": round(float(np.mean(tiemposRecuperacionFalla)), 2) if tiemposRecuperacionFalla else 0,
            "maximo":   round(float(np.max(tiemposRecuperacionFalla)),  2) if tiemposRecuperacionFalla else 0,
        },
        "eventos_por_consumidor":    dict(estado["eventos_por_consumidor"]),
        "eventos_por_tipo_consulta": dict(estado["eventos_por_tipo_consulta"]),
        "eventos_por_distribucion":  dict(estado["eventos_por_distribucion"]),
        "histograma_reintentos":     dict(estado["histograma_de_reintentos"]),
    }


@app.get("/metrics/backlog")
def obtenerBacklog():
    return obteneMensajesPendientes()


@app.get("/metrics/snapshot")
def obtenerElSnapshot():
    metricasActuales = obtener_metricas()
    metricasActuales["backlog"]    = obteneMensajesPendientes()
    metricasActuales["timestamp"]  = time.time()
    return metricasActuales


@app.post("/reset")
def reiniciarMetricas():
    with candado:
        estado["aciertos_cache"]               = 0
        estado["fallos_cache"]                 = 0
        estado["reintentos"]                   = 0
        estado["enviados_hacia_dlq"]                 = 0
        estado["total_consultas_procesadas"]             = 0
        estado["latencias_procesamiento"].clear()
        estado["latencias_extremo_a_extremo"].clear()
        estado["eventos_por_consumidor"].clear()
        estado["eventos_por_tipo_consulta"].clear()
        estado["eventos_por_distribucion"].clear()
        estado["linea_de_tiempo"].clear()
        estado["histograma_de_reintentos"].clear()
        estado["tiempos_recuperacion_tras_falla"].clear()
        estado["inicio_falla_activa"]  = None
        estado["consultas_con_fallo"].clear()
        estado["consultas_recuperadas_con_fallo_y_solucion"].clear()
        estado["consultas_perdidas_dlq"].clear()
        estado["primer_evento_exitoso"] = None
        estado["ultimo_evento_exitoso"] = None
        estado["momento_inicio"]       = time.time()

    return {"ok": True, "mensaje": "Metricas reiniciadas exitosamente"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)