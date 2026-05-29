"""
FASE 1: Consumer básico con lógica de Q1-Q5 y almacenamiento en CSV
"""

import os
import json
import time
import pandas as pd
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# CONFIGURACIÓN

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_MAIN = os.getenv("TOPIC_MAIN", "queries")
TOPIC_PROCESSED = os.getenv("TOPIC_PROCESSED", "queries-processed")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "queries-dlq")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumer-group-1")
CSV_PATH = os.getenv("CSV_PATH", "FiltradoFinal.csv")

# Cargar datos una sola vez
print("[Consumer] Cargando datos...")
datosCVS = pd.read_csv(CSV_PATH)
print(f"[Consumer] Datos cargados: {len(datosCVS)} registros")

# Áreas para densidad
zonasSantiago = {
    'Z1': 15.0,
    'Z2': 25.0,
    'Z3': 30.0,
    'Z4': 10.0,
    'Z5': 20.0
}

# FUNCIONES DE CONSULTA (Q1-Q5)

def q1_contarEdificios(zone_id: str, confidence_min: float = 0.0):
    """Q1: Contar edificios en una zona con confianza mínima."""
    filtro = datosCVS[
        (datosCVS['zone_id'] == zone_id) & 
        (datosCVS['confidence'] >= confidence_min)
    ]
    return len(filtro)


def q2_calcularArea(zone_id: str, confidence_min: float = 0.0):
    """Q2: Calcular área promedio y total de edificios."""
    filtro = datosCVS[
        (datosCVS['zone_id'] == zone_id) & 
        (datosCVS['confidence'] >= confidence_min)
    ]
    areas = filtro['area_in_meters']
    if len(areas) == 0:
        return {"avg_area": 0, "total_area": 0, "n": 0}
    return {
        "avg_area": float(areas.mean()),
        "total_area": float(areas.sum()),
        "n": len(areas)
    }


def q3_calcularDensidad(zone_id: str, confidence_min: float = 0.0):
    """Q3: Calcular densidad de edificios (edificios/km²)."""
    filtro = datosCVS[
        (datosCVS['zone_id'] == zone_id) & 
        (datosCVS['confidence'] >= confidence_min)
    ]
    contador = len(filtro)
    area_km2 = zonasSantiago.get(zone_id, 10.0)
    return contador / area_km2


def q4_compararDensidades(zone_a: str, zone_b: str, confidence_min: float = 0.0):
    """Q4: Comparar densidades entre dos zonas."""
    densidad_a = q3_calcularDensidad(zone_a, confidence_min)
    densidad_b = q3_calcularDensidad(zone_b, confidence_min)
    ganador = zone_a if densidad_a > densidad_b else zone_b
    return {
        "zone_a": densidad_a,
        "zone_b": densidad_b,
        "winner": ganador
    }


def q5_confidence_dist(zone_id: str, bins: int = 5):
    """Q5: Distribución de confianza de edificios."""
    filtro = datosCVS[datosCVS['zone_id'] == zone_id]
    datos_zona = filtro['confidence']
    cantidad_x_grupo, limites_intervalo = np.histogram(
        datos_zona,
        bins=bins,
        range=(0.0, 1.0)
    )
    
    resultado_final = []
    for i in range(bins):
        resultado_final.append({
            "bucket": i,
            "min": float(limites_intervalo[i]),
            "max": float(limites_intervalo[i+1]),
            "count": int(cantidad_x_grupo[i])
        })
    return resultado_final


# PROCESADOR DE CONSULTAS

def procesar_consulta(consulta: dict) -> tuple[dict, bool]:
    """
    Procesa una consulta Kafka.
    Retorna: (resultado, success)
    """
    try:
        query_type = consulta.get('tipo')
        params = consulta.get('params', {})
        query_id = consulta.get('query_id')
        
        # Ejecutar la consulta apropiadta
        if query_type == 'q1':
            resultado = q1_contarEdificios(**params)
        elif query_type == 'q2':
            resultado = q2_calcularArea(**params)
        elif query_type == 'q3':
            resultado = q3_calcularDensidad(**params)
        elif query_type == 'q4':
            resultado = q4_compararDensidades(**params)
        elif query_type == 'q5':
            resultado = q5_confidence_dist(**params)
        else:
            raise ValueError(f"Tipo de consulta inválido: {query_type}")
        
        respuesta = {
            "query_id": query_id,
            "tipo": query_type,
            "resultado": resultado,
            "processed_at": time.time(),
            "success": True
        }
        return respuesta, True
        
    except Exception as e:
        print(f"[Consumer] Error procesando consulta {query_id}: {str(e)}")
        respuesta = {
            "query_id": query_id,
            "tipo": consulta.get('tipo'),
            "error": str(e),
            "processed_at": time.time(),
            "success": False
        }
        return respuesta, False


# CONEXIÓN A KAFKA

def conectar_consumer():
    """Reintenta conectarse a Kafka hasta lograrlo."""
    print(f"[Consumer] Esperando Kafka en {KAFKA_BROKER}...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_MAIN,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=30000,
            )
            print("[Consumer] ✓ Conectado a Kafka")
            return consumer
        except NoBrokersAvailable:
            print("[Consumer] Kafka no disponible, reintentando en 3s...")
            time.sleep(3)


def conectar_producer():
    """Conecta el productor para enviar respuestas."""
    print(f"[Producer] Esperando Kafka en {KAFKA_BROKER}...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3
            )
            print("[Producer] ✓ Conectado a Kafka")
            return producer
        except NoBrokersAvailable:
            print("[Producer] Kafka no disponible, reintentando en 3s...")
            time.sleep(3)


# LOOP PRINCIPAL

def consumir():
    """Loop principal de consumo y procesamiento."""
    consumer = conectar_consumer()
    producer = conectar_producer()
    
    contador = 0
    print(f"\n[Consumer] Iniciando consumo desde tópico '{TOPIC_MAIN}'...")
    print(f"[Consumer] Grupo: {GROUP_ID}\n")
    
    try:
        for mensaje in consumer:
            contador += 1
            consulta = mensaje.value
            query_id = consulta.get('query_id')
            
            # Procesar
            respuesta, success = procesar_consulta(consulta)
            
            # Enviar resultado
            topico_salida = TOPIC_PROCESSED if success else TOPIC_DLQ
            producer.send(topico_salida, value=respuesta)
            
            # Log
            if contador % 50 == 0:
                print(f"[Consumer] Procesadas {contador} consultas")
            
    except KeyboardInterrupt:
        print("\n[Consumer] Detenido por usuario")
    finally:
        consumer.close()
        producer.close()
        print(f"[Consumer] Total procesadas: {contador}")



if __name__ == "__main__":
    time.sleep(15)  # Esperar a que Kafka esté listo
    consumir()