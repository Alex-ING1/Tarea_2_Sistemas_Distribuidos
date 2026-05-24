import random
import time
import json
import uuid
import os
import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Las variables que usa el entorno
direccionKafka         = os.getenv("KAFKA_BROKER", "kafka:9092")
consultasPrinncipales     = os.getenv("TOPIC_MAIN", "queries")
totalConsultas      = int(os.getenv("NUM_QUERIES", "500"))
tasaConsultas       = float(os.getenv("QUERY_RATE", "0.05"))   # segundos entre consultas

spikeEsHabilitado      = os.getenv("SPIKE_ENABLED", "false").lower() == "true"
spikeMultiplicador   = int(os.getenv("SPIKE_MULTIPLIER", "5"))
spikeNumeroConsultaComienza     = int(os.getenv("SPIKE_AT", "200"))        # nº de consulta donde inicia el pico
spikeDuracionn        = int(os.getenv("SPIKE_DURATION", "50"))   # cuántas consultas dura el pico

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]


def esperandoKafka():
    """Reintenta la conexión a Kafka hasta que este disponible."""
    print("Esperando a Kafka........")
   


def zipf_En_N_zonas(n, exponente=1.5):
    """Retorna un indice de zona siguiendo la distribucio Zipf."""
 

def contruyendoConsultaKafka(numSecuencia, distribucion):
    """Construye un mensaje de consulta con todos sus campos requeridos."""
    


def generandoTrafico():
    productor = esperandoKafka()
    print(f"Iniciando la generacion del trafico..... : {totalConsultas} consultas, "
          f"tasa={tasaConsultas}s, pico={spikeEsHabilitado}")
    time.sleep(5)

    

   
if __name__ == "__main__":
    time.sleep(15)
    generandoTrafico()
