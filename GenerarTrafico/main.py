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
    """Reintenta la conexión a Kafka hasta que esté disponible."""
    print("Esperando a Kafka........")
    while True:
        try:
            productor = KafkaProducer(
                bootstrap_servers=direccionKafka,
                value_serializer=lambda mensaje: json.dumps(mensaje).encode("utf-8"),
                retries=3
            )
            print("Kafka esta disponible!!!")
            return productor
        except NoBrokersAvailable:
            print("Kafka no disponible aun pero reintentando en 3 segundos.......")
            time.sleep(3)


def zipf_En_N_zonas(n, exponente=1.5):
    """Retorna un índice de zona siguiendo la distribución de Zipf."""
    while True:
        valor = np.random.zipf(exponente)
        if valor <= n:
            return valor - 1


def contruyendoConsultaKafka(numSecuencia, distribucion):
    """Construye un mensaje de consulta con todos sus campos requeridos."""
    if distribucion == "zipf":
        indiceZona = zipf_En_N_zonas(len(ZONAS))
        zonaEscogia = ZONAS[indiceZona]
    else:
        zonaEscogia = random.choice(ZONAS)

    tipoDeConsulta = random.choice(["q1", "q2", "q3", "q4", "q5"])

    if tipoDeConsulta in ["q1", "q2", "q3"]:
        confianzaMIN = round(random.choice([0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]), 2)
        parametros = {"zone_id": zonaEscogia, "confidence_min": confianzaMIN}

    elif tipoDeConsulta == "q4":
        SegundaZONA = random.choice([z for z in ZONAS if z != zonaEscogia])
        confianzaMIN = round(random.choice([0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]), 2)
        parametros = {"zone_a": zonaEscogia, "zone_b": SegundaZONA, "confidence_min": confianzaMIN}

    else:  # q5
        CantidadIntervalos = random.choice([5, 10, 15, 20, 25, 50])
        parametros = {"zone_id": zonaEscogia, "bins": CantidadIntervalos}

    return {
        "query_id":    str(uuid.uuid4()),
        "tipo":        tipoDeConsulta,
        "params":      parametros,
        "distribucion": distribucion,
        "retry_count": 0,
        "created_at":  time.time(),
        "seq":         numSecuencia
    }


def generandoTrafico():
    productor = esperandoKafka()
    print(f"Iniciando la generacion del trafico..... : {totalConsultas} consultas, "
          f"tasa={tasaConsultas}s, pico={spikeEsHabilitado}")
    time.sleep(5)

    ccontadorConsultasPublicadas = 0

    for numConsulta in range(totalConsultas):
        distribucionACTUAL = "zipf" if numConsulta < totalConsultas // 2 else "uniforme"
        consulta = contruyendoConsultaKafka(numConsulta, distribucionACTUAL)

    
        rangoDelSpike = spikeNumeroConsultaComienza <= numConsulta < spikeNumeroConsultaComienza + spikeDuracionn
        if spikeEsHabilitado and rangoDelSpike:
            for _ in range(spikeMultiplicador - 1):
                consultaAdicional = contruyendoConsultaKafka(numConsulta, distribucionACTUAL)
                productor.send(consultasPrinncipales, value=consultaAdicional)
                ccontadorConsultasPublicadas += 1

        productor.send(consultasPrinncipales, value=consulta)
        ccontadorConsultasPublicadas += 1

        if numConsulta % 100 == 0:
            print(f"[Productor] Publicadas {ccontadorConsultasPublicadas} consultas (seq={numConsulta})")

        time.sleep(tasaConsultas)

    productor.flush()
    print(f"[Productor] Listo. Total publicadas: {ccontadorConsultasPublicadas}")

    
@app.get("/health")
def health():
    return {"status": "ok"}e


if __name__ == "__main__":
    time.sleep(15)
    generandoTrafico()
