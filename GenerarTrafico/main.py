import random, time, json, uuid, os
import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

direccionKafka              = os.getenv("DIRECCION_KAFKA", "kafka:9092")
consultasPrinncipales       = os.getenv("TOPICO_PRINCIPAL", "queries")
totalConsultas              = int(os.getenv("NUM_CONSULTAS", "500"))
tasaConsultas               = float(os.getenv("TASA_CONSULTAS", "0.05"))
spikeEsHabilitado           = os.getenv("SPIKE_HABILITADO", "false").lower() == "true"
spikeMultiplicador          = int(os.getenv("SPIKE_MULTIPLICADOR", "5"))
spikeNumeroConsultaComienza = int(os.getenv("SPIKE_INICIO", "200"))
spikeDuracionn              = int(os.getenv("SPIKE_DURACION", "50"))
ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]

def esperandoKafka():
    while True:
        try:
            p = KafkaProducer(bootstrap_servers=direccionKafka,
                              value_serializer=lambda m: json.dumps(m).encode("utf-8"), retries=3)
            return p
        except NoBrokersAvailable:
            time.sleep(3)

def zipf_En_N_zonas(n, exp=1.5):
    while True:
        v = np.random.zipf(exp)
        if v <= n: return v - 1

def construirConsulta(seq, dist):
    zona = ZONAS[zipf_En_N_zonas(len(ZONAS))] if dist == "zipf" else random.choice(ZONAS)
    tipo = random.choice(["q1","q2","q3","q4","q5"])
    if tipo in ["q1","q2","q3"]:
        params = {"zone_id": zona, "confidence_min": round(random.choice([0.65,0.70,0.75,0.80,0.85,0.90,0.95]),2)}
    elif tipo == "q4":
        params = {"zone_a": zona, "zone_b": random.choice([z for z in ZONAS if z!=zona]),
                  "confidence_min": round(random.choice([0.65,0.70,0.75,0.80,0.85,0.90,0.95]),2)}
    else:
        params = {"zone_id": zona, "bins": random.choice([5,10,15,20,25,50])}
    return {"query_id": str(uuid.uuid4()), "tipo": tipo, "params": params,
            "distribucion": dist, "retry_count": 0, "created_at": time.time(), "seq": seq}

def generandoTrafico():
    p = esperandoKafka()
    time.sleep(5)
    cnt = 0
    for i in range(totalConsultas):
        dist = "zipf" if i < totalConsultas//2 else "uniforme"
        if spikeEsHabilitado and spikeNumeroConsultaComienza <= i < spikeNumeroConsultaComienza + spikeDuracionn:
            for _ in range(spikeMultiplicador - 1):
                p.send(consultasPrinncipales, value=construirConsulta(i, dist)); cnt += 1
        p.send(consultasPrinncipales, value=construirConsulta(i, dist)); cnt += 1
        time.sleep(tasaConsultas)
    p.flush()
    print(f"[Productor] Total publicadas: {cnt}")

if __name__ == "__main__":
    time.sleep(15)
    generandoTrafico()
