from fastapi import FastAPI
import pandas as pd
import numpy as np

app = FastAPI()

# Cargamos los datos en memoria sin base de datos externa
datosCVS = pd.read_csv('FiltradoFinal.csv')

# Areas para el calculo de densidad
zonasSantiago = {
    'Z1': 15.0,
    'Z2': 25.0,
    'Z3': 30.0,
    'Z4': 10.0,
    'Z5': 20.0
}

@app.get("/q1")
def q1_contarEdificios(zone_id: str, confidence_min: float = 0.0):
    filtro = datosCVS[(datosCVS['zone_id'] == zone_id) & (datosCVS['confidence'] >= confidence_min)]
    return len(filtro)

@app.get("/q2")
def q2_calcularArea(zone_id: str, confidence_min: float = 0.0):
    filtro = datosCVS[(datosCVS['zone_id'] == zone_id) & (datosCVS['confidence'] >= confidence_min)]
    areas = filtro['area_in_meters']
    if len(areas) == 0:
        return {"avg_area": 0, "total_area": 0, "n": 0}
    return {"avg_area": float(areas.mean()), "total_area": float(areas.sum()), "n": len(areas)}

@app.get("/q3")
def q3_calcularDensidad(zone_id: str, confidence_min: float = 0.0):
    filtro = datosCVS[(datosCVS['zone_id'] == zone_id) & (datosCVS['confidence'] >= confidence_min)]
    contador = len(filtro)
    area_km2 = zonasSantiago.get(zone_id, 10.0)
    return contador / area_km2

@app.get("/q4")
def q4_compararDensidades(zone_a: str, zone_b: str, confidence_min: float = 0.0):
    densidadZona_A = q3_calcularDensidad(zone_a, confidence_min)   # ← sacar ["density"]
    densidadZona_b = q3_calcularDensidad(zone_b, confidence_min)   # ← sacar ["density"]
    ganador = zone_a if densidadZona_A > densidadZona_b else zone_b
    return {"zone_a": densidadZona_A, "zone_b": densidadZona_b, "winner": ganador}

@app.get("/q5")
def q5_confidence_dist(zone_id: str, bins: int = 5):
    filtro = datosCVS[datosCVS['zone_id'] == zone_id]
    datosZona = filtro['confidence']
    cantidadXgrupo, limitesIntervalo = np.histogram(datosZona, bins=bins, range=(0.0, 1.0))
    
    resultadoFinal = []
    for i in range(bins):
        resultadoFinal.append({
            "bucket": i,
            "min": float(limitesIntervalo[i]),
            "max": float(limitesIntervalo[i+1]),
            "count": int(cantidadXgrupo[i])
        })
    return resultadoFinal