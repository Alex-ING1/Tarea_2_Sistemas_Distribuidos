#!/bin/bash
# experimentos.sh — Tarea 2: Ejecuta los 7 escenarios de evaluacion
# Uso: bash experimentos.sh
# Requiere: docker compose v2, curl, jq

set -e
COMPOSE="docker compose"
RESULTS_DIR="resultados"
URL_METRICAS="http://localhost:8080"
mkdir -p "$RESULTS_DIR"

# ─── Helpers ───────────────────────────────────────────────────────────────────
wait_traffic_done() {
    echo "  Esperando que el generador de trafico termine..."
    while $COMPOSE ps generador_trafico | grep -q "running"; do
        sleep 5
    done
    sleep 5
}

save_metrics() {
    local name="$1"
    echo "  Guardando metricas: $name"
    curl -s "${URL_METRICAS}/metrics/snapshot" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); json.dump(d, open('${RESULTS_DIR}/${name}.json','w'), indent=2)"
    echo "  → ${RESULTS_DIR}/${name}.json"
}

reset_metrics() {
    curl -s -X POST "${URL_METRICAS}/reset" > /dev/null
}

teardown() {
    echo "  Bajando contenedores..."
    $COMPOSE down -v --remove-orphans 2>/dev/null || true
}

# Captura la evolucion temporal del backlog/throughput en segundo plano.
SNAP_PID=""
start_snapshotter() {
    local name="$1"
    local dur="${2:-120}"
    echo "  Iniciando snapshotter de backlog: ${name} (${dur}s, cada 2s)"
    python3 scripts/backlog_snapshotter.py "$name" "$dur" 2 &
    SNAP_PID=$!
}

wait_snapshotter() {
    if [ -n "$SNAP_PID" ]; then
        echo "  Esperando que termine el snapshotter..."
        wait "$SNAP_PID" 2>/dev/null || true
        SNAP_PID=""
    fi
}

print_separator() {
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "  $1"
    echo "════════════════════════════════════════════════════════════════"
}

# ──────────────────────────────────────────────────────────────────────────────
# ESCENARIO 1 — Sistema Base (Tarea 1 — sin Kafka)
# ──────────────────────────────────────────────────────────────────────────────
print_separator "ESCENARIO 1: Sistema Base (sin Kafka)"
teardown

# Ruta al directorio de la Tarea 1 (configurable). Si no se encuentra, se saltea.
# Por defecto intenta varias ubicaciones comunes.
TAREA1_PATH="${TAREA1_PATH:-}"
if [ -z "$TAREA1_PATH" ]; then
    for candidato in "./V_final" "V_final" "../V_final" "../tarea1/V_final" "../tarea_1/V_final" "../tarea1_sistemas_distribuidos/V_final" "../tarea_1_sistemas_distribuidos/V_final"; do
        if [ -d "$candidato" ]; then
            TAREA1_PATH="$candidato"
            break
        fi
    done
fi

if [ -n "$TAREA1_PATH" ] && [ -d "$TAREA1_PATH" ]; then
    echo "  Usando Tarea 1 desde: $TAREA1_PATH"
    pushd "$TAREA1_PATH" > /dev/null
    docker compose up --build -d redis_db response_gen cache_service
    sleep 15
    docker compose run --rm traffic_gen python main.py || true
    # Copiar metricas de tarea1 directamente
    if [ -f "GenerarTrafico/metricas.json" ]; then
        cp GenerarTrafico/metricas.json "$OLDPWD/${RESULTS_DIR}/escenario1_base_sinKafka.json"
        echo "  → ${RESULTS_DIR}/escenario1_base_sinKafka.json"
    else
        echo "  AVISO: GenerarTrafico/metricas.json no se genero en Tarea 1."
    fi
    docker compose down
    popd > /dev/null
else
    echo "  AVISO: directorio Tarea 1 no encontrado."
    echo "  Para incluir el escenario 1, exporta TAREA1_PATH=/ruta/a/tarea1/V_final"
    echo "  Saltando escenario 1."
fi
echo "  Escenario 1 completado."

# ──────────────────────────────────────────────────────────────────────────────
# ESCENARIO 2 — Kafka + 1 Consumidor
# ──────────────────────────────────────────────────────────────────────────────
print_separator "ESCENARIO 2: Kafka + 1 Consumidor"
teardown

export NUM_CONSUMIDORES=1 NUM_CONSULTAS=800 TASA_CONSULTAS=0.008 TASA_FALLOS=0 \
       LATENCIA_SIMULADA_MS=35 LATENCIA_JITTER_MS=10 \
       SPIKE_HABILITADO=false MEMORIA_CACHE=200mb POLITICA_CACHE=allkeys-lru TTL_CACHE=300

$COMPOSE up --build -d zookeeper kafka redis_db generador_respuestas servicio_metricas
sleep 30
$COMPOSE up --scale consumidor_kafka=1 -d consumidor_kafka
sleep 10
reset_metrics
start_snapshotter "escenario2_kafka_1consumidor" 60
$COMPOSE up -d generador_trafico
wait_traffic_done
sleep 10
wait_snapshotter
save_metrics "escenario2_kafka_1consumidor"
teardown

# ──────────────────────────────────────────────────────────────────────────────
# ESCENARIO 3 — Kafka + Multiples Consumidores (2 y 4)
# ──────────────────────────────────────────────────────────────────────────────
for N in 2 4; do
    print_separator "ESCENARIO 3: Kafka + ${N} Consumidores"
    teardown

    export NUM_CONSUMIDORES=$N NUM_CONSULTAS=800 TASA_CONSULTAS=0.008 TASA_FALLOS=0 \
           LATENCIA_SIMULADA_MS=35 LATENCIA_JITTER_MS=10 \
           SPIKE_HABILITADO=false MEMORIA_CACHE=200mb POLITICA_CACHE=allkeys-lru TTL_CACHE=300

    $COMPOSE up --build -d zookeeper kafka redis_db generador_respuestas servicio_metricas
    sleep 30
    $COMPOSE up --scale consumidor_kafka=$N -d consumidor_kafka
    sleep 15
    reset_metrics
    start_snapshotter "escenario3_kafka_${N}consumidores" 60
    $COMPOSE up -d generador_trafico
    wait_traffic_done
    sleep 10
    wait_snapshotter
    save_metrics "escenario3_kafka_${N}consumidores"
    teardown
done

# ──────────────────────────────────────────────────────────────────────────────
# ESCENARIO 4 — Falla Temporal del Generador de Respuestas
# ──────────────────────────────────────────────────────────────────────────────
print_separator "ESCENARIO 4: Falla Temporal del Generador de Respuestas"
teardown

export NUM_CONSUMIDORES=2 NUM_CONSULTAS=600 TASA_CONSULTAS=0.05 TASA_FALLOS=0 \
       LATENCIA_SIMULADA_MS=35 LATENCIA_JITTER_MS=10 \
       SPIKE_HABILITADO=false MEMORIA_CACHE=200mb POLITICA_CACHE=allkeys-lru TTL_CACHE=300

$COMPOSE up --build -d zookeeper kafka redis_db generador_respuestas servicio_metricas
sleep 30
$COMPOSE up --scale consumidor_kafka=2 -d consumidor_kafka
sleep 10
reset_metrics
start_snapshotter "escenario4_falla_temporal" 130
$COMPOSE up -d generador_trafico

# Simular caida del generador de respuestas a los 20 segundos
echo "  Esperando 20s antes de bajar generador_respuestas..."
sleep 20
echo "  Bajando generador_respuestas (simulando falla)..."
$COMPOSE stop generador_respuestas

echo "  generador_respuestas caido durante 30s..."
sleep 30

echo "  Restaurando generador_respuestas..."
$COMPOSE start generador_respuestas

wait_traffic_done
sleep 20
wait_snapshotter
save_metrics "escenario4_falla_temporal"
teardown

# ──────────────────────────────────────────────────────────────────────────────
# ESCENARIO 5 — Reintentos (fallos artificiales al 30%)
# ──────────────────────────────────────────────────────────────────────────────
print_separator "ESCENARIO 5: Reintentos (TASA_FALLOS=30%)"
teardown

export NUM_CONSUMIDORES=2 NUM_CONSULTAS=400 TASA_CONSULTAS=0.05 TASA_FALLOS=30 \
       LATENCIA_SIMULADA_MS=35 LATENCIA_JITTER_MS=10 \
       SPIKE_HABILITADO=false MEMORIA_CACHE=200mb POLITICA_CACHE=allkeys-lru TTL_CACHE=300

$COMPOSE up --build -d zookeeper kafka redis_db generador_respuestas servicio_metricas
sleep 30
$COMPOSE up --scale consumidor_kafka=2 -d consumidor_kafka
sleep 10
reset_metrics
$COMPOSE up -d generador_trafico
wait_traffic_done
sleep 20
save_metrics "escenario5_reintentos"
teardown

# ──────────────────────────────────────────────────────────────────────────────
# ESCENARIO 6 — Spike de Trafico
# ──────────────────────────────────────────────────────────────────────────────
print_separator "ESCENARIO 6: Spike de Trafico"
teardown

export NUM_CONSUMIDORES=2 NUM_CONSULTAS=500 TASA_CONSULTAS=0.03 TASA_FALLOS=0 \
       LATENCIA_SIMULADA_MS=35 LATENCIA_JITTER_MS=10 \
       SPIKE_HABILITADO=true SPIKE_MULTIPLICADOR=8 SPIKE_INICIO=150 SPIKE_DURACION=80 \
       MEMORIA_CACHE=200mb POLITICA_CACHE=allkeys-lru TTL_CACHE=300

$COMPOSE up --build -d zookeeper kafka redis_db generador_respuestas servicio_metricas
sleep 30
$COMPOSE up --scale consumidor_kafka=2 -d consumidor_kafka
sleep 10
reset_metrics
start_snapshotter "escenario6_spike_trafico" 120
$COMPOSE up -d generador_trafico
wait_traffic_done
sleep 20
wait_snapshotter
save_metrics "escenario6_spike_trafico"
teardown

# ──────────────────────────────────────────────────────────────────────────────
# ESCENARIO 7 — Recuperacion ante Fallos (comparacion sincrona vs Kafka)
# ──────────────────────────────────────────────────────────────────────────────
print_separator "ESCENARIO 7: Recuperacion ante Fallos"
teardown

export NUM_CONSUMIDORES=2 NUM_CONSULTAS=500 TASA_CONSULTAS=0.04 TASA_FALLOS=0 \
       LATENCIA_SIMULADA_MS=35 LATENCIA_JITTER_MS=10 \
       SPIKE_HABILITADO=false MEMORIA_CACHE=200mb POLITICA_CACHE=allkeys-lru TTL_CACHE=300

$COMPOSE up --build -d zookeeper kafka redis_db generador_respuestas servicio_metricas
sleep 30
$COMPOSE up --scale consumidor_kafka=2 -d consumidor_kafka
sleep 10
reset_metrics
start_snapshotter "escenario7_recuperacion_kafka" 110
$COMPOSE up -d generador_trafico

# Bajada y restauracion rapida a los 15s
sleep 15
$COMPOSE stop generador_respuestas
sleep 20
$COMPOSE start generador_respuestas

wait_traffic_done
sleep 20
wait_snapshotter
save_metrics "escenario7_recuperacion_kafka"
teardown

# ──────────────────────────────────────────────────────────────────────────────
# RESUMEN
# ──────────────────────────────────────────────────────────────────────────────
print_separator "RESUMEN DE RESULTADOS"
echo "Archivos generados en ${RESULTS_DIR}/:"
ls -1 "${RESULTS_DIR}/"*.json 2>/dev/null || echo "  (ninguno)"

echo ""
echo "Para ver metricas individuales:"
echo "  cat ${RESULTS_DIR}/escenario2_kafka_1consumidor.json | python3 -m json.tool"
echo ""
echo "Todos los experimentos completados exitosamente."
