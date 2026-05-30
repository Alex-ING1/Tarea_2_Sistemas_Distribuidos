# Tarea 2 - Sistemas Distribuidos

Apache Kafka + Tolerancia a Fallos + Procesamiento Asíncrono

## Integrantes
- Alex (Alex-ING1)
- Cristóbal Castro (camioner)

## Requisitos
- Docker Compose v2
- curl, jq

## Estructura

```
├── consumidor_kafka/    # Consumidor Kafka (cache + reintentos + DLQ)
├── servicio_metricas/   # API de métricas (puerto 8080)
├── generarRespuesta/    # Generador de respuestas (Q1-Q5)
├── GenerarTrafico/      # Generador de tráfico Kafka Producer
├── resultados/          # JSONs y gráficos de los 7 escenarios
├── scripts/             # Scripts auxiliares
├── docker-compose.yml
└── experimentos.sh      # Ejecuta los 7 escenarios
```

## Ejecución

```bash
chmod +x experimentos.sh
./experimentos.sh
python3 scripts/generar_graficos.py
```

## Tópicos Kafka

| Tópico | Propósito |
|--------|-----------|
| `queries` | Consultas iniciales |
| `queries_retry` | Reintentos automáticos |
| `queries_dlq` | Dead Letter Queue |

## Escenarios evaluados
1. Sistema Base (síncrono, Tarea 1)
2. Kafka + 1 Consumer
3. Kafka + Múltiples Consumers (2 y 4)
4. Falla Temporal del Generador de Respuestas
5. Reintentos con fallos artificiales al 30%
6. Spike de Tráfico
7. Recuperación ante Fallos: Kafka vs Síncrono
