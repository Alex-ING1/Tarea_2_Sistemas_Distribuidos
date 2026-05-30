--------Tarea 2 - Sistemas Distribuidos--------
Apache Kafka + Tolerancia a Fallos + Procesamiento Asíncrono

-----------Requisitos-----------
- Docker Compose
- curl, jq

-----------Estructura del proyecto-----------

├── consumidor_kafka/          # Consumidor Kafka (cache + reintentos)
├── servicio_metricas/         # API de métricas (puerto 8080)
├── generador_respuestas/      # Generador de respuestas (Tarea 1)
├── GenerarTrafico/            # Generador de tráfico (Tarea 1)
├── V_final/                   # Tarea 1 sin modificar
├── resultados/                # JSONs y gráficos
├── docker-compose.yml
├── experimentos.sh            # Ejecuta 7 escenarios
└── README.md

-----------Instalar todo lo necesario-----------
Los comandos son:
	sudo apt update && sudo apt install -y jq curl python3 python3-pip wget && \
	mkdir -p ~/.docker/cli-plugins && \
	wget -O ~/.docker/cli-plugins/docker-compose \
	  https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 && \
	chmod +x ~/.docker/cli-plugins/docker-compose && \
	pip3 install matplotlib && \
	echo "Todo instalado. Verificando..." && \
	docker compose version && jq --version && python3 --version && pip3 show matplotlib | grep Version

-----------Ejecución-----------

Todos los experimentos, los 7 escenarios, la duración es de aprox 20 minutos
	chmod +x experimentos.sh
	./experimentos.sh
	python3 scripts/generar_graficos.py  


Experimento manual:
	docker compose up --build -d
	sleep 30
	docker compose up --scale consumidor_kafka=1 -d consumidor_kafka
	docker compose up -d generador_trafico
	sleep 10

Verificar
	curl http://localhost:8080/metrics | jq '.total_procesadas'

Bajar
	docker compose down -v


-----------Escenarios-----------

1. Sistema Base, síncrono
2. Kafka + 1 Consumer
3. Kafka + N Consumers (2, 4)
4. Falla Temporal
5. Reintentos
6. Spike de Trafico
7. Recuperación ante Fallos


