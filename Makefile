# The name of the Docker Compose file
COMPOSE_FILE = docker-compose.yml

.PHONY: help up down rebuild logs browse ml-demo analise otimizacao ml-all topics produce stream ingest dbt-run dbt-test pipeline kafka-pipeline

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Run the containers in the background
up: ## Start development environment
	docker compose -f $(COMPOSE_FILE) up -d

# Stop the containers
down: ## Stop containers
	docker compose -f $(COMPOSE_FILE) down

# Rebuild the Docker image and restart the containers
rebuild: ## Rebuild images and restart containers
	docker compose -f $(COMPOSE_FILE) up -d --build

# Show logs
logs: ## Show container logs
	docker compose -f $(COMPOSE_FILE) logs

# Open the browser
browse: ## Open Spark UI in browser
	open http://localhost:4040

# Machine Learning targets
ml-demo: ## Execute complete ML pipeline demo
	docker compose exec dev python src/ml_pipeline_demo.py

analise: ## Run exploratory data analysis
	docker compose exec dev python src/analise_exploratoria.py

otimizacao: ## Run model optimization and comparison
	docker compose exec dev python src/otimizacao_modelo.py

ml-all: ## Run all ML scripts in sequence
	make analise
	make ml-demo
	make otimizacao

teste: ## Quick environment test
	docker compose exec dev python src/teste_rapido.py

# Data Pipeline: Spark ingest + dbt transform
topics: ## Create all Kafka topics and DLQs
	docker compose exec dev python src/create_kafka_topics.py

produce: ## Produce all CSV files to Kafka topics
	docker compose exec dev python src/producer_csv_kafka.py

stream: ## Start Spark Structured Streaming (Kafka → Iceberg Bronze)
	docker compose exec dev python src/stream_kafka_to_iceberg.py

ingest: ## Ingest CSV files directly into raw Hive tables via Spark batch
	docker compose exec dev python src/ingest_csv_to_raw.py

dbt-run: ## Run dbt models (staging + marts)
	docker compose exec dev bash -c "cd dbt_olist && dbt run --profiles-dir ."

dbt-test: ## Run dbt tests
	docker compose exec dev bash -c "cd dbt_olist && dbt test --profiles-dir ."

pipeline: ingest dbt-run dbt-test ## Full batch pipeline: ingest raw + dbt transform + tests

kafka-pipeline: topics produce stream ## Full streaming pipeline: create topics + produce + stream
