# The name of the Docker Compose file
COMPOSE_FILE = docker-compose.yml

.PHONY: help up down rebuild logs browse ml-demo analise otimizacao ml-all topics produce stream ingest dbt-run dbt-test pipeline kafka-pipeline reset-bronze reset-marts

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

reset-bronze: ## Drop Bronze tables, checkpoints and data — required before re-streaming
	docker compose exec trino trino --execute "DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_orders; DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_customers; DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_items; DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_payments; DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_reviews; DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_products; DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_sellers; DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_geolocation; DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_category_translation;"
	docker run --rm --network spark_dbt_kafka_spark_network spark_dbt_kafka-minio-client:latest "mcli alias set m http://minio:9000 minioadmin minioadmin && mcli rm --recursive --force m/spark-demo/stream-1/ ; mcli rm --recursive --force m/warehouse/raw_olist_staging.db/ ; echo Bronze reset concluido"

reset-marts: ## Drop Gold tables and data — required when mart schema changes
	docker compose exec postgres psql -U hive -d metastore -c "DO $$$$ DECLARE tbl_ids BIGINT[]; sd_ids BIGINT[]; serde_ids BIGINT[]; cd_ids BIGINT[]; BEGIN SELECT ARRAY_AGG(t.\"TBL_ID\") INTO tbl_ids FROM \"TBLS\" t JOIN \"DBS\" d ON t.\"DB_ID\" = d.\"DB_ID\" WHERE d.\"NAME\" = 'raw_olist_staging_raw_olist_marts'; IF tbl_ids IS NULL THEN RETURN; END IF; SELECT ARRAY_AGG(\"SD_ID\") INTO sd_ids FROM \"TBLS\" WHERE \"TBL_ID\" = ANY(tbl_ids); SELECT ARRAY_AGG(\"SERDE_ID\") INTO serde_ids FROM \"SDS\" WHERE \"SD_ID\" = ANY(sd_ids); SELECT ARRAY_AGG(\"CD_ID\") INTO cd_ids FROM \"SDS\" WHERE \"SD_ID\" = ANY(sd_ids); DELETE FROM \"TBL_PRIVS\" WHERE \"TBL_ID\" = ANY(tbl_ids); DELETE FROM \"TBL_COL_PRIVS\" WHERE \"TBL_ID\" = ANY(tbl_ids); DELETE FROM \"TABLE_PARAMS\" WHERE \"TBL_ID\" = ANY(tbl_ids); DELETE FROM \"TAB_COL_STATS\" WHERE \"TBL_ID\" = ANY(tbl_ids); DELETE FROM \"PARTITION_KEYS\" WHERE \"TBL_ID\" = ANY(tbl_ids); DELETE FROM \"TBLS\" WHERE \"TBL_ID\" = ANY(tbl_ids); DELETE FROM \"COLUMNS_V2\" WHERE \"CD_ID\" = ANY(cd_ids); DELETE FROM \"SERDE_PARAMS\" WHERE \"SERDE_ID\" = ANY(serde_ids); DELETE FROM \"SD_PARAMS\" WHERE \"SD_ID\" = ANY(sd_ids); DELETE FROM \"SDS\" WHERE \"SD_ID\" = ANY(sd_ids); DELETE FROM \"CDS\" WHERE \"CD_ID\" = ANY(cd_ids); DELETE FROM \"SERDES\" WHERE \"SERDE_ID\" = ANY(serde_ids); END $$$$;"
	docker run --rm --network spark_dbt_kafka_spark_network spark_dbt_kafka-minio-client:latest "mcli alias set m http://minio:9000 minioadmin minioadmin && mcli rm --recursive --force m/warehouse/raw_olist_staging_raw_olist_marts.db/ ; echo Marts reset concluido"

pipeline: ingest dbt-run dbt-test ## Full batch pipeline: ingest raw + dbt transform + tests

kafka-pipeline: topics produce stream ## Full streaming pipeline: create topics + produce + stream
