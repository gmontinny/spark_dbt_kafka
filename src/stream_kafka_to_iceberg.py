"""
Pipeline de Streaming: Kafka → Apache Iceberg (Camada Bronze).

Conecta ao Spark Connect Server via gRPC, consome todos os tópicos Olist
do Kafka e persiste os dados crus em tabelas Iceberg com metadados de ingestão.

Cada tópico é mapeado para uma tabela Bronze independente com schema explícito,
garantindo contrato de dados e evitando inferência custosa em produção.

Deserialização Avro (wire format Confluent):
  O produtor envia [0x00 | schema_id 4B | avro binary]. Os primeiros 5 bytes
  são o prefixo Confluent e devem ser ignorados antes de chamar from_avro.
  O schema JSON é lido do Schema Registry via HTTP antes de iniciar os streams.

Uso:
    python src/stream_kafka_to_iceberg.py

Variáveis de ambiente (via .env):
    SPARK_CONNECT_URL         — ex: sc://localhost:15002
    KAFKA_BROKERS             — ex: kafka:9093
    KAFKA_SCHEMA_REGISTRY     — ex: http://schema-registry:8081
"""

import json
import logging
import os

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.sql.types import StringType, StructField, StructType

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("SparkStreamingBronze")

SPARK_CONNECT_URL = os.getenv("SPARK_CONNECT_URL", "sc://localhost:15002")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9093")
SCHEMA_REGISTRY_URL = os.getenv("KAFKA_SCHEMA_REGISTRY", "http://schema-registry:8081")
CHECKPOINT_BASE = "s3a://spark-demo/stream-1/checkpoints"
CATALOG = "spark_catalog"
BRONZE_DB = "raw_olist_staging"

# ---------------------------------------------------------------------------
# Schemas explícitos por tópico (contrato de dados — sem inferência)
# ---------------------------------------------------------------------------
SCHEMAS: dict[str, StructType] = {
    "olist.raw.orders": StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_status", StringType()),
            StructField("order_purchase_timestamp", StringType()),
            StructField("order_approved_at", StringType()),
            StructField("order_delivered_carrier_date", StringType()),
            StructField("order_delivered_customer_date", StringType()),
            StructField("order_estimated_delivery_date", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
    "olist.raw.customers": StructType(
        [
            StructField("customer_id", StringType()),
            StructField("customer_unique_id", StringType()),
            StructField("customer_zip_code_prefix", StringType()),
            StructField("customer_city", StringType()),
            StructField("customer_state", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
    "olist.raw.order_items": StructType(
        [
            StructField("order_id", StringType()),
            StructField("order_item_id", StringType()),
            StructField("product_id", StringType()),
            StructField("seller_id", StringType()),
            StructField("shipping_limit_date", StringType()),
            StructField("price", StringType()),
            StructField("freight_value", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
    "olist.raw.order_payments": StructType(
        [
            StructField("order_id", StringType()),
            StructField("payment_sequential", StringType()),
            StructField("payment_type", StringType()),
            StructField("payment_installments", StringType()),
            StructField("payment_value", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
    "olist.raw.order_reviews": StructType(
        [
            StructField("review_id", StringType()),
            StructField("order_id", StringType()),
            StructField("review_score", StringType()),
            StructField("review_comment_title", StringType()),
            StructField("review_comment_message", StringType()),
            StructField("review_creation_date", StringType()),
            StructField("review_answer_timestamp", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
    "olist.raw.products": StructType(
        [
            StructField("product_id", StringType()),
            StructField("product_category_name", StringType()),
            StructField("product_name_lenght", StringType()),
            StructField("product_description_lenght", StringType()),
            StructField("product_photos_qty", StringType()),
            StructField("product_weight_g", StringType()),
            StructField("product_length_cm", StringType()),
            StructField("product_height_cm", StringType()),
            StructField("product_width_cm", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
    "olist.raw.sellers": StructType(
        [
            StructField("seller_id", StringType()),
            StructField("seller_zip_code_prefix", StringType()),
            StructField("seller_city", StringType()),
            StructField("seller_state", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
    "olist.raw.geolocation": StructType(
        [
            StructField("geolocation_zip_code_prefix", StringType()),
            StructField("geolocation_lat", StringType()),
            StructField("geolocation_lng", StringType()),
            StructField("geolocation_city", StringType()),
            StructField("geolocation_state", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
    "olist.raw.category_translation": StructType(
        [
            StructField("product_category_name", StringType()),
            StructField("product_category_name_english", StringType()),
            StructField("_source_file", StringType()),
        ]
    ),
}

# Mapeamento tópico → nome da tabela Bronze
TABLE_MAP: dict[str, str] = {
    "olist.raw.orders": "bronze_orders",
    "olist.raw.customers": "bronze_customers",
    "olist.raw.order_items": "bronze_order_items",
    "olist.raw.order_payments": "bronze_order_payments",
    "olist.raw.order_reviews": "bronze_order_reviews",
    "olist.raw.products": "bronze_products",
    "olist.raw.sellers": "bronze_sellers",
    "olist.raw.geolocation": "bronze_geolocation",
    "olist.raw.category_translation": "bronze_category_translation",
}


def _fetch_avro_schema(topic: str) -> str:
    """
    Busca o schema Avro mais recente do subject <topic>-value no Schema Registry.
    Retorna o schema como string JSON — formato esperado por from_avro.
    """
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()["schema"]


def get_spark() -> SparkSession:
    return SparkSession.builder.remote(SPARK_CONNECT_URL).getOrCreate()


def _ensure_bronze_table(spark: SparkSession, table: str, schema: StructType) -> None:
    """Cria a tabela Iceberg Bronze se ainda não existir."""
    full_table = f"{CATALOG}.{BRONZE_DB}.{table}"
    # Adiciona colunas de metadados ao schema para o CREATE TABLE
    meta_fields = [
        StructField("kafka_key", StringType()),
        StructField("kafka_offset", StringType()),
        StructField("kafka_timestamp", StringType()),
        StructField("ingestion_timestamp", StringType()),
    ]
    all_fields = schema.fields + meta_fields
    cols_ddl = ", ".join(f"`{f.name}` STRING" for f in all_fields)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{BRONZE_DB}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table} ({cols_ddl})
        USING iceberg
        LOCATION 's3a://warehouse/{BRONZE_DB}/{table}'
        """
    )
    logger.info("Tabela garantida: %s", full_table)


def _start_stream(spark: SparkSession, topic: str, avro_schema: str, schema: StructType, table: str):
    """Inicia um stream Kafka → Iceberg para um único tópico.

    Deserialização Avro:
      O value Kafka chega como bytes no wire format Confluent:
        [0x00 magic | schema_id uint32 BE (4 bytes) | avro binary payload]
      substring(value, 6) descarta os 5 bytes de prefixo (índice base 1 no Spark SQL)
      antes de passar o payload puro para from_avro.
    """
    full_table = f"{CATALOG}.{BRONZE_DB}.{table}"
    checkpoint = f"{CHECKPOINT_BASE}/{table}"

    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_parsed = (
        df_raw.select(
            expr("CAST(key AS STRING)").alias("kafka_key"),
            # Descarta os 5 bytes do prefixo Confluent antes de deserializar
            from_avro(expr("substring(value, 6)"), avro_schema).alias("_data"),
            expr("CAST(offset AS STRING)").alias("kafka_offset"),
            expr("CAST(timestamp AS STRING)").alias("kafka_timestamp"),
        )
        .select(
            *[col(f"_data.{f.name}").alias(f.name) for f in schema.fields],
            "kafka_key",
            "kafka_offset",
            "kafka_timestamp",
            current_timestamp().cast(StringType()).alias("ingestion_timestamp"),
        )
    )

    return (
        df_parsed.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .option("checkpointLocation", checkpoint)
        .toTable(full_table)
    )


def main() -> None:
    spark = get_spark()
    logger.info("Spark Connect inicializado: %s", SPARK_CONNECT_URL)

    queries = []
    for topic, table in TABLE_MAP.items():
        schema = SCHEMAS[topic]
        avro_schema = _fetch_avro_schema(topic)
        _ensure_bronze_table(spark, table, schema)
        query = _start_stream(spark, topic, avro_schema, schema, table)
        queries.append(query)
        logger.info("Stream iniciado: %s → %s.%s.%s", topic, CATALOG, BRONZE_DB, table)

    logger.info("%d streams ativos. Aguardando dados...", len(queries))
    # Aguarda o primeiro stream terminar (ou falhar)
    queries[0].awaitTermination()


if __name__ == "__main__":
    main()
