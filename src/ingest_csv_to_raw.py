"""
Ingestão Batch: CSV → Tabelas Parquet (Camada Raw) via Spark Connect.

Alternativa ao pipeline Kafka para carga inicial ou reprocessamento completo.
Lê todos os CSVs da pasta data/ com schemas explícitos e persiste como tabelas
Hive externas no MinIO (s3a://warehouse/raw_olist.db/<tabela>).

Uso:
    python src/ingest_csv_to_raw.py

Variáveis de ambiente (via .env):
    SPARK_CONNECT_URL — ex: sc://localhost:15002
"""

import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("BatchIngest")

SPARK_CONNECT_URL = os.getenv("SPARK_CONNECT_URL", "sc://localhost:15002")
DATA_DIR = os.getenv("DATA_DIR", "/opt/spark/work-dir/data")
RAW_DB = "raw_olist"
CATALOG = "spark_catalog"
WAREHOUSE = "s3a://warehouse"


@dataclass
class TableConfig:
    csv_file: str
    table_name: str
    schema: StructType
    partition_cols: list[str] | None = None


# ---------------------------------------------------------------------------
# Schemas tipados para cada dataset Olist
# ---------------------------------------------------------------------------
TABLES: list[TableConfig] = [
    TableConfig(
        csv_file="olist_orders_dataset.csv",
        table_name="orders",
        schema=StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("order_status", StringType(), True),
                StructField("order_purchase_timestamp", StringType(), True),
                StructField("order_approved_at", StringType(), True),
                StructField("order_delivered_carrier_date", StringType(), True),
                StructField("order_delivered_customer_date", StringType(), True),
                StructField("order_estimated_delivery_date", StringType(), True),
            ]
        ),
    ),
    TableConfig(
        csv_file="olist_customers_dataset.csv",
        table_name="customers",
        schema=StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("customer_unique_id", StringType(), True),
                StructField("customer_zip_code_prefix", StringType(), True),
                StructField("customer_city", StringType(), True),
                StructField("customer_state", StringType(), True),
            ]
        ),
        partition_cols=["customer_state"],
    ),
    TableConfig(
        csv_file="olist_order_items_dataset.csv",
        table_name="order_items",
        schema=StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("order_item_id", IntegerType(), True),
                StructField("product_id", StringType(), True),
                StructField("seller_id", StringType(), True),
                StructField("shipping_limit_date", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("freight_value", DoubleType(), True),
            ]
        ),
    ),
    TableConfig(
        csv_file="olist_order_payments_dataset.csv",
        table_name="order_payments",
        schema=StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("payment_sequential", IntegerType(), True),
                StructField("payment_type", StringType(), True),
                StructField("payment_installments", IntegerType(), True),
                StructField("payment_value", DoubleType(), True),
            ]
        ),
    ),
    TableConfig(
        csv_file="olist_order_reviews_dataset.csv",
        table_name="order_reviews",
        schema=StructType(
            [
                StructField("review_id", StringType(), False),
                StructField("order_id", StringType(), True),
                StructField("review_score", IntegerType(), True),
                StructField("review_comment_title", StringType(), True),
                StructField("review_comment_message", StringType(), True),
                StructField("review_creation_date", StringType(), True),
                StructField("review_answer_timestamp", StringType(), True),
            ]
        ),
    ),
    TableConfig(
        csv_file="olist_products_dataset.csv",
        table_name="products",
        schema=StructType(
            [
                StructField("product_id", StringType(), False),
                StructField("product_category_name", StringType(), True),
                StructField("product_name_lenght", IntegerType(), True),
                StructField("product_description_lenght", IntegerType(), True),
                StructField("product_photos_qty", IntegerType(), True),
                StructField("product_weight_g", DoubleType(), True),
                StructField("product_length_cm", DoubleType(), True),
                StructField("product_height_cm", DoubleType(), True),
                StructField("product_width_cm", DoubleType(), True),
            ]
        ),
    ),
    TableConfig(
        csv_file="olist_sellers_dataset.csv",
        table_name="sellers",
        schema=StructType(
            [
                StructField("seller_id", StringType(), False),
                StructField("seller_zip_code_prefix", StringType(), True),
                StructField("seller_city", StringType(), True),
                StructField("seller_state", StringType(), True),
            ]
        ),
        partition_cols=["seller_state"],
    ),
    TableConfig(
        csv_file="olist_geolocation_dataset.csv",
        table_name="geolocation",
        schema=StructType(
            [
                StructField("geolocation_zip_code_prefix", StringType(), True),
                StructField("geolocation_lat", DoubleType(), True),
                StructField("geolocation_lng", DoubleType(), True),
                StructField("geolocation_city", StringType(), True),
                StructField("geolocation_state", StringType(), True),
            ]
        ),
        partition_cols=["geolocation_state"],
    ),
    TableConfig(
        csv_file="product_category_name_translation.csv",
        table_name="product_category_name_translation",
        schema=StructType(
            [
                StructField("product_category_name", StringType(), False),
                StructField("product_category_name_english", StringType(), True),
            ]
        ),
    ),
]


def get_spark() -> SparkSession:
    return SparkSession.builder.remote(SPARK_CONNECT_URL).getOrCreate()


def ingest_table(spark: SparkSession, cfg: TableConfig) -> None:
    """Lê um CSV e persiste como tabela Iceberg no catálogo spark_catalog."""
    csv_path = f"{DATA_DIR}/{cfg.csv_file}"
    table_location = f"{WAREHOUSE}/{RAW_DB}.db/{cfg.table_name}"
    full_table = f"{CATALOG}.{RAW_DB}.{cfg.table_name}"

    logger.info("Lendo: %s", csv_path)
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("encoding", "UTF-8")
        .schema(cfg.schema)
        .load(csv_path)
    )

    corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count() if "_corrupt_record" in df.columns else 0
    if corrupt_count:
        logger.warning("%s: %d registros corrompidos detectados.", cfg.csv_file, corrupt_count)

    writer = (
        df.write.format("iceberg")
        .mode("overwrite")
        .option("path", table_location)
    )

    if cfg.partition_cols:
        writer = writer.partitionBy(*cfg.partition_cols)

    writer.saveAsTable(full_table)
    logger.info("Tabela '%s' gravada em %s (%d linhas).", full_table, table_location, df.count())


def main() -> None:
    spark = get_spark()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{RAW_DB}")
    logger.info("Iniciando ingestão batch de %d tabelas...", len(TABLES))

    for cfg in TABLES:
        try:
            ingest_table(spark, cfg)
        except Exception as exc:
            logger.error("Falha ao ingerir '%s': %s", cfg.table_name, exc)

    logger.info("Ingestão batch concluída.")


# Importação necessária para o filtro de corrompidos
from pyspark.sql.functions import col  # noqa: E402

if __name__ == "__main__":
    main()
