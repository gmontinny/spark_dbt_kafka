"""
Produtor idempotente de CSVs para Apache Kafka com contrato Avro.

Fluxo por mensagem:
  1. Lê linha do CSV como dict Python
  2. Valida presença da chave de partição
  3. Serializa com AvroSerializer → [magic_byte | schema_id | avro_binary]
  4. Produz no tópico com chave de negócio (murmur2 determinístico)
  5. Em caso de falha de validação ou serialização → DLQ com envelope Avro

Garantias de confiabilidade:
  - enable.idempotence=True: Producer ID + sequence number por partição
    → broker descarta duplicatas em caso de retry de rede
  - acks=all: confirmação de todos os ISRs antes de considerar entregue
  - max.in.flight.requests.per.connection=5: máximo com idempotência ativa
  - retries=10 com backoff exponencial gerenciado pelo librdkafka

Contrato de dados (Schema Registry):
  - Schemas Avro registrados sob <topic>-value (TopicNameStrategy)
  - Compatibilidade BACKWARD: consumidores antigos leem mensagens novas
  - Wire format Confluent: [0x00 | schema_id 4B | avro payload]
  - Mensagens que violam o schema são rejeitadas ANTES de chegar ao broker

Uso:
    python src/producer_csv_kafka.py

Variáveis de ambiente (via .env):
    KAFKA_BROKERS         — kafka:9093 (Docker) ou localhost:9092 (host)
    KAFKA_SCHEMA_REGISTRY — http://schema-registry:8081
    CSV_DIR               — caminho para os CSVs (padrão: ./data)
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

from src.schemas.registry import (
    build_dlq_serializer,
    build_serializer,
    get_registry_client,
    register_all,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("KafkaCSVProducer")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
CSV_DIR = os.getenv("CSV_DIR", "./data")
CHUNK_SIZE = 5_000


@dataclass(frozen=True)
class TopicMapping:
    topic: str
    partition_key_field: str


TOPIC_MAP: dict[str, TopicMapping] = {
    "olist_orders_dataset.csv": TopicMapping("olist.raw.orders", "order_id"),
    "olist_order_items_dataset.csv": TopicMapping("olist.raw.order_items", "order_id"),
    "olist_order_payments_dataset.csv": TopicMapping("olist.raw.order_payments", "order_id"),
    "olist_order_reviews_dataset.csv": TopicMapping("olist.raw.order_reviews", "review_id"),
    "olist_customers_dataset.csv": TopicMapping("olist.raw.customers", "customer_id"),
    "olist_products_dataset.csv": TopicMapping("olist.raw.products", "product_id"),
    "olist_sellers_dataset.csv": TopicMapping("olist.raw.sellers", "seller_id"),
    "olist_geolocation_dataset.csv": TopicMapping("olist.raw.geolocation", "geolocation_zip_code_prefix"),
    "product_category_name_translation.csv": TopicMapping("olist.raw.category_translation", "product_category_name"),
}


def _build_producer() -> Producer:
    """
    Producer com idempotência e throughput otimizados.

    Idempotência (enable.idempotence=True):
      O broker atribui um Producer ID (PID) e rastreia sequence numbers
      por partição. Retentativas de rede não geram duplicatas — o broker
      descarta mensagens com sequence number já confirmado para o PID+partição.

    acks=all:
      Aguarda confirmação de todos os ISRs. Em single-broker (dev/staging)
      com min.insync.replicas=1, o comportamento é equivalente a acks=1,
      mas a configuração já está correta para escalar para multi-broker.

    compression.type=zstd:
      Melhor ratio de compressão para payloads Avro binários (~40-60% menor
      que sem compressão). O broker armazena comprimido e descomprime apenas
      na entrega ao consumidor.
    """
    return Producer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "client.id": "olist-avro-producer",
            # --- Confiabilidade / Idempotência ---
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 200,
            "max.in.flight.requests.per.connection": 5,
            # --- Throughput ---
            "compression.type": "zstd",
            "linger.ms": 20,
            "batch.size": 65_536,
            "queue.buffering.max.kbytes": 32_768,
        }
    )


def _on_delivery(err, msg) -> None:
    """Callback assíncrono de confirmação de entrega por mensagem."""
    if err is not None:
        logger.error(
            "Falha na entrega → %s [part=%s]: %s",
            msg.topic(), msg.partition(), err,
        )
    else:
        logger.debug(
            "Entregue → %s [part=%d] offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


def _send_to_dlq(
    producer: Producer,
    dlq_serializer: AvroSerializer,
    source_topic: str,
    source_file: str,
    partition_key: str | None,
    raw_record: dict,
    exc: Exception,
) -> None:
    """
    Serializa e envia um registro com falha para o DLQ correspondente.

    O envelope DLQ (schema DeadLetterRecord) contém:
      - source_topic / source_file: rastreabilidade de origem
      - partition_key: chave original (pode ser None se ausente no CSV)
      - raw_payload: JSON do registro bruto para reprocessamento
      - error_type / error_message: diagnóstico da falha
      - failed_at_ms: timestamp para ordenação cronológica no DLQ

    O DLQ também usa Avro (schema DLQ_SCHEMA), garantindo que ferramentas
    de monitoramento consigam deserializar os erros sem parsing ad-hoc.
    """
    dlq_topic = f"{source_topic}.dlq"
    envelope = {
        "source_topic": source_topic,
        "source_file": source_file,
        "partition_key": partition_key,
        "raw_payload": json.dumps(raw_record, ensure_ascii=False),
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "failed_at_ms": int(time.time() * 1000),
    }

    ctx = SerializationContext(dlq_topic, MessageField.VALUE)
    try:
        value_bytes = dlq_serializer(envelope, ctx)
        producer.produce(
            topic=dlq_topic,
            # Chave do DLQ: nome do arquivo (chave de negócio pode estar corrompida)
            key=(source_file.encode("utf-8")),
            value=value_bytes,
        )
    except Exception as dlq_exc:
        # Falha no DLQ é crítica — loga mas não propaga para não travar o loop
        logger.critical(
            "Falha ao enviar para DLQ '%s': %s | envelope=%s",
            dlq_topic, dlq_exc, envelope,
        )


def produce_file(
    producer: Producer,
    file_path: Path,
    mapping: TopicMapping,
    value_serializer: AvroSerializer,
    dlq_serializer: AvroSerializer,
) -> tuple[int, int]:
    """
    Lê um CSV em chunks e produz cada linha serializada em Avro.

    Retorna (total_sent, total_dlq).

    Serialização Avro (wire format Confluent):
      [0x00 magic byte | schema_id uint32 BE | avro binary payload]

    O magic byte 0x00 identifica o formato Confluent para deserializadores
    como o Spark Kafka connector e o ksqlDB.
    """
    ctx = SerializationContext(mapping.topic, MessageField.VALUE)
    total_sent = 0
    total_dlq = 0

    for chunk_idx, chunk in enumerate(
        pd.read_csv(file_path, chunksize=CHUNK_SIZE, dtype=str, keep_default_na=False)
    ):
        for _, row in chunk.iterrows():
            record: dict = row.to_dict()
            record["_source_file"] = file_path.name

            raw_key = record.get(mapping.partition_key_field, "")
            partition_key = str(raw_key).strip() or None

            try:
                if not partition_key:
                    raise ValueError(
                        f"Chave '{mapping.partition_key_field}' ausente ou vazia."
                    )

                # Serializa para Avro binário com schema_id embutido
                value_bytes = value_serializer(record, ctx)

                producer.produce(
                    topic=mapping.topic,
                    key=partition_key.encode("utf-8"),
                    value=value_bytes,
                    on_delivery=_on_delivery,
                )
                total_sent += 1

            except Exception as exc:
                _send_to_dlq(
                    producer, dlq_serializer,
                    mapping.topic, file_path.name,
                    partition_key, record, exc,
                )
                total_dlq += 1

        producer.poll(0)
        logger.info(
            "%s — chunk %d processado (%d msgs).",
            file_path.name, chunk_idx + 1, len(chunk),
        )

    return total_sent, total_dlq


def main() -> None:
    # 1. Registra todos os schemas no Schema Registry antes de produzir
    #    Garante que o broker rejeite mensagens fora do contrato
    logger.info("Registrando schemas no Schema Registry (%s)...", os.getenv("KAFKA_SCHEMA_REGISTRY"))
    sr_client = get_registry_client()
    register_all(sr_client)

    # 2. Serializer do DLQ (compartilhado entre todos os tópicos)
    dlq_serializer = build_dlq_serializer()

    # 3. Produtor Kafka com idempotência
    producer = _build_producer()

    csv_files = list(Path(CSV_DIR).glob("*.csv"))
    if not csv_files:
        logger.warning("Nenhum CSV encontrado em '%s'.", CSV_DIR)
        return

    logger.info("Iniciando ingestão — %d arquivo(s) | broker: %s", len(csv_files), KAFKA_BROKERS)

    total_sent_all = 0
    total_dlq_all = 0

    for file_path in sorted(csv_files):
        mapping = TOPIC_MAP.get(file_path.name)
        if not mapping:
            logger.warning("'%s' sem mapeamento — ignorado.", file_path.name)
            continue

        # Serializer específico do tópico (cacheado via lru_cache)
        value_serializer = build_serializer(mapping.topic)

        sent, dlq = produce_file(producer, file_path, mapping, value_serializer, dlq_serializer)
        total_sent_all += sent
        total_dlq_all += dlq

        logger.info(
            "%-50s → %-35s  enviados=%-6d  dlq=%d",
            file_path.name, mapping.topic, sent, dlq,
        )

    logger.info("Aguardando flush...")
    producer.flush(timeout=120)
    logger.info(
        "Ingestão concluída — enviados: %d | DLQ: %d",
        total_sent_all, total_dlq_all,
    )


if __name__ == "__main__":
    main()
