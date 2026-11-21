"""
Criação de tópicos Kafka para o pipeline Olist (modo KRaft).

Cada tópico tem configuração explícita de:
  - Partições: dimensionadas pelo volume e paralelismo esperado
  - Retenção: 7 dias para eventos transacionais, compacted para dimensões
  - min.insync.replicas: alinhado ao replication_factor do ambiente

Idempotente: tópicos já existentes são ignorados sem erro.

Uso:
    python src/create_kafka_topics.py

Variáveis de ambiente (via .env):
    KAFKA_BROKERS — ex: kafka:9093 (interno) ou localhost:9092 (host)
"""

import logging
import os
import time
from dataclasses import dataclass, field

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from confluent_kafka.error import KafkaException
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("KafkaTopicSetup")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

# Retenção padrão: 7 dias em ms
_7_DAYS_MS = str(7 * 24 * 60 * 60 * 1000)
# Retenção DLQ: 30 dias (auditoria e reprocessamento)
_30_DAYS_MS = str(30 * 24 * 60 * 60 * 1000)


@dataclass
class TopicConfig:
    """Definição completa de um tópico Kafka."""

    name: str
    partitions: int
    replication_factor: int = 1
    # Configurações adicionais passadas diretamente à Admin API
    config: dict[str, str] = field(default_factory=dict)


def _event_topic(name: str, partitions: int) -> TopicConfig:
    """
    Tópico de eventos transacionais (orders, items, payments).

    - delete: remove segmentos antigos por tempo/tamanho
    - retention.ms: 7 dias
    - min.insync.replicas: 1 (single-broker; aumentar para 2+ em produção)
    - compression.type: producer — respeita a compressão zstd do produtor
    """
    return TopicConfig(
        name=name,
        partitions=partitions,
        config={
            "cleanup.policy": "delete",
            "retention.ms": _7_DAYS_MS,
            "min.insync.replicas": "1",
            "compression.type": "producer",
            # Segmentos de 256 MB: equilíbrio entre compactação e tempo de rollover
            "segment.bytes": str(256 * 1024 * 1024),
        },
    )


def _dimension_topic(name: str) -> TopicConfig:
    """
    Tópico de dados dimensionais (products, sellers, customers, geolocation).

    - compact: mantém apenas o último valor por chave (upsert semântico)
    - Ideal para tabelas de referência que mudam raramente
    - min.compaction.lag.ms: garante que mensagens recentes não sejam compactadas
      antes de 1 hora (janela de segurança para consumidores lentos)
    """
    return TopicConfig(
        name=name,
        partitions=2,
        config={
            "cleanup.policy": "compact",
            "min.compaction.lag.ms": str(60 * 60 * 1000),  # 1 hora
            "min.insync.replicas": "1",
            "compression.type": "producer",
            "segment.bytes": str(128 * 1024 * 1024),
        },
    )


def _dlq_topic(name: str) -> TopicConfig:
    """
    Dead Letter Queue: retenção longa para auditoria e reprocessamento manual.
    Sempre 1 partição — volume baixo, sem necessidade de paralelismo.
    """
    return TopicConfig(
        name=name,
        partitions=1,
        config={
            "cleanup.policy": "delete",
            "retention.ms": _30_DAYS_MS,
            "min.insync.replicas": "1",
            "compression.type": "producer",
        },
    )


# ---------------------------------------------------------------------------
# Topologia completa do pipeline Olist
#
# Particionamento por volume estimado de registros:
#   orders       → 99.441 registros  → 4 partições (maior volume, mais paralelismo)
#   order_items  → 112.650 registros → 4 partições
#   order_payments → 103.886        → 4 partições
#   demais       → 2 partições (volume menor, dimensões ou dados de referência)
# ---------------------------------------------------------------------------
TOPICS: list[TopicConfig] = [
    # Eventos transacionais (alta cardinalidade, particionados por order_id)
    _event_topic("olist.raw.orders", partitions=4),
    _event_topic("olist.raw.order_items", partitions=4),
    _event_topic("olist.raw.order_payments", partitions=4),
    _event_topic("olist.raw.order_reviews", partitions=2),
    # Dimensões (compacted — upsert por chave de negócio)
    _dimension_topic("olist.raw.customers"),
    _dimension_topic("olist.raw.products"),
    _dimension_topic("olist.raw.sellers"),
    _dimension_topic("olist.raw.geolocation"),
    # Tabela de referência pequena (1 partição é suficiente)
    TopicConfig(
        name="olist.raw.category_translation",
        partitions=1,
        config={
            "cleanup.policy": "compact",
            "min.insync.replicas": "1",
            "compression.type": "producer",
        },
    ),
    # Dead Letter Queues (um por tópico de dados)
    _dlq_topic("olist.raw.orders.dlq"),
    _dlq_topic("olist.raw.order_items.dlq"),
    _dlq_topic("olist.raw.order_payments.dlq"),
    _dlq_topic("olist.raw.order_reviews.dlq"),
    _dlq_topic("olist.raw.customers.dlq"),
    _dlq_topic("olist.raw.products.dlq"),
    _dlq_topic("olist.raw.sellers.dlq"),
    _dlq_topic("olist.raw.geolocation.dlq"),
    _dlq_topic("olist.raw.category_translation.dlq"),
]


def _wait_for_broker(admin: AdminClient, retries: int = 12, delay: float = 5.0) -> None:
    """Aguarda o broker KRaft estar pronto antes de criar tópicos."""
    for attempt in range(1, retries + 1):
        try:
            metadata = admin.list_topics(timeout=5)
            logger.info("Broker acessível — %d tópico(s) existente(s).", len(metadata.topics))
            return
        except KafkaException as exc:
            logger.warning("Tentativa %d/%d — broker não disponível: %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"Broker '{KAFKA_BROKERS}' não respondeu após {retries} tentativas.")


def _register_schemas() -> None:
    """
    Registra todos os schemas Avro no Schema Registry após criar os tópicos.
    Separado do produtor para que o CI/CD possa validar schemas sem produzir dados.
    """
    from src.schemas.registry import get_registry_client, register_all  # noqa: PLC0415

    sr_url = os.getenv("KAFKA_SCHEMA_REGISTRY", "http://localhost:8081")
    logger.info("Registrando schemas no Schema Registry (%s)...", sr_url)
    try:
        register_all(get_registry_client())
        logger.info("Schemas registrados com sucesso.")
    except Exception as exc:
        logger.error("Falha ao registrar schemas: %s", exc)
        raise


def main() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    _wait_for_broker(admin)

    new_topics = [
        NewTopic(
            t.name,
            num_partitions=t.partitions,
            replication_factor=t.replication_factor,
            config=t.config,
        )
        for t in TOPICS
    ]

    results = admin.create_topics(new_topics)

    created, skipped, failed = 0, 0, 0
    for topic_name, future in results.items():
        try:
            future.result()
            logger.info("✓ Criado: %s", topic_name)
            created += 1
        except Exception as exc:
            msg = str(exc).lower()
            if "already exists" in msg or "topic_already_exists" in msg:
                logger.debug("— Já existe (ok): %s", topic_name)
                skipped += 1
            else:
                logger.error("✗ Falha em '%s': %s", topic_name, exc)
                failed += 1

    logger.info(
        "Resumo: %d criados, %d já existiam, %d falhas.",
        created,
        skipped,
        failed,
    )
    if failed:
        raise SystemExit(1)

    # Registra schemas Avro após garantir que os tópicos existem
    _register_schemas()


if __name__ == "__main__":
    main()
