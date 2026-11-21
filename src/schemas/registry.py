"""
Registro e resolução de schemas Avro no Confluent Schema Registry.

Responsabilidades:
  - Registrar todos os schemas do pipeline na inicialização
  - Configurar compatibilidade BACKWARD por subject
  - Fornecer AvroSerializer/AvroDeserializer prontos para uso no produtor
  - Cache local de serializers para evitar round-trips repetidos ao Registry

Compatibilidade BACKWARD:
  Consumidores na versão N conseguem ler mensagens produzidas na versão N-1.
  Regra prática: só é permitido ADICIONAR campos opcionais (com default).
  Remover ou renomear campos quebra a compatibilidade e é rejeitado pelo Registry.

Uso:
    from src.schemas.registry import SchemaRegistryClient, build_serializer

    client = SchemaRegistryClient()
    client.register_all()
    serializer = build_serializer(client, "olist.raw.orders")
"""

import json
import logging
import os
from functools import lru_cache

from confluent_kafka.schema_registry import SchemaRegistryClient as _SRClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.schema_registry import Schema
from dotenv import load_dotenv

from src.schemas.avro_schemas import DLQ_SCHEMA, TOPIC_SCHEMAS

load_dotenv()

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = os.getenv("KAFKA_SCHEMA_REGISTRY", "http://localhost:8081")

# Subject naming: <topic>-value  (convenção Confluent TopicNameStrategy)
# O produtor registra o schema sob este subject antes de serializar.
_SUBJECT_SUFFIX = "-value"


def _subject(topic: str) -> str:
    return f"{topic}{_SUBJECT_SUFFIX}"


def get_registry_client() -> _SRClient:
    """Retorna um cliente conectado ao Schema Registry."""
    return _SRClient({"url": SCHEMA_REGISTRY_URL})


def register_all(client: _SRClient) -> None:
    """
    Registra todos os schemas de dados + DLQ no Schema Registry.

    Idempotente: se o schema já existe e é idêntico, o Registry retorna
    o schema_id existente sem criar uma nova versão.

    Configura BACKWARD como política de compatibilidade por subject,
    garantindo que evoluções de schema não quebrem consumidores ativos.
    """
    # Schemas de dados (um por tópico)
    all_schemas = {**TOPIC_SCHEMAS}

    # DLQ usa um único schema compartilhado — registrado sob subject genérico
    all_schemas["olist.dlq"] = DLQ_SCHEMA

    for topic, schema_dict in all_schemas.items():
        subject = _subject(topic)
        schema_str = json.dumps(schema_dict)

        try:
            schema_id = client.register_schema(subject, Schema(schema_str, "AVRO"))
            logger.info("Schema registrado — subject='%s' id=%d", subject, schema_id)
        except Exception as exc:
            logger.error("Falha ao registrar schema '%s': %s", subject, exc)
            raise

        # Configura compatibilidade BACKWARD por subject
        try:
            client.set_compatibility(subject_name=subject, level="BACKWARD")
            logger.debug("Compatibilidade BACKWARD configurada: %s", subject)
        except Exception as exc:
            # Não fatal: o Registry pode já ter a compatibilidade configurada
            logger.warning("Não foi possível configurar compatibilidade em '%s': %s", subject, exc)


@lru_cache(maxsize=32)
def build_serializer(topic: str) -> AvroSerializer:
    """
    Constrói e cacheia um AvroSerializer para o tópico informado.

    O serializer:
      1. Busca o schema_id do subject <topic>-value no Registry
      2. Prefixa cada mensagem com [0x00 | schema_id (4 bytes)] — wire format Confluent
      3. Serializa o dict Python para bytes Avro binários

    O cache (lru_cache) evita instanciar um novo serializer a cada mensagem,
    reduzindo round-trips HTTP ao Registry durante a ingestão em bulk.
    """
    schema_dict = TOPIC_SCHEMAS.get(topic)
    if schema_dict is None:
        raise ValueError(f"Schema não encontrado para o tópico '{topic}'.")

    client = get_registry_client()
    return AvroSerializer(
        schema_registry_client=client,
        schema_str=json.dumps(schema_dict),
        # to_dict: o produtor passa dicts Python diretamente
        to_dict=lambda obj, _ctx: obj,
    )


@lru_cache(maxsize=32)
def build_dlq_serializer() -> AvroSerializer:
    """AvroSerializer para o envelope de erro do DLQ."""
    client = get_registry_client()
    return AvroSerializer(
        schema_registry_client=client,
        schema_str=json.dumps(DLQ_SCHEMA),
        to_dict=lambda obj, _ctx: obj,
    )


def build_deserializer(topic: str) -> AvroDeserializer:
    """
    Constrói um AvroDeserializer para leitura/validação de mensagens.
    Útil em testes de integração e no consumer de reprocessamento do DLQ.
    """
    schema_dict = TOPIC_SCHEMAS.get(topic)
    if schema_dict is None:
        raise ValueError(f"Schema não encontrado para o tópico '{topic}'.")

    client = get_registry_client()
    return AvroDeserializer(
        schema_registry_client=client,
        schema_str=json.dumps(schema_dict),
        from_dict=lambda obj, _ctx: obj,
    )
