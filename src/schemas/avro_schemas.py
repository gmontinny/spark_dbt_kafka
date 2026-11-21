"""
Schemas Avro para todos os tópicos do pipeline Olist.

Cada schema define o contrato de dados do tópico correspondente.
Campos opcionais usam union ["null", "<type>"] com default null —
padrão Avro para campos anuláveis sem quebrar compatibilidade futura.

Compatibilidade configurada como BACKWARD no Schema Registry:
  - Consumidores na versão N conseguem ler mensagens produzidas na versão N-1
  - Permite adicionar campos opcionais sem redeployment dos consumidores
"""

# ---------------------------------------------------------------------------
# Eventos transacionais
# ---------------------------------------------------------------------------

ORDERS_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "olist.raw",
    "doc": "Pedido realizado na plataforma Olist.",
    "fields": [
        {"name": "order_id", "type": "string", "doc": "Identificador único do pedido."},
        {"name": "customer_id", "type": ["null", "string"], "default": None},
        {"name": "order_status", "type": ["null", "string"], "default": None},
        {"name": "order_purchase_timestamp", "type": ["null", "string"], "default": None},
        {"name": "order_approved_at", "type": ["null", "string"], "default": None},
        {"name": "order_delivered_carrier_date", "type": ["null", "string"], "default": None},
        {"name": "order_delivered_customer_date", "type": ["null", "string"], "default": None},
        {"name": "order_estimated_delivery_date", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

ORDER_ITEMS_SCHEMA = {
    "type": "record",
    "name": "OrderItem",
    "namespace": "olist.raw",
    "doc": "Item de um pedido com preço e frete.",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "order_item_id", "type": ["null", "string"], "default": None},
        {"name": "product_id", "type": ["null", "string"], "default": None},
        {"name": "seller_id", "type": ["null", "string"], "default": None},
        {"name": "shipping_limit_date", "type": ["null", "string"], "default": None},
        {"name": "price", "type": ["null", "string"], "default": None},
        {"name": "freight_value", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

ORDER_PAYMENTS_SCHEMA = {
    "type": "record",
    "name": "OrderPayment",
    "namespace": "olist.raw",
    "doc": "Pagamento de um pedido.",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "payment_sequential", "type": ["null", "string"], "default": None},
        {"name": "payment_type", "type": ["null", "string"], "default": None},
        {"name": "payment_installments", "type": ["null", "string"], "default": None},
        {"name": "payment_value", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

ORDER_REVIEWS_SCHEMA = {
    "type": "record",
    "name": "OrderReview",
    "namespace": "olist.raw",
    "doc": "Avaliação de um pedido pelo cliente.",
    "fields": [
        {"name": "review_id", "type": "string"},
        {"name": "order_id", "type": ["null", "string"], "default": None},
        {"name": "review_score", "type": ["null", "string"], "default": None},
        {"name": "review_comment_title", "type": ["null", "string"], "default": None},
        {"name": "review_comment_message", "type": ["null", "string"], "default": None},
        {"name": "review_creation_date", "type": ["null", "string"], "default": None},
        {"name": "review_answer_timestamp", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

# ---------------------------------------------------------------------------
# Dimensões
# ---------------------------------------------------------------------------

CUSTOMERS_SCHEMA = {
    "type": "record",
    "name": "Customer",
    "namespace": "olist.raw",
    "doc": "Cadastro de cliente.",
    "fields": [
        {"name": "customer_id", "type": "string"},
        {"name": "customer_unique_id", "type": ["null", "string"], "default": None},
        {"name": "customer_zip_code_prefix", "type": ["null", "string"], "default": None},
        {"name": "customer_city", "type": ["null", "string"], "default": None},
        {"name": "customer_state", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

PRODUCTS_SCHEMA = {
    "type": "record",
    "name": "Product",
    "namespace": "olist.raw",
    "doc": "Produto do catálogo Olist.",
    "fields": [
        {"name": "product_id", "type": "string"},
        {"name": "product_category_name", "type": ["null", "string"], "default": None},
        {"name": "product_name_lenght", "type": ["null", "string"], "default": None},
        {"name": "product_description_lenght", "type": ["null", "string"], "default": None},
        {"name": "product_photos_qty", "type": ["null", "string"], "default": None},
        {"name": "product_weight_g", "type": ["null", "string"], "default": None},
        {"name": "product_length_cm", "type": ["null", "string"], "default": None},
        {"name": "product_height_cm", "type": ["null", "string"], "default": None},
        {"name": "product_width_cm", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

SELLERS_SCHEMA = {
    "type": "record",
    "name": "Seller",
    "namespace": "olist.raw",
    "doc": "Vendedor cadastrado na plataforma.",
    "fields": [
        {"name": "seller_id", "type": "string"},
        {"name": "seller_zip_code_prefix", "type": ["null", "string"], "default": None},
        {"name": "seller_city", "type": ["null", "string"], "default": None},
        {"name": "seller_state", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

GEOLOCATION_SCHEMA = {
    "type": "record",
    "name": "Geolocation",
    "namespace": "olist.raw",
    "doc": "Coordenadas geográficas por CEP.",
    "fields": [
        {"name": "geolocation_zip_code_prefix", "type": "string"},
        {"name": "geolocation_lat", "type": ["null", "string"], "default": None},
        {"name": "geolocation_lng", "type": ["null", "string"], "default": None},
        {"name": "geolocation_city", "type": ["null", "string"], "default": None},
        {"name": "geolocation_state", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

CATEGORY_TRANSLATION_SCHEMA = {
    "type": "record",
    "name": "CategoryTranslation",
    "namespace": "olist.raw",
    "doc": "Tradução de categorias de produto para inglês.",
    "fields": [
        {"name": "product_category_name", "type": "string"},
        {"name": "product_category_name_english", "type": ["null", "string"], "default": None},
        {"name": "_source_file", "type": ["null", "string"], "default": None},
    ],
}

# ---------------------------------------------------------------------------
# Schema do DLQ — envelope de erro uniforme para todos os tópicos
# ---------------------------------------------------------------------------

DLQ_SCHEMA = {
    "type": "record",
    "name": "DeadLetterRecord",
    "namespace": "olist.dlq",
    "doc": "Envelope de erro para registros que falharam na validação do schema.",
    "fields": [
        {"name": "source_topic", "type": "string", "doc": "Tópico de origem do registro."},
        {"name": "source_file", "type": "string", "doc": "Arquivo CSV de origem."},
        {"name": "partition_key", "type": ["null", "string"], "default": None, "doc": "Chave de partição original (pode estar ausente)."},
        {"name": "raw_payload", "type": "string", "doc": "Payload JSON bruto que falhou na serialização."},
        {"name": "error_type", "type": "string", "doc": "Tipo da exceção (ex: SchemaParseException, ValueError)."},
        {"name": "error_message", "type": "string", "doc": "Mensagem de erro detalhada."},
        {"name": "failed_at_ms", "type": "long", "doc": "Timestamp Unix em milissegundos do momento da falha."},
    ],
}

# ---------------------------------------------------------------------------
# Índice: tópico → schema Avro
# Usado pelo produtor e pelo módulo de registro no Schema Registry
# ---------------------------------------------------------------------------

TOPIC_SCHEMAS: dict[str, dict] = {
    "olist.raw.orders": ORDERS_SCHEMA,
    "olist.raw.order_items": ORDER_ITEMS_SCHEMA,
    "olist.raw.order_payments": ORDER_PAYMENTS_SCHEMA,
    "olist.raw.order_reviews": ORDER_REVIEWS_SCHEMA,
    "olist.raw.customers": CUSTOMERS_SCHEMA,
    "olist.raw.products": PRODUCTS_SCHEMA,
    "olist.raw.sellers": SELLERS_SCHEMA,
    "olist.raw.geolocation": GEOLOCATION_SCHEMA,
    "olist.raw.category_translation": CATEGORY_TRANSLATION_SCHEMA,
}
