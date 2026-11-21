# Modern Data Lakehouse — Olist E-Commerce Pipeline

Pipeline de dados end-to-end sobre o dataset público [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), implementando uma arquitetura **Medallion (Bronze → Silver → Gold)** com Apache Kafka, Spark 4.x via Spark Connect, Apache Iceberg e dbt sobre Trino.

---

## Arquitetura

```
CSV Files (data/)
      │
      ▼  [src/producer_csv_kafka.py]
Apache Kafka (9 tópicos + 9 DLQs)
      │
      ▼  [src/stream_kafka_to_iceberg.py]
Iceberg Bronze (raw_olist_staging.bronze_*)
      │
      ▼  [dbt_olist/models/staging/]
Silver Views (raw_olist_staging.stg_*)
      │
      ▼  [dbt_olist/models/marts/]
Gold Tables (raw_olist_marts.fct_orders, dim_customers, dim_sellers)
      │
      ▼
Trino SQL / Apache Superset / StarRocks
```

**Alternativa batch** (sem Kafka):
```
CSV Files (data/)
      │
      ▼  [src/ingest_csv_to_raw.py]
Hive/Parquet Raw (raw_olist.*)
      │
      ▼  [dbt_olist/]
Silver + Gold (mesmo fluxo dbt)
```

---

## Pré-requisitos

- Docker + Docker Compose
- Python 3.10+ com `uv` (ou `pip`)
- Arquivo `.env` configurado (já incluso no repositório)

---

## Estrutura do Projeto

```
spark_dbt_kafka/
├── data/                          # CSVs do dataset Olist
├── docs/solucao.md                # Documentação arquitetural detalhada
├── src/
│   ├── create_kafka_topics.py     # Cria tópicos e DLQs no Kafka
│   ├── producer_csv_kafka.py      # Produz CSVs → Kafka (idempotente)
│   ├── stream_kafka_to_iceberg.py # Streaming Kafka → Iceberg Bronze
│   └── ingest_csv_to_raw.py       # Ingestão batch CSV → Hive/Parquet
├── dbt_olist/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/               # Views Silver (limpeza + tipagem)
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_orders.sql
│       │   ├── stg_customers.sql
│       │   ├── stg_order_items.sql
│       │   ├── stg_order_payments.sql
│       │   ├── stg_products.sql
│       │   └── stg_sellers.sql
│       └── marts/                 # Tabelas Gold (fatos + dimensões)
│           ├── schema.yml
│           ├── fct_orders.sql
│           ├── dim_customers.sql
│           └── dim_sellers.sql
├── docker-compose.yml
├── Makefile
└── .env
```

---

## Configuração

### Variáveis de Ambiente (`.env`)

| Variável | Padrão | Descrição |
|---|---|---|
| `KAFKA_BROKERS` | `kafka:9093` | Endereço do broker Kafka (interno) |
| `SPARK_CONNECT_URL` | `sc://spark-connect:15002` | URL do Spark Connect Server |
| `MINIO_ACCESS_KEY` | `minioadmin` | Credencial MinIO |
| `MINIO_SECRET_KEY` | `minioadmin` | Credencial MinIO |
| `TRINO_PORT` | `8080` | Porta do Trino |

Para execução local (fora do Docker), use `localhost` nos hosts:
```bash
KAFKA_BROKERS=localhost:9092
SPARK_CONNECT_URL=sc://localhost:15002
```

---

## Execução

### 1. Subir a infraestrutura

```bash
make up
# ou
docker compose up -d
```

Aguarde todos os serviços ficarem saudáveis (~2 minutos):
```bash
docker compose ps
```

### 2. Instalar dependências Python

```bash
# Com uv (recomendado)
uv sync

# Com pip
pip install -r requirements.txt
```

---

## Pipeline A — Streaming via Kafka (Recomendado para produção)

### Passo 1: Criar tópicos Kafka

```bash
make topics
# ou
python src/create_kafka_topics.py
```

Cria 9 tópicos de dados + 9 DLQs:
```
olist.raw.orders          (4 partições)
olist.raw.customers       (2 partições)
olist.raw.order_items     (4 partições)
olist.raw.order_payments  (2 partições)
olist.raw.order_reviews   (2 partições)
olist.raw.products        (2 partições)
olist.raw.sellers         (2 partições)
olist.raw.geolocation     (2 partições)
olist.raw.category_translation (1 partição)
+ DLQs correspondentes (*.dlq)
```

### Passo 2: Iniciar o consumer Spark (Bronze)

Em um terminal separado:
```bash
make stream
# ou
python src/stream_kafka_to_iceberg.py
```

O stream fica ativo aguardando mensagens. Checkpoints são salvos em `s3a://spark-demo/stream-1/checkpoints/`.

### Passo 3: Produzir os CSVs para o Kafka

```bash
make produce
# ou
python src/producer_csv_kafka.py
```

Saída esperada:
```
2026-01-01 10:00:00 [INFO] KafkaCSVProducer — Iniciando ingestão de 9 arquivos CSV...
2026-01-01 10:00:01 [INFO] KafkaCSVProducer — olist_orders_dataset.csv — chunk 1 processado.
2026-01-01 10:00:05 [INFO] KafkaCSVProducer — olist_orders_dataset.csv → tópico 'olist.raw.orders': 99441 enviados, 0 para DLQ.
...
2026-01-01 10:01:30 [INFO] KafkaCSVProducer — Ingestão concluída.
```

### Passo 4: Executar transformações dbt

```bash
make dbt-run
make dbt-test
```

---

## Pipeline B — Batch Direto (Desenvolvimento / Reprocessamento)

```bash
# Ingestão batch: CSV → Hive/Parquet (raw_olist.*)
make ingest

# Transformações dbt: staging + marts
make dbt-run

# Testes de qualidade
make dbt-test

# Ou tudo de uma vez:
make pipeline
```

---

## Modelos dbt

### Staging (Silver) — Views

| Modelo | Fonte (Pipeline A) | Transformações |
|---|---|---|
| `stg_orders` | `iceberg_bronze.bronze_orders` | Cast de timestamps, `is_delivered`, `delivery_days`, deduplicação por `kafka_offset DESC` |
| `stg_customers` | `iceberg_bronze.bronze_customers` | Deduplicação por `customer_unique_id`, normalização de cidade/estado |
| `stg_order_items` | `iceberg_bronze.bronze_order_items` | Cast de preços para `DECIMAL(10,2)`, cálculo de `total_item_value` |
| `stg_order_payments` | `iceberg_bronze.bronze_order_payments` | Normalização de `payment_type`, cast de valores |
| `stg_products` | `iceberg_bronze.bronze_products` + `bronze_category_translation` | Enriquecimento com categoria em inglês via LEFT JOIN |
| `stg_sellers` | `iceberg_bronze.bronze_sellers` | Normalização de cidade/estado, deduplicação por `seller_id` |

### Marts (Gold) — Tabelas Incrementais

| Modelo | Chave | Descrição |
|---|---|---|
| `fct_orders` | `order_id` | Fato de pedidos com pagamentos e itens agregados, SLA de entrega |
| `dim_customers` | `customer_unique_id` | Dimensão de clientes com LTV e contagem de pedidos |
| `dim_sellers` | `seller_id` | Dimensão de vendedores com receita e volume de vendas |

Todos os marts usam `incremental_strategy='merge'`, garantindo idempotência nas reexecuções.

---

## Consultas de Exemplo (Trino)

Conecte ao Trino em `http://localhost:8080` ou via CLI:

```bash
docker exec -it trino trino
```

### Receita por mês

```sql
SELECT
    DATE_TRUNC('month', order_date)     AS month,
    COUNT(*)                            AS total_orders,
    SUM(total_payment_value)            AS total_revenue,
    AVG(delivery_days)                  AS avg_delivery_days
FROM iceberg.raw_olist_marts.fct_orders
WHERE is_delivered = TRUE
GROUP BY 1
ORDER BY 1 DESC;
```

### Top 10 estados por receita

```sql
SELECT
    c.customer_state,
    COUNT(DISTINCT o.order_id)          AS orders,
    SUM(o.total_payment_value)          AS revenue
FROM iceberg.raw_olist_marts.fct_orders o
JOIN iceberg.raw_olist_marts.dim_customers c
    ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY revenue DESC
LIMIT 10;
```

### Top vendedores por receita

```sql
SELECT
    seller_id,
    seller_state,
    total_orders,
    total_revenue,
    avg_item_price
FROM iceberg.raw_olist_marts.dim_sellers
ORDER BY total_revenue DESC
LIMIT 20;
```

---

## Serviços e Portas

| Serviço | URL | Credenciais |
|---|---|---|
| Spark UI | http://localhost:4040 | — |
| Trino UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Superset | http://localhost:8088 | admin / admin |
| Schema Registry | http://localhost:8081 | — |
| Hive Metastore | thrift://localhost:9083 | — |

---

## Manutenção das Tabelas Iceberg

Execute periodicamente via Trino para evitar proliferação de arquivos pequenos:

```sql
-- Compactar arquivos pequenos (< 32 MB → ~128 MB)
ALTER TABLE iceberg.raw_olist_staging.bronze_orders
EXECUTE optimize(file_size_threshold => '32MB');

-- Expirar snapshots com mais de 7 dias
ALTER TABLE iceberg.raw_olist_staging.bronze_orders
EXECUTE expire_snapshots(retention_threshold => '7d');

-- Remover arquivos órfãos
ALTER TABLE iceberg.raw_olist_staging.bronze_orders
EXECUTE remove_orphan_files(retention_threshold => '7d');
```

---

## Boas Práticas Implementadas

- **Idempotência**: Produtor Kafka com `enable.idempotence=true` + `acks=all`. Modelos dbt com `merge` incremental.
- **Schemas explícitos**: Nenhuma inferência automática de tipos em produção (`StructType` no Spark, `schema.yml` no dbt).
- **Dead Letter Queue**: Registros inválidos são roteados para `*.dlq` sem travar o pipeline principal.
- **Deduplicação**: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY ...)` nos modelos staging.
- **Tratamento de nulos**: `COALESCE` em todos os campos monetários e `TRY_CAST` para timestamps.
- **Checkpointing**: Offsets Kafka persistidos em MinIO (`s3a://spark-demo/stream-1/checkpoints/`).
- **Testes de qualidade**: `unique`, `not_null` e `accepted_values` em todas as chaves primárias e campos críticos.
