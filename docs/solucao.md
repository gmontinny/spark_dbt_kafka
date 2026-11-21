# Arquitetura e Solução Integrada: Modern Data Lakehouse

**Stack Tecnológica:**
* **Engine de Processamento:** Apache Spark 4.x (PySpark + Spark Connect)
* **Transformação & Data Modeling:** dbt (`dbt-trino` / `dbt-spark`)
* **Armazenamento & Metadata:** Apache Hadoop HDFS / MinIO (S3A) + Hive Metastore / Iceberg REST Catalog
* **Table Format:** Apache Iceberg
* **Distributed SQL Query Engine:** Trino
* **Streaming & Event Streaming Ingestion:** Apache Kafka (Ecosistema, KRaft, Schema Registry, Kafka Connect)
* **Cenário de Entrada:** Ingestão de múltiplos arquivos CSV (Heterogêneos, Alta Volumetria e Resiliência)

---

## 1. Visão Geral do Cenário e Arquitetura

O objetivo desta solução é estruturar um pipeline de dados de ponta a ponta (**End-to-End Lakehouse Architecture**) resiliente, escalável e governado, capaz de processar múltiplos arquivos CSV (tanto em batelada quanto em tempo real/micro-batch), transportá-los com segurança e sem perda de dados através do **Apache Kafka**, processá-los com **Apache Spark 4.x** via **Spark Connect**, persisti-los no formato aberto de tabelas **Apache Iceberg** sobre storage compatível com **Hadoop/S3**, orquestrar transformações com **dbt**, e disponibilizar consultas federadas de baixa latência através do **Trino**.

### Diagrama Arquitetural de Referência

```mermaid
flowchart TD
    subgraph Ingestion_Sources ["Fontes de Dados (CSV Ingestion)"]
        CSV1["Arquivos CSV de Pedidos"]
        CSV2["Arquivos CSV de Clientes"]
        CSV3["Arquivos CSV de Pagamentos"]
    end

    subgraph Messaging_Tier ["Camada de Mensageria & Streaming (Apache Kafka)"]
        KC["Kafka Connect / Producer Idempotente"]
        SR["Schema Registry (Avro / JSON Schema)"]
        KT_Raw["Tópicos Kafka (Particionados por Chave de Negócio)"]
        DLQ["Dead Letter Queue (DLQ - Erros de Parsing)"]
        
        KC --> SR
        KC --> KT_Raw
        KC -. Falha de Parsing .-> DLQ
    end

    subgraph Processing_Tier ["Processamento & Lakehouse (Apache Spark 4.x)"]
        ClientApp["PySpark Thin Client (via Spark Connect)"]
        SC_Server["Spark Connect Server (gRPC Port 15002)"]
        SparkStream["Spark Structured Streaming / Batch Engine"]
        
        ClientApp -->|gRPC / sc://...| SC_Server
        SC_Server --> SparkStream
    end

    subgraph Storage_Metadata ["Storage & Metadados (Hadoop / Iceberg)"]
        HMS["Hive Metastore / REST Catalog (PostgreSQL Backend)"]
        MinIO["Storage S3A / HDFS (MinIO / Hadoop)"]
        
        subgraph Medallion_Tables ["Tabelas Apache Iceberg (ACID, Snapshot Isolation)"]
            Bronze["Tabelas Bronze (Raw Ingested)"]
            Silver["Tabelas Silver (Cleansed / Deduplicated)"]
            Gold["Tabelas Gold (Marts / Star Schema)"]
        end
    end

    subgraph Transformation_Serving ["Transformação & Servicing"]
        DBT["dbt Core (dbt-trino / dbt-spark)"]
        TrinoEngine["Trino Distributed SQL Engine"]
        BI["Dashboards / Analytics (Superset / StarRocks)"]
    end

    %% Conexões do Fluxo
    Ingestion_Sources -->|Spooling / Event-Driven| KC
    KT_Raw -->|Consume Stream com Checkpoint| SparkStream
    SparkStream -->|Append / Merge ACID| Bronze
    HMS <-->|Cataloga Schemas e Partições| Medallion_Tables
    MinIO <-->|Persiste Parquet + Manifests| Medallion_Tables

    Bronze -->|dbt Staging Models| DBT
    DBT -->|Executa Transformações| TrinoEngine
    TrinoEngine -->|Compila e Escreve| Silver
    Silver -->|dbt Marts Models (Incremental Merge)| DBT
    DBT -->|Escreve Tabelas Analíticas| Gold
    Gold -->|Consultas Analíticas Rápidas| TrinoEngine
    TrinoEngine --> BI
```

---

## 2. Ingestão de Múltiplos CSVs: Desafios e Estratégias

Arquivos CSV são o formato de intercâmbio mais comum, porém apresentam desafios críticos em produção:
1. **Ausência de tipagem intrínseca:** Tudo é interpretado como string ou depende de inferência de esquema (inferSchema consome múltiplos passos e é custoso em grandes volumes).
2. **Schema Drift e inconsistência de cabeçalho:** Colunas novas, reordenação de campos ou colunas renomeadas.
3. **Caracteres malformados e quebras de linha não escapadas:** Podem corromper a leitura do lote inteiro.
4. **Encoding variado:** UTF-8, ISO-8859-1 (Latin1), etc.

### Estratégias de Abordagem

| Estratégia | Cenário Ideal | Vantagens | Desvantagens |
| :--- | :--- | :--- | :--- |
| **Opção A: Kafka Connect (Spooldir)** | Arquivos são depositados periodicamente em diretórios monitorados. | Declarativo, sem necessidade de código, integração com Schema Registry e controle de offset de arquivo automático. | Requer configuração de conectores de terceiros (ex: Confluent Spooldir Connector). |
| **Opção B: Produtor Python Dedicado (Fast & Micro-batch)** | Arquivos chegam via SFTP, S3 ou API com necessidade de validação prévia. | Validação fina linha a linha, roteamento dinâmico para tópicos, envio para DLQ com contexto detalhado. | Custo de manutenção de código de ingestão. |
| **Opção C: Spark Connect Leitura Direta de Diretório** | Arquivos volumosos diários (dezenas a centenas de GBs) diretamente no data lake. | Leitura paralela nativa via Spark 4.x com schema fixo e `badRecordsPath`. | Não desacopla eventos pontuais como o Kafka. |

---

## 3. Apache Kafka: Potencialidades, Modos de Implementação e Boas Práticas

O Apache Kafka atua como o **amortecedor de carga (backpressure buffer)** e **espinha dorsal de desacoplamento** entre a geração de dados e o processamento no Data Lakehouse.

### 3.1. Modos de Implementação do Cluster
1. **KRaft Mode (Kafka Raft Metadata - Recomendado):**
   * Elimina a dependência externa do Apache ZooKeeper a partir das versões 3.x+ e consolidado na 4.x.
   * Simplifica a arquitetura operacional, diminui o footprint de memória e acelera a eleição de partições e recuperação de falhas.
2. **Cluster Multi-Broker com Rack Awareness:**
   * Distribuição de réplicas em zonas de disponibilidade distintas (`broker.rack`) garantindo alta disponibilidade sem perda de dados (`min.insync.replicas=2` para `replication.factor=3`).

### 3.2. Potencialidades e Recursos Essenciais do Kafka

* **Schema Registry (Contrato de Dados - Confluent Schema Registry):**
  * Evita a ingestão de registros com esquemas corrompidos.
  * O produtor serializa a linha CSV em **Avro** ou **JSON Schema**, registrando a versão no Schema Registry. Mensagens fora de conformidade são rejeitadas na borda.
* **Topologia de Particionamento (Partition Keying):**
  * Uso de chave de partição determinística (ex: `customer_id` ou `order_id`) para garantir a ordem estrita de eventos da mesma entidade.
  * Partições sem chave usam *Sticky Partitioning* para maximizar throughput e compressão em batch (`linger.ms` e `batch.size`).
* **Log Compaction vs. Log Retention:**
  * **Retention por Tempo/Tamanho:** Ideal para streams de eventos e transações (ex: pedidos criados, cliques).
  * **Compacted Topics:** Ideal para tabelas dimensionais e dados de estado (ex: cadastro de clientes, tabela de produtos), mantendo apenas a versão mais recente de cada chave primária.
* **Idempotência e Semântica Exatamente Uma Vez (Exactly-Once Semantics - EOS):**
  * Produtor com `enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection <= 5`.
  * Garante que falhas de rede e retentativas não gerem mensagens duplicadas no tópico.
* **Dead Letter Queue (DLQ):**
  * Linhas com erros de separação, campos faltando ou problemas de conversão numérica são enviadas a um tópico dedicado de auditoria/erros (`*.dlq`), permitindo reprocessamento sem travar o pipeline principal.

---

## 4. Apache Spark 4.x com PySpark e Spark Connect

O **Apache Spark 4.0** traz evoluções fundamentais que modificam a arquitetura de engenharia de dados:

### 4.1. Por que Spark Connect no Spark 4.x?
* **Arquitetura Cliente-Servidor Desacoplada:** O cliente PySpark roda como um cliente fino (thin client), enviando apenas planos lógicos não resolvidos via **gRPC** para o servidor Spark Connect.
* **Isolamento de Dependências:** Aplicações de usuário, ambientes virtuais (`uv`, `venv`), Airflow workers ou notebooks não precisam ter Java instalado nem as dezenas de dependências de JARs pesadas localmente.
* **Depuração Interativa e Escalabilidade:** Permite conectar clientes leves (ex: containers de microsserviços, dbt) a clusters Spark compartilhados e centralizados.
* **Performance com Apache Arrow:** Conversões nativas e transferência colunar otimizada entre a JVM e o processo Python do PySpark.

### 4.2. Inovações do Spark 4.0 Relevantes para a Solução
* **ANSI SQL por padrão:** Garante conformidade com o padrão SQL e previne truncamentos e overflows silenciosos.
* **Structured Logging:** Logs estruturados em formato JSON para melhor observabilidade em ferramentas como ElasticSearch, Loki ou Cloud Logging.
* **Melhorias em Structured Streaming:** Introdução do novo operador de estado `transformWithState` e maior otimização de commits com Apache Iceberg.
* **Adaptive Query Execution (AQE) 2.0:** Ajuste dinâmico de partições e resolução de assimetria (skew join) automática.

---

## 5. Camada de Tabela: Apache Iceberg no Hadoop / S3

Diferente de manter arquivos CSV ou Parquet soltos em pastas Hive tradicionais, o **Apache Iceberg** eleva o data lake ao nível de um banco de dados relacional distribuído.

### 5.1. Benefícios do Apache Iceberg
1. **Transações ACID:** Garante que leituras feitas pelo Trino nunca encontrem tabelas em estado inconsistente enquanto o Spark grava micro-batches.
2. **Hidden Partitioning (Particionamento Oculto):** Os usuários consultam colunas originais (ex: `order_timestamp`), e o Iceberg cuida da tradução para a partição física subjacente (ex: `days(order_timestamp)`), sem necessidade de filtros artificiais como `year=2026/month=11`.
3. **Evolução de Esquema Segura (Full Schema Evolution):** Permite adicionar, renomear, reordenar ou remover colunas sem reescrever dados históricos e sem quebrar consumidores.
4. **Time Travel & Rollback:** Permite auditar ou reverter dados para versões anteriores via snapshots (`FOR SYSTEM_VERSION AS OF <snapshot_id>`).
5. **Manutenção Automática de Arquivos:** Utilitários nativos para compactação de arquivos pequenos (*small files problem*) gerados pelo streaming (`rewrite_data_files`), remoção de arquivos órfãos (`remove_orphan_files`) e expiração de snapshots antigos.

---

## 6. Camada de Consulta: Trino

O **Trino** (antigo PrestoSQL) atua como o motor de consultas SQL distribuído de altíssima performance para ad-hoc queries, relatórios e serving:
* **Leitura Otimizada de Metadados Iceberg:** O conector Iceberg do Trino lê os arquivos de metadados (`metadata.json` e manifests Avro) diretamente, ignorando arquivos de dados Parquet que não atendam às cláusulas de filtro (`min/max statistics pushdown`).
* **Cost-Based Optimizer (CBO) & Dynamic Filtering:** Reordena junções e filtra partições dinamicamente em tempo de execução.
* **Alimentação de dbt:** O Trino atua como engine de execução para os modelos dbt, permitindo SQL padrão de alta velocidade sobre as tabelas Iceberg.

---

## 7. Modelagem e Transformação: dbt (Data Build Tool)

O **dbt** organiza a arquitetura em medalhão (Bronze -> Silver -> Gold):

```
       [ Kafka / CSV Ingestion ]
                  │
                  ▼ (Spark Structured Streaming)
        ┌───────────────────┐
        │   Bronze (Raw)    │  Tabelas Iceberg com dados crus e metadados de ingestão
        └───────────────────┘
                  │
                  ▼ (dbt Staging Models - dbt-trino / dbt-spark)
        ┌───────────────────┐
        │  Silver (Cleansed)│  Dados tipados, limpos, desduplicados e normalizados
        └───────────────────┘
                  │
                  ▼ (dbt Marts Models - dbt incremental / merge)
        ┌───────────────────┐
        │   Gold (Marts)    │  Fatos e Dimensões (Star Schema), Métricas de Negócio
        └───────────────────┘
```

* **Camada Bronze (Raw):** Tabelas Iceberg brutas geradas pela ingestão do Spark. Armazena o payload original, timestamps de ingestão e offset do Kafka.
* **Camada Silver (Cleansed/Enriched):** Modelos `staging` do dbt executados via Trino. Realiza cast de tipos, higienização de strings, regras de deduplicação (Window Functions ou `QUALIFY ROW_NUMBER()`).
* **Camada Gold (Analytics / Marts):** Modelos analíticos (`fct_orders`, `dim_customers`), materializados de forma incremental usando a estratégia de `merge` nativa do Iceberg (`incremental_strategy='merge'`).

---

## 8. Implementação Prática: Código e Configurações

Abaixo estão os códigos e arquivos de configuração prontos para produção.

### 8.1. Produtor Idempotente de CSV para Kafka (`producer_csv_kafka.py`)

Este script implementa as melhores práticas de produção: leitura em chunks (evitando OOM na memória), serialização Avro com Schema Registry, envio com idempotência habilitada, compressão zstd e envio para Dead Letter Queue em caso de erro.

O mapeamento de arquivos para tópicos é feito via `TOPIC_MAP` (dict `filename → TopicMapping`). Cada tópico tem sua chave de partição específica (ex: `order_id` para pedidos, `seller_id` para vendedores).

O DLQ usa um envelope Avro estruturado (`DeadLetterRecord`) com campos `source_topic`, `source_file`, `partition_key`, `raw_payload`, `error_type`, `error_message` e `failed_at_ms`.

**Wire format Confluent:** `[0x00 magic byte | schema_id uint32 BE | avro binary payload]` — compatível com Spark Kafka connector e ksqlDB.

Variáveis de ambiente relevantes:
```
KAFKA_BROKERS         — kafka:9093 (Docker) ou localhost:9092 (host)
KAFKA_SCHEMA_REGISTRY — http://schema-registry:8081
CSV_DIR               — caminho para os CSVs (padrão: ./data)
```

Tópicos produzidos (prefixo `olist.raw.`):
```
olist.raw.orders, olist.raw.order_items, olist.raw.order_payments,
olist.raw.order_reviews, olist.raw.customers, olist.raw.products,
olist.raw.sellers, olist.raw.geolocation, olist.raw.category_translation
+ DLQs: <topic>.dlq
```

---

### 8.2. Ingestão e Processamento Streaming com Spark 4.x & Spark Connect (`stream_kafka_to_iceberg.py`)

Este script utiliza o cliente PySpark para conectar remotamente via **Spark Connect** (`sc://...`), consome todos os 9 tópicos Olist do Kafka em paralelo com checkpointing no MinIO e persiste em tabelas Iceberg Bronze.

Características da implementação:
- **9 streams paralelos**: um por tópico, cada um com seu próprio checkpoint em `s3a://spark-demo/stream-1/checkpoints/<table>`
- **Schemas `StructType` explícitos** por tópico — sem inferência automática
- **`_ensure_bronze_table()`**: cria as tabelas Iceberg se não existirem antes de iniciar o stream
- **Catálogo**: `spark_catalog` (configurado no `spark-defaults.conf` do servidor Spark Connect)
- **Tabelas alvo**: `spark_catalog.raw_olist_staging.bronze_*`
- **Metadados de ingestão** adicionados a cada linha: `kafka_key`, `kafka_offset`, `kafka_timestamp`, `ingestion_timestamp`
- **Trigger**: `processingTime='15 seconds'` (micro-batch)
- **Wire format Avro Confluent**: o campo `value` do Kafka é deserializado via `from_json` após extração do payload binário

Variáveis de ambiente relevantes:
```
SPARK_CONNECT_URL — sc://localhost:15002
KAFKA_BROKERS     — kafka:9093
```

---

### 8.3. Configuração do Catálogo Iceberg no Trino (`trino/catalog/iceberg.properties`)

```properties
connector.name=iceberg
hive.metastore.uri=thrift://hive-metastore:9083
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD

# HDFS / S3 Storage (MinIO)
fs.s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin

# Otimizações de Consulta e Escrita
iceberg.target-split-size=128MB
iceberg.pushdown-filter-enabled=true
```

---

### 8.4. Modelagem com dbt (`dbt-trino`)

#### Configuração do Perfil (`~/.dbt/profiles.yml` ou `profiles.yml` no projeto):
```yaml
lakehouse_dbt:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      user: dbt_user
      host: localhost
      port: 8080
      database: iceberg
      schema: raw_olist_staging
      threads: 4
      session_properties:
        query_max_run_time: 15m
```

#### Configuração do Projeto (`dbt_project.yml`):
```yaml
name: 'lakehouse_dbt'
version: '1.0.0'
config-version: 2

profile: 'lakehouse_dbt'

model-paths: ["models"]
test-paths: ["tests"]

models:
  lakehouse_dbt:
    staging:
      +materialized: view
      +schema: raw_olist_staging
    marts:
      +materialized: incremental
      +incremental_strategy: merge
      +unique_key: order_id
      +schema: raw_olist_marts
      +table_properties:
        format: "'PARQUET'"
        partitioning: "ARRAY['month(order_date)']"
```

#### Modelo Staging Silver (`models/staging/stg_orders.sql`):
```sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('iceberg_bronze', 'bronze_orders') }}
),

deduped AS (
    SELECT
        order_id,
        customer_id,
        LOWER(TRIM(order_status))                                    AS order_status,
        TRY_CAST(order_purchase_timestamp   AS TIMESTAMP)            AS order_purchased_at,
        TRY_CAST(order_approved_at          AS TIMESTAMP)            AS order_approved_at,
        TRY_CAST(order_delivered_carrier_date  AS TIMESTAMP)         AS order_delivered_carrier_at,
        TRY_CAST(order_delivered_customer_date AS TIMESTAMP)         AS order_delivered_customer_at,
        TRY_CAST(order_estimated_delivery_date AS TIMESTAMP)         AS order_estimated_delivery_at,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY CAST(kafka_offset AS BIGINT) DESC
        ) AS row_num
    FROM source
    WHERE order_id IS NOT NULL AND TRIM(order_id) != ''
)

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchased_at,
    CAST(order_purchased_at AS DATE)                                 AS order_date,
    order_approved_at,
    order_delivered_carrier_at,
    order_delivered_customer_at,
    order_estimated_delivery_at,
    CASE WHEN order_status = 'delivered' THEN TRUE ELSE FALSE END    AS is_delivered,
    CASE
        WHEN order_delivered_customer_at IS NOT NULL
         AND order_purchased_at IS NOT NULL
        THEN DATE_DIFF('day', order_purchased_at, order_delivered_customer_at)
    END                                                              AS delivery_days,
    ingestion_timestamp
FROM deduped
WHERE row_num = 1
```

#### Modelo Marts Gold Incremental com Iceberg Merge (`models/marts/fct_orders.sql`):
```sql
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = 'order_id',
    on_schema_change     = 'sync_all_columns',
    post_hook = [
        "ALTER TABLE {{ this }} EXECUTE optimize(file_size_threshold => '32MB')",
        "ALTER TABLE {{ this }} EXECUTE expire_snapshots(retention_threshold => '7d')"
    ]
) }}

-- table_properties (format, partitioning) definidas globalmente no dbt_project.yml

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE ingestion_timestamp >= (
            SELECT MAX(ingestion_timestamp) - INTERVAL '3' DAY FROM {{ this }}
        )
    {% endif %}
),

payments_agg AS (
    SELECT
        order_id,
        SUM(payment_value)                          AS total_payment_value,
        COUNT(DISTINCT payment_sequential)          AS payment_count,
        MAX_BY(payment_type, payment_value)         AS primary_payment_type,
        MAX(payment_installments)                   AS max_installments
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

items_agg AS (
    SELECT
        order_id,
        COUNT(*)                                    AS total_items,
        COUNT(DISTINCT product_id)                  AS distinct_products,
        COUNT(DISTINCT seller_id)                   AS distinct_sellers,
        CAST(SUM(price)            AS DECIMAL(12,2)) AS total_items_price,
        CAST(SUM(freight_value)    AS DECIMAL(12,2)) AS total_freight_value,
        CAST(SUM(total_item_value) AS DECIMAL(12,2)) AS total_order_value
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchased_at,
    o.order_date,
    o.order_approved_at,
    o.order_delivered_customer_at,
    o.order_estimated_delivery_at,
    o.is_delivered,
    o.delivery_days,
    CASE
        WHEN o.is_delivered
         AND o.order_delivered_customer_at <= o.order_estimated_delivery_at
        THEN TRUE ELSE FALSE
    END                                             AS delivered_on_time,
    COALESCE(p.total_payment_value, 0)              AS total_payment_value,
    COALESCE(p.payment_count, 0)                    AS payment_count,
    p.primary_payment_type,
    COALESCE(p.max_installments, 1)                 AS max_installments,
    COALESCE(i.total_items, 0)                      AS total_items,
    COALESCE(i.distinct_products, 0)                AS distinct_products,
    COALESCE(i.distinct_sellers, 0)                 AS distinct_sellers,
    COALESCE(i.total_items_price, 0)                AS total_items_price,
    COALESCE(i.total_freight_value, 0)              AS total_freight_value,
    COALESCE(i.total_order_value, 0)                AS total_order_value,
    CURRENT_TIMESTAMP                               AS dbt_updated_at
FROM orders o
LEFT JOIN payments_agg p ON o.order_id = p.order_id
LEFT JOIN items_agg    i ON o.order_id = i.order_id
```

#### Testes de Qualidade de Dados (`models/staging/schema.yml`):
```yaml
version: 2

sources:
  - name: iceberg_bronze
    database: iceberg
    schema: raw_olist_staging
    tables:
      - name: bronze_orders
      - name: bronze_customers
      - name: bronze_order_items
      - name: bronze_order_payments
      - name: bronze_order_reviews
      - name: bronze_products
      - name: bronze_sellers
      - name: bronze_geolocation
      - name: bronze_category_translation

models:
  - name: stg_orders
    description: "Pedidos limpos, tipados e desduplicados — Camada Silver"
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: order_status
        tests:
          - accepted_values:
              values: ['delivered','shipped','canceled','invoiced','processing','created','approved','unavailable']
      - name: is_delivered
        tests: [not_null]
      - name: delivery_days
        description: "Dias entre compra e entrega ao cliente (NULL se não entregue)."

  - name: stg_customers
    columns:
      - name: customer_unique_id
        tests: [unique, not_null]

  - name: stg_order_items
    columns:
      - name: order_id
        tests: [not_null]
      - name: order_item_id
        tests: [not_null]

  - name: stg_order_payments
    columns:
      - name: order_id
        tests: [not_null]
      - name: payment_sequential
        tests: [not_null]

  - name: stg_products
    columns:
      - name: product_id
        tests: [unique, not_null]

  - name: stg_sellers
    columns:
      - name: seller_id
        tests: [unique, not_null]
```

---

## 9. Boas Práticas de Programação e Engenharia de Dados

### 9.1. Idempotência e Tratamento de Falhas
* **Semântica Exatamente Uma Vez (EOS):** 
  * O Kafka Producer deve utilizar `enable.idempotence=true`.
  * O Spark Structured Streaming utiliza write em tabelas Iceberg com controle transacional e checkpoints duráveis no S3A/HDFS. Se o worker cair e reiniciar, ele recupera exatamente o offset persistido no checkpoint.
* **dbt Incremental Merge:**
  * O uso de `incremental_strategy='merge'` garante que a reexecução do modelo dbt substitua o registro existente com a mesma chave `order_id` sem criar duplicatas.

### 9.2. Governança de Esquema e Tratamento de Erros
* **Contratos Estritos & Schema Registry:** Não confiar na inferência automática de tipos de CSVs em ambientes de produção. Utilizar sempre schemas tipados (`StructType` explícito no Spark e Schema Registry no Kafka).
* **Tratamento de Bad Records:** No caso de leitura direta de arquivos CSV pelo Spark, habilitar:
  ```python
  spark.read.option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("badRecordsPath", "s3a://spark-demo/bad_records/orders/")
  ```
  Isso isola linhas com delimitadores inconsistentes sem abortar a ingestão dos dados saudáveis.

### 9.3. Manutenção e Otimização do Apache Iceberg
Tabelas que recebem streaming contínuo sofrem com a proliferação de arquivos pequenos (*Small Files Problem*). Execute rotinas agendadas (via Trino ou Spark) de manutenção:

```sql
-- 1. Compactação de pequenos arquivos Parquet em arquivos de ~128MB a 512MB
ALTER TABLE iceberg.raw_olist_staging.bronze_orders 
EXECUTE optimize(file_size_threshold => '32MB');

-- 2. Expiração de snapshots com mais de 7 dias (libera espaço em disco no MinIO/HDFS)
ALTER TABLE iceberg.raw_olist_staging.bronze_orders 
EXECUTE expire_snapshots(retention_threshold => '7d');

-- 3. Limpeza de arquivos órfãos (arquivos não indexados nos manifests)
ALTER TABLE iceberg.raw_olist_staging.bronze_orders 
EXECUTE remove_orphan_files(retention_threshold => '7d');
```

### 9.4. Observabilidade e Monitoramento
* **Métricas do Kafka:** Monitorar `records-lag-max` dos consumidores Spark para garantir que a taxa de consumo acompanha a taxa de produção dos arquivos CSV.
* **Spark Web UI & Structured Streaming Metrics:** Monitorar métricas de `inputRowsPerSecond`, `processedRowsPerSecond` e latência de micro-batch na porta `4040`.
* **Trino Web UI:** Monitorar queries concorrentes, consumo de memória de nós workers e estágios de execução em `http://localhost:8080`.
* **dbt Logs e Testes Automatizados:** Integrar `dbt test` no CI/CD e em DAGs de orquestração (Apache Airflow / Composer) para bloquear a publicação em caso de violação de chaves ou nulos indevidos.

---

## 10. Guia de Execução Passo a Passo no Ambiente Local

Para subir e testar a solução no ambiente configurado no repositório:

1. **Subir os serviços via Docker Compose:**
   ```bash
   make up
   # ou: docker compose up -d
   ```

2. **Instalar dependências Python:**
   ```bash
   uv sync
   # ou: pip install -r requirements.txt
   ```

3. **Criar tópicos Kafka e registrar schemas Avro:**
   ```bash
   make topics
   # ou: python src/create_kafka_topics.py
   ```
   Cria 9 tópicos (`olist.raw.*`) + 9 DLQs e registra todos os schemas no Schema Registry.

4. **Iniciar o consumer Spark (Bronze) — terminal separado:**
   ```bash
   make stream
   # ou: python src/stream_kafka_to_iceberg.py
   ```

5. **Produzir os CSVs para o Kafka:**
   ```bash
   make produce
   # ou: python src/producer_csv_kafka.py
   ```

6. **Executar as Transformações e Testes com dbt:**
   ```bash
   make dbt-run
   make dbt-test
   ```

7. **Consultar os Dados Prontos via Trino CLI ou Superset:**
   ```sql
   SELECT
       DATE_TRUNC('month', order_date)  AS month,
       COUNT(*)                         AS total_orders,
       SUM(total_payment_value)         AS total_revenue,
       AVG(delivery_days)               AS avg_delivery_days
   FROM iceberg.raw_olist_marts.fct_orders
   WHERE is_delivered = TRUE
   GROUP BY 1
   ORDER BY 1 DESC;
   ```

**Pipeline B (batch, sem Kafka):**
```bash
make pipeline
# equivale a: make ingest && make dbt-run && make dbt-test
```
