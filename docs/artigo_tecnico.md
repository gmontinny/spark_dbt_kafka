# Modern Data Lakehouse: Arquitetura Medallion com Apache Kafka, Spark 4.x, Apache Iceberg e dbt sobre Trino

**Área:** Engenharia de Dados / Arquitetura de Sistemas Distribuídos  
**Palavras-chave:** Data Lakehouse, Apache Iceberg, Apache Kafka, Spark Connect, dbt, Trino, Arquitetura Medallion, Streaming, Batch Processing

---

## Resumo

Este artigo descreve a concepção, implementação e validação de um pipeline de dados end-to-end sobre o dataset público Olist de e-commerce brasileiro, adotando a arquitetura Medallion (Bronze → Silver → Gold) sobre uma stack moderna de código aberto. A solução integra Apache Kafka com Schema Registry Avro para ingestão resiliente, Apache Spark 4.x via Spark Connect para processamento em streaming e batch, Apache Iceberg como formato de tabela transacional sobre MinIO (S3-compatível), dbt para transformação e modelagem dimensional, e Trino como motor de consultas SQL distribuído. São apresentados dois pipelines complementares — streaming via Kafka e batch direto — com estratégias de idempotência, deduplicação, evolução de schema e manutenção de tabelas Iceberg. Os resultados demonstram PASS=61 em testes de qualidade de dados automatizados, cobrindo chaves primárias, integridade referencial e valores aceitos em todas as camadas da arquitetura.

---

## 1. Introdução

A crescente demanda por plataformas de dados que combinem a flexibilidade de um data lake com as garantias transacionais de um data warehouse deu origem ao conceito de **Data Lakehouse** [1]. Diferentemente das arquiteturas tradicionais em dois silos — lake para armazenamento bruto e warehouse para dados curados —, o Lakehouse unifica ambas as camadas sobre um único storage, eliminando movimentações de dados redundantes e reduzindo latência de disponibilização analítica.

O dataset Olist [2], com mais de 100 mil pedidos reais de e-commerce brasileiro entre 2016 e 2018, representa um cenário realista de ingestão heterogênea: nove entidades relacionadas distribuídas em arquivos CSV independentes, com inconsistências de tipagem, valores nulos e duplicatas oriundas de reprocessamentos. Esse contexto torna o dataset adequado para validar arquiteturas de dados em condições próximas à produção.

Este trabalho apresenta uma implementação completa e reproduzível de um Modern Data Lakehouse sobre infraestrutura containerizada, com foco em três contribuições principais:

1. Integração de Apache Kafka com Schema Registry Avro como camada de desacoplamento e contrato de dados explícito entre produtores e consumidores.
2. Uso de Apache Spark 4.x via Spark Connect em modo cliente-servidor desacoplado, com Structured Streaming gravando diretamente em tabelas Apache Iceberg com semântica transacional ACID.
3. Modelagem dimensional automatizada com dbt sobre Trino, suportando dois modos de execução (streaming e batch) via variáveis de compilação, com testes de qualidade de dados integrados ao pipeline.

---

## 2. Arquitetura da Solução

A arquitetura segue o padrão Medallion em três camadas, com dois pipelines de ingestão alternativos convergindo para o mesmo fluxo de transformação dbt.

### 2.1 Visão Geral

```
CSV Files (data/)
      │
      ├─── [Pipeline A] producer_csv_kafka.py
      │         │
      │    Apache Kafka (9 tópicos + 9 DLQs)
      │    Schema Registry (Avro)
      │         │
      │    stream_kafka_to_iceberg.py (Spark Structured Streaming)
      │         │
      │    Iceberg Bronze — raw_olist_staging.bronze_*
      │
      └─── [Pipeline B] ingest_csv_to_raw.py
                │
           Iceberg Raw — raw_olist.*
                │
           (ambos convergem aqui)
                │
         dbt_olist/models/staging/   →  Silver Views (stg_*)
                │
         dbt_olist/models/marts/     →  Gold Tables (fct_orders, dim_*)
                │
         Trino SQL / Superset / StarRocks
```

### 2.2 Componentes e Responsabilidades

| Componente | Versão | Responsabilidade |
|---|---|---|
| Apache Kafka (KRaft) | 3.7+ | Transporte de eventos, desacoplamento produtor/consumidor |
| Confluent Schema Registry | 7.x | Contrato Avro por tópico, evolução de schema controlada |
| Apache Spark | 4.x | Processamento streaming e batch, gravação Iceberg |
| Spark Connect | 4.x | Arquitetura cliente-servidor via gRPC |
| Apache Iceberg | 1.5+ | Formato de tabela ACID, time travel, evolução de schema |
| Hive Metastore | 3.x | Catálogo de metadados (backend PostgreSQL) |
| MinIO | RELEASE.2024+ | Object storage S3-compatível |
| dbt Core + dbt-trino | 1.12+ | Transformação SQL, testes de qualidade, documentação |
| Trino | 446+ | Motor SQL distribuído, serving analítico |
| Apache Superset | 3.x | Visualização e dashboards |

Toda a infraestrutura é provisionada via Docker Compose, permitindo reprodução completa em ambiente local com um único comando (`make up`).

---

## 3. Camada de Ingestão

### 3.1 Pipeline A — Streaming via Kafka

O produtor Python (`producer_csv_kafka.py`) lê os CSVs em chunks de 5.000 linhas para evitar pressão de memória, serializa cada linha em Avro usando o Schema Registry Confluent e publica nos tópicos correspondentes com as seguintes garantias:

- **Idempotência**: `enable.idempotence=True`, `acks=all`, `max.in.flight.requests.per.connection=5` — elimina duplicatas causadas por retentativas de rede.
- **Compressão**: codec `zstd`, reduzindo em média 60–70% o volume transferido em relação ao CSV original.
- **Dead Letter Queue (DLQ)**: linhas que falham na serialização Avro são encaminhadas para tópicos `*.dlq` com envelope estruturado contendo `source_file`, `raw_payload`, `error_type` e `failed_at_ms`, sem interromper o fluxo principal.

O wire format segue o padrão Confluent: `[0x00 magic byte | schema_id uint32 BE | avro binary payload]`, compatível com Kafka Connect, ksqlDB e o conector Kafka do Spark.

A topologia de tópicos distingue dois padrões de retenção:

- **Log Retention** (delete, 7 dias): tópicos de eventos transacionais (`orders`, `order_items`, `order_payments`, `order_reviews`) — dados têm validade temporal.
- **Log Compaction** (compact): tópicos de entidades dimensionais (`customers`, `products`, `sellers`, `geolocation`, `category_translation`) — mantém apenas a versão mais recente de cada chave primária.

### 3.2 Pipeline B — Batch Direto

O script `ingest_csv_to_raw.py` conecta ao Spark Connect Server e grava os CSVs diretamente como tabelas Iceberg em `spark_catalog.raw_olist.*`, sem passar pelo Kafka. Esse pipeline é adequado para reprocessamento histórico, desenvolvimento local e cenários onde a latência de streaming não é requisito.

A escolha do formato Iceberg (em vez de Parquet simples) garante que as tabelas Raw sejam acessíveis pelo Trino via catálogo `iceberg`, mantendo consistência de acesso entre os dois pipelines.

### 3.3 Spark Connect: Arquitetura Cliente-Servidor

O Spark 4.x introduz o Spark Connect como interface padrão de comunicação entre aplicações cliente e o cluster Spark. O cliente PySpark envia planos lógicos não resolvidos via gRPC (`sc://spark-connect:15002`); o servidor resolve, otimiza e executa o plano, retornando apenas os resultados.

Essa separação traz benefícios operacionais relevantes:

- Clientes não precisam de JVM local nem de JARs de dependência — apenas o pacote `pyspark`.
- Múltiplos clientes (scripts de ingestão, notebooks, workers Airflow) compartilham o mesmo cluster Spark sem interferência de classpath.
- Atualizações do cluster não exigem redistribuição de dependências nos clientes.

---

## 4. Camada de Armazenamento — Apache Iceberg

### 4.1 Por que Iceberg em vez de Parquet/Hive

O formato Parquet sobre partições Hive tradicionais apresenta limitações conhecidas em ambientes de streaming contínuo: ausência de transações ACID, impossibilidade de atualizações atômicas, proliferação de arquivos pequenos e quebra de leitores durante escritas concorrentes [3].

O Apache Iceberg resolve esses problemas através de uma camada de metadados em três níveis:

```
metadata.json          ← snapshot atual + histórico de snapshots
      │
manifest-list.avro     ← lista de manifests do snapshot
      │
manifest.avro          ← lista de arquivos Parquet + estatísticas min/max por coluna
      │
*.parquet              ← dados físicos
```

Essa estrutura permite:

- **Leituras isoladas de snapshot**: leitores veem sempre um estado consistente, independentemente de escritas concorrentes do Spark Streaming.
- **Pushdown de predicados**: o Trino lê apenas os manifests cujas estatísticas de coluna intersectam o filtro da query, sem varrer arquivos desnecessários.
- **Evolução de schema segura**: adição de colunas não reescreve dados históricos — novos arquivos incluem a coluna, arquivos antigos retornam NULL.
- **Time travel**: `SELECT * FROM tabela FOR SYSTEM_VERSION AS OF <snapshot_id>` para auditoria ou rollback.

### 4.2 Manutenção de Tabelas

O Spark Structured Streaming com trigger de 15 segundos gera arquivos Parquet pequenos (~1–5 MB cada). Sem manutenção periódica, o número de arquivos cresce linearmente com o tempo, degradando a performance de leitura do Trino.

Os marts Gold executam automaticamente via `post_hook` no dbt após cada run:

```sql
ALTER TABLE {{ this }} EXECUTE optimize(file_size_threshold => '32MB');
ALTER TABLE {{ this }} EXECUTE expire_snapshots(retention_threshold => '7d');
```

O `optimize` reescreve arquivos menores que 32 MB em arquivos de ~128 MB, reduzindo o número de splits de leitura. O `expire_snapshots` remove snapshots com mais de 7 dias do histórico de metadados, liberando espaço no MinIO.

---

## 5. Camada de Transformação — dbt sobre Trino

### 5.1 Arquitetura Medallion com dbt

O dbt organiza as transformações em dois grupos de modelos:

**Staging (Silver) — Views**: seis modelos que limpam, tipam e deduplicam os dados brutos. Por serem views, não materializam dados adicionais — o Trino executa a lógica diretamente sobre as tabelas Bronze ou Raw no momento da consulta.

**Marts (Gold) — Tabelas Incrementais**: três modelos analíticos materializados como tabelas Iceberg com estratégia `merge`:

| Modelo | Chave | Descrição |
|---|---|---|
| `fct_orders` | `order_id` | Fato de pedidos com pagamentos e itens agregados, SLA de entrega |
| `dim_customers` | `customer_unique_id` | Dimensão de clientes com LTV e contagem de pedidos |
| `dim_sellers` | `seller_id` | Dimensão de vendedores com receita e volume de vendas |

### 5.2 Suporte Dual Pipeline via Variáveis dbt

Um desafio central da implementação foi suportar dois pipelines com fontes de dados distintas sem duplicar os modelos SQL. A solução utiliza variáveis de compilação dbt:

```sql
-- stg_orders.sql (trecho)
{% if var('pipeline', 'streaming') == 'batch' %}
    FROM {{ source('raw_olist', 'orders') }}
    -- sem kafka_offset, deduplicação por order_id
{% else %}
    FROM {{ source('iceberg_bronze', 'bronze_orders') }}
    ORDER BY CAST(kafka_offset AS BIGINT) DESC
{% endif %}
```

O Pipeline A executa `dbt run --profiles-dir .` (padrão `pipeline=streaming`). O Pipeline B executa `dbt run --profiles-dir . --vars '{pipeline: batch}'`. Os modelos de marts são idênticos em ambos os casos — apenas as views de staging alteram a fonte.

### 5.3 Estratégia Incremental e Watermark

Os marts usam `incremental_strategy='merge'` com `on_schema_change='append_new_columns'`. A escolha de `append_new_columns` em vez de `sync_all_columns` é deliberada: o Apache Iceberg não suporta ALTER de tipo de coluna existente — tentativas resultam em `ICEBERG_COMMIT_ERROR`. Com `append_new_columns`, novas colunas são adicionadas automaticamente; mudanças de tipo exigem reset explícito da tabela.

O watermark incremental do `fct_orders` usa `dbt_updated_at` (coluna gerada pelo próprio mart) em vez de `ingestion_timestamp` (coluna do Bronze, inexistente na tabela Gold):

```sql
{% if is_incremental() %}
WITH _watermark AS (
    SELECT MAX(dbt_updated_at) - INTERVAL '3' DAY AS cutoff
    FROM {{ this }}
),
orders AS (
    SELECT o.* FROM {{ ref('stg_orders') }} o
    CROSS JOIN _watermark w
    WHERE o.order_purchased_at >= w.cutoff
)
{% else %}
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)
{% endif %}
```

A janela de 3 dias de sobreposição garante que pedidos com timestamps atrasados (entregues após a última execução) sejam capturados no próximo run.

### 5.4 Testes de Qualidade de Dados

O dbt executa 61 testes automatizados cobrindo:

- `unique` + `not_null` em todas as chaves primárias (order_id, customer_id, seller_id, product_id)
- `not_null` em campos críticos de negócio (payment_value, freight_value, order_status, is_delivered)
- `accepted_values` para campos categóricos (order_status com 8 valores válidos, payment_type com 5 valores, customer_state com 27 estados brasileiros)

Resultado validado: **PASS=61, ERROR=0** em ambos os pipelines.

---

## 6. Desafios Encontrados e Soluções

### 6.1 ICEBERG_MISSING_METADATA e Reset via PostgreSQL

O Trino não consegue executar `DROP TABLE` quando os arquivos de metadata Iceberg foram removidos do MinIO sem passar pelo catálogo — retorna `ICEBERG_MISSING_METADATA`. A solução implementada contorna o problema removendo as entradas diretamente do PostgreSQL (backend do Hive Metastore), respeitando a ordem de foreign keys:

```
PART_COL_STATS → PART_PRIVS → PARTITION_KEY_VALS → PARTITIONS
→ TABLE_PARAMS → PARTITION_KEYS → TBLS
→ COLUMNS_V2 → SERDE_PARAMS → SD_PARAMS → SDS → CDS → SERDES
```

Após a limpeza do metastore, os arquivos residuais no MinIO são removidos via `mcli rm --recursive`. Essa abordagem funciona em qualquer estado: metadata presente, ausente ou parcialmente corrompida.

### 6.2 Subquery Correlacionada no Trino

O Trino não suporta subqueries não-correlacionadas dentro de CTEs que referenciam tabelas externas ao bloco `{% if is_incremental() %}`. A tentativa original:

```sql
-- FALHA: NOT_SUPPORTED: correlated subquery
WITH orders AS (
    SELECT * FROM stg_orders
    WHERE ingestion_timestamp >= (
        SELECT MAX(ingestion_timestamp) - INTERVAL '3' DAY FROM {{ this }}
    )
)
```

Foi resolvida extraindo o watermark como CTE separada antes das demais, conforme demonstrado na seção 5.3.

### 6.3 Produtos sem Categoria no Dataset

O dataset Olist contém 610 produtos com `product_category_name` nulo — sem categoria definida na fonte. O `COALESCE` original tratava apenas o caso de "categoria sem tradução para inglês" (fallback para o nome em português), mas não o caso de ausência total de categoria. A correção adicionou um terceiro nível de fallback:

```sql
COALESCE(
    t.product_category_name_english,  -- tradução disponível
    p.product_category_name,          -- nome original em português
    'uncategorized'                   -- produto sem categoria
) AS product_category_name_en
```

---

## 7. Resultados e Validação

### 7.1 Volume de Dados Processados

| Entidade | Registros (CSV) | Enviados Kafka | DLQ |
|---|---|---|---|
| orders | 99.441 | 99.441 | 0 |
| order_items | 112.650 | 112.650 | 0 |
| order_payments | 103.886 | 103.886 | 0 |
| order_reviews | 99.224 | 99.224 | 0 |
| customers | 99.441 | 99.441 | 0 |
| products | 32.951 | 32.951 | 0 |
| sellers | 3.095 | 3.095 | 0 |
| geolocation | 1.000.163 | 1.000.163 | 0 |
| category_translation | 71 | 71 | 0 |
| **Total** | **~1,55M** | **~1,55M** | **0** |

### 7.2 Qualidade de Dados — dbt test

```
Pipeline A (streaming):  PASS=61  WARN=0  ERROR=0
Pipeline B (batch):      PASS=61  WARN=0  ERROR=0
```

### 7.3 Consultas Analíticas de Exemplo

Receita mensal com SLA de entrega, disponível via Trino imediatamente após o `dbt run`:

```sql
SELECT
    DATE_TRUNC('month', order_date)  AS month,
    COUNT(*)                         AS total_orders,
    SUM(total_payment_value)         AS total_revenue,
    AVG(delivery_days)               AS avg_delivery_days,
    AVG(CAST(delivered_on_time AS INTEGER)) * 100 AS sla_pct
FROM iceberg.raw_olist_marts.fct_orders
WHERE is_delivered = TRUE
GROUP BY 1
ORDER BY 1 DESC;
```

---

## 8. Boas Práticas Consolidadas

A implementação consolidou um conjunto de práticas recomendadas para pipelines de dados em produção:

**Contrato de dados explícito**: schemas Avro registrados no Schema Registry por tópico eliminam a inferência automática de tipos, que é custosa e não determinística em grandes volumes. Mensagens fora do schema são rejeitadas na borda, antes de chegar ao broker.

**Idempotência em todas as camadas**: produtor Kafka com `enable.idempotence=True`; Spark Structured Streaming com checkpoints duráveis no MinIO; modelos dbt com `incremental_strategy='merge'`. Qualquer componente pode ser reiniciado sem gerar duplicatas.

**Deduplicação baseada em offset**: nos modelos staging do Pipeline A, a deduplicação usa `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY CAST(kafka_offset AS BIGINT) DESC)`, garantindo que a versão mais recente de cada entidade seja preservada em caso de reprocessamento.

**Separação de responsabilidades por camada**: Bronze armazena dados brutos com metadados de ingestão intactos (kafka_offset, ingestion_timestamp); Silver aplica limpeza e tipagem sem agregar; Gold materializa métricas de negócio com merge incremental. Cada camada pode ser reconstruída independentemente.

**Reset cirúrgico via metastore**: em vez de depender do Trino para `DROP TABLE` (que falha com metadata corrompida), os scripts de reset operam diretamente no PostgreSQL do Hive Metastore, garantindo funcionamento em qualquer estado de consistência.

---

## 9. Conclusão

Este trabalho demonstrou a viabilidade e a robustez de uma arquitetura Modern Data Lakehouse inteiramente baseada em tecnologias de código aberto, implementada sobre infraestrutura containerizada e reproduzível. A combinação de Apache Kafka para ingestão resiliente, Spark 4.x via Spark Connect para processamento desacoplado, Apache Iceberg para armazenamento transacional e dbt sobre Trino para transformação e serving analítico forma uma stack coesa, onde cada componente resolve um problema específico sem sobreposição de responsabilidades.

A arquitetura Medallion com suporte a dois pipelines complementares — streaming e batch — via variáveis de compilação dbt demonstrou ser uma abordagem pragmática para ambientes que precisam equilibrar latência de dados em produção com facilidade de reprocessamento em desenvolvimento. A convergência dos dois pipelines na mesma camada de transformação elimina duplicação de lógica de negócio e garante consistência dos resultados independentemente da origem dos dados.

Os 61 testes de qualidade automatizados, cobrindo unicidade de chaves, integridade de campos críticos e validação de domínios categóricos, estabelecem uma linha de base de confiança nos dados que pode ser integrada a pipelines de CI/CD, bloqueando a publicação de modelos com regressões de qualidade.

Como trabalhos futuros, destacam-se: (i) integração com Apache Airflow para orquestração e agendamento dos pipelines; (ii) implementação de particionamento oculto Iceberg por data nos marts Gold para otimização de queries temporais; (iii) adoção de StarRocks como camada de serving de baixíssima latência sobre as tabelas Iceberg Gold; e (iv) extensão do Schema Registry para suportar evolução compatível de schemas Avro sem quebra de consumidores existentes.

A solução completa, incluindo todos os scripts, modelos dbt, configurações Docker e documentação, está disponível publicamente para reprodução e extensão pela comunidade.

---

## Referências

[1] ARMBRUST, M. et al. **Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics**. In: *Proceedings of CIDR 2021 — 11th Annual Conference on Innovative Data Systems Research*, 2021. Disponível em: https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf

[2] OLIST. **Brazilian E-Commerce Public Dataset by Olist**. Kaggle, 2018. Disponível em: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

[3] APACHE SOFTWARE FOUNDATION. **Apache Iceberg: An Open Table Format for Huge Analytic Datasets**. Documentação oficial, 2024. Disponível em: https://iceberg.apache.org/docs/latest/

[4] NARKHEDE, N.; SHAPIRA, G.; PALINO, T. **Kafka: The Definitive Guide — Real-Time Data and Stream Processing at Scale**. 2. ed. Sebastopol: O'Reilly Media, 2021.

[5] ZAHARIA, M. et al. **Apache Spark: A Unified Engine for Big Data Processing**. *Communications of the ACM*, v. 59, n. 11, p. 56–65, nov. 2016. DOI: 10.1145/2934664

[6] APACHE SOFTWARE FOUNDATION. **Spark Connect Overview — Apache Spark 4.0 Documentation**. 2024. Disponível em: https://spark.apache.org/docs/latest/spark-connect-overview.html

[7] TRINO PROJECT. **Trino: Fast Distributed SQL Query Engine for Big Data Analytics**. Documentação oficial, 2024. Disponível em: https://trino.io/docs/current/

[8] DBT LABS. **dbt Documentation: Transform Data in Your Warehouse**. 2024. Disponível em: https://docs.getdbt.com/

[9] KLEPPMANN, M. **Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems**. Sebastopol: O'Reilly Media, 2017.

[10] INMON, W. H. **Building the Data Warehouse**. 4. ed. Indianapolis: Wiley Publishing, 2005.

[11] KIMBALL, R.; ROSS, M. **The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling**. 3. ed. Indianapolis: Wiley, 2013.

[12] CONFLUENT. **Schema Registry Documentation — Confluent Platform**. 2024. Disponível em: https://docs.confluent.io/platform/current/schema-registry/

[13] MINIO. **MinIO High Performance Object Storage**. Documentação oficial, 2024. Disponível em: https://min.io/docs/minio/linux/index.html

[14] APACHE SOFTWARE FOUNDATION. **Apache Hive Metastore**. Documentação oficial, 2024. Disponível em: https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration
