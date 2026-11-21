# Guia Operacional Docker

Referência rápida para subir, resetar e solucionar problemas do ambiente local.

---

## Subindo o ambiente

```bash
make up
# ou: docker compose up -d
```

Aguarde todos os serviços ficarem `healthy` (~2 minutos):

```bash
docker compose ps
```

Ordem de inicialização garantida pelo `depends_on`:

```
postgres (healthy)
    └── hive-metastore (healthy)
minio (healthy)
    └── minio-client (completed) ──┘
            └── hive-metastore
                    └── trino
                    └── spark-connect
                    └── dev
kafka (healthy)
    └── schema-registry
    └── spark-connect
    └── dev
```

> O `hive-metastore` só inicia após o `minio-client` criar os buckets `spark-demo` e `warehouse`. Sem isso, o Hive falha com `Bucket warehouse does not exist`.

---

## Serviços e portas

| Serviço          | URL / Porta                  | Credenciais           |
|------------------|------------------------------|-----------------------|
| Kafka UI         | http://localhost:8082        | —                     |
| Spark UI         | http://localhost:4040        | —                     |
| Trino UI         | http://localhost:8080        | —                     |
| MinIO Console    | http://localhost:9001        | minioadmin / minioadmin |
| Superset         | http://localhost:8088        | admin / admin         |
| Schema Registry  | http://localhost:8081        | —                     |
| Hive Metastore   | thrift://localhost:9083      | —                     |
| StarRocks FE     | http://localhost:8030        | —                     |
| PostgreSQL       | localhost:5432               | hive / hive           |

---

## Reset completo

Use quando quiser recomeçar do zero (apaga todos os dados):

```bash
./reset.sh
```

O script executa:
1. `docker compose down -v` — para containers e remove volumes nomeados
2. `rm -rf minio_data/` — remove dados locais do MinIO
3. `docker compose up -d` — sobe tudo novamente
4. Aguarda `hive-metastore` e `trino` ficarem `healthy`
5. Executa `make pipeline` (ingest + dbt-run + dbt-test)

> No Windows, use `reset.ps1` em vez de `reset.sh`.

---

## Dependências Python

```bash
# Com uv (recomendado — usa uv.lock com hashes)
uv sync

# Com pip
pip install -r requirements-pip.txt
```

> `requirements.txt` é gerado por `uv export --frozen` e contém hashes para reprodutibilidade no CI/container. O `pip` não consegue instalar pacotes editáveis (`-e .`) junto com hashes — use `requirements-pip.txt` para instalação com pip.

---

## Variáveis de ambiente (`.env`)

| Variável                  | Padrão                          | Descrição                                              |
|---------------------------|---------------------------------|--------------------------------------------------------|
| `MINIO_ACCESS_KEY`        | `minioadmin`                    | Usuário root do MinIO                                  |
| `MINIO_SECRET_KEY`        | `minioadmin`                    | Senha root do MinIO                                    |
| `MINIO_ENDPOINT`          | `http://minio:9000`             | Endpoint S3A (rede interna Docker)                     |
| `MINIO_REGION`            | `us-east-1`                     | Região S3 simulada                                     |
| `KAFKA_BROKERS`           | `kafka:9093`                    | Broker interno (serviços Docker)                       |
| `KAFKA_BROKERS_EXTERNAL`  | `localhost:9092`                | Broker externo (scripts no host)                       |
| `KAFKA_SCHEMA_REGISTRY`   | `http://schema-registry:8081`   | URL do Schema Registry                                 |
| `POSTGRES_USER`           | `hive`                          | Usuário do PostgreSQL (backend do Hive Metastore)      |
| `POSTGRES_PASSWORD`       | `hive`                          | Senha do PostgreSQL                                    |
| `POSTGRES_DB`             | `metastore`                     | Database do PostgreSQL                                 |
| `TRINO_HOST`              | `trino`                         | Host do Trino (rede interna)                           |
| `TRINO_PORT`              | `8080`                          | Porta do Trino                                         |
| `SPARK_CONNECT_URL`       | `sc://spark-connect:15002`      | URL do Spark Connect Server                            |
| `CSV_DIR`                 | `./data`                        | CSVs para o producer (roda no host ou container `dev`) |
| `DATA_DIR`                | `/opt/spark/work-dir/data`      | CSVs para ingestão batch (roda dentro do `spark-connect`) |

Para rodar scripts **fora do Docker** (direto no host), sobrescreva os hosts internos:

```bash
KAFKA_BROKERS=localhost:9092
KAFKA_SCHEMA_REGISTRY=http://localhost:8081
SPARK_CONNECT_URL=sc://localhost:15002
```

---

## Imagens Docker

Três serviços usam build local em vez de imagens do Docker Hub:

### `minio-server/Dockerfile`
```
FROM alpine:3.20
RUN apk add --no-cache minio
```
Motivo: imagens `minio/minio` e `quay.io/minio/minio` requerem autenticação no Docker Hub. O pacote `minio` do repositório Alpine community é público e não requer login.

### `minio-client/Dockerfile`
```
FROM alpine:3.20
RUN apk add --no-cache minio-client
```
Motivo: mesmo problema com `minio/mc` e `bitnami/minio-client`. O binário instalado pelo Alpine se chama **`mcli`** (não `mc`) — atenção ao escrever entrypoints.

### `hive-metastore/Dockerfile`
```
FROM apache/hive:3.1.3
```
A imagem base usa Debian Bullseye, que chegou ao EOL. Os repositórios de segurança retornam 404. O Dockerfile redireciona para o arquivo:
```dockerfile
RUN sed -i 's|http://deb.debian.org/debian-security|http://archive.debian.org/debian-security|g' \
        /etc/apt/sources.list \
    && sed -i '/bullseye-updates/d' /etc/apt/sources.list \
    && apt-get update -o Acquire::Check-Valid-Until=false \
    && apt-get install -y --no-install-recommends netcat-openbsd wget
```

---

## Problemas comuns

### `hive-metastore` falha com `Bucket warehouse does not exist`

O Hive iniciou antes do `minio-client` criar os buckets. Solução já aplicada no `docker-compose.yml`:
```yaml
hive-metastore:
  depends_on:
    minio-client:
      condition: service_completed_successfully
```
Se ocorrer novamente, verifique se o `minio-client` terminou com sucesso:
```bash
docker compose logs minio-client
```

---

### `hive-metastore` falha com `Version information not found in metastore`

O PostgreSQL tem um schema do Hive corrompido ou incompleto de uma execução anterior. A solução é remover o volume e recriar:
```bash
docker compose down -v
docker compose up -d
```
O `SKIP_SCHEMA_INIT: "false"` (padrão) garante que o Hive sempre inicializa o schema corretamente em um banco limpo.

---

### MinIO falha ao iniciar com `xl meta version 3 is not supported`

Os dados em `minio_data/` foram gerados por uma versão mais recente do MinIO e são incompatíveis com a versão Alpine. Remova os dados e reinicie:
```bash
docker compose down
rm -rf minio_data/
docker compose up -d
```

---

### Build falha com `README.md: not found`

O `pyproject.toml` declara `readme = "README.md"`, então o hatchling exige o arquivo no contexto de build. O `.devcontainer/Dockerfile` já inclui:
```dockerfile
COPY pyproject.toml uv.lock* README.md ./
```
Se o erro reaparecer após renomear ou mover o README, atualize essa linha.

---

### Reconstruir apenas um serviço

```bash
docker compose build hive-metastore
docker compose up -d hive-metastore
```

---

### `schema-registry` falha com `No supported Kafka endpoints are configured`

A variável `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` estava configurada com o prefixo do listener (`INTERNAL://kafka:9093`). O Schema Registry 8.x espera apenas `host:porta` — o protocolo é resolvido via `SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL`. Solução já aplicada:
```yaml
SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9093
SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: PLAINTEXT
```

---

### `make stream` falha com `Failed to find the data source: iceberg`

O JAR do Iceberg não estava nos `--packages` do `spark-connect`. Solução já aplicada no `docker-compose.yml`:
```
org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0
```
E no `spark-connect/conf/spark-defaults.conf`:
```properties
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

> A versão do JAR deve existir no Maven Central. Para verificar versões disponíveis:
> `curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/maven-metadata.xml`

---

### Tabelas Bronze aparecem em `hive.*` em vez de `iceberg.*` no Trino

O Hive Metastore armazena metadados de qualquer tabela independente do formato. Com dois conectores (`hive` e `iceberg`) apontando para o mesmo metastore, ambos enxergavam as mesmas tabelas — mas o conector `hive` não consegue ler tabelas Iceberg e retorna `Cannot query Iceberg table`.

Solução já aplicada: `trino/catalog/hive.properties` foi removido. O Trino agora expõe apenas o catálogo `iceberg`. Todas as tabelas devem ser acessadas como:
```sql
SELECT * FROM iceberg.raw_olist_staging.bronze_orders;
SELECT * FROM iceberg.raw_olist_marts.fct_orders;
```

---

### `make stream` falha com `kafka_key is out of order`

O Iceberg valida a ordem das colunas ao escrever. O stream estava produzindo as colunas de metadados Kafka (`kafka_key`, `kafka_offset`, `kafka_timestamp`) antes das colunas de dados, mas a tabela foi criada com os dados primeiro. Solução já aplicada em `stream_kafka_to_iceberg.py`: o `select` do stream agora segue a mesma ordem do `CREATE TABLE` — colunas de dados primeiro, metadados depois.

Se as tabelas Bronze já existirem com o schema antigo (ordem errada), é necessário dropá-las antes de reiniciar o stream:
```bash
docker compose exec trino trino --execute "
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_orders;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_customers;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_items;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_payments;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_reviews;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_products;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_sellers;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_geolocation;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_category_translation;
"
```

---

### `make topics` falha com `No module named 'confluent_kafka'`

O volume `.:/workspace` sobrescreve o `/workspace` inteiro no runtime, incluindo o `.venv` criado durante o build da imagem. Solução já aplicada no `.devcontainer/Dockerfile`:
```dockerfile
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
```
O venv fica em `/opt/venv` (fora do workspace) e não é apagado pelo volume. O `PATH` inclui `/opt/venv/bin` para que todos os comandos do container usem o venv correto.

---

### Tabelas Bronze com todas as colunas `null` após `make produce`

O produtor serializa as mensagens em **Avro** com wire format Confluent:
```
[0x00 magic byte | schema_id uint32 BE (4 bytes) | avro binary payload]
```
O stream estava usando `CAST(value AS STRING)` + `from_json`, que espera JSON puro. Ao receber bytes Avro binários, o `from_json` retorna `null` em todos os campos.

Solução já aplicada em `stream_kafka_to_iceberg.py`:
- `from_json` substituído por `from_avro` (módulo `pyspark.sql.avro.functions`)
- Os 5 bytes do prefixo Confluent são descartados antes da deserialização: `substring(value, 6)`
- O schema Avro de cada tópico é buscado do Schema Registry via HTTP antes de iniciar o stream

O JAR `spark-avro` também precisa estar nos `--packages` do `spark-connect` (já aplicado no `docker-compose.yml`):
```
org.apache.spark:spark-avro_2.13:4.0.1
```

Se as tabelas Bronze já existirem com dados nulos, é necessário dropá-las e limpar os checkpoints antes de reiniciar o stream:
```bash
# 1. Dropar tabelas Bronze
docker compose exec trino trino --execute "
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_orders;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_customers;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_items;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_payments;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_order_reviews;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_products;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_sellers;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_geolocation;
  DROP TABLE IF EXISTS iceberg.raw_olist_staging.bronze_category_translation;
"

# 2. Limpar checkpoints
docker run --rm --network spark_dbt_kafka_spark_network \
  spark_dbt_kafka-minio-client:latest \
  "mcli alias set m http://minio:9000 minioadmin minioadmin && \
   mcli rm --recursive --force m/spark-demo/stream-1/"
```

---

### `make stream` falha com `AVRO_NOT_LOADED_SQL_FUNCTIONS_UNUSABLE`

O JAR `spark-avro` não estava nos `--packages` do `spark-connect`. O `from_avro` existe no PySpark como função Python, mas a implementação roda na JVM e exige o JAR no classpath do servidor. Solução já aplicada no `docker-compose.yml`:
```
org.apache.spark:spark-avro_2.13:4.0.1
```
Após adicionar, é necessário recriar o container:
```bash
docker compose up -d --build spark-connect
```

---

### Warning `too_many_pings` / `GOAWAY ENHANCE_YOUR_CALM` no `make stream`

Mensagem cosmética do gRPC — não afeta os streams. O cliente PySpark faz keepalive a cada 60s via HTTP/2 ping frames; o servidor Spark Connect throttlea e responde com `GOAWAY`, o cliente recua e reconecta automaticamente. Os 9 streams continuam processando normalmente.

---

### `ICEBERG_COMMIT_ERROR: types incompatible` no `make dbt-run`

Ocorre quando o schema de uma tabela Gold mudou (ex: tipo de coluna alterado) e o dbt tenta fazer `sync_all_columns` via ALTER — o Iceberg não permite mudança de tipo de coluna in-place.

Solução:
```bash
make reset-marts
make dbt-run
```

> Para resetar também as tabelas Bronze e checkpoints do stream (ex: mudança de schema no Bronze), use `make reset-bronze`.

---

### `ICEBERG_MISSING_METADATA` no `make dbt-run`

```
TrinoExternalError(type=EXTERNAL, name=ICEBERG_MISSING_METADATA,
  message="Metadata not found in metadata location for table raw_olist_staging_raw_olist_marts.dim_sellers")
```

Ocorre quando o Hive Metastore tem entradas para as tabelas Gold mas os arquivos de metadata Iceberg no MinIO foram removidos (ex: `reset-marts` anterior limpou o MinIO mas o DROP via Trino falhou silenciosamente porque a metadata já estava corrompida). O Trino não consegue executar `DROP TABLE` em tabelas com metadata ausente — o comando retorna erro em vez de remover a entrada do metastore.

Solução: o `make reset-marts` contorna isso removendo as entradas diretamente do PostgreSQL (backend do Hive Metastore), respeitando todas as foreign keys, e depois limpa o MinIO:

```bash
make reset-marts
make dbt-run
```

Se precisar inspecionar manualmente quais tabelas estão no metastore:
```bash
docker compose exec postgres psql -U hive -d metastore -c \
  "SELECT t.\"TBL_NAME\", s.\"LOCATION\" FROM \"TBLS\" t JOIN \"SDS\" s ON t.\"SD_ID\" = s.\"SD_ID\" JOIN \"DBS\" d ON t.\"DB_ID\" = d.\"DB_ID\" WHERE d.\"NAME\" = 'raw_olist_staging_raw_olist_marts';"
```

Tabelas `__dbt_tmp` órfãs (criadas por runs interrompidos) também são removidas pelo `reset-marts`, pois o script limpa todas as entradas do schema `raw_olist_staging_raw_olist_marts` de uma vez.

> Para resetar também as tabelas Bronze e checkpoints do stream (ex: mudança de schema no Bronze), use `make reset-bronze`.

---

### `make dbt-run` falha com `Connection refused` em `localhost:8080`

O `profiles.yml` tinha `host: localhost` fixo. O dbt roda dentro do container `dev`, onde o Trino é acessível via hostname `trino` (rede Docker interna). Solução já aplicada em `dbt_olist/profiles.yml`:
```yaml
host: "{{ env_var('TRINO_HOST', 'trino') }}"
port: "{{ env_var('TRINO_PORT', '8080') | int }}"
```
As variáveis `TRINO_HOST` e `TRINO_PORT` já estão definidas no serviço `dev` do `docker-compose.yml`. Para rodar o dbt fora do Docker, sobrescreva:
```bash
TRINO_HOST=localhost dbt run --profiles-dir .
```

---

### `make topics` falha com `No module named 'src'`

O `/workspace` não estava no `sys.path` do Python dentro do container. Solução já aplicada no `docker-compose.yml`:
```yaml
dev:
  environment:
    - PYTHONPATH=/workspace
```

---

### `make <qualquer>` falha com `docker-compose: command not found`

O Makefile usava `docker-compose` (CLI v1, removida em sistemas modernos). Solução já aplicada — todos os targets usam `docker compose` (v2).

---

### Ver logs em tempo real

```bash
# Todos os serviços
docker compose logs -f

# Serviço específico
docker compose logs -f hive-metastore
docker compose logs -f spark-connect
```

---

## Conectando o DataGrip ao Trino

**1. Adicionar datasource**

`File → New → Data Source → Trino`

Se não aparecer na lista: `File → New → Data Source → Other → Trino`.

**2. Configurações da conexão**

| Campo    | Valor                          |
|----------|--------------------------------|
| Host     | `localhost`                    |
| Port     | `8080`                         |
| User     | `admin`                        |
| Database | `iceberg`                      |

> O Trino não autentica por padrão — qualquer string funciona no campo User.

**3. Driver**

Na primeira conexão o DataGrip exibe **Download missing driver files** — clica para baixar o driver JDBC oficial automaticamente.

**4. URL JDBC** (alternativa manual)

```
jdbc:trino://localhost:8080/iceberg
```

Para apontar direto para um schema Gold:
```
jdbc:trino://localhost:8080/iceberg/raw_olist_marts
```

**5. Schemas disponíveis**

Após conectar, no painel lateral marque:
- `iceberg` → `raw_olist_staging` — tabelas Bronze e views Silver
- `iceberg` → `raw_olist_marts` — tabelas Gold (fatos e dimensões)

> Se der erro de SSL, adicione `?SSL=false` na URL: `jdbc:trino://localhost:8080/iceberg?SSL=false`
