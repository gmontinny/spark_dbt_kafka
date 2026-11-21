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

### Ver logs em tempo real

```bash
# Todos os serviços
docker compose logs -f

# Serviço específico
docker compose logs -f hive-metastore
docker compose logs -f spark-connect
```
