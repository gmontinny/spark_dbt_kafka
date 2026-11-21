FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/root/.local/bin:${PATH}" \
    # Venv fora do /workspace para não ser sobrescrito pelo volume do docker-compose
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Ferramentas de sistema mínimas necessárias para o pipeline
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        make \
        bash \
        git \
        ca-certificates \
        procps \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Instala o uv (gerenciador de pacotes/ambientes Python)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /workspace

# Copia apenas os arquivos de manifesto para aproveitar o cache de camadas.
# O README.md é necessário pelo hatchling (declarado em pyproject.toml).
# O código-fonte é montado via volume no docker-compose, não copiado aqui.
COPY pyproject.toml uv.lock* README.md ./

# Cria o virtualenv e instala todas as dependências (prod + dev) declaradas
# no pyproject.toml, incluindo o grupo "dev" (ruff, black, pytest, pre-commit).
# O cache do uv é montado para acelerar rebuilds sem persistir no layer.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-groups

# Garante que o venv esteja no PATH para todos os comandos do container
ENV PATH="/opt/venv/bin:${PATH}"
