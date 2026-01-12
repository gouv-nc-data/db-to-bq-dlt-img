FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

WORKDIR /app

ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_CACHE=1

# Installation des dépendances système (libpq5 pour Postgres, libaio pour Oracle IC)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libaio1t64 \
    ca-certificates \
    wget \
    unzip \
    && ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1 \
    && rm -rf /var/lib/apt/lists/*

# Installation de l'Oracle Instant Client (Basic Light)
# On utilise la version 19.25 qui est compatible avec Oracle 11.2
WORKDIR /opt/oracle
RUN wget https://download.oracle.com/otn_software/linux/instantclient/1925000/instantclient-basiclite-linux.x64-19.25.0.0.0dbru.zip \
    && unzip instantclient-basiclite-linux.x64-19.25.0.0.0dbru.zip \
    && rm instantclient-basiclite-linux.x64-19.25.0.0.0dbru.zip \
    && echo /opt/oracle/instantclient_19_25 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

WORKDIR /app

# Copie des fichiers de dépendances
COPY pyproject.toml ./

# Installation des dépendances (sans lock car nouveau projet, uv va le générer au build)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-install-project

# Copie du code
COPY main.py .

# Sync final
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync

# Utilisateur non-root
USER 1000

ENV PATH="/app/.venv/bin:$PATH"
ENV LD_LIBRARY_PATH="/opt/oracle/instantclient_19_25"

ENTRYPOINT []

CMD ["python", "main.py"]
