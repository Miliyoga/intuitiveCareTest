FROM python:3.10-slim

WORKDIR /app

# Dependências do sistema:
# - curl: debug
# - postgresql-client: psql para rodar schema/import
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
  && rm -rf /var/lib/apt/lists/*

# Dependências Python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Código
COPY . /app

# Scripts de container
RUN chmod +x /app/docker/*.sh

EXPOSE 8000

# default (API)
CMD ["/app/docker/api_start.sh"]
