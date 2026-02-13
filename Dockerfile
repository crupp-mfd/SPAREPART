FROM python:3.11-slim

WORKDIR /app

# Java für jaydebeapi / JPype
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
 && rm -rf /var/lib/apt/lists/*

# Python deps
COPY python/requirements.txt /app/python/requirements.txt
RUN pip install --no-cache-dir -r /app/python/requirements.txt
COPY packages/sparepart-shared /app/packages/sparepart-shared
RUN pip install --no-cache-dir -e /app/packages/sparepart-shared

# App code
COPY . /app
RUN mkdir -p /app/data && touch /app/data/cache.db

EXPOSE 8000
CMD ["uvicorn", "python.web_server:app", "--host", "0.0.0.0", "--port", "8000"]
