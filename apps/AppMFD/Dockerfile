FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends     default-jre-headless  && rm -rf /var/lib/apt/lists/*

COPY python/requirements.txt /app/python/requirements.txt
RUN pip install --no-cache-dir -r /app/python/requirements.txt

COPY packages/sparepart-shared /app/packages/sparepart-shared
RUN pip install --no-cache-dir -e /app/packages/sparepart-shared

COPY python /app/python
COPY apps/AppMFD/frontend /app/frontend
COPY apps/AppMFD /app/apps/AppMFD

RUN mkdir -p /runtime

ENV MFDAPPS_HOME="/app"
ENV MFDAPPS_RUNTIME_ROOT="/runtime"
ENV MFDAPPS_CREDENTIALS_DIR="/app/apps/AppMFD/credentials"
ENV MFDAPPS_FRONTEND_DIR="/app/frontend"
ENV MFDAPPS_SERVICE="appmfd"
ENV PYTHONPATH="/app:/app/packages/sparepart-shared/src:/app/apps/AppMFD/src"

EXPOSE 8000
CMD ["uvicorn", "app_mfd.main:app", "--host", "0.0.0.0", "--port", "8000"]
