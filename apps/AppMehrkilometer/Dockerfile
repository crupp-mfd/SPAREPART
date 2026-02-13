FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends     default-jre-headless  && rm -rf /var/lib/apt/lists/*

COPY python/requirements.txt /app/python/requirements.txt
RUN pip install --no-cache-dir -r /app/python/requirements.txt

COPY packages/sparepart-shared /app/packages/sparepart-shared
RUN pip install --no-cache-dir -e /app/packages/sparepart-shared

COPY python /app/python
COPY apps/AppMehrkilometer/frontend /app/frontend
COPY apps/AppMehrkilometer /app/apps/AppMehrkilometer

RUN mkdir -p /runtime

ENV MFDAPPS_HOME="/app"
ENV MFDAPPS_RUNTIME_ROOT="/runtime"
ENV MFDAPPS_CREDENTIALS_DIR="/app/apps/AppMFD/credentials"
ENV MFDAPPS_FRONTEND_DIR="/app/frontend"
ENV MFDAPPS_SERVICE="mehrkilometer"
ENV PYTHONPATH="/app:/app/packages/sparepart-shared/src:/app/apps/AppMehrkilometer/src"

EXPOSE 8000
CMD ["uvicorn", "app_mehrkilometer.main:app", "--host", "0.0.0.0", "--port", "8000"]
