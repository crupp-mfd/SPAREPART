from __future__ import annotations

from fastapi import FastAPI

from python.web_server import app as legacy_app
from sparepart_shared.asgi import PathFilteredASGI

app = FastAPI(title="MFDApps AppSQL-API")


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok", "service": "AppSQL-API"}


app.mount("/", PathFilteredASGI(legacy_app, service="sql_api"))
