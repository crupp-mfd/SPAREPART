from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from python.web_server import app as legacy_app
from sparepart_shared.asgi import PathFilteredASGI

APP_DIR = Path(__file__).resolve().parents[2]
PORTAL_DIR = APP_DIR / "frontend"

app = FastAPI(title="MFDApps AppMFD")


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok", "service": "AppMFD"}


@app.get("/", include_in_schema=False)
def index() -> Response:
    target = PORTAL_DIR / "index.html"
    if target.exists():
        return FileResponse(target)
    return JSONResponse({"detail": "Portal index not configured."}, status_code=404)


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    target = PORTAL_DIR / "favicon.ico"
    if target.exists():
        return FileResponse(target)
    return JSONResponse({"detail": "favicon not configured."}, status_code=404)


@app.get("/portal-config.js", include_in_schema=False)
def portal_config_js() -> Response:
    target = PORTAL_DIR / "portal-config.js"
    if target.exists():
        return FileResponse(target, media_type="application/javascript")
    return JSONResponse({"detail": "portal-config not configured."}, status_code=404)


if PORTAL_DIR.exists():
    app.mount("/portal", StaticFiles(directory=PORTAL_DIR, html=False), name="portal")

app.mount("/", PathFilteredASGI(legacy_app, service="appmfd"))
