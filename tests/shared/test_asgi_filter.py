from fastapi import FastAPI
from fastapi.testclient import TestClient

from sparepart_shared.asgi import PathFilteredASGI


def _build_legacy() -> FastAPI:
    app = FastAPI()

    @app.get("/api/wagons/count")
    def wagons_count() -> dict:
        return {"count": 1}

    @app.get("/api/rsrd2/wagons")
    def rsrd2_wagons() -> dict:
        return {"items": []}

    @app.get("/api/goldenview/list")
    def gv_list() -> dict:
        return {"items": []}

    @app.get("/query")
    def sql_query() -> dict:
        return {"ok": True}

    @app.get("/styles.css")
    def styles() -> dict:
        return {"ok": True}

    return app


def test_objektstruktur_filter() -> None:
    proxy = FastAPI()
    proxy.mount("/", PathFilteredASGI(_build_legacy(), service="objektstruktur"))
    client = TestClient(proxy)

    assert client.get("/api/wagons/count").status_code == 200
    assert client.get("/api/rsrd2/wagons").status_code == 404


def test_rsrd_filter() -> None:
    proxy = FastAPI()
    proxy.mount("/", PathFilteredASGI(_build_legacy(), service="rsrd"))
    client = TestClient(proxy)

    assert client.get("/api/rsrd2/wagons").status_code == 200
    assert client.get("/api/wagons/count").status_code == 404


def test_goldenview_filter() -> None:
    proxy = FastAPI()
    proxy.mount("/", PathFilteredASGI(_build_legacy(), service="goldenview"))
    client = TestClient(proxy)

    assert client.get("/api/goldenview/list").status_code == 200
    assert client.get("/api/rsrd2/wagons").status_code == 404
    assert client.get("/styles.css").status_code == 200


def test_sql_api_filter() -> None:
    proxy = FastAPI()
    proxy.mount("/", PathFilteredASGI(_build_legacy(), service="sql_api"))
    client = TestClient(proxy)

    assert client.get("/query").status_code == 200
    assert client.get("/api/goldenview/list").status_code == 404


def test_static_assets_are_shared() -> None:
    proxy = FastAPI()
    proxy.mount("/", PathFilteredASGI(_build_legacy(), service="rsrd"))
    client = TestClient(proxy)

    assert client.get("/styles.css").status_code == 200
