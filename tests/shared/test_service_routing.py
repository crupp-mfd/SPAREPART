from sparepart_shared.service_routing import classify_path, should_serve_path


def test_classify_appmfd_paths() -> None:
    assert classify_path("/api/health") == "appmfd"
    assert classify_path("/index.html") == "appmfd"
    assert classify_path("/portal-config.js") == "appmfd"


def test_classify_rsrd_paths() -> None:
    assert classify_path("/api/rsrd2/wagons") == "rsrd"


def test_classify_domain_paths() -> None:
    assert classify_path("/objektstrukturtausch.html") == "objektstruktur"
    assert classify_path("/wagenumbau.html") == "bremsenumbau"
    assert classify_path("/teilenummer.html") == "teilenummer"
    assert classify_path("/rsrd2.html") == "rsrd"
    assert classify_path("/gpt-goldenview.html") == "goldenview"
    assert classify_path("/api/wagons/count") == "objektstruktur"
    assert classify_path("/api/renumber/update") == "bremsenumbau"
    assert classify_path("/api/teilenummer/check") == "teilenummer"
    assert classify_path("/api/wagensuche/suggest") == "wagensuche"
    assert classify_path("/api/goldenview/list") == "goldenview"
    assert classify_path("/query") == "sql_api"
    assert classify_path("/api/ask_m3_knowledge") == "goldenview"


def test_should_serve_path() -> None:
    assert should_serve_path("objektstruktur", "/api/wagons/count")
    assert should_serve_path("bremsenumbau", "/api/renumber/update")
    assert should_serve_path("teilenummer", "/api/teilenummer/check")
    assert should_serve_path("wagensuche", "/api/wagensuche/suggest")
    assert should_serve_path("rsrd", "/api/rsrd2/wagons")
    assert should_serve_path("goldenview", "/api/goldenview/list")
    assert should_serve_path("sql_api", "/query")
    assert not should_serve_path("objektstruktur", "/api/rsrd2/wagons")
    assert not should_serve_path("rsrd", "/api/goldenview/list")


def test_legacy_aliases_stay_compatible() -> None:
    assert should_serve_path("core", "/api/renumber/update")
    assert should_serve_path("rsrd2", "/api/rsrd2/wagons")


def test_shared_static_assets_allowed_in_all_apps() -> None:
    assert should_serve_path("objektstruktur", "/styles.css")
    assert should_serve_path("rsrd", "/styles.css")
    assert should_serve_path("goldenview", "/bilder/logo.png")
