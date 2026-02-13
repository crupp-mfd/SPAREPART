const envSwitch = document.getElementById("envSwitch");
const envToggle = document.getElementById("envToggle");
const reloadBtn = document.getElementById("reloadBtn");
const statusText = document.getElementById("wagensucheStatus");
const searchInput = document.getElementById("wagonSearch");
const suggestions = document.getElementById("suggestions");
const selectedInfo = document.getElementById("selectedInfo");
const positionInfo = document.getElementById("positionInfo");
const mapWrap = document.getElementById("mapWrap");
const wagonMapEl = document.getElementById("wagonMap");
const loaderOverlay = document.getElementById("loaderOverlay");
const progressBar = document.getElementById("progressBar");
const progressDetail = document.getElementById("progressDetail");
const mainMenuBtn = document.getElementById("mainMenuBtn");
const mainMenu = document.getElementById("mainMenu");

const resolveEnvValue = (value) => (value && value.toUpperCase() === "TEST" ? "TEST" : "LIVE");
let currentEnv = resolveEnvValue(window.localStorage.getItem("sparepart.env") || "LIVE");
const getEnvParam = () => currentEnv.toLowerCase();
const stripTrailingSlash = (value) => String(value || "").replace(/\/+$/, "");
const runtimeApiConfig = window.__SPAREPART_API_CONFIG__ || {};
const coreBaseUrl = stripTrailingSlash(runtimeApiConfig.CORE_API_BASE_URL || "");
const coreApi = (path) => {
  const targetPath = String(path || "");
  if (!targetPath.startsWith("/")) return targetPath;
  return coreBaseUrl ? `${coreBaseUrl}${targetPath}` : targetPath;
};
const withEnv = (url) =>
  `${coreApi(url)}${String(url).includes("?") ? "&" : "?"}env=${encodeURIComponent(getEnvParam())}`;

const setLoaderVisible = (visible, message) => {
  loaderOverlay.classList.toggle("hidden", !visible);
  if (message) {
    progressDetail.textContent = message;
  }
  progressBar.style.width = visible ? "70%" : "0%";
};

const setStatus = (message, kind = "") => {
  statusText.textContent = message;
  statusText.dataset.status = kind;
};

const updateEnvSwitch = () => {
  if (envSwitch) {
    envSwitch.dataset.env = getEnvParam();
  }
};

const setEnvironment = (envValue) => {
  const normalized = resolveEnvValue(envValue);
  if (normalized === currentEnv) return;
  currentEnv = normalized;
  window.localStorage.setItem("sparepart.env", currentEnv);
  updateEnvSwitch();
  reloadData();
};

const renderSuggestions = (items) => {
  suggestions.innerHTML = "";
  if (!items.length) {
    suggestions.classList.add("hidden");
    return;
  }
  items.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "suggestion-item";
    button.innerHTML = `
      <span class="suggestion-sern">${item.sern || "-"}</span>
      <span class="suggestion-model">${item.itds || "-"}</span>
      <span class="suggestion-series">${item.itno || "-"}</span>
    `;
    button.addEventListener("click", () => {
      searchInput.value = item.sern || "";
      selectedInfo.textContent = `Modell: ${item.itds || "-"} · Modellreihe: ${item.itno || "-"}`;
      suggestions.classList.add("hidden");
      fetchLatestPosition(item.sern || "");
    });
    suggestions.appendChild(button);
  });
  suggestions.classList.remove("hidden");
};

const formatTimestamp = (value) => {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("de-DE");
};

const updatePositionInfo = (data) => {
  if (!data) {
    positionInfo.textContent = "Letzte Position: keine Daten verfügbar.";
    mapWrap.classList.add("hidden");
    return;
  }
  const stamp = formatTimestamp(data.timestamp);
  const lat = data.latitude ?? "-";
  const lon = data.longitude ?? "-";
  const mileage = data.mileage ?? "-";
  positionInfo.textContent = `Letzte Position (${stamp}): Lat ${lat}, Lon ${lon}, km ${mileage}`;
  mapWrap.classList.remove("hidden");
};

let googleMap = null;
let googleMarker = null;
let mapsLoaded = false;

const loadGoogleMaps = async () => {
  if (mapsLoaded) return true;
  try {
    const resp = await fetch(coreApi("/api/wagensuche/maps_key"));
    if (!resp.ok) {
      const payload = await resp.json().catch(() => ({}));
      throw new Error(payload.detail || "Maps-Key fehlt.");
    }
    const data = await resp.json();
    const key = data.key;
    if (!key) throw new Error("Maps-Key fehlt.");
    await new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(key)}`;
      script.async = true;
      script.defer = true;
      script.onload = resolve;
      script.onerror = reject;
      document.head.appendChild(script);
    });
    mapsLoaded = true;
    return true;
  } catch (err) {
    positionInfo.textContent = err.message || "Google Maps konnte nicht geladen werden.";
    return false;
  }
};

const parseCoord = (value) => {
  if (value == null) return NaN;
  const text = String(value).trim().replace(",", ".");
  return Number(text);
};

const updateMap = async (data) => {
  if (!data || data.latitude == null || data.longitude == null) return;
  const ok = await loadGoogleMaps();
  if (!ok || !window.google || !window.google.maps) return;
  const lat = parseCoord(data.latitude);
  const lng = parseCoord(data.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    positionInfo.textContent = "Letzte Position: Koordinaten ungültig.";
    mapWrap.classList.add("hidden");
    return;
  }
  const center = { lat, lng };
  if (!googleMap) {
    googleMap = new window.google.maps.Map(wagonMapEl, {
      center,
      zoom: 5,
      mapTypeControl: false,
      streetViewControl: false,
      fullscreenControl: false,
    });
    googleMarker = new window.google.maps.Marker({ position: center, map: googleMap });
  } else {
    window.setTimeout(() => {
      if (window.google && window.google.maps) {
        window.google.maps.event.trigger(googleMap, "resize");
      }
      googleMap.setCenter(center);
    }, 50);
    googleMap.setCenter(center);
    if (googleMarker) googleMarker.setPosition(center);
  }
};

const fetchLatestPosition = async (sern) => {
  const value = (sern || "").trim();
  if (!value) return;
  positionInfo.textContent = "Letzte Position: lade ...";
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), 6000);
  try {
    const resp = await fetch(coreApi(`/api/wagensuche/position?sern=${encodeURIComponent(value)}`), {
      signal: controller.signal,
    });
    if (!resp.ok) {
      const payload = await resp.json().catch(() => ({}));
      throw new Error(payload.detail || "Positionsabfrage fehlgeschlagen.");
    }
    const data = await resp.json();
    if (!data.found) {
      updatePositionInfo(null);
      return;
    }
    updatePositionInfo(data.data);
    updateMap(data.data);
  } catch (err) {
    const message = err.name === "AbortError" ? "Positionsabfrage timeout." : err.message;
    positionInfo.textContent = message || "Positionsabfrage fehlgeschlagen.";
  } finally {
    window.clearTimeout(timeoutId);
  }
};

const fetchSuggestions = async (value) => {
  const trimmed = value.trim();
  if (trimmed.length < 2) {
    renderSuggestions([]);
    return;
  }
  try {
    const resp = await fetch(withEnv(`/api/wagensuche/suggest?q=${encodeURIComponent(trimmed)}`));
    if (!resp.ok) {
      const payload = await resp.json().catch(() => ({}));
      throw new Error(payload.detail || "Suggest fehlgeschlagen.");
    }
    const data = await resp.json();
    renderSuggestions(data.items || []);
  } catch (err) {
    renderSuggestions([]);
    setStatus(err.message || "Suggest fehlgeschlagen.", "error");
  }
};

const reloadData = async () => {
  setLoaderVisible(true, "Daten werden geladen ...");
  reloadBtn.disabled = true;
  setStatus("Lade Wagendaten ...");
  try {
    const resp = await fetch(withEnv("/api/wagensuche/reload"), { method: "POST" });
    if (!resp.ok) {
      const payload = await resp.json().catch(() => ({}));
      throw new Error(payload.detail || "Reload fehlgeschlagen.");
    }
    const data = await resp.json();
    setStatus(`Cache aktualisiert: ${data.count ?? 0} Wagennummern verfügbar.`);
  } catch (err) {
    setStatus(err.message || "Reload fehlgeschlagen.", "error");
  } finally {
    setLoaderVisible(false);
    reloadBtn.disabled = false;
  }
};

let searchTimeout = null;
if (searchInput) {
  searchInput.addEventListener("input", (event) => {
    const value = event.target.value || "";
    if (searchTimeout) window.clearTimeout(searchTimeout);
    searchTimeout = window.setTimeout(() => fetchSuggestions(value), 220);
  });
  searchInput.addEventListener("focus", () => {
    if (searchInput.value.trim().length >= 2) {
      fetchSuggestions(searchInput.value);
    }
  });
  searchInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      fetchLatestPosition(searchInput.value);
      suggestions.classList.add("hidden");
    }
  });
  searchInput.addEventListener("blur", () => {
    if (searchInput.value.trim().length >= 2) {
      fetchLatestPosition(searchInput.value);
    }
  });
}

if (reloadBtn) {
  reloadBtn.addEventListener("click", () => reloadData());
}

if (envToggle) {
  envToggle.addEventListener("click", () => {
    const next = currentEnv === "LIVE" ? "TEST" : "LIVE";
    setEnvironment(next);
  });
}

if (mainMenuBtn && mainMenu) {
  mainMenuBtn.addEventListener("click", () => {
    mainMenu.classList.toggle("hidden");
  });
  mainMenu.addEventListener("click", (event) => {
    const target = event.target.closest(".menu-item");
    if (!target) return;
    const page = target.dataset.page;
    if (page) window.location.href = page;
  });
  document.addEventListener("click", (event) => {
    if (event.target.closest(".menu-wrapper")) return;
    mainMenu.classList.add("hidden");
  });
}

updateEnvSwitch();
// Preload Google Maps script so it is ready when a wagon is selected.
loadGoogleMaps();
