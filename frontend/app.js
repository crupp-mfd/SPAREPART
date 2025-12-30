const statusText = document.getElementById("statusText");
const loaderSubtitle = document.getElementById("loaderSubtitle");
const progressBar = document.getElementById("progressBar");
const progressDetail = document.getElementById("progressDetail");
const loaderPanel = document.getElementById("loaderPanel");
const loaderOverlay = document.getElementById("loaderOverlay");
const mainMenuBtn = document.getElementById("mainMenuBtn");
const mainMenu = document.getElementById("mainMenu");
const envSwitch = document.getElementById("envSwitch");
const envToggle = document.getElementById("envToggle");
const moduleTitle = document.getElementById("moduleTitle");
const moduleSubtitle = document.getElementById("moduleSubtitle");
const sparepartModule = document.getElementById("sparepartModule");
const rsrd2Module = document.getElementById("rsrd2Module");
const menuItems = document.querySelectorAll(".menu-item[data-module]");
const headerSearch = document.getElementById("headerSearch");
const rsrd2LoadErpBtn = document.getElementById("rsrd2LoadErpBtn");
const rsrd2LoadErpFullBtn = document.getElementById("rsrd2LoadErpFullBtn");
const rsrd2FetchJsonBtn = document.getElementById("rsrd2FetchJsonBtn");
const rsrd2ProcessJsonBtn = document.getElementById("rsrd2ProcessJsonBtn");
const rsrd2Status = document.getElementById("rsrd2Status");
const rsrd2Log = document.getElementById("rsrd2Log");
const tablePanel = document.getElementById("tablePanel");
const tableHead = document.getElementById("tableHead");
const tableBody = document.getElementById("tableBody");
const countInfo = document.getElementById("countInfo");
const paginationInfo = document.getElementById("paginationInfo");
const prevBtn = document.getElementById("prevPage");
const nextBtn = document.getElementById("nextPage");
const reloadBtn = document.getElementById("reloadBtn");
const pageSizeSelect = document.getElementById("pageSizeSelect");
const wagonsView = document.getElementById("wagonsView");
const objstrkView = document.getElementById("objstrkView");
const swapView = document.getElementById("swapView");
const objstrkHead = document.getElementById("objstrkHead");
const objstrkBody = document.getElementById("objstrkBody");
const objstrkMeta = document.getElementById("objstrkMeta");
const backToWagonsBtn = document.getElementById("backToWagonsBtn");
const collapseAllBtn = document.getElementById("collapseAllBtn");
const expandAllBtn = document.getElementById("expandAllBtn");
const openSwapFromWagonsBtn = document.getElementById("openSwapDbFromWagons");
const openSwapFromObjBtn = document.getElementById("openSwapDbFromObj");
const swapTableBody = document.getElementById("swapTableBody");
const swapCountInfo = document.getElementById("swapCountInfo");
const swapBackBtn = document.getElementById("swapBackBtn");
const swapExecuteBtn = document.getElementById("swapExecuteBtn");
const swapSelectAllBtn = document.getElementById("swapSelectAllBtn");
const swapClearSelectionBtn = document.getElementById("swapClearSelectionBtn");
const swapPrevPageBtn = document.getElementById("swapPrevPage");
const swapNextPageBtn = document.getElementById("swapNextPage");
const swapPaginationInfo = document.getElementById("swapPaginationInfo");
const partsModal = document.getElementById("partsModal");
const partsModalContent = partsModal ? partsModal.querySelector(".modal-inner") : null;
const partsModalHeader = partsModal ? partsModal.querySelector(".modal-header") : null;
const partsResizeHandle = document.getElementById("partsResizeHandle");
const closePartsModalBtn = document.getElementById("closePartsModalBtn");
const partsModalSubtitle = document.getElementById("partsModalSubtitle");
const partsSearchForm = document.getElementById("partsSearchForm");
const partsFilterType = document.getElementById("partsFilterType");
const partsFilterItem = document.getElementById("partsFilterItem");
const partsFilterSerial = document.getElementById("partsFilterSerial");
const partsFilterFacility = document.getElementById("partsFilterFacility");
const partsFilterBin = document.getElementById("partsFilterBin");
const partsTypeOptions = document.getElementById("partsTypeOptions");
const partsItemOptions = document.getElementById("partsItemOptions");
const partsSerialOptions = document.getElementById("partsSerialOptions");
const partsFacilityOptions = document.getElementById("partsFacilityOptions");
const partsBinOptions = document.getElementById("partsBinOptions");
const partsClearFiltersBtn = document.getElementById("partsClearFiltersBtn");
const partsPrevPageBtn = document.getElementById("partsPrevPage");
const partsNextPageBtn = document.getElementById("partsNextPage");
const partsPaginationInfo = document.getElementById("partsPaginationInfo");
const partsResultsBody = document.getElementById("partsResultsBody");
const serialFilterInput = document.getElementById("serialFilter");
const itemFilterInput = document.getElementById("itemFilter");
const customerFilterInput = document.getElementById("customerFilter");
const customerNameFilterInput = document.getElementById("customerNameFilter");
const typeFilterInput = document.getElementById("typeFilter");
const fulltextInput = document.getElementById("fulltextSearch");
const facilityFilterInput = document.getElementById("facilityFilter");
const binFilterInput = document.getElementById("binFilter");
const clearFiltersBtn = document.getElementById("clearFiltersBtn");
const customerNumbersList = document.getElementById("customerNumbers");
const customerNamesList = document.getElementById("customerNames");
const itemNumbersList = document.getElementById("itemNumbers");
const typeOptionsList = document.getElementById("typeOptions");
const serialOptionsList = document.getElementById("serialOptions");
const facilityOptionsList = document.getElementById("facilityOptions");
const binOptionsList = document.getElementById("binOptions");

const DEFAULT_PAGE_SIZE = 10;
const CHUNK_SIZE = 150;
const MIN_OVERLAY_MS = 900;
const PARTS_PAGE_SIZE = 10;
const SWAP_PAGE_SIZE = 10;
const RSRD2_LOG_LIMIT = 80;
const RSRD2_JOB_POLL_MS = 1500;
const rsrd2JobOffsets = {};
const COLUMN_ORDER = [
  "SERIENNUMMER",
  "BAUREIHE",
  "WAGEN-TYP",
  "KUNDEN-NUMMER",
  "KUNDEN-NAME",
  "LAGERORT",
  "LAGERPLATZ",
  "OBJSTRK",
];

const COLUMN_LABELS = {
  "SERIENNUMMER": "Seriennummer",
  "BAUREIHE": "Modellreihe",
  "WAGEN-TYP": "Modell-Typ",
  "KUNDEN-NUMMER": "Kd.Nr.",
  "KUNDEN-NAME": "Kundenname",
  "LAGERORT": "StOrt",
  "LAGERPLATZ": "LgOrt",
  "OBJSTRK": "ObjStrk",
};

const OBJSTRK_COLUMNS = ["MFGL", "TX40", "ITDS", "ITNO", "SER2", "MVA1", "ERSATZ_ITNO", "ERSATZ_SERN", "PARTS"];
const OBJSTRK_SEARCH_COLUMNS = ["TX40", "ITDS"];
const OBJSTRK_FIELD_SOURCES = {
  MFGL: ["CFGL", "MFGL"],
};
const OBJSTRK_LABELS = {
  MFGL: "Position",
  TX40: "BaugruppeID",
  ITDS: "Baugruppe",
  ITNO: "TeileNr",
  SER2: "Seriennummer",
  MVA1: "Kilometer",
  ERSATZ_ITNO: "Neu-ArtNr",
  ERSATZ_SERN: "Neu-SerNr",
  PARTS: "Tausch",
};

const FILTER_FIELDS = [
  { key: "KUNDEN-NUMMER", input: customerFilterInput, list: customerNumbersList },
  { key: "KUNDEN-NAME", input: customerNameFilterInput, list: customerNamesList },
  { key: "BAUREIHE", input: itemFilterInput, list: itemNumbersList },
  { key: "WAGEN-TYP", input: typeFilterInput, list: typeOptionsList },
  { key: "SERIENNUMMER", input: serialFilterInput, list: serialOptionsList },
  { key: "LAGERORT", input: facilityFilterInput, list: facilityOptionsList },
  { key: "LAGERPLATZ", input: binFilterInput, list: binOptionsList },
];

const resolveEnvValue = (value) => (value && value.toUpperCase() === "TEST" ? "TEST" : "LIVE");
let currentEnv = resolveEnvValue(window.localStorage.getItem("sparepart.env") || "LIVE");
const getEnvParam = () => currentEnv.toLowerCase();
const withEnv = (url) => `${url}${url.includes("?") ? "&" : "?"}env=${encodeURIComponent(getEnvParam())}`;
const MODULE_META = {
  sparepart: {
    title: "MFD Automation",
    subtitle: "SPAREPART · Objektstrukturtausch / Wagenumbau",
    showSearch: true,
    loaderSubtitle: "Objektstrukturtausch",
  },
  rsrd2: {
    title: "MFD Rail Automatisation",
    subtitle: "RSRD2 Sync",
    showSearch: false,
    loaderSubtitle: "RSRD2 Sync",
  },
};
const escapeHtml = (value) =>
  String(value).replace(/[&<>"']/g, (ch) =>
    ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[ch] || ch),
  );

let allWagons = [];
let wagons = [];
let currentPage = 1;
let totalRows = 0;
let pageSize = DEFAULT_PAGE_SIZE;
let objStrkRows = [];
let objStrkRoots = [];
let objStrkNodesById = new Map();
const objStrkExpanded = new Set();
let objStrkSearchText = "";
let swapRows = [];
let swapPage = 1;
let swapSelected = new Set();
let currentModule = "sparepart";
let currentView = "wagons";
let rsrd2Loaded = false;
let currentPartsEqtp = "";
let currentPartsContext = "";
let sparePartsRows = [];
let sparePartsPage = 1;
let sparePartsOptions = { item: [], serial: [], facility: [], bin: [] };
let partsDragState = null;
let partsResizeState = null;
let partsFilterTimer = null;
let currentWagonItem = "";
let currentWagonSerial = "";
let currentPartsNodeId = null;
let currentPartsNode = null;
let currentOriginalItno = "";
let currentOriginalSern = "";
let sparePartUser = window.localStorage.getItem("sparepart.user") || "";

let overlayShownAt = 0;

const showOverlay = () => {
  overlayShownAt = Date.now();
  loaderOverlay.classList.remove("hidden");
};

const hideOverlay = () => {
  const elapsed = Date.now() - overlayShownAt;
  const remaining = Math.max(0, MIN_OVERLAY_MS - elapsed);
  window.setTimeout(() => loaderOverlay.classList.add("hidden"), remaining);
};

const setIndeterminate = (enabled) => {
  progressBar.classList.toggle("indeterminate", enabled);
  if (enabled) {
    progressDetail.textContent = "";
  }
};

const setStatus = (text) => {
  statusText.textContent = text;
};

const updateProgress = (value, total) => {
  setIndeterminate(false);
  const safeTotal = Math.max(total, 1);
  const percent = Math.min(100, Math.round((value / safeTotal) * 100));
  progressBar.style.width = `${percent}%`;
  progressDetail.textContent = `${value} / ${total}`;
};

const fetchJSON = async (url) => {
  const resp = await fetch(url);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || resp.statusText);
  }
  return resp.json();
};

const ensureSparePartUser = () => {
  if (sparePartUser) return sparePartUser;
  const input = window.prompt("Bitte Benutzerkennung für Teiletausch eingeben:", "");
  sparePartUser = (input || "UNBEKANNT").trim() || "UNBEKANNT";
  window.localStorage.setItem("sparepart.user", sparePartUser);
  return sparePartUser;
};

const showWagonsView = () => {
  if (wagonsView) wagonsView.classList.remove("hidden");
  if (objstrkView) objstrkView.classList.add("hidden");
  if (swapView) swapView.classList.add("hidden");
  currentView = "wagons";
};

const showObjStrkView = () => {
  if (wagonsView) wagonsView.classList.add("hidden");
  if (objstrkView) objstrkView.classList.remove("hidden");
  if (swapView) swapView.classList.add("hidden");
  currentView = "obj";
};

const showSwapDbView = () => {
  if (wagonsView) wagonsView.classList.add("hidden");
  if (objstrkView) objstrkView.classList.add("hidden");
  if (swapView) swapView.classList.remove("hidden");
  currentView = "swap";
};

const applyModuleVisibility = () => {
  if (sparepartModule) {
    sparepartModule.classList.toggle("hidden", currentModule !== "sparepart");
  }
  if (rsrd2Module) {
    rsrd2Module.classList.toggle("hidden", currentModule !== "rsrd2");
  }
};

const setModule = (nextModule, force = false) => {
  const resolved = nextModule === "rsrd2" ? "rsrd2" : "sparepart";
  if (!force && resolved === currentModule) return;
  currentModule = resolved;
  const meta = MODULE_META[resolved] || MODULE_META.sparepart;
  if (moduleTitle) {
    moduleTitle.textContent = meta.title;
  }
  if (moduleSubtitle) {
    moduleSubtitle.textContent = meta.subtitle;
  }
  if (loaderSubtitle && meta.loaderSubtitle) {
    loaderSubtitle.textContent = meta.loaderSubtitle;
  }
  if (headerSearch) {
    headerSearch.classList.toggle("hidden", !meta.showSearch);
  }
  applyModuleVisibility();
  if (resolved === "sparepart") {
    showWagonsView();
  } else if (resolved === "rsrd2" && !rsrd2Loaded) {
    loadRsrd2Wagons();
  }
  menuItems.forEach((btn) => {
    btn.classList.toggle("menu-item--active", btn.dataset.module === resolved);
  });
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
  swapSelected.clear();
  swapRows = [];
  swapPage = 1;
  currentPage = 1;
  updateEnvSwitch();
  startLoadingWorkflow();
};

const buildObjStrkHierarchy = (rows) => {
  const map = new Map();
  const nodes = rows.map((row, index) => {
    const id = (row.__id || row.SER2 || "").trim() || `node-${index}`;
    const parentId = (row.__parentId || "").trim() || null;
    const node = { id, parentId, data: row, children: [] };
    map.set(id, node);
    return node;
  });
  const roots = [];
  nodes.forEach((node) => {
    if (node.parentId && map.has(node.parentId)) {
      map.get(node.parentId).children.push(node);
    } else {
      roots.push(node);
    }
  });
  const sortNodes = (list) => {
    list.sort((a, b) => (a.data.MFGL || "").localeCompare(b.data.MFGL || ""));
    list.forEach((node) => sortNodes(node.children));
  };
  sortNodes(roots);
  return { roots, map };
};

const matchesObjStrkSearch = (node) => {
  if (!objStrkSearchText) return true;
  return OBJSTRK_SEARCH_COLUMNS.some((column) =>
    (node.data[column] || "").toLowerCase().includes(objStrkSearchText)
  );
};

const flattenExpandedObjNodes = (nodes, level = 0, acc = []) => {
  nodes.forEach((node) => {
    acc.push({ node, level });
    if (objStrkExpanded.has(node.id) && node.children.length) {
      flattenExpandedObjNodes(node.children, level + 1, acc);
    }
  });
  return acc;
};

const collectSearchRows = (node, level) => {
  let rows = [];
  node.children.forEach((child) => {
    rows = rows.concat(collectSearchRows(child, level + 1));
  });
  const matches = matchesObjStrkSearch(node);
  if (matches || rows.length) {
    return [{ node, level }, ...rows];
  }
  return [];
};

const getObjStrkDisplayRows = () => {
  if (!objStrkRoots.length) return [];
  if (!objStrkSearchText) {
    return flattenExpandedObjNodes(objStrkRoots);
  }
  let rows = [];
  objStrkRoots.forEach((root) => {
    rows = rows.concat(collectSearchRows(root, 0));
  });
  return rows;
};

const expandAllObjNodes = () => {
  objStrkNodesById.forEach((node) => {
    if (node.children.length) {
      objStrkExpanded.add(node.id);
    }
  });
};

const collapseAllObjNodes = () => {
  objStrkExpanded.clear();
};

const updateObjStrkSearch = () => {
  if (!fulltextInput) return;
  objStrkSearchText = fulltextInput.value.trim().toLowerCase();
  if (objStrkSearchText) {
    expandAllObjNodes();
  }
  if (objStrkRoots.length) {
    renderObjStrkTable();
  }
};

const loadWagons = async () => {
  setStatus("VERBINDE ...");
  setIndeterminate(true);
  const countPayload = await fetchJSON(withEnv("/api/wagons/count"));
  totalRows = countPayload.total;
  updateProgress(0, totalRows);

  setStatus("LADE DATEN VON M3");
  wagons = [];
  let fetched = 0;

  while (fetched < totalRows) {
    const url = withEnv(`/api/wagons/chunk?offset=${fetched}&limit=${CHUNK_SIZE}`);
    const chunk = await fetchJSON(url);
    wagons = wagons.concat(chunk.rows);
    fetched += chunk.returned;
    updateProgress(fetched, totalRows);
  }

  allWagons = wagons.slice();
  populateDatalists();

  if (totalRows === 0) {
    setStatus("Keine Datensätze gefunden.");
  } else {
    setStatus("Ladevorgang abgeschlossen.");
  }
};

const populateDatalists = () => {
  FILTER_FIELDS.forEach(({ key, list }) => {
    if (!list) return;
    const values = Array.from(
      new Set(
        allWagons
          .map((row) => (row[key] || "").trim())
          .filter((value) => value.length > 0)
      )
    ).sort((a, b) => a.localeCompare(b));
    list.innerHTML = values.map((value) => `<option value="${value}"></option>`).join("");
  });
};

const normalizeObjStrkRows = (payload) => {
  const records =
    payload?.response?.MIRecord ??
    payload?.response?.MIRecords ??
    payload?.response?.MIResponse ??
    [];
  const recordArray = Array.isArray(records)
    ? records
    : records
      ? [records]
      : [];
  return recordArray.map((record) => {
    const row = {};
    const nameValues = record?.NameValue || record?.nameValue || [];
    const pairArray = Array.isArray(nameValues) ? nameValues : [nameValues];
    pairArray.forEach((entry) => {
      if (!entry) return;
      const name = entry.Name || entry.name;
      if (!name) return;
      row[name] = entry.Value ?? entry.value ?? "";
    });
    return row;
  });
};


const renderTableHead = () => {
  tableHead.innerHTML = "";
  const headerRow = document.createElement("tr");
  COLUMN_ORDER.forEach((key) => {
    const th = document.createElement("th");
    th.textContent = COLUMN_LABELS[key] || key;
    if (key === "OBJSTRK") th.classList.add("objstrk-col");
    headerRow.appendChild(th);
  });
  tableHead.appendChild(headerRow);
};

const renderObjStrkHead = () => {
  if (!objstrkHead) return;
  objstrkHead.innerHTML = "";
  const row = document.createElement("tr");
  OBJSTRK_COLUMNS.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = OBJSTRK_LABELS[column] || column;
     if (column === "PARTS") th.classList.add("objstrk-col-action");
    row.appendChild(th);
  });
  objstrkHead.appendChild(row);
};

const applyFilters = () => {
  const serial = serialFilterInput.value.trim().toLowerCase();
  const item = itemFilterInput.value.trim().toLowerCase();
  const customer = customerFilterInput.value.trim().toLowerCase();
  const customerName = customerNameFilterInput.value.trim().toLowerCase();
  const type = typeFilterInput.value.trim().toLowerCase();
  const fulltext = fulltextInput.value.trim().toLowerCase();
  const facility = facilityFilterInput.value.trim().toLowerCase();
  const bin = binFilterInput.value.trim().toLowerCase();

  wagons = allWagons.filter((row) => {
    const serialMatch = !serial || (row["SERIENNUMMER"] || "").toLowerCase().includes(serial);
    const itemMatch = !item || (row["BAUREIHE"] || "").toLowerCase().includes(item);
    const customerMatch = !customer || (row["KUNDEN-NUMMER"] || "").toLowerCase().includes(customer);
    const customerNameMatch = !customerName || (row["KUNDEN-NAME"] || "").toLowerCase().includes(customerName);
    const typeMatch = !type || (row["WAGEN-TYP"] || "").toLowerCase().includes(type);
    const facilityMatch = !facility || (row["LAGERORT"] || "").toLowerCase().includes(facility);
    const binMatch = !bin || (row["LAGERPLATZ"] || "").toLowerCase().includes(bin);
    const fulltextMatch =
      !fulltext ||
      COLUMN_ORDER.some((key) => (row[key] || "").toLowerCase().includes(fulltext));
    return (
      serialMatch &&
      itemMatch &&
      customerMatch &&
      customerNameMatch &&
      typeMatch &&
      facilityMatch &&
      binMatch &&
      fulltextMatch
    );
  });

  currentPage = 1;
  countInfo.textContent = `${wagons.length} von ${allWagons.length} Datensätzen`;
  renderPage();
};


const renderPage = () => {
  tableBody.innerHTML = "";
  if (!wagons.length) {
    tableBody.innerHTML = `<tr><td colspan="${COLUMN_ORDER.length}">Keine Daten vorhanden.</td></tr>`;
    return;
  }

  const maxPage = Math.max(1, Math.ceil(wagons.length / pageSize));
  currentPage = Math.min(currentPage, maxPage);

  const start = (currentPage - 1) * pageSize;
  const rows = wagons.slice(start, start + pageSize);

  rows.forEach((wagon) => {
    const tr = document.createElement("tr");
    COLUMN_ORDER.forEach((key) => {
      const td = document.createElement("td");
      if (key === "OBJSTRK") {
        td.classList.add("objstrk-col");
        const button = document.createElement("button");
        button.type = "button";
        button.className = "objstrk-btn";
        button.title = "Objektstruktur";
        button.setAttribute("aria-label", "Objektstruktur");
        button.dataset.mtrl = wagon["BAUREIHE"] ?? "";
        button.dataset.sern = wagon["SERIENNUMMER"] ?? "";
        button.innerHTML = `
          <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false">
            <path d="M22.7 19.3l-6.1-6.1c.9-2.3.4-5-1.5-6.9-1.9-1.9-4.6-2.4-6.9-1.5l3 3-2.1 2.1-3-3c-.9 2.3-.4 5 1.5 6.9 1.9 1.9 4.6 2.4 6.9 1.5l6.1 6.1c.4.4 1 .4 1.4 0l.7-.7c.4-.4.4-1 0-1.4z"></path>
          </svg>
        `;
        td.appendChild(button);
      } else {
        td.textContent = wagon[key] ?? "";
      }
      tr.appendChild(td);
    });
    tableBody.appendChild(tr);
  });

  paginationInfo.textContent = `Seite ${currentPage} / ${maxPage}`;
  prevBtn.disabled = currentPage <= 1;
  nextBtn.disabled = currentPage >= maxPage;
};

const renderObjStrkTable = () => {
  if (!objstrkBody) return;
  objstrkBody.innerHTML = "";
  const rows = getObjStrkDisplayRows();
  if (!rows.length) {
    objstrkBody.innerHTML = '<tr><td colspan="6">Keine Objektstruktur gefunden.</td></tr>';
    return;
  }
  rows.forEach(({ node, level }) => {
    const tr = document.createElement("tr");
    OBJSTRK_COLUMNS.forEach((column, index) => {
      const td = document.createElement("td");
      const value = node.data[column] ?? "";
      if (column === "PARTS") {
        td.classList.add("objstrk-col-action");
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "objstrk-btn";
        btn.title = "Tausch";
        btn.setAttribute("aria-label", "Tausch");
        btn.dataset.eqtp = node.data.EQTP || "";
        btn.dataset.context = `${node.data.ITDS || ""} (${node.data.ITNO || ""})`;
        btn.dataset.originalItno = node.data.ITNO || "";
        btn.dataset.originalSern = node.data.SER2 || "";
        btn.dataset.nodeId = node.id;
        const hasReplacement = Boolean(
          (node.data.ERSATZ_ITNO && node.data.ERSATZ_ITNO.trim()) ||
          (node.data.ERSATZ_SERN && node.data.ERSATZ_SERN.trim())
        );
        if (hasReplacement) {
          btn.dataset.action = "remove";
          btn.classList.add("objstrk-btn--danger");
          btn.title = "Auswahl löschen";
          btn.setAttribute("aria-label", "Auswahl löschen");
          btn.textContent = "X";
        } else {
          btn.dataset.action = "select";
          btn.innerHTML = `
            <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false">
              <path d="M22.7 19.3l-6.1-6.1c.9-2.3.4-5-1.5-6.9-1.9-1.9-4.6-2.4-6.9-1.5l3 3-2.1 2.1-3-3c-.9 2.3-.4 5 1.5 6.9 1.9 1.9 4.6 2.4 6.9 1.5l6.1 6.1c.4.4 1 .4 1.4 0l.7-.7c.4-.4.4-1 0-1.4z"></path>
            </svg>
          `;
        }
        td.appendChild(btn);
      } else if (index === 0) {
        const cell = document.createElement("div");
        cell.className = "objstrk-cell";
        cell.style.setProperty("--objstrk-level", String(level));
        const showToggle = !objStrkSearchText && node.children.length;
        if (showToggle) {
          const toggle = document.createElement("button");
          toggle.type = "button";
          toggle.className = "objstrk-toggle";
          toggle.dataset.nodeId = node.id;
          toggle.setAttribute("aria-label", objStrkExpanded.has(node.id) ? "Einklappen" : "Ausklappen");
          toggle.textContent = objStrkExpanded.has(node.id) ? "−" : "+";
          cell.appendChild(toggle);
        } else {
          const spacer = document.createElement("span");
          spacer.className = "objstrk-spacer";
          cell.appendChild(spacer);
        }
        const text = document.createElement("span");
        text.textContent = value;
        cell.appendChild(text);
        td.appendChild(cell);
      } else {
        td.textContent = value;
      }
      tr.appendChild(td);
    });
    objstrkBody.appendChild(tr);
  });
};

const getSwapKey = (row) =>
  String(row.ID ?? `${row.WAGEN_ITNO || ""}::${row.WAGEN_SERN || ""}::${row.ORIGINAL_ITNO || ""}::${row.ORIGINAL_SERN || ""}`);

const findSwapRowByKey = (key) => swapRows.find((row) => getSwapKey(row) === key);

const renderSwapTable = () => {
  if (!swapTableBody) return;
  if (!swapRows.length) {
    swapTableBody.innerHTML = '<tr><td colspan="10">Keine offenen Tauschdatensätze.</td></tr>';
    if (swapPaginationInfo) swapPaginationInfo.textContent = "Seite 1 / 1";
    if (swapPrevPageBtn) swapPrevPageBtn.disabled = true;
    if (swapNextPageBtn) swapNextPageBtn.disabled = true;
  } else {
    const totalPages = Math.max(1, Math.ceil(swapRows.length / SWAP_PAGE_SIZE));
    swapPage = Math.min(Math.max(1, swapPage), totalPages);
    const start = (swapPage - 1) * SWAP_PAGE_SIZE;
    const rows = swapRows.slice(start, start + SWAP_PAGE_SIZE);
    swapTableBody.innerHTML = rows
      .map((row) => {
        const key = getSwapKey(row);
        const isSelected = swapSelected.has(key);
        return `
          <tr>
            <td>${row.WAGEN_ITNO || ""}</td>
            <td>${row.WAGEN_SERN || ""}</td>
            <td>${row.ORIGINAL_ITNO || ""}</td>
            <td>${row.ORIGINAL_SERN || ""}</td>
            <td>${row.ERSATZ_ITNO || ""}</td>
            <td>${row.ERSATZ_SERN || ""}</td>
            <td>${row.USER || ""}</td>
            <td>${row.TIMESTAMP || ""}</td>
            <td class="swap-action-cell">
              <button class="swap-check-btn ${isSelected ? "swap-check-btn--active" : ""}" data-key="${key}" title="${
                isSelected ? "Markierung entfernen" : "Für Tausch markieren"
              }">${isSelected ? "J" : "N"}</button>
            </td>
            <td>
              <button class="swap-delete-btn" data-key="${key}" data-wagen="${row.WAGEN_ITNO || ""}" data-sern="${
          row.WAGEN_SERN || ""
        }" data-original-itno="${row.ORIGINAL_ITNO || ""}" data-original-sern="${row.ORIGINAL_SERN || ""}" title="Datensatz löschen">X</button>
            </td>
          </tr>
        `;
      })
      .join("");
    if (swapPaginationInfo) {
      swapPaginationInfo.textContent = `Seite ${swapPage} / ${totalPages}`;
    }
    if (swapPrevPageBtn) swapPrevPageBtn.disabled = swapPage <= 1;
    if (swapNextPageBtn) swapNextPageBtn.disabled = swapPage >= totalPages;
  }
  if (swapCountInfo) {
    swapCountInfo.textContent = swapRows.length
      ? `${swapRows.length} offene Tauschdatensätze`
      : "Keine offenen Tauschdatensätze";
  }
};

const renderPartsTable = () => {
  if (!partsResultsBody) return;
  if (!sparePartsRows.length) {
    partsResultsBody.innerHTML = '<tr><td colspan="6">Keine Treffer gefunden.</td></tr>';
    if (partsPaginationInfo) partsPaginationInfo.textContent = "Seite 1 / 1";
    if (partsPrevPageBtn) partsPrevPageBtn.disabled = true;
    if (partsNextPageBtn) partsNextPageBtn.disabled = true;
    return;
  }
  const totalPages = Math.max(1, Math.ceil(sparePartsRows.length / PARTS_PAGE_SIZE));
  sparePartsPage = Math.min(sparePartsPage, totalPages);
  const start = (sparePartsPage - 1) * PARTS_PAGE_SIZE;
  const rows = sparePartsRows.slice(start, start + PARTS_PAGE_SIZE);
  partsResultsBody.innerHTML = rows
    .map(
      (row, idx) => `
      <tr>
        <td>${row["WAGEN-TYP"] || ""}</td>
        <td>${row["BAUREIHE"] || ""}</td>
        <td>${row["SERIENNUMMER"] || ""}</td>
        <td>${row["LAGERORT"] || ""}</td>
        <td>${row["LAGERPLATZ"] || ""}</td>
        <td><button class="parts-select-btn" data-row-index="${start + idx}" data-part-id="${row.ID || ""}">&#x276F;</button></td>
      </tr>`
    )
    .join("");
  if (partsPaginationInfo) partsPaginationInfo.textContent = `Seite ${sparePartsPage} / ${totalPages}`;
  if (partsPrevPageBtn) partsPrevPageBtn.disabled = sparePartsPage <= 1;
  if (partsNextPageBtn) partsNextPageBtn.disabled = sparePartsPage >= totalPages;
};

const getPartsFilterValues = () => ({
  typ: (partsFilterType?.value || "").trim(),
  item: (partsFilterItem?.value || "").trim(),
  serial: (partsFilterSerial?.value || "").trim(),
  facility: (partsFilterFacility?.value || "").trim(),
  bin: (partsFilterBin?.value || "").trim(),
});

const clearPartsFilters = (shouldReload = true) => {
  [partsFilterType, partsFilterItem, partsFilterSerial, partsFilterFacility, partsFilterBin].forEach((input) => {
    if (input) input.value = "";
  });
  sparePartsPage = 1;
  if (shouldReload) {
    loadSpareParts();
  }
};

const loadSpareParts = async () => {
  if (!currentPartsEqtp) {
    sparePartsRows = [];
    renderPartsTable();
    return;
  }
  const params = new URLSearchParams({ eqtp: currentPartsEqtp });
  const { typ, item, serial, facility, bin } = getPartsFilterValues();
  if (typ) params.append("type", typ);
  if (item) params.append("item", item);
  if (serial) params.append("serial", serial);
  if (facility) params.append("facility", facility);
  if (bin) params.append("bin", bin);
  params.append("limit", "200");
  const data = await fetchJSON(withEnv(`/api/spareparts/search?${params.toString()}`));
  sparePartsRows = data.rows || data;
  sparePartsPage = 1;
  renderPartsTable();
};

const setDatalistOptions = (datalist, values) => {
  if (!datalist) return;
  datalist.innerHTML = values.map((value) => `<option value="${value}"></option>`).join("");
};

const loadPartsFilterOptions = async () => {
  if (!currentPartsEqtp) return;
  const data = await fetchJSON(withEnv(`/api/spareparts/filters?eqtp=${currentPartsEqtp}`));
  sparePartsOptions = {
    typ: data.types || [],
    item: data.items || [],
    serial: data.serials || [],
    facility: data.facilities || [],
    bin: data.bins || [],
  };
  setDatalistOptions(partsTypeOptions, sparePartsOptions.typ);
  setDatalistOptions(partsItemOptions, sparePartsOptions.item);
  setDatalistOptions(partsSerialOptions, sparePartsOptions.serial);
  setDatalistOptions(partsFacilityOptions, sparePartsOptions.facility);
  setDatalistOptions(partsBinOptions, sparePartsOptions.bin);
};

const schedulePartsReload = () => {
  if (partsFilterTimer) {
    window.clearTimeout(partsFilterTimer);
  }
  partsFilterTimer = window.setTimeout(() => {
    sparePartsPage = 1;
    loadSpareParts();
  }, 200);
};

const setPartsModalGeometry = () => {
  if (!partsModalContent) return;
  const width = Math.min(1080, Math.max(420, window.innerWidth - 40));
  const height = Math.min(640, Math.max(320, window.innerHeight - 40));
  const left = Math.max(20, (window.innerWidth - width) / 2);
  const top = Math.max(20, (window.innerHeight - height) / 2);
  partsModalContent.style.width = `${width}px`;
  partsModalContent.style.height = `${height}px`;
  partsModalContent.style.left = `${left}px`;
  partsModalContent.style.top = `${top}px`;
};

const clampPartsModalPosition = () => {
  if (!partsModalContent) return;
  const rect = partsModalContent.getBoundingClientRect();
  const maxLeft = window.innerWidth - 40;
  const maxTop = window.innerHeight - 40;
  const width = rect.width;
  const height = rect.height;
  const left = Math.min(Math.max(10, rect.left), Math.max(10, maxLeft - width));
  const top = Math.min(Math.max(10, rect.top), Math.max(10, maxTop - height));
  partsModalContent.style.left = `${left}px`;
  partsModalContent.style.top = `${top}px`;
  const maxWidth = window.innerWidth - left - 20;
  const maxHeight = window.innerHeight - top - 20;
  partsModalContent.style.width = `${Math.min(width, maxWidth)}px`;
  partsModalContent.style.height = `${Math.min(height, maxHeight)}px`;
};

const openPartsModal = async (eqtp, contextText) => {
  if (!partsModal) return;
  currentPartsEqtp = eqtp || "";
  currentPartsContext = contextText || "";
  if (partsModalSubtitle) {
    partsModalSubtitle.textContent = eqtp
      ? `Teileart: ${eqtp}${contextText ? ` · ${contextText}` : ""}`
      : contextText || "Keine Teileart verfügbar";
  }
  clearPartsFilters(false);
  sparePartsRows = [];
  renderPartsTable();
  setPartsModalGeometry();
  partsModal.classList.remove("hidden");
  if (currentPartsEqtp) {
    await loadSpareParts();
  }
};

const closePartsModal = () => {
  if (!partsModal) return;
  partsModal.classList.add("hidden");
  if (partsFilterTimer) {
    window.clearTimeout(partsFilterTimer);
    partsFilterTimer = null;
  }
  currentPartsNodeId = null;
  currentPartsNode = null;
  currentOriginalItno = "";
  currentOriginalSern = "";
};

const loadExistingSelections = async (mtrl, sern) => {
  try {
    const data = await fetchJSON(
      withEnv(`/api/spareparts/selections?mtrl=${encodeURIComponent(mtrl)}&sern=${encodeURIComponent(sern)}`)
    );
    if (!Array.isArray(data.rows)) return;
    const map = new Map(
      data.rows.map((row) => [
        `${row.ORIGINAL_ITNO}::${row.ORIGINAL_SERN}`,
        { ersatzItno: row.ERSATZ_ITNO, ersatzSern: row.ERSATZ_SERN },
      ])
    );
    objStrkRows.forEach((row) => {
      const key = `${row.ITNO || ""}::${row.SER2 || ""}`;
      const replacement = map.get(key);
      if (replacement) {
        row.ERSATZ_ITNO = replacement.ersatzItno;
        row.ERSATZ_SERN = replacement.ersatzSern;
        const node = objStrkNodesById.get(row.__id);
        if (node) {
          node.data.ERSATZ_ITNO = replacement.ersatzItno;
          node.data.ERSATZ_SERN = replacement.ersatzSern;
        }
      }
    });
    renderObjStrkTable();
  } catch (error) {
    console.error("Konnte Ersatzteilzuordnungen nicht laden", error);
  }
  return undefined;
};

const startPartsDrag = (event) => {
  if (!partsModalContent || event.button !== 0) return;
  if (event.target instanceof Element && event.target.closest("button")) return;
  event.preventDefault();
  const rect = partsModalContent.getBoundingClientRect();
  partsDragState = {
    offsetX: event.clientX - rect.left,
    offsetY: event.clientY - rect.top,
  };
  document.addEventListener("mousemove", onPartsDrag);
  document.addEventListener("mouseup", stopPartsDrag);
};

const onPartsDrag = (event) => {
  if (!partsModalContent || !partsDragState) return;
  const left = event.clientX - partsDragState.offsetX;
  const top = event.clientY - partsDragState.offsetY;
  partsModalContent.style.left = `${Math.max(10, Math.min(left, window.innerWidth - 20))}px`;
  partsModalContent.style.top = `${Math.max(10, Math.min(top, window.innerHeight - 20))}px`;
};

const stopPartsDrag = () => {
  document.removeEventListener("mousemove", onPartsDrag);
  document.removeEventListener("mouseup", stopPartsDrag);
  partsDragState = null;
};

const startPartsResize = (event) => {
  if (!partsModalContent || event.button !== 0) return;
  event.preventDefault();
  const rect = partsModalContent.getBoundingClientRect();
  partsResizeState = {
    startX: event.clientX,
    startY: event.clientY,
    startWidth: rect.width,
    startHeight: rect.height,
  };
  document.addEventListener("mousemove", onPartsResize);
  document.addEventListener("mouseup", stopPartsResize);
};

const onPartsResize = (event) => {
  if (!partsModalContent || !partsResizeState) return;
  const minWidth = 360;
  const minHeight = 260;
  const deltaX = event.clientX - partsResizeState.startX;
  const deltaY = event.clientY - partsResizeState.startY;
  let newWidth = partsResizeState.startWidth + deltaX;
  let newHeight = partsResizeState.startHeight + deltaY;
  newWidth = Math.min(Math.max(newWidth, minWidth), window.innerWidth - 20);
  newHeight = Math.min(Math.max(newHeight, minHeight), window.innerHeight - 20);
  partsModalContent.style.width = `${newWidth}px`;
  partsModalContent.style.height = `${newHeight}px`;
};

const stopPartsResize = () => {
  document.removeEventListener("mousemove", onPartsResize);
  document.removeEventListener("mouseup", stopPartsResize);
  partsResizeState = null;
};

const submitReplacementSelection = async (rowIndex) => {
  if (
    !currentPartsNode ||
    !currentWagonItem ||
    !currentWagonSerial ||
    !currentOriginalItno ||
    !currentOriginalSern
  ) {
    return;
  }
  const row = sparePartsRows[rowIndex];
  if (!row) return;
  const ersatzItno = row.ITNO || row["BAUREIHE"] || "";
  const ersatzSern = row["SERIENNUMMER"] || "";
  const user = ensureSparePartUser();
  const payload = {
    WAGEN_ITNO: currentWagonItem,
    WAGEN_SERN: currentWagonSerial,
    ORIGINAL_ITNO: currentOriginalItno,
    ORIGINAL_SERN: currentOriginalSern,
    ERSATZ_ITNO: ersatzItno,
    ERSATZ_SERN: ersatzSern,
    USER: user,
    UPLOAD: "N",
  };
  try {
    const resp = await fetch(withEnv("/api/spareparts/select"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Auswahl konnte nicht gespeichert werden");
    }
    currentPartsNode.data.ERSATZ_ITNO = ersatzItno;
    currentPartsNode.data.ERSATZ_SERN = ersatzSern;
    renderObjStrkTable();
    closePartsModal();
  } catch (error) {
    console.error(error);
    window.alert(error.message || "Fehler beim Speichern der Auswahl");
  }
};

const removeReplacementSelection = async (nodeId, originalItno, originalSern) => {
  if (!nodeId || !currentWagonItem || !currentWagonSerial || !originalItno || !originalSern) {
    return;
  }
  const payload = {
    WAGEN_ITNO: currentWagonItem,
    WAGEN_SERN: currentWagonSerial,
    ORIGINAL_ITNO: originalItno,
    ORIGINAL_SERN: originalSern,
  };
  try {
    const resp = await fetch(withEnv("/api/spareparts/select"), {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Auswahl konnte nicht gelöscht werden");
    }
    const node = objStrkNodesById.get(nodeId);
    if (node) {
      node.data.ERSATZ_ITNO = "";
      node.data.ERSATZ_SERN = "";
    }
    const row = objStrkRows.find((entry) => entry.__id === nodeId);
    if (row) {
      row.ERSATZ_ITNO = "";
      row.ERSATZ_SERN = "";
    }
    renderObjStrkTable();
  } catch (error) {
    console.error(error);
    window.alert(error.message || "Fehler beim Löschen der Auswahl");
  }
};

const loadSwapData = async () => {
  showOverlay();
  setStatus("LADE TAUSCHDATEN ...");
  setIndeterminate(true);
  try {
    const data = await fetchJSON(withEnv("/api/spareparts/swaps?upload=N"));
    const rows = Array.isArray(data.rows) ? data.rows : [];
    const retained = new Set();
    rows.forEach((row) => {
      const key = getSwapKey(row);
      if (swapSelected.has(key)) retained.add(key);
    });
    swapSelected = retained;
    swapRows = rows;
    swapPage = 1;
    renderSwapTable();
    showSwapDbView();
    hideOverlay();
  } catch (error) {
    console.error(error);
    showError(error.message || "Tauschdaten konnten nicht geladen werden");
  }
};

const formatDateTime = (value) => {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString("de-DE");
};

const appendRsrd2Log = (message, variant = "info") => {
  if (!rsrd2Log || !message) return;
  const entry = document.createElement("div");
  entry.className = `rsrd2-log-entry rsrd2-log-entry--${variant}`;
  const timeSpan = document.createElement("span");
  timeSpan.className = "rsrd2-log-time";
  timeSpan.textContent = new Date().toLocaleTimeString("de-DE", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  const messageSpan = document.createElement("span");
  messageSpan.className = "rsrd2-log-message";
  messageSpan.textContent = message;
  entry.append(timeSpan, messageSpan);
  rsrd2Log.append(entry);
  rsrd2Log.classList.remove("rsrd2-log-empty");
  while (rsrd2Log.childElementCount > RSRD2_LOG_LIMIT) {
    rsrd2Log.removeChild(rsrd2Log.firstElementChild);
  }
  rsrd2Log.scrollTop = rsrd2Log.scrollHeight;
};

const parseErrorResponse = async (resp) => {
  const text = await resp.text();
  try {
    const parsed = JSON.parse(text);
    if (parsed && parsed.detail) return parsed.detail;
  } catch {
    // ignore JSON parse errors
  }
  if (text) return text;
  return resp.statusText || "Unbekannter Fehler";
};

const loadRsrd2Wagons = async (force = false) => {
  if (!rsrd2Status) return;
  if (rsrd2Loaded && !force) return;
  rsrd2Status.textContent = "Lade Wagennummern aus dem Cache ...";
  try {
    const data = await fetchJSON("/api/rsrd2/wagons?limit=1");
    const count = data.total || 0;
    const summary = count
      ? `${count} Datensätze vorhanden (Stand ${formatDateTime(new Date().toISOString())})`
      : "Noch keine Wagennummern geladen.";
    rsrd2Status.textContent = summary;
    if (force) {
      appendRsrd2Log(
        count ? `Cache aktualisiert: ${count} Wagennummern verfügbar.` : "Cache aktualisiert: Keine Wagennummern vorhanden.",
        "info",
      );
    }
    rsrd2Loaded = true;
  } catch (error) {
    console.error(error);
    rsrd2Status.textContent = error.message || "Abruf fehlgeschlagen.";
    appendRsrd2Log(error.message || "Abruf fehlgeschlagen.", "error");
  }
};

const ensureRsrdLoaderSubtitle = () => {
  if (loaderSubtitle && MODULE_META.rsrd2?.loaderSubtitle) {
    loaderSubtitle.textContent = MODULE_META.rsrd2.loaderSubtitle;
  }
};

const runRsrdAction = async ({ url, startMessage, successMessage, onSuccess }) => {
  if (!rsrd2Status) return;
  const startText = startMessage || "Operation wird ausgeführt ...";
  rsrd2Status.textContent = startText;
  appendRsrd2Log(startText, "info");
  ensureRsrdLoaderSubtitle();
  showOverlay();
  setStatus(startText);
  setIndeterminate(true);
  try {
    const resp = await fetch(url, { method: "POST" });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Aktion fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    if (successMessage) {
      const finalMessage = typeof successMessage === "function" ? successMessage(data) : successMessage;
      rsrd2Status.textContent = finalMessage;
      appendRsrd2Log(finalMessage, "success");
    }
    if (Array.isArray(data?.logs)) {
      data.logs.forEach((entry) => appendRsrd2Log(entry, "info"));
    }
    if (onSuccess) await onSuccess(data);
  } catch (error) {
    const fallback = error.message || "Aktion fehlgeschlagen.";
    rsrd2Status.textContent = fallback;
    appendRsrd2Log(fallback, "error");
    throw error;
  } finally {
    hideOverlay();
  }
};

const watchRsrdJob = (jobId, successBuilder) =>
  new Promise((resolve, reject) => {
    if (!jobId) {
      reject(new Error("Job-ID fehlt."));
      return;
    }
    const poll = async () => {
      try {
        const resp = await fetch(`/api/rsrd2/jobs/${encodeURIComponent(jobId)}`);
        if (!resp.ok) {
          const message = await parseErrorResponse(resp);
          throw new Error(message || "Statusabfrage fehlgeschlagen.");
        }
        const data = await resp.json();
        const logs = Array.isArray(data.logs) ? data.logs : [];
        const last = rsrd2JobOffsets[jobId] || 0;
        for (let idx = last; idx < logs.length; idx += 1) {
          appendRsrd2Log(logs[idx], "info");
        }
        rsrd2JobOffsets[jobId] = logs.length;
        if (data.status === "running") {
          window.setTimeout(poll, RSRD2_JOB_POLL_MS);
          return;
        }
        delete rsrd2JobOffsets[jobId];
        if (data.status === "success") {
          const summary =
            typeof successBuilder === "function" ? successBuilder(data.result || {}, data) : "Aktion abgeschlossen.";
          if (summary) {
            appendRsrd2Log(summary, "success");
            if (rsrd2Status) rsrd2Status.textContent = summary;
          }
          resolve(data);
        } else {
          const message = data.error || "Aktion fehlgeschlagen.";
          appendRsrd2Log(message, "error");
          if (rsrd2Status) rsrd2Status.textContent = message;
          reject(new Error(message));
        }
      } catch (error) {
        appendRsrd2Log(error.message || "Statusabfrage fehlgeschlagen.", "error");
        if (rsrd2Status) rsrd2Status.textContent = error.message || "Statusabfrage fehlgeschlagen.";
        reject(error);
      }
    };
    poll();
  });

const startRsrdJob = async ({ url, startMessage, successBuilder, afterSuccess }) => {
  if (!rsrd2Status) return;
  const startText = startMessage || "Vorgang gestartet ...";
  rsrd2Status.textContent = startText;
  appendRsrd2Log(startText, "info");
  ensureRsrdLoaderSubtitle();
  showOverlay();
  setStatus(startText);
  setIndeterminate(true);
  try {
    const resp = await fetch(url, { method: "POST" });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Aktion fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    const jobId = data.job_id;
    await watchRsrdJob(jobId, successBuilder);
    if (afterSuccess) {
      await afterSuccess();
    }
  } catch (error) {
    const fallback = error.message || "Aktion fehlgeschlagen.";
    rsrd2Status.textContent = fallback;
    appendRsrd2Log(fallback, "error");
  } finally {
    hideOverlay();
  }
};

const loadErpWagons = () =>
  startRsrdJob({
    url: withEnv("/api/rsrd2/load_erp"),
    startMessage: "Lade Wagennummern aus dem ERP ...",
    successBuilder: (result) => {
      rsrd2Loaded = false;
      return `ERP-Wagennummern geladen: ${result?.count_wagons ?? 0}.`;
    },
  });

const loadErpAttributes = () =>
  startRsrdJob({
    url: withEnv("/api/rsrd2/load_erp_full"),
    startMessage: "Lade Wagenattribute aus dem ERP ...",
    successBuilder: (result) => `ERP-Wagenattribute geladen: ${result?.count_full ?? 0} Datensätze.`,
  });

const fetchRsrdJson = () =>
  runRsrdAction({
    url: "/api/rsrd2/fetch_json",
    startMessage: "JSON aus RSRD wird geladen ...",
    successMessage: (data) => `JSON geladen für ${data?.staged ?? 0} Wagen.`,
    onSuccess: async () => {
      rsrd2Loaded = false;
      await loadRsrd2Wagons(true);
    },
  });

const processRsrdJson = () =>
  runRsrdAction({
    url: "/api/rsrd2/process_json",
    startMessage: "Verarbeite JSON in strukturierte Daten ...",
    successMessage: (data) => `JSON verarbeitet für ${data?.processed ?? 0} Wagen.`,
  });

const syncRsrd2All = async () => {
  if (!rsrd2Status) return;
  rsrd2Status.textContent = "Starte RSRD2-Synchronisation ...";
  appendRsrd2Log("Starte RSRD2-Synchronisation ...", "info");
  ensureRsrdLoaderSubtitle();
  showOverlay();
  setStatus("RSRD2-Daten werden geladen ...");
  setIndeterminate(true);
  try {
    const resp = await fetch("/api/rsrd2/sync_all", { method: "POST" });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Synchronisation fehlgeschlagen");
    }
    const payload = await resp.json().catch(() => ({}));
    const synced = payload.synced ?? 0;
    const finalText = `RSRD2-Abruf abgeschlossen (${synced} Wagennummern).`;
    rsrd2Status.textContent = finalText;
    appendRsrd2Log(finalText, "success");
    rsrd2Loaded = false;
    await loadRsrd2Wagons(true);
  } catch (error) {
    console.error(error);
    const fail = error.message || "Synchronisation fehlgeschlagen.";
    rsrd2Status.textContent = fail;
    appendRsrd2Log(fail, "error");
  } finally {
    hideOverlay();
  }
};

const toggleSwapSelection = (key) => {
  if (!key) return;
  if (swapSelected.has(key)) {
    swapSelected.delete(key);
  } else {
    swapSelected.add(key);
  }
  renderSwapTable();
};

const markAllSwapRecords = () => {
  swapSelected = new Set(swapRows.map((row) => getSwapKey(row)));
  renderSwapTable();
};

const clearAllSwapSelections = () => {
  swapSelected.clear();
  renderSwapTable();
};

const deleteSwapRecord = async (key) => {
  if (!key) return;
  const row = findSwapRowByKey(key);
  if (!row) return;
  const payload = {
    WAGEN_ITNO: row.WAGEN_ITNO || "",
    WAGEN_SERN: row.WAGEN_SERN || "",
    ORIGINAL_ITNO: row.ORIGINAL_ITNO || "",
    ORIGINAL_SERN: row.ORIGINAL_SERN || "",
  };
  try {
    const resp = await fetch(withEnv("/api/spareparts/select"), {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Datensatz konnte nicht gelöscht werden");
    }
    swapRows = swapRows.filter((row) => getSwapKey(row) !== key);
    swapSelected.delete(key);
    renderSwapTable();
  } catch (error) {
    console.error(error);
    window.alert(error.message || "Fehler beim Löschen des Datensatzes");
  }
};

const updateObjStrkMeta = (count, mtrl, sern) => {
  if (!objstrkMeta) return;
  if (!count) {
    objstrkMeta.textContent = "Keine Positionen gefunden";
    return;
  }
  objstrkMeta.textContent = `${count} Positionen · BAUREIHE: ${mtrl} · SERIENNUMMER: ${sern}`;
};

const initPaginationControls = () => {
  prevBtn.addEventListener("click", () => {
    if (currentPage > 1) {
      currentPage -= 1;
      renderPage();
    }
  });
  nextBtn.addEventListener("click", () => {
    const maxPage = Math.max(1, Math.ceil(wagons.length / pageSize));
    if (currentPage < maxPage) {
      currentPage += 1;
      renderPage();
    }
  });
};

const initPageSizeControl = () => {
  const storedSize = Number(window.localStorage.getItem("sparepart.pageSize"));
  if ([10, 25, 50].includes(storedSize)) {
    pageSize = storedSize;
  }

  if (!pageSizeSelect) return;

  pageSizeSelect.value = String(pageSize);
  pageSizeSelect.addEventListener("change", () => {
    const nextSize = Number(pageSizeSelect.value);
    if (![10, 25, 50].includes(nextSize)) return;
    pageSize = nextSize;
    window.localStorage.setItem("sparepart.pageSize", String(pageSize));
    currentPage = 1;
    renderPage();
  });
};


const showTable = () => {
  hideOverlay();
  showWagonsView();
  tablePanel.classList.remove("hidden");
  renderTableHead();
  applyFilters();
};

const showError = (message) => {
  showOverlay();
  progressDetail.innerHTML = `<div class="error-banner">${message}</div>`;
};

const handleObjStrkResponse = (payload, { mtrl, sern }) => {
  const rows = normalizeObjStrkRows(payload);
  objStrkRows = rows.map((row, index) => {
    const normalized = {};
    OBJSTRK_COLUMNS.forEach((column) => {
      const sources = OBJSTRK_FIELD_SOURCES[column];
      if (sources) {
        let found = "";
        for (const source of sources) {
          const value = row[source];
          if (value !== null && value !== undefined && value !== "") {
            found = value;
            break;
          }
        }
        normalized[column] = found;
      } else {
        normalized[column] = row[column] ?? "";
      }
    });
    normalized.EQTP = row.EQTP ?? row["TEILEART"] ?? "";
    const id = (row.SER2 || "").trim() || `node-${index}`;
    const parentId = (row.SERN || "").trim() || "";
    normalized.__id = id;
    normalized.__parentId = parentId;
    return normalized;
  });
  const count = objStrkRows.length;
  updateObjStrkMeta(count, mtrl, sern);
  const { roots, map } = buildObjStrkHierarchy(objStrkRows);
  objStrkRoots = roots;
  objStrkNodesById = map;
  collapseAllObjNodes();
  expandAllObjNodes();
  objStrkSearchText = fulltextInput ? fulltextInput.value.trim().toLowerCase() : "";
  if (objStrkSearchText) {
    expandAllObjNodes();
  }
  renderObjStrkHead();
  renderObjStrkTable();
  loadExistingSelections(mtrl, sern).finally(() => {
    showObjStrkView();
    hideOverlay();
  });
};

const loadObjStrk = async (mtrl, sern) => {
  currentWagonItem = mtrl;
  currentWagonSerial = sern;
  showOverlay();
  setStatus("LADE OBJEKTSTRUKTUR ...");
  setIndeterminate(true);
  try {
    const payload = await fetchJSON(
      withEnv(`/api/objstrk?mtrl=${encodeURIComponent(mtrl)}&sern=${encodeURIComponent(sern)}`)
    );
    handleObjStrkResponse(payload, { mtrl, sern });
  } catch (error) {
    console.error(error);
    showError(error.message || "Objektstruktur konnte nicht geladen werden");
  }
};

const startLoadingWorkflow = async () => {
  showOverlay();
  try {
    await loadWagons();
    showTable();
  } catch (error) {
    console.error(error);
    showError(error.message || "Unbekannter Fehler");
  }
};

const reloadData = async () => {
  reloadBtn.disabled = true;
  const originalText = reloadBtn.textContent;
  reloadBtn.textContent = "Lädt ...";
  try {
    showOverlay();
    setStatus("Datenbank wird neu geladen ...");
    setIndeterminate(true);
    const resp = await fetch(withEnv("/api/reload"), { method: "POST" });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Reload fehlgeschlagen");
    }
    await startLoadingWorkflow();
  } catch (error) {
    console.error(error);
    showError(error.message || "Neu laden fehlgeschlagen");
  } finally {
    reloadBtn.disabled = false;
    reloadBtn.textContent = originalText;
  }
};

initPaginationControls();
initPageSizeControl();
reloadBtn.addEventListener("click", reloadData);
if (fulltextInput) {
  fulltextInput.addEventListener("input", () => {
    applyFilters();
    updateObjStrkSearch();
  });
}
FILTER_FIELDS.forEach(({ input }) => {
  if (input) {
    input.addEventListener("input", applyFilters);
  }
});
if (clearFiltersBtn) {
  clearFiltersBtn.addEventListener("click", () => {
    FILTER_FIELDS.forEach(({ input }) => {
      if (input) input.value = "";
    });
    if (fulltextInput) fulltextInput.value = "";
    applyFilters();
    updateObjStrkSearch();
  });
}
if (backToWagonsBtn) {
  backToWagonsBtn.addEventListener("click", () => {
    showWagonsView();
  });
}
if (collapseAllBtn) {
  collapseAllBtn.addEventListener("click", () => {
    collapseAllObjNodes();
    renderObjStrkTable();
  });
}
if (expandAllBtn) {
  expandAllBtn.addEventListener("click", () => {
    expandAllObjNodes();
    renderObjStrkTable();
  });
}
if (mainMenuBtn && mainMenu) {
  mainMenuBtn.addEventListener("click", () => {
    mainMenu.classList.toggle("hidden");
  });
  document.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) return;
    if (!mainMenuBtn.contains(event.target) && !mainMenu.contains(event.target)) {
      mainMenu.classList.add("hidden");
    }
  });
  menuItems.forEach((item) => {
    item.addEventListener("click", () => {
      mainMenu.classList.add("hidden");
      setModule(item.dataset.module || "sparepart");
    });
  });
}
if (envToggle) {
  envToggle.addEventListener("click", () => {
    setEnvironment(currentEnv === "LIVE" ? "TEST" : "LIVE");
  });
}
const openSwapDb = () => {
  loadSwapData();
};
if (openSwapFromWagonsBtn) {
  openSwapFromWagonsBtn.addEventListener("click", openSwapDb);
}
if (openSwapFromObjBtn) {
  openSwapFromObjBtn.addEventListener("click", openSwapDb);
}
if (tableBody) {
  tableBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const button = target ? target.closest(".objstrk-btn") : null;
    if (!button) return;
    const mtrl = button.dataset.mtrl || "";
    const sern = button.dataset.sern || "";
    if (!mtrl || !sern) return;
    loadObjStrk(mtrl, sern);
  });
}
if (swapBackBtn) {
  swapBackBtn.addEventListener("click", () => {
    showWagonsView();
  });
}
if (swapExecuteBtn) {
  swapExecuteBtn.addEventListener("click", () => {
    window.alert("Tauschdurchführung befindet sich noch in Arbeit.");
  });
}
if (swapSelectAllBtn) {
  swapSelectAllBtn.addEventListener("click", markAllSwapRecords);
}
if (swapClearSelectionBtn) {
  swapClearSelectionBtn.addEventListener("click", clearAllSwapSelections);
}
if (rsrd2LoadErpBtn) {
  rsrd2LoadErpBtn.addEventListener("click", () => {
    loadErpWagons();
  });
}

if (rsrd2LoadErpFullBtn) {
  rsrd2LoadErpFullBtn.addEventListener("click", () => {
    loadErpAttributes();
  });
}
if (rsrd2FetchJsonBtn) {
  rsrd2FetchJsonBtn.addEventListener("click", () => {
    fetchRsrdJson();
  });
}
if (rsrd2ProcessJsonBtn) {
  rsrd2ProcessJsonBtn.addEventListener("click", () => {
    processRsrdJson();
  });
}
if (swapPrevPageBtn) {
  swapPrevPageBtn.addEventListener("click", () => {
    if (swapPage > 1) {
      swapPage -= 1;
      renderSwapTable();
    }
  });
}
if (swapNextPageBtn) {
  swapNextPageBtn.addEventListener("click", () => {
    const maxPage = Math.max(1, Math.ceil(swapRows.length / SWAP_PAGE_SIZE));
    if (swapPage < maxPage) {
      swapPage += 1;
      renderSwapTable();
    }
  });
}
if (swapTableBody) {
  swapTableBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    const checkBtn = target.closest(".swap-check-btn");
    if (checkBtn) {
      toggleSwapSelection(checkBtn.dataset.key || "");
      return;
    }
    const deleteBtn = target.closest(".swap-delete-btn");
    if (deleteBtn) {
      deleteSwapRecord(deleteBtn.dataset.key || "");
    }
  });
}
if (objstrkBody) {
  objstrkBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const toggle = target ? target.closest(".objstrk-toggle") : null;
    if (toggle) {
      const nodeId = toggle.dataset.nodeId;
      if (!nodeId) return;
      if (objStrkExpanded.has(nodeId)) {
        objStrkExpanded.delete(nodeId);
      } else {
        objStrkExpanded.add(nodeId);
      }
      renderObjStrkTable();
      return;
    }
    const partsBtn = target ? target.closest(".objstrk-btn") : null;
    if (partsBtn && partsBtn.dataset.eqtp !== undefined && !partsBtn.dataset.mtrl) {
      const action = partsBtn.dataset.action || "select";
      const nodeId = partsBtn.dataset.nodeId || null;
      if (action === "remove" && nodeId) {
        removeReplacementSelection(nodeId, partsBtn.dataset.originalItno || "", partsBtn.dataset.originalSern || "");
        return;
      }
      currentPartsNodeId = nodeId;
      currentPartsNode = currentPartsNodeId ? objStrkNodesById.get(currentPartsNodeId) : null;
      currentOriginalItno = partsBtn.dataset.originalItno || "";
      currentOriginalSern = partsBtn.dataset.originalSern || "";
      openPartsModal(partsBtn.dataset.eqtp || "", partsBtn.dataset.context || "");
    }
  });
}
if (closePartsModalBtn) {
  closePartsModalBtn.addEventListener("click", closePartsModal);
}
if (partsModal) {
  partsModal.addEventListener("click", (event) => {
    if (event.target === partsModal) {
      closePartsModal();
    }
  });
  window.addEventListener("resize", () => {
    if (!partsModal.classList.contains("hidden")) {
      clampPartsModalPosition();
    }
  });
}
if (partsSearchForm) {
  partsSearchForm.addEventListener("submit", (event) => {
    event.preventDefault();
    loadSpareParts();
  });
}
[
  partsFilterType,
  partsFilterItem,
  partsFilterSerial,
  partsFilterFacility,
  partsFilterBin,
].forEach((input) => {
  if (input) {
    input.addEventListener("input", schedulePartsReload);
  }
});
if (partsClearFiltersBtn) {
  partsClearFiltersBtn.addEventListener("click", () => clearPartsFilters(true));
}
if (partsModalHeader) {
  partsModalHeader.addEventListener("mousedown", startPartsDrag);
}
if (partsResizeHandle) {
  partsResizeHandle.addEventListener("mousedown", startPartsResize);
}
if (partsPrevPageBtn) {
  partsPrevPageBtn.addEventListener("click", () => {
    if (sparePartsPage > 1) {
      sparePartsPage -= 1;
      renderPartsTable();
    }
  });
}
if (partsNextPageBtn) {
  partsNextPageBtn.addEventListener("click", () => {
    const totalPages = Math.max(1, Math.ceil(sparePartsRows.length / PARTS_PAGE_SIZE));
    if (sparePartsPage < totalPages) {
      sparePartsPage += 1;
      renderPartsTable();
    }
  });
}
if (partsResultsBody) {
  partsResultsBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const selectBtn = target ? target.closest(".parts-select-btn") : null;
    if (selectBtn) {
      const rowIndex = Number(selectBtn.dataset.rowIndex);
      submitReplacementSelection(rowIndex);
    }
  });
}
setModule(currentModule, true);
updateEnvSwitch();
startLoadingWorkflow();
