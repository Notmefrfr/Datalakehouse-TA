
/* ==========================================================================================
   Everything below talks directly to MinIO from the browser using the AWS S3 SDK (MinIO is
   S3-API-compatible). There is no separate backend server for this build.

   IMPORTANT — MinIO must allow browser (CORS) requests for this to work. See the connection
   panel (gear/pill button, top right) and the on-page banner if the connection fails — both
   explain the one-line MinIO config change needed.
   ========================================================================================== */

// ---------------------------------------------------------- MinIO connection state

let S3_CONFIG = {
  endpoint: "http://localhost:9000",
  bucket: "datalakev3",
  accessKey: "admin",
  secretKey: "password123",
};
let s3 = null;
let isConnected = false;

function buildS3Client() {
  return new AWS.S3({
    endpoint: S3_CONFIG.endpoint,
    accessKeyId: S3_CONFIG.accessKey,
    secretAccessKey: S3_CONFIG.secretKey,
    region: "us-east-1",
    s3ForcePathStyle: true,
    signatureVersion: "v4",
  });
}

async function testConnection() {
  s3 = buildS3Client();
  try {
    await s3.listObjectsV2({ Bucket: S3_CONFIG.bucket, MaxKeys: 1 }).promise();
    isConnected = true;
    updateConnPill();
    return true;
  } catch (e) {
    isConnected = false;
    updateConnPill(e.message);
    return false;
  }
}

function updateConnPill(errorMessage) {
  const pill = document.getElementById("conn-pill");
  const label = document.getElementById("conn-label");
  pill.classList.remove("connected", "error");
  if (isConnected) {
    pill.classList.add("connected");
    label.textContent = "Connected";
  } else if (errorMessage) {
    pill.classList.add("error");
    label.textContent = "Connection failed";
  } else {
    label.textContent = "Not connected";
  }
}

document.getElementById("conn-pill").addEventListener("click", () => {
  const panel = document.getElementById("settings-panel");
  panel.hidden = !panel.hidden;
  if (!panel.hidden) {
    document.getElementById("cfg-endpoint").value = S3_CONFIG.endpoint;
    document.getElementById("cfg-bucket").value = S3_CONFIG.bucket;
    document.getElementById("cfg-access").value = S3_CONFIG.accessKey;
    document.getElementById("cfg-secret").value = S3_CONFIG.secretKey;
  }
});

document.getElementById("cfg-connect").addEventListener("click", async () => {
  S3_CONFIG.endpoint = document.getElementById("cfg-endpoint").value.trim() || S3_CONFIG.endpoint;
  S3_CONFIG.bucket = document.getElementById("cfg-bucket").value.trim() || S3_CONFIG.bucket;
  S3_CONFIG.accessKey = document.getElementById("cfg-access").value.trim() || S3_CONFIG.accessKey;
  S3_CONFIG.secretKey = document.getElementById("cfg-secret").value.trim() || S3_CONFIG.secretKey;

  const ok = await testConnection();
  document.getElementById("settings-panel").hidden = true;
  if (ok) {
    showToast("Connected to MinIO");
    renderConnBanner();
    navigate();
  } else {
    showToast("Could not connect — check endpoint/credentials and MinIO's CORS settings", "error");
    renderConnBanner();
  }
});

function renderConnBanner() {
  const area = document.getElementById("conn-banner-area");
  if (isConnected) { area.innerHTML = ""; return; }
  area.innerHTML = `
    <div class="conn-banner">
      <svg><use href="#icon-alert"/></svg>
      <span>Not connected to MinIO yet. Click "Not connected" in the top right to set your endpoint and credentials — same instance as before, e.g. <code>http://localhost:9000</code>.</span>
      <button class="btn btn-secondary btn-sm" onclick="document.getElementById('conn-pill').click()">Connect</button>
    </div>`;
}

// ---------------------------------------------------------- storage layout (new prefixes, doesn't touch v1/v2 data)

const BRONZE_PREFIX = "bronze_v3/";
const SILVER_PREFIX = "silver_v3/";
const GOLD_PREFIX = "gold_v3/";
const METADATA_PREFIX = "metadata_v3/";

const DIVISIONS = [
  {
    id: "network_operation", label: "Network Operation", dataset_types: [
      { id: "daily_fiber_inspection", label: "Daily Fiber Inspection", required_columns: ["inspection_date", "fiber_id", "status", "technician"] },
      { id: "outage_report", label: "Outage Report", required_columns: ["outage_date", "region", "duration_minutes", "cause"] },
    ]
  },
  {
    id: "customer_service", label: "Customer Service", dataset_types: [
      { id: "ticket_log", label: "Ticket Log", required_columns: ["ticket_id", "customer_id", "opened_date", "status"] },
      { id: "satisfaction_survey", label: "Satisfaction Survey", required_columns: ["survey_date", "customer_id", "score"] },
    ]
  },
  {
    id: "finance", label: "Finance", dataset_types: [
      { id: "invoice_summary", label: "Invoice Summary", required_columns: ["invoice_date", "region", "amount"] },
    ]
  },
];
const DIVISION_LOOKUP = Object.fromEntries(DIVISIONS.map((d) => [d.id, d]));
function divisionLabel(id) { return DIVISION_LOOKUP[id] ? DIVISION_LOOKUP[id].label : id; }
function datasetTypeLookup(divisionId, typeId) {
  const d = DIVISION_LOOKUP[divisionId];
  return d ? d.dataset_types.find((t) => t.id === typeId) : null;
}

function sanitizeName(raw) {
  const stub = String(raw).replace(/[^a-zA-Z0-9_-]+/g, "_").replace(/^_+|_+$/g, "");
  return stub || "dataset";
}
function bronzeObjectKey(division, dtype, stub) { return `${BRONZE_PREFIX}${division}/${dtype}/${stub}.csv`; }
function parseBronzeName(name) {
  const parts = name.split("__");
  return parts.length === 3 ? parts : null;
}
function objectKeyFor(layer, name) {
  if (layer === "Bronze") {
    const parsed = parseBronzeName(name);
    if (!parsed) return null;
    const [division, dtype, stub] = parsed;
    return bronzeObjectKey(division, dtype, stub);
  }
  if (layer === "Silver") return `${SILVER_PREFIX}${name}/data.csv`;
  if (layer === "Gold") return `${GOLD_PREFIX}${name}/data.csv`;
  return null;
}

// ---------------------------------------------------------- low-level S3 helpers

async function listAllObjects(prefix) {
  let objects = [];
  let token;
  do {
    const params = { Bucket: S3_CONFIG.bucket, Prefix: prefix };
    if (token) params.ContinuationToken = token;
    const res = await s3.listObjectsV2(params).promise();
    objects = objects.concat(res.Contents || []);
    token = res.IsTruncated ? res.NextContinuationToken : null;
  } while (token);
  return objects;
}

async function getObjectText(key) {
  const res = await s3.getObject({ Bucket: S3_CONFIG.bucket, Key: key }).promise();
  const body = res.Body;
  if (typeof body === "string") return body;
  if (body instanceof Uint8Array || body instanceof ArrayBuffer) return new TextDecoder("utf-8").decode(body);
  return body.toString("utf-8");
}

async function putObjectText(key, text, contentType) {
  const blob = new Blob([text], { type: contentType || "text/plain" });
  await s3.putObject({ Bucket: S3_CONFIG.bucket, Key: key, Body: blob, ContentType: contentType || "text/plain" }).promise();
}

async function putObjectFile(key, file) {
  await s3.putObject({ Bucket: S3_CONFIG.bucket, Key: key, Body: file, ContentType: file.type || "text/csv" }).promise();
}

function parseCsv(text) {
  const result = Papa.parse(text, { header: true, dynamicTyping: true, skipEmptyLines: true });
  return { fields: result.meta.fields || [], rows: result.data };
}

async function readDataset(layer, name) {
  const key = objectKeyFor(layer, name);
  if (!key) throw new Error(`Unknown dataset '${name}' in layer '${layer}'`);
  const text = await getObjectText(key);
  return parseCsv(text);
}

// ---------------------------------------------------------- metadata (display name / archived)

function metaKey(layer, name) { return `${METADATA_PREFIX}${layer}__${name}.json`; }

async function loadMeta(layer, name) {
  try { return JSON.parse(await getObjectText(metaKey(layer, name))); }
  catch (e) { return {}; }
}
async function saveMeta(layer, name, meta) {
  await putObjectText(metaKey(layer, name), JSON.stringify(meta, null, 2), "application/json");
}

// ---------------------------------------------------------- catalog

function timeAgo(date) {
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} minute${minutes !== 1 ? "s" : ""} ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} hour${hours !== 1 ? "s" : ""} ago`;
  const days = Math.floor(hours / 24);
  return `${days} day${days !== 1 ? "s" : ""} ago`;
}
function formatDate(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

async function listCatalog(includeArchived = false) {
  const entries = [];

  for (const o of await listAllObjects(BRONZE_PREFIX)) {
    if (!o.Key.endsWith(".csv")) continue;
    const rel = o.Key.slice(BRONZE_PREFIX.length).replace(/\.csv$/, "");
    const pieces = rel.split("/");
    if (pieces.length !== 3) continue;
    const [division, dtype, stub] = pieces;
    entries.push({ layer: "Bronze", name: `${division}__${dtype}__${stub}`, division_id: division, last_modified: o.LastModified, size_bytes: o.Size });
  }

  for (const [layer, prefix] of [["Silver", SILVER_PREFIX], ["Gold", GOLD_PREFIX]]) {
    const seen = new Set();
    for (const o of await listAllObjects(prefix)) {
      if (!o.Key.endsWith("/data.csv")) continue;
      const name = o.Key.slice(prefix.length).replace(/\/data\.csv$/, "");
      if (seen.has(name)) continue;
      seen.add(name);
      entries.push({ layer, name, division_id: null, last_modified: o.LastModified, size_bytes: o.Size });
    }
  }

  const enriched = [];
  for (const e of entries) {
    const meta = await loadMeta(e.layer, e.name);
    if (meta.archived && !includeArchived) continue;

    let rows = 0, cols = 0, status = "Error";
    try {
      const { fields, rows: dataRows } = await readDataset(e.layer, e.name);
      rows = dataRows.length;
      cols = fields.length;
      status = rows > 0 ? "Active" : "Error";
    } catch (err) { /* leave defaults */ }

    const division = meta.division_override || (e.division_id ? divisionLabel(e.division_id) : null) || "General";

    enriched.push({
      layer: e.layer,
      name: e.name,
      display_name: meta.display_name || e.name.split("__").pop(),
      division,
      owner: meta.owner || "Workspace",
      rows, columns: cols, status,
      last_updated: e.last_modified ? formatDate(e.last_modified) : "—",
      _last_modified_raw: e.last_modified,
      _size_bytes: e.size_bytes,
      archived: !!meta.archived,
    });
  }
  return enriched;
}

async function computeStats() {
  const catalog = await listCatalog(false);
  const totalSizeMb = catalog.reduce((s, e) => s + e._size_bytes, 0) / 1024 / 1024;

  const byDivision = {};
  for (const e of catalog) {
    if (e.layer !== "Bronze") continue;
    byDivision[e.division] = (byDivision[e.division] || 0) + e._size_bytes / 1024 / 1024;
  }
  const storageList = Object.entries(byDivision).map(([division, mb]) => ({ division, mb })).sort((a, b) => b.mb - a.mb);

  const recent = catalog.filter((e) => e._last_modified_raw).sort((a, b) => b._last_modified_raw - a._last_modified_raw).slice(0, 6);
  const actionByLayer = { Bronze: "was uploaded", Silver: "was cleaned in Prepare", Gold: "was created in Merge" };
  const activity = recent.map((e) => ({ dataset: e.display_name, action: actionByLayer[e.layer] || "was updated", time_ago: timeAgo(e._last_modified_raw) }));

  return {
    total_datasets: catalog.length,
    storage_used_mb: totalSizeMb,
    total_files: catalog.length,
    active_jobs: 0,
    user_count: 2,
    storage_by_division: storageList,
    recent_activity: activity,
  };
}

// ==========================================================================================
// NAV / ROUTER / TOAST / THEME  (unchanged app shell logic)
// ==========================================================================================

const NAV_ITEMS = [
  { id: "dashboard", label: "Dashboard", icon: "icon-dashboard" },
  { id: "datasets", label: "Datasets", icon: "icon-grid" },
  { id: "upload", label: "Upload", icon: "icon-upload" },
  { id: "prepare", label: "Prepare", icon: "icon-broom" },
  { id: "merge", label: "Merge", icon: "icon-merge", adminOnly: true },
  { id: "visualize", label: "Visualize", icon: "icon-bar-chart" },
];
let currentRole = "Administrator";
let allDatasets = [];
let selectedMergeJoinType = "inner";
let vizChartInstance = null;

function showToast(message, kind = "success") {
  const region = document.getElementById("toast-region");
  const toast = document.createElement("div");
  toast.className = "toast";
  const icon = kind === "error" ? "icon-x-circle" : "icon-check-circle";
  const colorVar = kind === "error" ? "--destructive" : "--success";
  toast.innerHTML = `<svg style="color:var(${colorVar})"><use href="#${icon}"/></svg><span>${message}</span>`;
  region.appendChild(toast);
  setTimeout(() => { toast.style.opacity = "0"; toast.style.transition = "opacity .2s"; setTimeout(() => toast.remove(), 200); }, 3200);
}

function visibleNavItems() { return NAV_ITEMS.filter((i) => !(i.adminOnly && currentRole !== "Administrator")); }

function renderNav(activeId) {
  const items = visibleNavItems();
  document.getElementById("sidebar-nav").innerHTML = items.map((item) => `
    <button class="nav-link" data-nav="${item.id}" ${item.id === activeId ? 'aria-current="page"' : ""}>
      <svg><use href="#${item.icon}"/></svg><span>${item.label}</span>
    </button>`).join("");
}

// Single delegated handler covers the sidebar links (re-rendered on every
// navigate()) AND the static dashboard control-panel/quick-action buttons
// (rendered once in the HTML) — avoids stacking duplicate listeners on the
// static buttons, and closes the mobile drawer after any nav click.
document.addEventListener("click", (e) => {
  const btn = e.target.closest("[data-nav]");
  if (!btn) return;
  window.location.hash = `#/${btn.getAttribute("data-nav")}`;
  closeSidebarDrawer();
});

function openSidebarDrawer() {
  document.getElementById("sidebar").classList.add("open");
  document.getElementById("sidebar-backdrop").classList.add("visible");
}
function closeSidebarDrawer() {
  document.getElementById("sidebar").classList.remove("open");
  document.getElementById("sidebar-backdrop").classList.remove("visible");
}
document.getElementById("hamburger-btn").addEventListener("click", () => {
  document.getElementById("sidebar").classList.contains("open") ? closeSidebarDrawer() : openSidebarDrawer();
});
document.getElementById("sidebar-backdrop").addEventListener("click", closeSidebarDrawer);

function currentPageId() {
  const hash = window.location.hash.replace("#/", "").trim();
  return NAV_ITEMS.some((i) => i.id === hash) ? hash : "dashboard";
}

const PAGE_LOADERS = { dashboard: loadDashboard, datasets: loadDatasets, upload: loadUpload, prepare: loadPrepare, merge: loadMerge, visualize: loadVisualize };

function navigate() {
  const pageId = currentPageId();
  const item = NAV_ITEMS.find((i) => i.id === pageId);
  if (item && item.adminOnly && currentRole !== "Administrator") { window.location.hash = "#/dashboard"; return; }

  document.querySelectorAll(".page").forEach((el) => (el.hidden = true));
  const section = document.getElementById(`page-${pageId}`);
  section.hidden = false;
  document.getElementById("page-title").textContent = section.dataset.title;
  document.getElementById("page-subtitle").textContent = section.dataset.subtitle;
  renderNav(pageId);

  if (pageId === "dashboard") renderConnBanner();
  if (!isConnected) return;

  const loader = PAGE_LOADERS[pageId];
  if (loader) loader();
}
window.addEventListener("hashchange", navigate);

function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  document.querySelector("#theme-toggle use").setAttribute("href", theme === "dark" ? "#icon-moon" : "#icon-sun");
}
document.getElementById("theme-toggle").addEventListener("click", () => {
  const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
  applyTheme(next);
  if (vizChartInstance) { vizChartInstance.destroy(); vizChartInstance = null; }
});

document.getElementById("user-menu").addEventListener("click", () => {
  currentRole = currentRole === "Administrator" ? "Employee" : "Administrator";
  document.getElementById("user-role-label").textContent = currentRole;
  document.getElementById("avatar-initials").textContent = currentRole === "Administrator" ? "A" : "E";
  showToast(`Switched to ${currentRole} view (UI demo only — not real access control)`);
  navigate();
});

// ==========================================================================================
// DASHBOARD
// ==========================================================================================

const STAT_ICONS = { datasets: "icon-database", storage: "icon-inbox", jobs: "icon-activity", users: "icon-users" };

async function loadDashboard() {
  const grid = document.getElementById("stat-grid");
  grid.innerHTML = `<div class="card stat-card"><p class="stat-label">Loading…</p></div>`.repeat(4);
  try {
    const stats = await computeStats();
    grid.innerHTML = `
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.datasets}"/></svg>Total datasets</div><div class="stat-value data-num">${stats.total_datasets}</div><div class="stat-trend">Across Bronze, Silver &amp; Gold</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.storage}"/></svg>Storage used</div><div class="stat-value data-num">${stats.storage_used_mb.toFixed(1)} MB</div><div class="stat-trend">${stats.total_files} files in the lake</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.jobs}"/></svg>Active jobs</div><div class="stat-value data-num">${stats.active_jobs}</div><div class="stat-trend">Job monitor arrives with Spark scheduling</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.users}"/></svg>Users</div><div class="stat-value data-num">${stats.user_count}</div><div class="stat-trend">Full accounts arrive with login (phase 2)</div></div>`;

    const storageEl = document.getElementById("storage-by-division");
    storageEl.innerHTML = stats.storage_by_division.length === 0
      ? `<p style="font-size:12.5px;color:var(--muted-foreground)">No uploads yet.</p>`
      : (() => {
        const maxVal = Math.max(...stats.storage_by_division.map((d) => d.mb), 1);
        return stats.storage_by_division.map((d) => `
            <div class="progress-item"><div class="progress-label"><span class="name">${d.division}</span><span class="data-num">${d.mb.toFixed(1)} MB</span></div><div class="progress-track"><div class="progress-fill" style="width:${(d.mb / maxVal) * 100}%"></div></div></div>`).join("");
      })();

    const activityEl = document.getElementById("activity-list");
    activityEl.innerHTML = stats.recent_activity.length === 0
      ? `<p style="font-size:12.5px;color:var(--muted-foreground)">Nothing here yet — upload a dataset to get started.</p>`
      : stats.recent_activity.map((a) => `<div class="activity-item"><span class="activity-dot"></span><div><div class="activity-text"><strong>${a.dataset}</strong> ${a.action}</div><div class="activity-time">${a.time_ago}</div></div></div>`).join("");
  } catch (e) {
    grid.innerHTML = `<div class="card card-pad" style="grid-column:1/-1"><p style="color:var(--destructive);font-size:13px">Could not load stats: ${e.message}</p></div>`;
  }
}

// ==========================================================================================
// DATASETS
// ==========================================================================================

function layerBadgeClass(layer) { return { Bronze: "badge-bronze", Silver: "badge-silver", Gold: "badge-gold" }[layer] || "badge-muted"; }
function statusBadgeClass(status) { return { Active: "badge-success", Processing: "badge-warning", Error: "badge-destructive" }[status] || "badge-muted"; }

function renderDatasetGrid(datasets) {
  const grid = document.getElementById("dataset-grid");
  if (datasets.length === 0) {
    grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1"><svg><use href="#icon-inbox"/></svg><h3>No datasets match</h3><p>Try a different search or filter, or upload a new dataset.</p></div>`;
    return;
  }
  grid.innerHTML = datasets.map((d) => `
    <div class="card dataset-card">
      <div class="dataset-card-top">
        <div class="dataset-card-badges"><span class="badge ${layerBadgeClass(d.layer)}">${d.layer}</span><span class="badge ${statusBadgeClass(d.status)}">${d.status}</span></div>
        <button class="icon-btn btn-sm" style="width:28px;height:28px" data-more="${d.layer}::${d.name}" aria-label="More actions"><svg style="width:14px;height:14px"><use href="#icon-more"/></svg></button>
      </div>
      <div><h4>${d.display_name || d.name}</h4><div class="ds-meta">${d.division} · ${d.owner}</div></div>
      <div class="ds-stats"><div class="ds-stat"><span class="n data-num">${d.rows.toLocaleString()}</span><span class="l">Rows</span></div><div class="ds-stat"><span class="n data-num">${d.columns}</span><span class="l">Columns</span></div></div>
      <div class="ds-meta">Updated ${d.last_updated}</div>
      <div class="dataset-card-actions">
        <button class="btn btn-secondary btn-sm" data-preview="${d.layer}::${d.name}" style="flex:1"><svg><use href="#icon-eye"/></svg>Preview</button>
        <button class="btn btn-secondary btn-sm" data-download="${d.layer}::${d.name}" style="flex:1"><svg><use href="#icon-download"/></svg>Download</button>
      </div>
    </div>`).join("");

  grid.querySelectorAll("[data-preview]").forEach((btn) => btn.addEventListener("click", () => {
    const [layer, name] = btn.getAttribute("data-preview").split("::");
    window.location.hash = "#/prepare";
    setTimeout(() => { document.getElementById("prepare-dataset").value = `${layer}::${name}`; document.getElementById("prepare-dataset").dispatchEvent(new Event("change")); }, 50);
  }));

  grid.querySelectorAll("[data-download]").forEach((btn) => btn.addEventListener("click", async () => {
    const [layer, name] = btn.getAttribute("data-download").split("::");
    try {
      const text = await getObjectText(objectKeyFor(layer, name));
      const blob = new Blob([text], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = `${name.split("__").pop()}.csv`;
      document.body.appendChild(a); a.click(); a.remove();
      URL.revokeObjectURL(url);
    } catch (e) { showToast(e.message, "error"); }
  }));

  grid.querySelectorAll("[data-more]").forEach((btn) => btn.addEventListener("click", async () => {
    const [layer, name] = btn.getAttribute("data-more").split("::");
    const choice = window.prompt(`"${name}" — type "rename" or "archive"`, "rename");
    if (choice === "rename") {
      const newName = window.prompt("New display name:", name);
      if (newName && newName.trim()) {
        try { const meta = await loadMeta(layer, name); meta.display_name = newName.trim(); await saveMeta(layer, name, meta); showToast("Dataset renamed"); loadDatasets(); }
        catch (e) { showToast(e.message, "error"); }
      }
    } else if (choice === "archive") {
      if (window.confirm(`Archive "${name}"? It will be hidden from the library.`)) {
        try { const meta = await loadMeta(layer, name); meta.archived = true; await saveMeta(layer, name, meta); showToast("Dataset archived"); loadDatasets(); }
        catch (e) { showToast(e.message, "error"); }
      }
    }
  }));
}

function applyDatasetFilters() {
  const term = document.getElementById("dataset-search").value.trim().toLowerCase();
  const layer = document.getElementById("filter-layer").value;
  const division = document.getElementById("filter-division").value;
  const filtered = allDatasets.filter((d) => {
    const matchesTerm = !term || d.name.toLowerCase().includes(term) || (d.display_name || "").toLowerCase().includes(term) || d.division.toLowerCase().includes(term);
    const matchesLayer = !layer || d.layer === layer;
    const matchesDivision = !division || d.division === division;
    return matchesTerm && matchesLayer && matchesDivision;
  });
  renderDatasetGrid(filtered);
}

async function loadDatasets() {
  const grid = document.getElementById("dataset-grid");
  grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1"><p>Loading datasets…</p></div>`;
  try {
    allDatasets = await listCatalog();
    const divisionSelect = document.getElementById("filter-division");
    const divisions = [...new Set(allDatasets.map((d) => d.division))].sort();
    divisionSelect.innerHTML = `<option value="">All divisions</option>` + divisions.map((d) => `<option value="${d}">${d}</option>`).join("");
    applyDatasetFilters();
  } catch (e) {
    grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1"><p style="color:var(--destructive)">Could not load datasets: ${e.message}</p></div>`;
  }
}

document.getElementById("dataset-search").addEventListener("input", applyDatasetFilters);
document.getElementById("filter-layer").addEventListener("change", applyDatasetFilters);
document.getElementById("filter-division").addEventListener("change", applyDatasetFilters);
document.getElementById("datasets-refresh").addEventListener("click", loadDatasets);

// ==========================================================================================
// UPLOAD
// ==========================================================================================

let selectedUploadFile = null;

function loadUpload() {
  const divSelect = document.getElementById("upload-division");
  divSelect.innerHTML = `<option value="">Select division…</option>` + DIVISIONS.map((d) => `<option value="${d.id}">${d.label}</option>`).join("");
  resetUploadForm();
}
function resetUploadForm() {
  selectedUploadFile = null;
  document.getElementById("file-chip-area").innerHTML = "";
  document.getElementById("validation-area").innerHTML = "";
  document.getElementById("upload-submit").disabled = true;
}

document.getElementById("upload-division").addEventListener("change", (e) => {
  const typeSelect = document.getElementById("upload-type");
  const division = DIVISION_LOOKUP[e.target.value];
  if (!division) { typeSelect.innerHTML = `<option value="">Select division first…</option>`; typeSelect.disabled = true; return; }
  typeSelect.disabled = false;
  typeSelect.innerHTML = `<option value="">Select dataset type…</option>` + division.dataset_types.map((t) => `<option value="${t.id}">${t.label}</option>`).join("");
});

const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("file-input");
dropzone.addEventListener("click", () => fileInput.click());
dropzone.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fileInput.click(); } });
["dragover", "dragenter"].forEach((evt) => dropzone.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.add("dragover"); }));
["dragleave", "drop"].forEach((evt) => dropzone.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.remove("dragover"); }));
dropzone.addEventListener("drop", (e) => { if (e.dataTransfer.files.length) handleFileSelected(e.dataTransfer.files[0]); });
fileInput.addEventListener("change", (e) => { if (e.target.files.length) handleFileSelected(e.target.files[0]); });

async function handleFileSelected(file) {
  selectedUploadFile = file;
  document.getElementById("file-chip-area").innerHTML = `<div class="file-chip"><span><svg style="width:14px;height:14px;vertical-align:-2px;margin-right:6px"><use href="#icon-file"/></svg>${file.name} — ${(file.size / 1024).toFixed(0)} KB</span></div>`;
  await runValidation();
}

async function runValidationChecks(division, dtype, file) {
  const dtypeConfig = datasetTypeLookup(division, dtype);
  if (!dtypeConfig) return { valid: false, checks: [{ ok: false, message: "Choose a division and dataset type first." }] };

  const text = await file.text();
  const firstLine = text.split(/\r?\n/, 1)[0] || "";
  if (!firstLine.trim()) return { valid: false, checks: [{ ok: false, message: "File is empty." }] };

  const header = firstLine.split(",").map((h) => h.trim());
  const checks = [];

  const seen = new Set(), dupes = new Set();
  header.forEach((h) => { if (seen.has(h)) dupes.add(h); seen.add(h); });
  checks.push(dupes.size
    ? { ok: false, message: `Duplicate column(s): ${[...dupes].join(", ")}` }
    : { ok: true, message: "No duplicate columns." });

  const missing = dtypeConfig.required_columns.filter((c) => !header.includes(c));
  checks.push(missing.length
    ? { ok: false, message: `Missing required column(s): ${missing.join(", ")}` }
    : { ok: true, message: `All required columns present (${dtypeConfig.required_columns.join(", ")}).` });

  const dataLineCount = text.split(/\r?\n/).filter((l) => l.trim()).length - 1;
  checks.push(dataLineCount > 0
    ? { ok: true, message: `${dataLineCount} data row(s) found.` }
    : { ok: false, message: "File has a header but no data rows." });

  return { valid: checks.every((c) => c.ok), checks };
}

async function runValidation() {
  const division = document.getElementById("upload-division").value;
  const type = document.getElementById("upload-type").value;
  const validationArea = document.getElementById("validation-area");
  const submitBtn = document.getElementById("upload-submit");

  if (!division || !type || !selectedUploadFile) { submitBtn.disabled = true; return; }
  validationArea.innerHTML = `<p style="font-size:12.5px;color:var(--muted-foreground)">Validating…</p>`;

  try {
    const result = await runValidationChecks(division, type, selectedUploadFile);
    validationArea.innerHTML = `<div class="validation-list">${result.checks.map((c) => `<div class="validation-row ${c.ok ? "ok" : "fail"}"><svg><use href="#${c.ok ? "icon-check-circle" : "icon-x-circle"}"/></svg><span>${c.message}</span></div>`).join("")}</div>`;
    submitBtn.disabled = !result.valid;
  } catch (e) {
    validationArea.innerHTML = `<div class="validation-row fail"><svg><use href="#icon-alert"/></svg><span>${e.message}</span></div>`;
    submitBtn.disabled = true;
  }
}
document.getElementById("upload-type").addEventListener("change", runValidation);

document.getElementById("upload-submit").addEventListener("click", async () => {
  const division = document.getElementById("upload-division").value;
  const dtype = document.getElementById("upload-type").value;
  const submitBtn = document.getElementById("upload-submit");
  submitBtn.disabled = true;
  submitBtn.textContent = "Uploading…";

  try {
    const stub = sanitizeName(selectedUploadFile.name.replace(/\.csv$/i, "")) + "_" + Date.now();
    const key = bronzeObjectKey(division, dtype, stub);
    await putObjectFile(key, selectedUploadFile);

    const name = `${division}__${dtype}__${stub}`;
    const dtypeConfig = datasetTypeLookup(division, dtype);
    await saveMeta("Bronze", name, { display_name: `${dtypeConfig.label} — ${selectedUploadFile.name}`, owner: "Workspace", archived: false });

    showToast("Dataset uploaded to Bronze");
    document.getElementById("upload-division").value = "";
    document.getElementById("upload-type").innerHTML = `<option value="">Select division first…</option>`;
    document.getElementById("upload-type").disabled = true;
    resetUploadForm();
  } catch (e) {
    showToast(e.message, "error");
    submitBtn.disabled = false;
  } finally {
    submitBtn.innerHTML = `<svg><use href="#icon-upload"/></svg> Upload dataset`;
  }
});

// ==========================================================================================
// PREPARE
// ==========================================================================================

const CLEANING_OPERATIONS = [
  { id: "trim", label: "Trim whitespace", desc: "Remove leading and trailing spaces from text columns." },
  { id: "normalize_case", label: "Normalize case", desc: "Make text columns consistently title case." },
  { id: "fix_types", label: "Fix data types", desc: "Convert numeric- or date-looking text columns to proper types." },
  { id: "dedupe", label: "Remove duplicates", desc: "Drop exact duplicate rows." },
  { id: "fill_missing", label: "Fill missing values", desc: "Replace empty cells with a sensible default." },
];

function renderPreviewTable(tableEl, columns, rows) {
  if (!columns || columns.length === 0) { tableEl.innerHTML = `<tr><td style="padding:16px;color:var(--muted-foreground)">No data to preview.</td></tr>`; return; }
  const head = `<thead><tr>${columns.map((c) => `<th>${c}</th>`).join("")}</tr></thead>`;
  const body = `<tbody>${rows.map((row) => `<tr>${columns.map((c) => `<td>${row[c] ?? ""}</td>`).join("")}</tr>`).join("")}</tbody>`;
  tableEl.innerHTML = head + body;
}

function updateOpCount() {
  const checked = document.querySelectorAll("#op-list input[type=checkbox]:checked").length;
  document.getElementById("op-count").textContent = checked;
  document.getElementById("prepare-accept").disabled = checked === 0 || !document.getElementById("prepare-dataset").value;
}

async function loadPrepare() {
  const opList = document.getElementById("op-list");
  opList.innerHTML = CLEANING_OPERATIONS.map((op) => `<label class="op-item"><input type="checkbox" data-op="${op.id}"><span><div class="op-label">${op.label}</div><div class="op-desc">${op.desc}</div></span></label>`).join("");
  opList.querySelectorAll("input[type=checkbox]").forEach((cb) => cb.addEventListener("change", updateOpCount));

  const select = document.getElementById("prepare-dataset");
  try {
    if (allDatasets.length === 0) allDatasets = await listCatalog();
    select.innerHTML = `<option value="">Choose a dataset…</option>` + allDatasets.map((d) => `<option value="${d.layer}::${d.name}">${d.layer} · ${d.display_name || d.name}</option>`).join("");
  } catch (e) { showToast(`Could not load datasets: ${e.message}`, "error"); }

  updateOpCount();
}

document.getElementById("prepare-dataset").addEventListener("change", async (e) => {
  const table = document.getElementById("prepare-preview-table");
  const sub = document.getElementById("prepare-preview-sub");
  updateOpCount();
  if (!e.target.value) { table.innerHTML = ""; sub.textContent = "First rows of the selected dataset"; return; }

  const [layer, name] = e.target.value.split("::");
  sub.textContent = "Loading…";
  try {
    const { fields, rows } = await readDataset(layer, name);
    renderPreviewTable(table, fields, rows.slice(0, 15));
    sub.textContent = `First ${Math.min(15, rows.length)} rows of ${rows.length.toLocaleString()}`;
  } catch (e2) { sub.textContent = `Could not load preview: ${e2.message}`; }
});

function applyCleaningOperations(fields, rows, operations) {
  let cleaned = rows.map((r) => ({ ...r }));

  if (operations.includes("trim")) {
    cleaned = cleaned.map((r) => { const c = { ...r }; for (const f of fields) if (typeof c[f] === "string") c[f] = c[f].trim(); return c; });
  }
  if (operations.includes("normalize_case")) {
    cleaned = cleaned.map((r) => { const c = { ...r }; for (const f of fields) if (typeof c[f] === "string") c[f] = c[f].replace(/\w\S*/g, (t) => t[0].toUpperCase() + t.slice(1).toLowerCase()); return c; });
  }
  if (operations.includes("fix_types")) {
    for (const f of fields) {
      const values = cleaned.map((r) => r[f]).filter((v) => v !== "" && v !== null && v !== undefined);
      if (values.length === 0) continue;
      const numericOk = values.filter((v) => !isNaN(Number(v))).length / values.length;
      if (numericOk > 0.8) { cleaned = cleaned.map((r) => ({ ...r, [f]: r[f] === "" || r[f] == null ? r[f] : Number(r[f]) })); continue; }
      const dateOk = values.filter((v) => !isNaN(Date.parse(v))).length / values.length;
      if (dateOk > 0.8) { cleaned = cleaned.map((r) => { const d = new Date(r[f]); return { ...r, [f]: isNaN(d) ? r[f] : d.toISOString().slice(0, 10) }; }); }
    }
  }
  if (operations.includes("dedupe")) {
    const seen = new Set();
    cleaned = cleaned.filter((r) => { const key = JSON.stringify(r); if (seen.has(key)) return false; seen.add(key); return true; });
  }
  if (operations.includes("fill_missing")) {
    const numericFields = new Set(fields.filter((f) => cleaned.some((r) => typeof r[f] === "number")));
    cleaned = cleaned.map((r) => { const c = { ...r }; for (const f of fields) if (c[f] === "" || c[f] === null || c[f] === undefined) c[f] = numericFields.has(f) ? 0 : ""; return c; });
  }
  return cleaned;
}

document.getElementById("prepare-accept").addEventListener("click", async () => {
  const btn = document.getElementById("prepare-accept");
  const [layer, name] = document.getElementById("prepare-dataset").value.split("::");
  const ops = [...document.querySelectorAll("#op-list input[type=checkbox]:checked")].map((cb) => cb.getAttribute("data-op"));
  const originalLabel = btn.innerHTML;
  btn.disabled = true;
  btn.textContent = "Saving…";

  try {
    const { fields, rows } = await readDataset(layer, name);
    const cleanedRows = applyCleaningOperations(fields, rows, ops);
    const csvText = Papa.unparse(cleanedRows, { columns: fields });

    const outputStub = sanitizeName(name.split("__").pop()) + "_cleaned";
    await putObjectText(`${SILVER_PREFIX}${outputStub}/data.csv`, csvText, "text/csv");

    const sourceMeta = await loadMeta(layer, name);
    await saveMeta("Silver", outputStub, { display_name: `${sourceMeta.display_name || name} (cleaned)`, owner: "Workspace", division_override: sourceMeta.division_override, archived: false });

    btn.innerHTML = `<svg style="width:15px;height:15px"><use href="#icon-check-circle"/></svg> Saved`;
    showToast(`Saved as "${outputStub}"`);
    allDatasets = [];
    setTimeout(() => { btn.innerHTML = originalLabel; updateOpCount(); }, 1800);
  } catch (e) {
    showToast(e.message, "error");
    btn.innerHTML = originalLabel;
    updateOpCount();
  }
});

// ==========================================================================================
// MERGE
// ==========================================================================================

async function loadMerge() {
  const selectA = document.getElementById("merge-a");
  const selectB = document.getElementById("merge-b");
  try {
    if (allDatasets.length === 0) allDatasets = await listCatalog();
    const options = `<option value="">Choose a dataset…</option>` + allDatasets.map((d) => `<option value="${d.layer}::${d.name}">${d.layer} · ${d.display_name || d.name}</option>`).join("");
    selectA.innerHTML = options;
    selectB.innerHTML = options;
  } catch (e) { showToast(`Could not load datasets: ${e.message}`, "error"); }
  document.getElementById("merge-preview-table").innerHTML = "";
  document.getElementById("merge-result-sub").textContent = "Pick two datasets and preview the merge";
}

document.querySelectorAll(".join-type-option").forEach((btn) => btn.addEventListener("click", () => {
  document.querySelectorAll(".join-type-option").forEach((b) => b.setAttribute("aria-pressed", "false"));
  btn.setAttribute("aria-pressed", "true");
  selectedMergeJoinType = btn.getAttribute("data-join");
}));

function joinDatasets(fieldsA, rowsA, fieldsB, rowsB, joinColumn, joinType) {
  const mapB = new Map();
  for (const r of rowsB) { const key = r[joinColumn]; if (!mapB.has(key)) mapB.set(key, []); mapB.get(key).push(r); }

  function combine(a, b) {
    const merged = { ...a };
    for (const f of fieldsB) {
      if (f === joinColumn) continue;
      merged[f in merged ? `${f}_b` : f] = b[f];
    }
    if (!(joinColumn in merged)) merged[joinColumn] = b[joinColumn];
    return merged;
  }

  const result = [];
  const matchedBKeys = new Set();
  for (const a of rowsA) {
    const matches = mapB.get(a[joinColumn]);
    if (matches && matches.length) {
      matchedBKeys.add(a[joinColumn]);
      for (const b of matches) result.push(combine(a, b));
    } else if (joinType === "left" || joinType === "outer") {
      result.push(combine(a, {}));
    }
  }
  if (joinType === "right" || joinType === "outer") {
    for (const b of rowsB) {
      if (!matchedBKeys.has(b[joinColumn])) result.push(combine({}, b));
    }
  }

  const columns = [...fieldsA];
  for (const f of fieldsB) { const key = f === joinColumn ? null : (fieldsA.includes(f) ? `${f}_b` : f); if (key && !columns.includes(key)) columns.push(key); }
  return { columns, rows: result };
}

document.getElementById("merge-preview-btn").addEventListener("click", async () => {
  const a = document.getElementById("merge-a").value;
  const b = document.getElementById("merge-b").value;
  const sub = document.getElementById("merge-result-sub");
  const table = document.getElementById("merge-preview-table");

  if (!a || !b) { showToast("Pick both Dataset A and Dataset B", "error"); return; }
  if (a === b) { showToast("Pick two different datasets", "error"); return; }

  const [layerA, nameA] = a.split("::");
  const [layerB, nameB] = b.split("::");
  sub.textContent = "Merging…";
  try {
    const { fields: fieldsA, rows: rowsA } = await readDataset(layerA, nameA);
    const { fields: fieldsB, rows: rowsB } = await readDataset(layerB, nameB);
    const common = fieldsA.filter((c) => fieldsB.includes(c));
    if (common.length === 0) { sub.textContent = "These datasets have no column in common to join on."; table.innerHTML = ""; return; }
    const joinColumn = common[0];

    const { columns, rows } = joinDatasets(fieldsA, rowsA, fieldsB, rowsB, joinColumn, selectedMergeJoinType);
    renderPreviewTable(table, columns, rows.slice(0, 15));
    sub.textContent = `${rows.length.toLocaleString()} row(s) in the preview, matched on "${joinColumn}"`;
  } catch (e) { sub.textContent = `Could not merge: ${e.message}`; }
});

// ==========================================================================================
// VISUALIZE
// ==========================================================================================

async function loadVisualize() {
  const select = document.getElementById("viz-dataset");
  try {
    if (allDatasets.length === 0) allDatasets = await listCatalog();
    select.innerHTML = `<option value="">Choose a dataset…</option>` + allDatasets.map((d) => `<option value="${d.layer}::${d.name}">${d.layer} · ${d.display_name || d.name}</option>`).join("");
  } catch (e) { showToast(`Could not load datasets: ${e.message}`, "error"); }
}

document.getElementById("viz-dataset").addEventListener("change", async (e) => {
  const xSelect = document.getElementById("viz-x");
  const ySelect = document.getElementById("viz-y");
  if (!e.target.value) { xSelect.innerHTML = ""; ySelect.innerHTML = ""; return; }
  const [layer, name] = e.target.value.split("::");
  try {
    const { fields, rows } = await readDataset(layer, name);
    const numeric = fields.filter((f) => rows.some((r) => typeof r[f] === "number"));
    xSelect.innerHTML = fields.map((c) => `<option value="${c}">${c}</option>`).join("");
    ySelect.innerHTML = numeric.map((c) => `<option value="${c}">${c}</option>`).join("");
  } catch (e2) { showToast(`Could not load columns: ${e2.message}`, "error"); }
});

function cssVar(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }

function renderVizChart(chartType, labels, values, valueLabel) {
  const ctx = document.getElementById("viz-canvas").getContext("2d");
  if (vizChartInstance) vizChartInstance.destroy();

  const accent = cssVar("--accent") || "#e4002b";
  const gridColor = cssVar("--border") || "#e5e7eb";
  const textColor = cssVar("--muted-foreground") || "#6b7280";
  const palette = [accent, "#2563eb", "#16a34a", "#d97706", "#7c3aed", "#0891b2", "#db2777"];

  vizChartInstance = new Chart(ctx, {
    type: chartType,
    data: { labels, datasets: [{ label: valueLabel || "Value", data: values, backgroundColor: chartType === "pie" ? palette : accent, borderColor: chartType === "line" ? accent : "transparent", borderRadius: chartType === "bar" ? 6 : 0, tension: 0.35, fill: chartType === "line" ? false : true }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: chartType === "pie", labels: { color: textColor, font: { family: "Inter" } } } },
      scales: chartType === "pie" ? {} : {
        x: { grid: { display: false }, ticks: { color: textColor, font: { family: "IBM Plex Mono", size: 11 } } },
        y: { grid: { color: gridColor }, ticks: { color: textColor, font: { family: "IBM Plex Mono", size: 11 } } },
      },
    },
  });
}

document.getElementById("viz-render-btn").addEventListener("click", async () => {
  const dsValue = document.getElementById("viz-dataset").value;
  const chartType = document.getElementById("viz-chart-type").value;
  const x = document.getElementById("viz-x").value;
  const y = document.getElementById("viz-y").value;
  if (!dsValue || !x || !y) { showToast("Choose a data source, category column and value column", "error"); return; }

  const [layer, name] = dsValue.split("::");
  try {
    const { rows } = await readDataset(layer, name);
    const totals = new Map();
    for (const r of rows) {
      const val = Number(r[y]);
      if (Number.isNaN(val)) continue;
      const key = r[x];
      totals.set(key, (totals.get(key) || 0) + val);
    }
    if (totals.size === 0) { showToast(`'${y}' doesn't have usable numbers to chart`, "error"); return; }
    const sorted = [...totals.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20);
    renderVizChart(chartType, sorted.map((e) => String(e[0])), sorted.map((e) => e[1]), y);
  } catch (e) { showToast(e.message, "error"); }
});

// ==========================================================================================
// INIT
// ==========================================================================================

(async function init() {
  applyTheme("light");
  renderNav("dashboard");
  renderConnBanner();
  await testConnection();
  navigate();
})();
