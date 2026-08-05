/* ==========================================================================================
   This file talks ONLY to our own Flask backend, via fetch() against /api/*.
   It never imports or calls MinIO/S3, PostgreSQL, or Spark directly, and it holds no
   business logic (validation rules, cleaning/merge semantics, chart aggregation) — that
   all lives server-side now. This file is display + form-handling + fetch calls only.
   ========================================================================================== */

// ---------------------------------------------------------- tiny API client

async function api(path, options = {}) {
  const opts = { credentials: "same-origin", ...options };
  if (opts.body && !(opts.body instanceof FormData)) {
    opts.headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
    opts.body = JSON.stringify(opts.body);
  }
  const res = await fetch(`/api${path}`, opts);

  if (res.status === 401) {
    showLoginOverlay();
    throw new Error("Session expired — please sign in again.");
  }

  let data = null;
  try { data = await res.json(); } catch (e) { /* no body */ }

  if (!res.ok) throw new Error((data && data.error) || res.statusText);
  return data;
}

const apiGet = (path) => api(path);
const apiPost = (path, body) => api(path, { method: "POST", body });

// ---------------------------------------------------------- auth / session

let currentUser = null;
let serverConfig = { divisions: [], cleaning_operations: [] };

function showLoginOverlay() {
  currentUser = null;
  document.getElementById("app-shell").hidden = true;
  document.getElementById("login-overlay").hidden = false;
}

function showAppShell() {
  document.getElementById("login-overlay").hidden = true;
  document.getElementById("app-shell").hidden = false;
}

async function checkSession() {
  const me = await apiGet("/me");
  if (me.authenticated) {
    currentUser = me;
    return true;
  }
  return false;
}

document.getElementById("login-submit").addEventListener("click", async () => {
  const username = document.getElementById("login-username").value.trim();
  const password = document.getElementById("login-password").value;
  const errorEl = document.getElementById("login-error");
  const btn = document.getElementById("login-submit");
  errorEl.textContent = "";
  if (!username || !password) { errorEl.textContent = "Enter a username and password."; return; }

  btn.disabled = true;
  btn.textContent = "Signing in…";
  try {
    currentUser = await apiPost("/login", { username, password });
    document.getElementById("login-password").value = "";
    await boot();
  } catch (e) {
    errorEl.textContent = e.message;
  } finally {
    btn.disabled = false;
    btn.textContent = "Sign in";
  }
});
document.getElementById("login-password").addEventListener("keydown", (e) => {
  if (e.key === "Enter") document.getElementById("login-submit").click();
});

document.getElementById("logout-btn").addEventListener("click", async () => {
  try { await apiPost("/logout", {}); } catch (e) { /* ignore */ }
  showLoginOverlay();
});

function isAdmin() { return currentUser && currentUser.role === "Administrator"; }

function renderUserMenu() {
  if (!currentUser) return;
  document.getElementById("avatar-initials").textContent = (currentUser.full_name || currentUser.username || "?").charAt(0).toUpperCase();
  document.getElementById("user-role-label").textContent = currentUser.role;
  document.getElementById("user-panel-name").textContent = currentUser.full_name || currentUser.username;
  document.getElementById("user-panel-role").textContent = currentUser.role;
}

document.getElementById("user-menu").addEventListener("click", () => {
  document.getElementById("user-panel").hidden = !document.getElementById("user-panel").hidden;
});

// ---------------------------------------------------------- backend connection status

async function refreshConnPill() {
  const pill = document.getElementById("conn-pill");
  const label = document.getElementById("conn-label");
  try {
    const health = await apiGet("/health");
    const ok = health.minio === true && health.postgres === true;
    pill.classList.toggle("connected", ok);
    pill.classList.toggle("error", !ok);
    label.textContent = ok ? "Connected" : "Backend issue";
    renderConnBanner(ok, health);
    return ok;
  } catch (e) {
    pill.classList.remove("connected");
    pill.classList.add("error");
    label.textContent = "Unreachable";
    renderConnBanner(false, null);
    return false;
  }
}

function renderConnBanner(ok, health) {
  const area = document.getElementById("conn-banner-area");
  if (!area) return;
  if (ok) { area.innerHTML = ""; return; }
  const detail = health ? `MinIO: ${health.minio} · PostgreSQL: ${health.postgres}` : "Could not reach the server.";
  area.innerHTML = `
    <div class="conn-banner">
      <svg><use href="#icon-alert"/></svg>
      <span>Backend storage isn't fully reachable right now. ${detail}</span>
    </div>`;
}

// ---------------------------------------------------------- NAV / ROUTER / TOAST / THEME

const NAV_ITEMS = [
  { id: "dashboard", label: "Dashboard", icon: "icon-dashboard" },
  { id: "datasets", label: "Datasets", icon: "icon-grid" },
  { id: "upload", label: "Upload", icon: "icon-upload" },
  { id: "prepare", label: "Prepare", icon: "icon-broom" },
  { id: "merge", label: "Merge", icon: "icon-merge", adminOnly: true },
  { id: "visualize", label: "Visualize", icon: "icon-bar-chart" },
];
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

function visibleNavItems() { return NAV_ITEMS.filter((i) => !(i.adminOnly && !isAdmin())); }

function renderNav(activeId) {
  const items = visibleNavItems();
  document.getElementById("sidebar-nav").innerHTML = items.map((item) => `
    <button class="nav-link" data-nav="${item.id}" ${item.id === activeId ? 'aria-current="page"' : ""}>
      <svg><use href="#${item.icon}"/></svg><span>${item.label}</span>
    </button>`).join("");
}

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
  if (item && item.adminOnly && !isAdmin()) { window.location.hash = "#/dashboard"; return; }

  document.querySelectorAll(".page").forEach((el) => (el.hidden = true));
  const section = document.getElementById(`page-${pageId}`);
  section.hidden = false;
  document.getElementById("page-title").textContent = section.dataset.title;
  document.getElementById("page-subtitle").textContent = section.dataset.subtitle;
  renderNav(pageId);

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

// ==========================================================================================
// DASHBOARD
// ==========================================================================================

const STAT_ICONS = { datasets: "icon-database", storage: "icon-inbox", jobs: "icon-activity", users: "icon-users" };

async function loadDashboard() {
  const grid = document.getElementById("stat-grid");
  grid.innerHTML = `<div class="card stat-card"><p class="stat-label">Loading…</p></div>`.repeat(4);
  try {
    const stats = await apiGet("/dashboard/stats");
    grid.innerHTML = `
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.datasets}"/></svg>Total datasets</div><div class="stat-value data-num">${stats.total_datasets}</div><div class="stat-trend">Across Bronze, Silver &amp; Gold</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.storage}"/></svg>Storage used</div><div class="stat-value data-num">${stats.storage_used_mb.toFixed(1)} MB</div><div class="stat-trend">${stats.total_files} files in the lake</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.jobs}"/></svg>Active jobs</div><div class="stat-value data-num">${stats.active_jobs}</div><div class="stat-trend">Job monitor arrives with Spark scheduling</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.users}"/></svg>Users</div><div class="stat-value data-num">${stats.user_count}</div><div class="stat-trend">${currentUser.role} — signed in as ${currentUser.username}</div></div>`;

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
        ${isAdmin() ? `<button class="icon-btn btn-sm" style="width:28px;height:28px" data-more="${d.layer}::${d.name}" aria-label="More actions"><svg style="width:14px;height:14px"><use href="#icon-more"/></svg></button>` : ""}
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

  grid.querySelectorAll("[data-download]").forEach((btn) => btn.addEventListener("click", () => {
    const [layer, name] = btn.getAttribute("data-download").split("::");
    const a = document.createElement("a");
    a.href = `/api/datasets/${encodeURIComponent(layer)}/${encodeURIComponent(name)}/download`;
    document.body.appendChild(a); a.click(); a.remove();
  }));

  grid.querySelectorAll("[data-more]").forEach((btn) => btn.addEventListener("click", async () => {
    const [layer, name] = btn.getAttribute("data-more").split("::");
    const choice = window.prompt(`"${name}" — type "rename" or "archive"`, "rename");
    if (choice === "rename") {
      const newName = window.prompt("New display name:", name);
      if (newName && newName.trim()) {
        try { await apiPost(`/datasets/${layer}/${name}/rename`, { display_name: newName.trim() }); showToast("Dataset renamed"); loadDatasets(); }
        catch (e) { showToast(e.message, "error"); }
      }
    } else if (choice === "archive") {
      if (window.confirm(`Archive "${name}"? It will be hidden from the library.`)) {
        try { await apiPost(`/datasets/${layer}/${name}/archive`, {}); showToast("Dataset archived"); loadDatasets(); }
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
    allDatasets = await apiGet("/datasets");
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
let selectedKeyColumns = [];
let currentDtypeConfig = null;

function loadUpload() {
  const divSelect = document.getElementById("upload-division");
  divSelect.innerHTML = `<option value="">Select division…</option>` + serverConfig.divisions.map((d) => `<option value="${d.id}">${d.label}</option>`).join("");
  document.getElementById("upload-summary-area").innerHTML = "";
  document.getElementById("upload-layout").hidden = false;
  resetUploadForm();
}
function resetUploadForm() {
  selectedUploadFile = null;
  selectedKeyColumns = [];
  currentDtypeConfig = null;
  document.getElementById("file-chip-area").innerHTML = "";
  document.getElementById("validation-area").innerHTML = "";
  document.getElementById("format-info-area").innerHTML = "";
  document.getElementById("dedupe-section").hidden = true;
  document.getElementById("upload-submit").disabled = true;
}

document.getElementById("upload-division").addEventListener("change", (e) => {
  const typeSelect = document.getElementById("upload-type");
  const division = serverConfig.divisions.find((d) => d.id === e.target.value);
  if (!division) { typeSelect.innerHTML = `<option value="">Select division first…</option>`; typeSelect.disabled = true; return; }
  typeSelect.disabled = false;
  typeSelect.innerHTML = `<option value="">Select dataset type…</option>` + division.dataset_types.map((t) => `<option value="${t.id}">${t.label}</option>`).join("");
  document.getElementById("dedupe-section").hidden = true;
  document.getElementById("format-info-area").innerHTML = "";
});

document.getElementById("upload-type").addEventListener("change", async (e) => {
  const division = serverConfig.divisions.find((d) => d.id === document.getElementById("upload-division").value);
  currentDtypeConfig = division ? division.dataset_types.find((t) => t.id === e.target.value) : null;

  const dedupeSection = document.getElementById("dedupe-section");
  const infoArea = document.getElementById("format-info-area");

  if (!currentDtypeConfig || currentDtypeConfig.skip_merge) {
    dedupeSection.hidden = true;
    infoArea.innerHTML = currentDtypeConfig
      ? `<p style="font-size:12px;color:var(--muted-foreground)">"Other Format" files are stored individually — no merge, no shared dataset.</p>`
      : "";
  } else {
    dedupeSection.hidden = false;
    renderKeyColumnPicker(currentDtypeConfig.required_columns);
    await refreshFormatInfo();
  }
  await runValidation();
});

document.getElementById("upload-delimiter").addEventListener("change", runValidation);

document.getElementById("dedupe-mode").addEventListener("change", () => {
  const needsKey = ["remove", "replace"].includes(document.getElementById("dedupe-mode").value);
  document.getElementById("key-column-section").style.display = needsKey ? "" : "none";
});

function renderKeyColumnPicker(columns) {
  selectedKeyColumns = columns.slice(0, 1); // sensible default: first required column
  const picker = document.getElementById("key-column-picker");
  picker.innerHTML = columns.map((c) => `<button type="button" class="key-chip" data-key-col="${c}" aria-pressed="${selectedKeyColumns.includes(c)}">${c}</button>`).join("");
  picker.querySelectorAll("[data-key-col]").forEach((chip) => {
    chip.addEventListener("click", () => {
      const col = chip.getAttribute("data-key-col");
      if (selectedKeyColumns.includes(col)) selectedKeyColumns = selectedKeyColumns.filter((c) => c !== col);
      else selectedKeyColumns.push(col);
      chip.setAttribute("aria-pressed", String(selectedKeyColumns.includes(col)));
    });
  });
}

async function refreshFormatInfo() {
  const infoArea = document.getElementById("format-info-area");
  const division = document.getElementById("upload-division").value;
  const dtype = document.getElementById("upload-type").value;
  infoArea.innerHTML = `<p style="font-size:12px;color:var(--muted-foreground)">Checking existing dataset…</p>`;
  try {
    const info = await apiGet(`/upload/format-info?division=${encodeURIComponent(division)}&dataset_type=${encodeURIComponent(dtype)}`);
    infoArea.innerHTML = info.has_master
      ? `<div class="info-panel-stat"><span class="label">Existing master dataset</span><span class="value data-num">${info.total_rows.toLocaleString()} rows · ${info.upload_count} upload(s)</span></div>`
      : `<p style="font-size:12px;color:var(--muted-foreground)">No master dataset yet for this format — this upload will create it.</p>`;
  } catch (e) {
    infoArea.innerHTML = "";
  }
}

const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("file-input");
dropzone.addEventListener("click", () => fileInput.click());
dropzone.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fileInput.click(); } });
["dragover", "dragenter"].forEach((evt) => dropzone.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.add("dragover"); }));
["dragleave", "drop"].forEach((evt) => dropzone.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.remove("dragover"); }));
dropzone.addEventListener("drop", (e) => { if (e.dataTransfer.files.length) handleFileSelected(e.dataTransfer.files[0]); });
fileInput.addEventListener("change", (e) => { if (e.target.files.length) handleFileSelected(e.target.files[0]); });

async function handleFileSelected(file) {
  if (!file.name.toLowerCase().endsWith(".csv")) {
    showToast("Only .csv files are accepted", "error");
    document.getElementById("file-input").value = "";
    return;
  }
  selectedUploadFile = file;
  document.getElementById("file-chip-area").innerHTML = `<div class="file-chip"><span><svg style="width:14px;height:14px;vertical-align:-2px;margin-right:6px"><use href="#icon-file"/></svg>${file.name} — ${(file.size / 1024).toFixed(0)} KB</span></div>`;
  await runValidation();
}

async function runValidation() {
  const division = document.getElementById("upload-division").value;
  const dtype = document.getElementById("upload-type").value;
  const validationArea = document.getElementById("validation-area");
  const submitBtn = document.getElementById("upload-submit");

  if (!division || !dtype || !selectedUploadFile) { submitBtn.disabled = true; return; }
  validationArea.innerHTML = `<p style="font-size:12.5px;color:var(--muted-foreground)">Validating…</p>`;

  try {
    const form = new FormData();
    form.append("division", division);
    form.append("dataset_type", dtype);
    form.append("delimiter", document.getElementById("upload-delimiter").value);
    form.append("file", selectedUploadFile);
    const result = await apiPost("/upload/validate", form);
    validationArea.innerHTML = `<div class="validation-list">${result.checks.map((c) => `<div class="validation-row ${c.ok ? "ok" : "fail"}"><svg><use href="#${c.ok ? "icon-check-circle" : "icon-x-circle"}"/></svg><span>${c.message}</span></div>`).join("")}</div>`;
    submitBtn.disabled = !result.valid;
  } catch (e) {
    validationArea.innerHTML = `<div class="validation-row fail"><svg><use href="#icon-alert"/></svg><span>${e.message}</span></div>`;
    submitBtn.disabled = true;
  }
}

function renderUploadSummary(result) {
  document.getElementById("upload-layout").hidden = true;
  const dupLabel = result.dedupe_mode === "replace" ? "Duplicate Rows Replaced" : "Duplicate Rows Removed";
  document.getElementById("upload-summary-area").innerHTML = `
    <div class="card card-pad upload-summary" style="max-width:640px;">
      <div class="upload-summary-header">
        <span class="icon-circle"><svg><use href="#icon-check-circle"/></svg></span>
        <div><h3 style="font-size:17px;">Upload Successful</h3><p class="card-sub">${result.format_label}</p></div>
      </div>
      <div class="upload-summary-grid">
        <div class="us-stat"><div class="n data-num">${result.rows_uploaded.toLocaleString()}</div><div class="l">Rows Uploaded</div></div>
        <div class="us-stat"><div class="n data-num">${result.rows_added.toLocaleString()}</div><div class="l">Rows Added</div></div>
        <div class="us-stat"><div class="n data-num">${result.duplicates_handled.toLocaleString()}</div><div class="l">${dupLabel}</div></div>
        <div class="us-stat"><div class="n data-num">${result.total_rows.toLocaleString()}</div><div class="l">Current Total Rows</div></div>
      </div>
      <span class="badge badge-success" style="align-self:flex-start;">${result.status}</span>
      <button class="btn btn-secondary" style="margin-top:16px;" id="upload-summary-done">Upload another file</button>
    </div>`;
  document.getElementById("upload-summary-done").addEventListener("click", loadUpload);
}

document.getElementById("upload-submit").addEventListener("click", async () => {
  const division = document.getElementById("upload-division").value;
  const dtype = document.getElementById("upload-type").value;
  const submitBtn = document.getElementById("upload-submit");
  submitBtn.disabled = true;
  submitBtn.textContent = "Uploading…";

  try {
    const form = new FormData();
    form.append("division", division);
    form.append("dataset_type", dtype);
    form.append("delimiter", document.getElementById("upload-delimiter").value);
    form.append("file", selectedUploadFile);
    if (currentDtypeConfig && !currentDtypeConfig.skip_merge) {
      form.append("dedupe_mode", document.getElementById("dedupe-mode").value);
      form.append("key_columns", selectedKeyColumns.join(","));
    }
    const result = await apiPost("/upload", form);

    if (result.merged) {
      renderUploadSummary({ ...result, dedupe_mode: document.getElementById("dedupe-mode").value });
    } else {
      showToast("Dataset uploaded to Bronze");
      resetUploadForm();
      document.getElementById("upload-division").value = "";
      document.getElementById("upload-type").innerHTML = `<option value="">Select division first…</option>`;
      document.getElementById("upload-type").disabled = true;
    }
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

let preparePreviewFields = [];
let prepareEdits = new Map(); // rowIndex -> { column: newValue }

function renderPreviewTable(tableEl, columns, rows, opts = {}) {
  const { editable = false, onDelete = null } = opts;
  if (!columns || columns.length === 0) { tableEl.innerHTML = `<tr><td style="padding:16px;color:var(--muted-foreground)">No data to preview.</td></tr>`; return; }
  const head = `<thead><tr>${columns.map((c) => `<th>${c}</th>`).join("")}${editable ? "<th></th>" : ""}</tr></thead>`;
  const body = `<tbody>${rows.map((row, i) => `<tr data-row-index="${i}">${columns.map((c) => `<td ${editable ? `contenteditable="true" data-col="${c}"` : ""}>${row[c] ?? ""}</td>`).join("")}${editable ? `<td><button class="icon-btn btn-sm" data-delete-row="${i}" title="Delete row"><svg style="width:13px;height:13px"><use href="#icon-x-circle"/></svg></button></td>` : ""}</tr>`).join("")}</tbody>`;
  tableEl.innerHTML = head + body;

  if (editable) {
    tableEl.querySelectorAll("td[contenteditable]").forEach((cell) => {
      cell.addEventListener("blur", () => {
        const rowIndex = Number(cell.closest("tr").getAttribute("data-row-index"));
        const col = cell.getAttribute("data-col");
        const existing = prepareEdits.get(rowIndex) || {};
        existing[col] = cell.textContent;
        prepareEdits.set(rowIndex, existing);
        document.getElementById("prepare-save-edits-btn").hidden = false;
      });
    });
    tableEl.querySelectorAll("[data-delete-row]").forEach((btn) => btn.addEventListener("click", () => onDelete && onDelete(Number(btn.getAttribute("data-delete-row")))));
  }
}

function updateOpCount() {
  const checked = document.querySelectorAll("#op-list input[type=checkbox]:checked").length;
  document.getElementById("op-count").textContent = checked;
  document.getElementById("prepare-accept").disabled = checked === 0 || !document.getElementById("prepare-dataset").value;
}

async function loadPrepare() {
  const opList = document.getElementById("op-list");
  opList.innerHTML = serverConfig.cleaning_operations.map((op) => `<label class="op-item"><input type="checkbox" data-op="${op.id}"><span><div class="op-label">${op.label}</div><div class="op-desc">${op.desc}</div></span></label>`).join("");
  opList.querySelectorAll("input[type=checkbox]").forEach((cb) => cb.addEventListener("change", updateOpCount));

  const select = document.getElementById("prepare-dataset");
  try {
    if (allDatasets.length === 0) allDatasets = await apiGet("/datasets");
    select.innerHTML = `<option value="">Choose a dataset…</option>` + allDatasets.map((d) => `<option value="${d.layer}::${d.name}">${d.layer} · ${d.display_name || d.name}</option>`).join("");
  } catch (e) { showToast(`Could not load datasets: ${e.message}`, "error"); }

  ensureAdminRowToolsUi();
  updateOpCount();
}

function ensureAdminRowToolsUi() {
  if (!isAdmin() || document.getElementById("prepare-save-edits-btn")) return;
  const previewCard = document.getElementById("prepare-preview-table").closest(".card");
  const bar = document.createElement("div");
  bar.style.cssText = "margin-top:10px; display:flex; gap:8px; align-items:center;";
  bar.innerHTML = `
    <button class="btn btn-secondary btn-sm" id="prepare-save-edits-btn" hidden>Save row edits</button>
    <span style="font-size:11.5px;color:var(--muted-foreground)">Administrator tools — edit any cell, or delete a row with the × button.</span>`;
  previewCard.appendChild(bar);
  document.getElementById("prepare-save-edits-btn").addEventListener("click", saveRowEdits);
}

async function saveRowEdits() {
  const value = document.getElementById("prepare-dataset").value;
  if (!value || prepareEdits.size === 0) return;
  const [layer, name] = value.split("::");
  const btn = document.getElementById("prepare-save-edits-btn");
  btn.disabled = true;
  try {
    for (const [rowIndex, updates] of prepareEdits.entries()) {
      await apiPost("/admin/update-row", { layer, name, row_index: rowIndex, updates });
    }
    showToast("Row edits saved");
    prepareEdits.clear();
    btn.hidden = true;
    document.getElementById("prepare-dataset").dispatchEvent(new Event("change"));
  } catch (e) {
    showToast(e.message, "error");
  } finally {
    btn.disabled = false;
  }
}

async function deletePrepareRow(layer, name, rowIndex) {
  if (!window.confirm(`Delete row ${rowIndex + 1}? This cannot be undone.`)) return;
  try {
    await apiPost("/admin/delete-row", { layer, name, row_index: rowIndex });
    showToast("Row deleted");
    prepareEdits.clear();
    document.getElementById("prepare-dataset").dispatchEvent(new Event("change"));
  } catch (e) { showToast(e.message, "error"); }
}

document.getElementById("prepare-dataset").addEventListener("change", async (e) => {
  const table = document.getElementById("prepare-preview-table");
  const sub = document.getElementById("prepare-preview-sub");
  updateOpCount();
  prepareEdits.clear();
  const saveBtn = document.getElementById("prepare-save-edits-btn");
  if (saveBtn) saveBtn.hidden = true;

  if (!e.target.value) { table.innerHTML = ""; sub.textContent = "First rows of the selected dataset"; return; }

  const [layer, name] = e.target.value.split("::");
  sub.textContent = "Loading…";
  try {
    const { fields, rows, total_rows } = await apiGet(`/datasets/${layer}/${name}/preview?limit=15`);
    preparePreviewFields = fields;
    renderPreviewTable(table, fields, rows, {
      editable: isAdmin(),
      onDelete: (rowIndex) => deletePrepareRow(layer, name, rowIndex),
    });
    sub.textContent = `First ${Math.min(15, total_rows)} rows of ${total_rows.toLocaleString()}`;
  } catch (e2) { sub.textContent = `Could not load preview: ${e2.message}`; }
});

document.getElementById("prepare-accept").addEventListener("click", async () => {
  const btn = document.getElementById("prepare-accept");
  const [layer, name] = document.getElementById("prepare-dataset").value.split("::");
  const ops = [...document.querySelectorAll("#op-list input[type=checkbox]:checked")].map((cb) => cb.getAttribute("data-op"));
  const originalLabel = btn.innerHTML;
  btn.disabled = true;
  btn.textContent = "Saving…";

  try {
    const result = await apiPost("/etl/clean", { layer, name, operations: ops });
    btn.innerHTML = `<svg style="width:15px;height:15px"><use href="#icon-check-circle"/></svg> Saved`;
    showToast(`Saved as "${result.name}"`);
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
    if (allDatasets.length === 0) allDatasets = await apiGet("/datasets");
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

document.getElementById("merge-preview-btn").addEventListener("click", async () => {
  const a = document.getElementById("merge-a").value;
  const b = document.getElementById("merge-b").value;
  const sub = document.getElementById("merge-result-sub");
  const table = document.getElementById("merge-preview-table");

  if (!a || !b) { showToast("Pick both Dataset A and Dataset B", "error"); return; }
  if (a === b) { showToast("Pick two different datasets", "error"); return; }

  sub.textContent = "Merging…";
  try {
    const result = await apiPost("/merge/preview", { a, b, join_type: selectedMergeJoinType });
    renderPreviewTable(table, result.columns, result.rows);
    sub.textContent = `${result.total_rows.toLocaleString()} row(s) in the preview, matched on "${result.join_column}"`;
  } catch (e) { sub.textContent = `Could not merge: ${e.message}`; }
});

// ==========================================================================================
// VISUALIZE
// ==========================================================================================

async function loadVisualize() {
  const select = document.getElementById("viz-dataset");
  try {
    if (allDatasets.length === 0) allDatasets = await apiGet("/datasets");
    select.innerHTML = `<option value="">Choose a dataset…</option>` + allDatasets.map((d) => `<option value="${d.layer}::${d.name}">${d.layer} · ${d.display_name || d.name}</option>`).join("");
  } catch (e) { showToast(`Could not load datasets: ${e.message}`, "error"); }
}

document.getElementById("viz-dataset").addEventListener("change", async (e) => {
  const xSelect = document.getElementById("viz-x");
  const ySelect = document.getElementById("viz-y");
  if (!e.target.value) { xSelect.innerHTML = ""; ySelect.innerHTML = ""; return; }
  const [layer, name] = e.target.value.split("::");
  try {
    const { fields } = await apiGet(`/datasets/${layer}/${name}/preview?limit=1`);
    xSelect.innerHTML = fields.map((c) => `<option value="${c}">${c}</option>`).join("");
    ySelect.innerHTML = fields.map((c) => `<option value="${c}">${c}</option>`).join("");
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
    const { labels, values } = await apiPost("/visualize/aggregate", { layer, name, x, y });
    renderVizChart(chartType, labels, values, y);
  } catch (e) { showToast(e.message, "error"); }
});

// ==========================================================================================
// INIT
// ==========================================================================================

async function boot() {
  showAppShell();
  renderUserMenu();
  renderNav("dashboard");
  try {
    serverConfig = await apiGet("/config");
  } catch (e) {
    showToast(`Could not load configuration: ${e.message}`, "error");
  }
  await refreshConnPill();
  navigate();
}

(async function init() {
  applyTheme("light");
  showLoginOverlay();
  try {
    const authed = await checkSession();
    if (authed) await boot();
  } catch (e) {
    // stay on login overlay
  }
})();
