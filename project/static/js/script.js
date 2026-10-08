/* ==========================================================================================
   This file talks ONLY to our own Flask backend, via fetch() against /api/*.
   It never imports or calls MinIO/S3, PostgreSQL, or Spark directly, and it holds no
   business logic (validation rules, cleaning/merge semantics, chart aggregation) — that
   all lives server-side now. This file is display + form-handling + fetch calls only.
   ========================================================================================== */

// ---------------------------------------------------------- tiny API client

async function api(path, options = {}) {
  // timeoutMs (optional): give up and throw a "Time limit error" if the request
  // (including reading its body) hasn't finished in time. Used by the Visualize
  // page; every other caller omits it and behaves exactly as before.
  const { timeoutMs, ...fetchOptions } = options;
  const opts = { credentials: "same-origin", ...fetchOptions };
  if (opts.body && !(opts.body instanceof FormData)) {
    opts.headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
    opts.body = JSON.stringify(opts.body);
  }
  let timer = null;
  if (timeoutMs) {
    const controller = new AbortController();
    opts.signal = controller.signal;
    timer = setTimeout(() => controller.abort(), timeoutMs);
  }
  try {
    return await apiRequest(path, opts);
  } catch (e) {
    if (e && e.name === "AbortError" && timeoutMs) {
      const err = new Error(`Time limit error: this took longer than ${Math.round(timeoutMs / 1000)} seconds and was stopped. Try a smaller dataset, different columns, or try again later.`);
      err.isTimeout = true;
      throw err;
    }
    throw e;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function apiRequest(path, opts) {
  const res = await fetch(`/api${path}`, opts);

  // A 401 almost always means "your session expired, please log back in" —
  // except when the request WAS the login attempt itself, where a 401 means
  // "wrong username/password" and should show that message, not this one.
  if (res.status === 401 && path !== "/login") {
    showLoginOverlay();
    throw new Error("Session expired — please sign in again.");
  }

  let data = null;
  try { data = await res.json(); } catch (e) { /* no body */ }

  if (!res.ok) {
    const err = new Error((data && data.error) || res.statusText);
    // IMPORTANT: Flask-Limiter (headers_enabled=True) attaches a Retry-After
    // header to EVERY response on a rate-limited route — including normal,
    // well-within-budget failures — as general informational metadata, not
    // just when a request is actually blocked. Only status 429 means "you
    // are currently rate-limited"; checking for the header's mere presence
    // on any error (e.g. a plain 401 wrong-password) was the actual bug.
    if (res.status === 429) {
      const retryAfter = res.headers.get("Retry-After");
      if (retryAfter && /^\d+$/.test(retryAfter)) err.retryAfter = parseInt(retryAfter, 10);
    }
    throw err;
  }
  return data;
}

const apiGet = (path) => api(path);
const apiPost = (path, body, extra = {}) => api(path, { method: "POST", body, ...extra });

// Visualize page only: how long a chart/summary request may run before it is
// stopped with a "Time limit error". Change this one number to adjust it.
const VIZ_TIMEOUT_MS = 30 * 1000;

// ---------------------------------------------------------- HTML escaping
// Anything that came from user-controlled data (CSV headers/cells, uploaded
// filenames, display names, dataset names, etc.) MUST be passed through this
// before being interpolated into an innerHTML template string, whether it's
// going into text content or into a quoted HTML attribute.
function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[ch]));
}

// ---------------------------------------------------------- auth / session

let currentUser = null;
let serverConfig = { cleaning_operations: [] };

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

let loginLockInterval = null;
function startLoginLockCountdown(seconds) {
  const errorEl = document.getElementById("login-error");
  const btn = document.getElementById("login-submit");
  clearInterval(loginLockInterval);

  let remaining = seconds;
  const render = () => {
    const m = Math.floor(remaining / 60);
    const s = remaining % 60;
    errorEl.textContent = `Too many attempts — try again in ${m}:${String(s).padStart(2, "0")}`;
  };

  btn.disabled = true;
  render();
  loginLockInterval = setInterval(() => {
    remaining -= 1;
    if (remaining <= 0) {
      clearInterval(loginLockInterval);
      loginLockInterval = null;
      errorEl.textContent = "";
      btn.disabled = false;
      return;
    }
    render();
  }, 1000);
}

document.getElementById("login-submit").addEventListener("click", async () => {
  // Always clear any countdown left running from a previous attempt first —
  // otherwise its interval keeps firing every second and silently overwrites
  // whatever message THIS attempt's real server response should be showing.
  clearInterval(loginLockInterval);
  loginLockInterval = null;

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
    btn.disabled = false;
    btn.textContent = "Sign in";
  } catch (e) {
    btn.textContent = "Sign in";
    if (e.retryAfter) {
      startLoginLockCountdown(e.retryAfter); // keeps the button disabled until it hits 0
    } else {
      errorEl.textContent = e.message;
      btn.disabled = false;
    }
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
  { id: "chat", label: "AI Assistant", icon: "icon-sparkles" },
  { id: "approvals", label: "Approvals", icon: "icon-inbox", adminOnly: true },
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
  toast.innerHTML = `<svg style="color:var(${colorVar})"><use href="#${icon}"/></svg><span>${escapeHtml(message)}</span>`;
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
  if (isMobileWidth()) closeSidebarDrawer();
});

function isMobileWidth() { return window.innerWidth <= 768; }

function openSidebarDrawer() {
  document.getElementById("sidebar").classList.add("open");
  document.querySelector(".main-area").classList.add("sidebar-open");
  if (isMobileWidth()) document.getElementById("sidebar-backdrop").classList.add("visible");
}
function closeSidebarDrawer() {
  document.getElementById("sidebar").classList.remove("open");
  document.querySelector(".main-area").classList.remove("sidebar-open");
  document.getElementById("sidebar-backdrop").classList.remove("visible");
}
document.getElementById("hamburger-btn").addEventListener("click", () => {
  document.getElementById("sidebar").classList.contains("open") ? closeSidebarDrawer() : openSidebarDrawer();
});
document.getElementById("sidebar-backdrop").addEventListener("click", closeSidebarDrawer);

// Sidebar starts open on desktop (matches the previous always-visible
// sidebar) and closed on mobile (matches the previous drawer-on-demand
// behavior) — the hamburger button now genuinely does something at every
// width instead of being a decorative, non-functional icon on desktop.
if (isMobileWidth()) closeSidebarDrawer(); else openSidebarDrawer();

let wasMobileWidth = isMobileWidth();
window.addEventListener("resize", () => {
  const nowMobile = isMobileWidth();
  if (nowMobile === wasMobileWidth) return; // only react when actually crossing the breakpoint
  wasMobileWidth = nowMobile;
  nowMobile ? closeSidebarDrawer() : openSidebarDrawer();
});

function currentPageId() {
  const hash = window.location.hash.replace("#/", "").trim();
  return NAV_ITEMS.some((i) => i.id === hash) ? hash : "dashboard";
}

const PAGE_LOADERS = { dashboard: loadDashboard, datasets: loadDatasets, upload: loadUpload, prepare: loadPrepare, merge: loadMerge, visualize: loadVisualize, chat: loadChat, approvals: loadApprovals };

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
  // The four summary cards (and the Users card in particular, which shows
  // total user count + who's logged in) are admin-only info — non-admins
  // don't see this row at all.
  grid.hidden = !isAdmin();
  if (isAdmin()) grid.innerHTML = `<div class="card stat-card"><p class="stat-label">Loading…</p></div>`.repeat(4);
  try {
    const stats = await apiGet("/dashboard/stats");
    if (isAdmin()) {
      grid.innerHTML = `
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.datasets}"/></svg>Total datasets</div><div class="stat-value data-num">${stats.total_datasets}</div><div class="stat-trend">In the lakehouse</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.storage}"/></svg>Storage used</div><div class="stat-value data-num">${stats.storage_used_mb.toFixed(1)} MB</div><div class="stat-trend">${stats.total_files} files in the lake</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.jobs}"/></svg>Active jobs</div><div class="stat-value data-num">${stats.active_jobs}</div><div class="stat-trend">Job monitor arrives with Spark scheduling</div></div>
      <div class="card stat-card"><div class="stat-label"><svg><use href="#${STAT_ICONS.users}"/></svg>Users</div><div class="stat-value data-num">${stats.user_count}</div><div class="stat-trend">${currentUser.role} — signed in as ${currentUser.username}</div></div>`;
    }

    const storageEl = document.getElementById("storage-by-division");
    storageEl.innerHTML = stats.storage_by_dataset.length === 0
      ? `<p style="font-size:12.5px;color:var(--muted-foreground)">No uploads yet.</p>`
      : (() => {
        const maxVal = Math.max(...stats.storage_by_dataset.map((d) => d.mb), 1);
        return stats.storage_by_dataset.map((d) => `
            <div class="progress-item"><div class="progress-label"><span class="name">${escapeHtml(d.dataset)}</span><span class="data-num">${d.mb.toFixed(1)} MB</span></div><div class="progress-track"><div class="progress-fill" style="width:${(d.mb / maxVal) * 100}%"></div></div></div>`).join("");
      })();

    const activityEl = document.getElementById("activity-list");
    activityEl.innerHTML = stats.recent_activity.length === 0
      ? `<p style="font-size:12.5px;color:var(--muted-foreground)">Nothing here yet — upload a dataset to get started.</p>`
      : stats.recent_activity.map((a) => `<div class="activity-item"><span class="activity-dot"></span><div><div class="activity-text"><strong>${escapeHtml(a.dataset)}</strong> ${escapeHtml(a.action)}</div><div class="activity-time">${escapeHtml(a.time_ago)}</div></div></div>`).join("");
  } catch (e) {
    grid.innerHTML = `<div class="card card-pad" style="grid-column:1/-1"><p style="color:var(--destructive);font-size:13px">Could not load stats: ${e.message}</p></div>`;
  }
}

// ==========================================================================================
// DATASETS
// ==========================================================================================

function statusBadgeClass(status) { return { Active: "badge-success", Processing: "badge-warning", Error: "badge-destructive" }[status] || "badge-muted"; }

function renderDatasetGrid(datasets) {
  const grid = document.getElementById("dataset-grid");
  if (datasets.length === 0) {
    grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1"><svg><use href="#icon-inbox"/></svg><h3>No datasets match</h3><p>Try a different search, or upload a new dataset.</p></div>`;
    return;
  }
  grid.innerHTML = datasets.map((d) => {
    const key = escapeHtml(d.dataset_id);
    return `
    <div class="card dataset-card">
      <div class="dataset-card-top">
        <div class="dataset-card-badges"><span class="badge ${statusBadgeClass(d.status)}">${escapeHtml(d.status)}</span></div>
        ${isAdmin() ? `<button class="icon-btn btn-sm" style="width:28px;height:28px" data-more="${key}" aria-label="More actions"><svg style="width:14px;height:14px"><use href="#icon-more"/></svg></button>` : ""}
      </div>
      <div><h4>${escapeHtml(d.display_name || d.dataset_id)}</h4><div class="ds-meta">${escapeHtml(d.owner)}</div></div>
      <div class="ds-stats"><div class="ds-stat"><span class="n data-num">${d.rows.toLocaleString()}</span><span class="l">Rows</span></div><div class="ds-stat"><span class="n data-num">${d.columns}</span><span class="l">Columns</span></div></div>
      <div class="ds-meta">Updated ${escapeHtml(d.last_updated)}</div>
      <div class="dataset-card-actions">
        <button class="btn btn-secondary btn-sm" data-preview="${key}" style="flex:1"><svg><use href="#icon-eye"/></svg>Preview</button>
        <button class="btn btn-secondary btn-sm" data-download="${key}" style="flex:1"><svg><use href="#icon-download"/></svg>Download</button>
      </div>
    </div>`;
  }).join("");

  grid.querySelectorAll("[data-preview]").forEach((btn) => btn.addEventListener("click", () => {
    const datasetId = btn.getAttribute("data-preview");
    window.location.hash = "#/prepare";
    setTimeout(() => { document.getElementById("prepare-dataset").value = datasetId; document.getElementById("prepare-dataset").dispatchEvent(new Event("change")); }, 50);
  }));

  grid.querySelectorAll("[data-download]").forEach((btn) => btn.addEventListener("click", () => {
    const datasetId = btn.getAttribute("data-download");
    const a = document.createElement("a");
    a.href = `/api/datasets/${encodeURIComponent(datasetId)}/download`;
    document.body.appendChild(a); a.click(); a.remove();
  }));

  grid.querySelectorAll("[data-more]").forEach((btn) => btn.addEventListener("click", async () => {
    const datasetId = btn.getAttribute("data-more");
    const choice = window.prompt(`"${datasetId}" — type "rename", "archive", or "delete"`, "rename");
    if (choice === "rename") {
      const newName = window.prompt("New display name:", datasetId);
      if (newName && newName.trim()) {
        try {
          const result = await apiPost(`/datasets/${datasetId}/rename`, { display_name: newName.trim() });
          showToast(result.status === "pending_approval" ? "Rename requested — waiting on an Administrator's approval" : "Dataset renamed");
          loadDatasets();
        } catch (e) { showToast(e.message, "error"); }
      }
    } else if (choice === "archive") {
      if (window.confirm(`Archive "${datasetId}"? It will be hidden from the library.`)) {
        try { await apiPost(`/datasets/${datasetId}/archive`, {}); showToast("Dataset archived"); loadDatasets(); }
        catch (e) { showToast(e.message, "error"); }
      }
    } else if (choice === "delete") {
      // Deliberately a stronger warning than archive's — this actually
      // removes the data, it doesn't just hide it, and can't be undone.
      if (window.confirm(`Permanently delete "${datasetId}"? This CANNOT be undone — the data itself will be gone, not just hidden from the library.`)) {
        try { await apiPost(`/datasets/${datasetId}/delete`, {}); showToast("Dataset deleted"); loadDatasets(); }
        catch (e) { showToast(e.message, "error"); }
      }
    }
  }));
}

function applyDatasetFilters() {
  const term = document.getElementById("dataset-search").value.trim().toLowerCase();
  const filtered = allDatasets.filter((d) => {
    return !term || d.dataset_id.toLowerCase().includes(term) || (d.display_name || "").toLowerCase().includes(term);
  });
  renderDatasetGrid(filtered);
}

async function loadDatasets() {
  const grid = document.getElementById("dataset-grid");
  grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1"><p>Loading datasets…</p></div>`;
  try {
    allDatasets = await apiGet("/datasets");
    applyDatasetFilters();
  } catch (e) {
    grid.innerHTML = `<div class="empty-state" style="grid-column:1/-1"><p style="color:var(--destructive)">Could not load datasets: ${e.message}</p></div>`;
  }
}

document.getElementById("dataset-search").addEventListener("input", applyDatasetFilters);
document.getElementById("datasets-refresh").addEventListener("click", loadDatasets);

// ==========================================================================================
// UPLOAD
// ==========================================================================================

let selectedUploadFile = null;
let delimiterOverride = null; // null = auto-detect (normal case); set only via the fallback picker below
let selectedKeyColumns = []; // row-identity column(s) the user explicitly picked this upload; empty = let the server default

const DELIM_LABELS = { ",": "Comma ( , )", ";": "Semicolon ( ; )", "\t": "Tab", "|": "Pipe ( | )" };

function loadUpload() {
  document.getElementById("upload-summary-area").innerHTML = "";
  document.getElementById("upload-layout").hidden = false;
  resetUploadForm();
}
function resetUploadForm() {
  selectedUploadFile = null;
  delimiterOverride = null;
  selectedKeyColumns = [];
  document.getElementById("file-chip-area").innerHTML = "";
  document.getElementById("validation-area").innerHTML = "";
  document.getElementById("upload-new-name").value = "";
  document.getElementById("upload-submit").disabled = true;
  document.getElementById("delimiter-override-area").hidden = true;
  document.querySelectorAll("#delimiter-override-group .delim-choice-option").forEach((b) => b.setAttribute("aria-pressed", "false"));
  document.getElementById("key-column-area").hidden = true;
  document.getElementById("key-column-picker").innerHTML = "";
}

// Renders one toggle chip per column the just-validated file actually has
// (validate_upload's response now includes "columns" for exactly this).
// Toggling a chip adds/removes it from selectedKeyColumns, in click order,
// so a compound key ends up in the order the user picked it — that order
// doesn't matter to the backend (it's used as a set) but reads naturally.
function renderKeyColumnPicker(columns) {
  const area = document.getElementById("key-column-area");
  const picker = document.getElementById("key-column-picker");
  if (!columns || !columns.length) { area.hidden = true; picker.innerHTML = ""; return; }

  selectedKeyColumns = selectedKeyColumns.filter((c) => columns.includes(c));
  area.hidden = false;
  picker.innerHTML = columns.map((c) => `<button type="button" class="key-chip" data-col="${escapeHtml(c)}" aria-pressed="${selectedKeyColumns.includes(c)}">${escapeHtml(c)}</button>`).join("");
}

document.getElementById("key-column-picker").addEventListener("click", (e) => {
  const btn = e.target.closest(".key-chip");
  if (!btn) return;
  const col = btn.getAttribute("data-col");
  if (selectedKeyColumns.includes(col)) {
    selectedKeyColumns = selectedKeyColumns.filter((c) => c !== col);
    btn.setAttribute("aria-pressed", "false");
  } else {
    selectedKeyColumns.push(col);
    btn.setAttribute("aria-pressed", "true");
  }
});

const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("file-input");
dropzone.addEventListener("click", () => fileInput.click());
dropzone.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fileInput.click(); } });
["dragover", "dragenter"].forEach((evt) => dropzone.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.add("dragover"); }));
["dragleave", "drop"].forEach((evt) => dropzone.addEventListener(evt, (e) => { e.preventDefault(); dropzone.classList.remove("dragover"); }));
dropzone.addEventListener("drop", (e) => { if (e.dataTransfer.files.length) handleFileSelected(e.dataTransfer.files[0]); });
fileInput.addEventListener("change", (e) => { if (e.target.files.length) handleFileSelected(e.target.files[0]); });

// Read from the server (Config.MAX_UPLOAD_SIZE_GB via index()'s
// render_template call) instead of a second hardcoded copy of the number —
// this is a UX fast-path only (fails instantly instead of making someone
// wait through an upload the server would reject anyway); the server-side
// MAX_CONTENT_LENGTH is what actually enforces it, this check is not a
// security boundary. Falls back to 2GB if the data attribute is ever missing.
const MAX_UPLOAD_GB = Number(document.getElementById("file-input")?.dataset.maxUploadGb) || 2;
const MAX_UPLOAD_BYTES = MAX_UPLOAD_GB * 1024 * 1024 * 1024;

async function handleFileSelected(file) {
  if (!file.name.toLowerCase().endsWith(".csv")) {
    showToast("Only .csv files are accepted", "error");
    document.getElementById("file-input").value = "";
    return;
  }
  if (file.size > MAX_UPLOAD_BYTES) {
    showToast(`File is too large — maximum allowed size is ${MAX_UPLOAD_GB}GB (this file is ${(file.size / (1024 ** 3)).toFixed(2)}GB).`, "error");
    document.getElementById("file-input").value = "";
    return;
  }
  selectedUploadFile = file;
  delimiterOverride = null; // new file — let auto-detection run fresh, don't carry over a prior override
  document.getElementById("delimiter-override-area").hidden = true;
  document.querySelectorAll("#delimiter-override-group .delim-choice-option").forEach((b) => b.setAttribute("aria-pressed", "false"));
  document.getElementById("file-chip-area").innerHTML = `<div class="file-chip"><span><svg style="width:14px;height:14px;vertical-align:-2px;margin-right:6px"><use href="#icon-file"/></svg>${escapeHtml(file.name)} — ${(file.size / 1024).toFixed(0)} KB</span><button type="button" id="file-chip-remove" class="icon-btn btn-sm" aria-label="Remove file" title="Remove file"><svg style="width:14px;height:14px"><use href="#icon-x-circle"/></svg></button></div>`;
  await runValidation();
}

// Delegated so it keeps working no matter how many times the chip above is
// re-rendered by handleFileSelected — no listener ever needs re-attaching.
document.getElementById("file-chip-area").addEventListener("click", (e) => {
  if (e.target.closest("#file-chip-remove")) clearSelectedFile();
});

function clearSelectedFile() {
  selectedUploadFile = null;
  delimiterOverride = null;
  selectedKeyColumns = [];
  document.getElementById("file-input").value = ""; // lets the same file be re-picked
  document.getElementById("file-chip-area").innerHTML = "";
  document.getElementById("validation-area").innerHTML = "";
  document.getElementById("delimiter-override-area").hidden = true;
  document.querySelectorAll("#delimiter-override-group .delim-choice-option").forEach((b) => b.setAttribute("aria-pressed", "false"));
  document.getElementById("upload-submit").disabled = true;
  document.getElementById("key-column-area").hidden = true;
  document.getElementById("key-column-picker").innerHTML = "";
}

function uploadFailedAlertHtml() {
  return `
    <div class="validation-row fail" style="align-items:flex-start;flex-direction:column;gap:8px;">
      <div style="display:flex;align-items:center;gap:8px;">
        <svg><use href="#icon-x-circle"/></svg>
        <strong>Upload Failed</strong>
      </div>
      <p style="margin:0;font-size:12.5px;line-height:1.5;">This file cannot be uploaded because it contains invalid or potentially unsafe content, or it does not match the required dataset template.</p>
      <p style="margin:0;font-size:12.5px;line-height:1.5;">Please verify that:</p>
      <ul style="margin:0;padding-left:18px;font-size:12.5px;line-height:1.6;">
        <li>The uploaded file follows the selected dataset template.</li>
        <li>The file does not contain HTML, JavaScript, or executable code.</li>
        <li>All required fields contain valid values before uploading.</li>
      </ul>
    </div>`;
}

async function runValidation() {
  const validationArea = document.getElementById("validation-area");
  const overrideArea = document.getElementById("delimiter-override-area");
  const submitBtn = document.getElementById("upload-submit");

  if (!selectedUploadFile) { submitBtn.disabled = true; return; }
  validationArea.innerHTML = `<p style="font-size:12.5px;color:var(--muted-foreground)">Validating…</p>`;

  try {
    const form = new FormData();
    form.append("file", selectedUploadFile);
    if (delimiterOverride) form.append("delimiter_override", delimiterOverride);
    const result = await apiPost("/upload/validate", form);

    // Mutually exclusive states: a fatal validation error hides every green
    // check and shows exactly one red alert — never both at once.
    if (result.valid) {
      const checksHtml = result.checks.map((c) => `<div class="validation-row ok"><svg><use href="#icon-check-circle"/></svg><span>${escapeHtml(c.message)}</span></div>`).join("");
      validationArea.innerHTML = `<div class="validation-list">${checksHtml}</div>`;
      renderKeyColumnPicker(result.columns);
    } else {
      validationArea.innerHTML = uploadFailedAlertHtml();
      document.getElementById("key-column-area").hidden = true;
      document.getElementById("key-column-picker").innerHTML = "";
    }
    submitBtn.disabled = !result.valid;

    // Safety-net fallback: only appears when the backend thinks the failure
    // looks like a wrong-delimiter problem specifically — not for every
    // validation failure, and never in the normal successful-detection case.
    if (result.delimiter_issue) {
      overrideArea.hidden = false;
      const hint = document.getElementById("delimiter-override-hint");
      if (result.suggested_delimiter) {
        const detectedLabel = DELIM_LABELS[result.detected_delimiter] || result.detected_delimiter;
        const suggestedLabel = DELIM_LABELS[result.suggested_delimiter] || result.suggested_delimiter;
        hint.textContent = `We read this as ${detectedLabel}, but it might actually be ${suggestedLabel} — pick the right one below.`;
      } else {
        hint.textContent = "Not the right format? Tell us what this file actually uses:";
      }
    } else {
      overrideArea.hidden = true;
    }
  } catch (e) {
    validationArea.innerHTML = uploadFailedAlertHtml();
    submitBtn.disabled = true;
    overrideArea.hidden = true;
  }
}

document.querySelectorAll("#delimiter-override-group .delim-choice-option").forEach((btn) => btn.addEventListener("click", () => {
  document.querySelectorAll("#delimiter-override-group .delim-choice-option").forEach((b) => b.setAttribute("aria-pressed", "false"));
  btn.setAttribute("aria-pressed", "true");
  delimiterOverride = btn.getAttribute("data-delim");
  runValidation();
}));

function renderUploadSummary(result) {
  document.getElementById("upload-layout").hidden = true;

  if (result.status === "pending_approval") {
    document.getElementById("upload-summary-area").innerHTML = `
      <div class="card card-pad upload-summary" style="max-width:640px;">
        <div class="upload-summary-header">
          <span class="icon-circle"><svg><use href="#icon-inbox"/></svg></span>
          <div><h3 style="font-size:17px;">Waiting on Approval</h3><p class="card-sub">This upload adds new column(s) to <strong>${escapeHtml(result.dataset_id)}</strong> — an Administrator needs to approve it before it's written.</p></div>
        </div>
        <p class="hint">New column(s): ${(result.new_columns || []).map(escapeHtml).join(", ")}</p>
        <button class="btn btn-secondary" style="margin-top:16px;" id="upload-summary-done">Upload another file</button>
      </div>`;
    document.getElementById("upload-summary-done").addEventListener("click", loadUpload);
    return;
  }

  const title = result.status === "created" ? "Dataset Created" : "Upload Successful";
  // rows_updated only exists on a "merged" outcome (a real row-identity
  // MERGE happened) — a brand-new dataset has nothing to update yet, so
  // that stat tile is left out rather than shown as a confusing "0".
  const updatedTile = result.rows_updated != null
    ? `<div class="us-stat"><div class="n data-num">${result.rows_updated.toLocaleString()}</div><div class="l">Rows Updated</div></div>`
    : "";
  const keyLine = (result.key_columns && result.key_columns.length)
    ? `<p class="hint">Matched rows by: ${result.key_columns.map(escapeHtml).join(", ")}</p>`
    : "";
  document.getElementById("upload-summary-area").innerHTML = `
    <div class="card card-pad upload-summary" style="max-width:640px;">
      <div class="upload-summary-header">
        <span class="icon-circle"><svg><use href="#icon-check-circle"/></svg></span>
        <div><h3 style="font-size:17px;">${title}</h3><p class="card-sub">${escapeHtml(result.display_name || result.dataset_id)}</p></div>
      </div>
      <div class="upload-summary-grid">
        <div class="us-stat"><div class="n data-num">${(result.rows_added ?? 0).toLocaleString()}</div><div class="l">Rows Added</div></div>
        ${updatedTile}
        <div class="us-stat"><div class="n data-num">${(result.total_rows ?? 0).toLocaleString()}</div><div class="l">Current Total Rows</div></div>
      </div>
      ${keyLine}
      <span class="badge badge-success" style="align-self:flex-start;">${escapeHtml(result.status)}</span>
      <button class="btn btn-secondary" style="margin-top:16px;" id="upload-summary-done">Upload another file</button>
    </div>`;
  document.getElementById("upload-summary-done").addEventListener("click", loadUpload);
}

document.getElementById("upload-submit").addEventListener("click", async () => {
  const submitBtn = document.getElementById("upload-submit");
  submitBtn.disabled = true;
  submitBtn.textContent = "Uploading…";

  try {
    const form = new FormData();
    form.append("file", selectedUploadFile);
    if (delimiterOverride) form.append("delimiter_override", delimiterOverride);
    const newName = document.getElementById("upload-new-name").value.trim();
    if (newName) form.append("new_dataset_name", newName);
    if (selectedKeyColumns.length) form.append("key_columns", selectedKeyColumns.join(","));
    const result = await apiPost("/upload", form);
    renderUploadSummary(result);
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
  const head = `<thead><tr>${columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}${editable ? "<th></th>" : ""}</tr></thead>`;
  const body = `<tbody>${rows.map((row, i) => `<tr data-row-index="${i}">${columns.map((c) => `<td ${editable ? `contenteditable="true" data-col="${escapeHtml(c)}"` : ""}>${escapeHtml(row[c] ?? "")}</td>`).join("")}${editable ? `<td><button class="icon-btn btn-sm" data-delete-row="${i}" title="Delete row"><svg style="width:13px;height:13px"><use href="#icon-x-circle"/></svg></button></td>` : ""}</tr>`).join("")}</tbody>`;
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

async function loadPrepare() {
  const select = document.getElementById("prepare-dataset");
  try {
    if (allDatasets.length === 0) allDatasets = await apiGet("/datasets");
    select.innerHTML = `<option value="">Choose a dataset…</option>` + allDatasets.map((d) => `<option value="${escapeHtml(d.dataset_id)}">${escapeHtml(d.display_name || d.dataset_id)}</option>`).join("");
  } catch (e) { showToast(`Could not load datasets: ${e.message}`, "error"); }

  ensureAdminRowToolsUi();
}

// The 5 operations run automatically on every upload (see
// AUTO_CLEAN_OPERATIONS in routes/upload.py) — "Clean dataset" here re-runs
// the exact same set against a dataset's CURRENT data and overwrites it in
// place, for data that was appended before a formatting bug (like the
// Rp/thousands-separator one) was fixed, or that just needs a re-pass.
const RECLEAN_OPERATIONS = ["trim", "normalize_case", "fix_types", "dedupe", "fill_missing"];

function ensureAdminRowToolsUi() {
  if (!isAdmin() || document.getElementById("prepare-save-edits-btn")) return;
  const previewCard = document.getElementById("prepare-preview-table").closest(".card");
  const bar = document.createElement("div");
  bar.style.cssText = "margin-top:10px; display:flex; gap:8px; align-items:center; flex-wrap:wrap;";
  bar.innerHTML = `
    <button class="btn btn-secondary btn-sm" id="prepare-save-edits-btn" hidden>Save row edits</button>
    <button class="btn btn-secondary btn-sm" id="prepare-clean-btn"><svg style="width:13px;height:13px"><use href="#icon-broom"/></svg> Clean dataset</button>
    <span style="font-size:11.5px;color:var(--muted-foreground)">Administrator tools — edit any cell, delete a row with the × button, or re-run cleaning on the whole dataset.</span>`;
  previewCard.appendChild(bar);
  document.getElementById("prepare-save-edits-btn").addEventListener("click", saveRowEdits);
  document.getElementById("prepare-clean-btn").addEventListener("click", async () => {
    const datasetId = document.getElementById("prepare-dataset").value;
    if (!datasetId) { showToast("Pick a dataset first", "error"); return; }
    if (!window.confirm(`Re-clean "${datasetId}"? This trims whitespace, normalizes case, fixes number/date types (including Rp / thousands-separator formatting), removes exact duplicate rows, and fills missing values — overwriting the dataset with the result.`)) return;
    const btn = document.getElementById("prepare-clean-btn");
    btn.disabled = true;
    try {
      await apiPost("/etl/clean", { dataset_id: datasetId, operations: RECLEAN_OPERATIONS });
      showToast("Dataset cleaned");
      document.getElementById("prepare-dataset").dispatchEvent(new Event("change"));
    } catch (e) {
      showToast(e.message, "error");
    } finally {
      btn.disabled = false;
    }
  });
}

async function saveRowEdits() {
  const datasetId = document.getElementById("prepare-dataset").value;
  if (!datasetId || prepareEdits.size === 0) return;
  const btn = document.getElementById("prepare-save-edits-btn");
  btn.disabled = true;
  try {
    for (const [rowIndex, updates] of prepareEdits.entries()) {
      await apiPost("/admin/update-row", { dataset_id: datasetId, row_index: rowIndex, updates });
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

async function deletePrepareRow(datasetId, rowIndex) {
  if (!window.confirm(`Delete row ${rowIndex + 1}? This cannot be undone.`)) return;
  try {
    await apiPost("/admin/delete-row", { dataset_id: datasetId, row_index: rowIndex });
    showToast("Row deleted");
    prepareEdits.clear();
    document.getElementById("prepare-dataset").dispatchEvent(new Event("change"));
  } catch (e) { showToast(e.message, "error"); }
}

document.getElementById("prepare-dataset").addEventListener("change", async (e) => {
  const table = document.getElementById("prepare-preview-table");
  const sub = document.getElementById("prepare-preview-sub");
  prepareEdits.clear();
  const saveBtn = document.getElementById("prepare-save-edits-btn");
  if (saveBtn) saveBtn.hidden = true;

  if (!e.target.value) { table.innerHTML = ""; sub.textContent = "First rows of the selected dataset"; return; }

  const datasetId = e.target.value;
  sub.textContent = "Loading…";
  try {
    const { fields, rows, total_rows } = await apiGet(`/datasets/${datasetId}/preview?limit=100`);
    preparePreviewFields = fields;
    renderPreviewTable(table, fields, rows, {
      editable: isAdmin(),
      onDelete: (rowIndex) => deletePrepareRow(datasetId, rowIndex),
    });
    sub.textContent = `First ${Math.min(100, total_rows)} rows of ${total_rows.toLocaleString()}`;
  } catch (e2) { sub.textContent = `Could not load preview: ${e2.message}`; }
});

// ==========================================================================================
// MERGE
// ==========================================================================================

async function loadMerge() {
  const selectA = document.getElementById("merge-a");
  const selectB = document.getElementById("merge-b");
  try {
    if (allDatasets.length === 0) allDatasets = await apiGet("/datasets");
    const options = `<option value="">Choose a dataset…</option>` + allDatasets.map((d) => `<option value="${escapeHtml(d.dataset_id)}">${escapeHtml(d.display_name || d.dataset_id)}</option>`).join("");
    selectA.innerHTML = options;
    selectB.innerHTML = options;
  } catch (e) { showToast(`Could not load datasets: ${e.message}`, "error"); }
  document.getElementById("merge-preview-table").innerHTML = "";
  document.getElementById("merge-result-sub").textContent = "Pick two datasets and preview the merge";
  document.getElementById("merge-download-btn").hidden = true;
}

document.querySelectorAll(".join-type-option").forEach((btn) => btn.addEventListener("click", () => {
  document.querySelectorAll(".join-type-option").forEach((b) => b.setAttribute("aria-pressed", "false"));
  btn.setAttribute("aria-pressed", "true");
  selectedMergeJoinType = btn.getAttribute("data-join");
  document.getElementById("merge-download-btn").hidden = true; // stale relative to the new join type until re-previewed
}));

document.getElementById("merge-preview-btn").addEventListener("click", async () => {
  const a = document.getElementById("merge-a").value;
  const b = document.getElementById("merge-b").value;
  const sub = document.getElementById("merge-result-sub");
  const table = document.getElementById("merge-preview-table");
  const downloadBtn = document.getElementById("merge-download-btn");

  if (!a || !b) { showToast("Pick both Dataset A and Dataset B", "error"); return; }
  if (a === b) { showToast("Pick two different datasets", "error"); return; }

  sub.textContent = "Merging…";
  downloadBtn.hidden = true;
  try {
    const result = await apiPost("/merge/preview", { a, b, join_type: selectedMergeJoinType });
    renderPreviewTable(table, result.columns, result.rows);
    sub.textContent = `${result.total_rows.toLocaleString()} row(s) in the preview, matched on "${result.join_column}"`;
    downloadBtn.hidden = false; // merge succeeded — the real result is now downloadable
  } catch (e) {
    sub.textContent = `Could not merge: ${e.message}`;
  }
});

document.getElementById("merge-download-btn").addEventListener("click", async () => {
  const a = document.getElementById("merge-a").value;
  const b = document.getElementById("merge-b").value;
  const btn = document.getElementById("merge-download-btn");
  const originalLabel = btn.innerHTML;
  btn.disabled = true;
  btn.textContent = "Downloading…";
  try {
    const res = await fetch("/api/merge/download", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ a, b, join_type: selectedMergeJoinType }),
    });
    if (!res.ok) {
      const data = await res.json().catch(() => null);
      throw new Error((data && data.error) || res.statusText);
    }
    const blob = await res.blob();
    const disposition = res.headers.get("Content-Disposition") || "";
    const match = disposition.match(/filename="?([^"]+)"?/);
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = match ? match[1] : "merged_data.csv";
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  } catch (e) {
    showToast(e.message, "error");
  } finally {
    btn.disabled = false;
    btn.innerHTML = originalLabel;
  }
});

// ==========================================================================================
// VISUALIZE
// ==========================================================================================

async function loadVisualize() {
  const select = document.getElementById("viz-dataset");
  try {
    if (allDatasets.length === 0) allDatasets = await apiGet("/datasets");
    select.innerHTML = `<option value="">Choose a dataset…</option>` + allDatasets.map((d) => `<option value="${escapeHtml(d.dataset_id)}">${escapeHtml(d.display_name || d.dataset_id)}</option>`).join("");
  } catch (e) { showToast(`Could not load datasets: ${e.message}`, "error"); }

  // Force a clean baseline every time this page opens. Some browsers
  // silently restore a <select>'s previous value after its innerHTML is
  // rebuilt above — WITHOUT firing a "change" event — which would leave
  // every dependent bit of UI (Value column visibility, the compat
  // message, the Daily section, etc.) stuck showing whatever was computed
  // last time, instead of matching what's actually selected now.
  select.value = "";
  vizColumnTypes = {};
  document.getElementById("viz-x").innerHTML = "";
  document.getElementById("viz-y").innerHTML = "";
  document.getElementById("viz-compat-msg").hidden = true;
  document.getElementById("daily-section").hidden = true;
  document.getElementById("daily-summary-area").innerHTML = "";
  document.getElementById("daily-preview-area").innerHTML = "";
  document.getElementById("summary-area").innerHTML = "";
  document.getElementById("summary-kpis").innerHTML = "";
  document.getElementById("summary-charts-area").innerHTML = "";
  document.getElementById("summary-chart-msg").hidden = true;
}

// Which column types are valid for X and Y per chart type — the single
// source of truth for dropdown filtering below.
const VIZ_CHART_COMPAT = {
  bar: { x: ["categorical", "date"], y: ["numeric"], xLabel: "Category column", yLabel: "Value column" },
  line: { x: ["date"], y: ["numeric"], xLabel: "Date column", yLabel: "Value column" },
  pie: { x: ["categorical"], y: ["numeric"], xLabel: "Category", yLabel: "Value" },
};
// Pie charts become unreadable with too many slices — same spirit as "don't
// force a chart that doesn't fit the data" from the brief.
const VIZ_PIE_MAX_CATEGORIES = 12;

let vizColumnTypes = {}; // populated on dataset change: { colName: {type, unique_count} }

function updateVizColumnOptions() {
  const chartType = document.getElementById("viz-chart-type").value;
  const method = document.getElementById("viz-method").value;
  const xSelect = document.getElementById("viz-x");
  const ySelect = document.getElementById("viz-y");
  const compatMsg = document.getElementById("viz-compat-msg");
  const renderBtn = document.getElementById("viz-render-btn");
  const compat = VIZ_CHART_COMPAT[chartType] || VIZ_CHART_COMPAT.bar;
  // "Count" is just "how many rows per category" — it never needs a Value
  // column, regardless of what the chart type would otherwise require.
  const needsY = compat.y.length > 0 && method !== "count";

  document.getElementById("viz-x-label").textContent = compat.xLabel;
  document.getElementById("viz-y-field").hidden = !needsY;

  const entries = Object.entries(vizColumnTypes);
  let xCols = entries.filter(([, info]) => compat.x.includes(info.type));
  const yCols = needsY ? entries.filter(([, info]) => compat.y.includes(info.type)) : [];
  if (chartType === "pie") {
    xCols = xCols.filter(([, info]) => info.unique_count > 0 && info.unique_count <= VIZ_PIE_MAX_CATEGORIES);
  }

  if (entries.length === 0) {
    // No dataset picked yet, or it has no columns — stay quiet, not an error.
    xSelect.innerHTML = ""; ySelect.innerHTML = "";
    compatMsg.hidden = true;
    renderBtn.disabled = false;
    return;
  }

  if (xCols.length === 0 || (needsY && yCols.length === 0)) {
    xSelect.innerHTML = ""; ySelect.innerHTML = "";
    compatMsg.textContent = "This visualization is unavailable for the selected dataset because the required data type is not available.";
    compatMsg.hidden = false;
    renderBtn.disabled = true;
    return;
  }

  compatMsg.hidden = true;
  renderBtn.disabled = false;
  xSelect.innerHTML = xCols.map(([c]) => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join("");
  ySelect.innerHTML = needsY ? yCols.map(([c]) => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join("") : "";
}

document.getElementById("viz-chart-type").addEventListener("change", updateVizColumnOptions);
document.getElementById("viz-method").addEventListener("change", updateVizColumnOptions);

document.getElementById("viz-dataset").addEventListener("change", async (e) => {
  const compatMsg = document.getElementById("viz-compat-msg");
  document.getElementById("summary-area").innerHTML = ""; // stale summary would refer to the old dataset
  document.getElementById("summary-kpis").innerHTML = "";
  document.getElementById("summary-charts-area").innerHTML = "";
  document.getElementById("summary-chart-msg").hidden = true;

  if (!e.target.value) {
    vizColumnTypes = {};
    document.getElementById("viz-x").innerHTML = "";
    document.getElementById("viz-y").innerHTML = "";
    compatMsg.hidden = true;
    document.getElementById("daily-section").hidden = true;
    document.getElementById("daily-summary-area").innerHTML = "";
    document.getElementById("daily-preview-area").innerHTML = "";
    return;
  }
  const datasetId = e.target.value;
  try {
    const { columns } = await apiGet(`/datasets/${datasetId}/column-types`);
    vizColumnTypes = columns;

    // Default the Calculation dropdown per-dataset rather than to one fixed
    // global choice: a dataset with a real numeric column (Invoice Summary,
    // amount) defaults to Sum — same behavior this page always had, before
    // "Calculation" existed at all — while a dataset with none (Asset
    // Inventory) defaults to Count, the only method that works with no
    // numbers at all. Either way it "just visualizes" without the person
    // having to think about Calculation first; they can still change it.
    const hasNumericColumn = Object.values(columns).some((info) => info.type === "numeric");
    document.getElementById("viz-method").value = hasNumericColumn ? "sum" : "count";

    updateVizColumnOptions();

    // Daily Summary & Preview — optional/secondary, only shown when the
    // dataset actually has a usable date column. Never assumes a single
    // fixed date-column name: whatever detect_column_types finds as "date"
    // is offered, whatever it's called.
    const { date_columns } = await apiGet(`/datasets/${datasetId}/date-columns`);
    const dailySection = document.getElementById("daily-section");
    const dateColField = document.getElementById("daily-date-column-field");
    const dateColSelect = document.getElementById("daily-date-column");
    document.getElementById("daily-summary-area").innerHTML = "";
    document.getElementById("daily-preview-area").innerHTML = "";
    if (date_columns.length === 0) {
      dailySection.hidden = true;
    } else {
      dailySection.hidden = false;
      dateColSelect.innerHTML = date_columns.map((c) => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join("");
      // Only show the picker when there's a genuine choice (e.g. Inventory's
      // Item Receive Date vs Item Return Date) — with just one, there's
      // nothing to choose, so don't clutter the UI with a single-option select.
      dateColField.hidden = date_columns.length <= 1;
    }
  } catch (e2) { showToast(`Could not load columns: ${e2.message}`, "error"); }
});

document.getElementById("daily-render-btn").addEventListener("click", async () => {
  const dsValue = document.getElementById("viz-dataset").value;
  const dateColumn = document.getElementById("daily-date-column").value;
  const date = document.getElementById("daily-date-input").value;
  const summaryArea = document.getElementById("daily-summary-area");
  const previewArea = document.getElementById("daily-preview-area");

  if (!dsValue || !dateColumn || !date) { showToast("Pick a date column and a date", "error"); return; }

  summaryArea.innerHTML = `<p class="hint">Loading…</p>`;
  previewArea.innerHTML = "";
  try {
    const result = await apiPost("/visualize/daily", { dataset_id: dsValue, date_column: dateColumn, date }, { timeoutMs: VIZ_TIMEOUT_MS });

    if (result.total_records === 0) {
      summaryArea.innerHTML = `<p class="hint">No records found for ${escapeHtml(date)}.</p>`;
      return;
    }

    // Daily Summary: total + whatever categorical/numeric columns actually
    // exist for this format — never a fixed, hardcoded metric list.
    let summaryHtml = `<p style="font-weight:600;margin-bottom:8px;">Total Records: ${result.total_records.toLocaleString()}</p>`;
    for (const group of result.category_breakdown) {
      summaryHtml += `<div style="margin-bottom:8px;"><div style="font-size:12px;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.04em;">${escapeHtml(group.column)}</div>`;
      summaryHtml += group.counts.map((c) => `<span class="badge badge-muted" style="margin:2px 4px 0 0;">${escapeHtml(c.value)}: ${c.count.toLocaleString()}</span>`).join("");
      summaryHtml += `</div>`;
    }
    for (const num of result.numeric_summary) {
      summaryHtml += `<div style="margin-bottom:8px;"><div style="font-size:12px;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.04em;">${escapeHtml(num.column)}</div>`;
      summaryHtml += `<span>Total: <strong>${num.sum.toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong> · Average: <strong>${num.avg.toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong></span>`;
      summaryHtml += `</div>`;
    }
    summaryArea.innerHTML = summaryHtml;

    // Preview Table — reuses the same renderer as every other preview
    // table in the app (Prepare, Merge, etc.), so it's styled/escaped
    // consistently rather than being a one-off.
    const table = document.createElement("table");
    table.className = "preview-table";
    previewArea.innerHTML = `<div class="preview-table-wrap" style="max-height:320px;"></div>`;
    previewArea.querySelector(".preview-table-wrap").appendChild(table);
    renderPreviewTable(table, result.preview_fields, result.preview_rows);
  } catch (e) {
    summaryArea.innerHTML = `<p class="hint" style="color:var(--destructive);">${escapeHtml(e.message)}</p>`;
  }
});

// Greys out chart types with no valid hardcoded comparison for the current
// dataset's format (e.g. Bar/Pie for a format with no categorical column),
// instead of letting someone pick one and land on an empty/error state.
function updateSummaryChartTypeOptions(availableTypes) {
  const select = document.getElementById("summary-chart-type");
  const labels = { bar: "Bar chart", pie: "Pie chart", line: "Line chart" };
  const order = ["bar", "pie", "line"];
  const previous = select.value;
  select.innerHTML = order.map((t) => {
    const disabled = !availableTypes.includes(t);
    return `<option value="${t}"${disabled ? " disabled" : ""}>${labels[t]}${disabled ? " (not available)" : ""}</option>`;
  }).join("");
  if (availableTypes.includes(previous)) {
    select.value = previous;
  } else if (availableTypes.length) {
    select.value = availableTypes[0];
  }
}

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
    data: {
      labels,
      datasets: [{
        label: valueLabel || "Value",
        data: values,
        backgroundColor: chartType === "pie" ? palette : accent,
        borderColor: chartType === "line" ? accent : "transparent",
        borderRadius: chartType === "bar" ? 6 : 0,
        tension: 0.35,
        fill: chartType === "line" ? false : true,
      }],
    },
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
  const method = document.getElementById("viz-method").value;
  const needsY = method !== "count";
  if (!dsValue || !x || (needsY && !y)) { showToast("Choose a data source, category column and value column", "error"); return; }

  const renderBtn = document.getElementById("viz-render-btn");
  const originalLabel = renderBtn.innerHTML;
  renderBtn.disabled = true;
  renderBtn.textContent = "Generating…";
  try {
    const { labels, values } = await apiPost("/visualize/aggregate", { dataset_id: dsValue, x, y: needsY ? y : undefined, method }, { timeoutMs: VIZ_TIMEOUT_MS });
    renderVizChart(chartType, labels, values, needsY ? y : "Count");
  } catch (e) { showToast(e.message, "error"); }
  finally {
    renderBtn.disabled = false;
    renderBtn.innerHTML = originalLabel;
  }
});

// ------------------------------------------------------------------------
// SUMMARY — deliberately separate from the chart above: its own dataset
// selector, its own state, its own failure mode. A chart error never
// touches this, and a summary error never touches the chart.
//
// Renders one card+canvas PER valid hardcoded comparison for the current
// dataset's format and the chosen chart type (see Config.SUMMARY_CONFIG in
// config.py) — deliberately not a single "best guess" chart, since that
// previously produced nonsense pairings (e.g. summing a reference-number
// column grouped by region) that a generic relationship score couldn't
// tell apart from a real one.
// ------------------------------------------------------------------------

let summaryChartInstances = [];
let summaryChartConfigs = []; // remembers each card's {type, labels, values, label, title} so "maximize" can re-render the same chart bigger

function renderSummaryCharts(chartType, charts) {
  const area = document.getElementById("summary-charts-area");
  summaryChartInstances.forEach((c) => c.destroy());
  summaryChartInstances = [];
  summaryChartConfigs = charts.map((c) => ({ type: chartType, labels: c.labels, values: c.values, label: c.y || "Count", title: c.label }));

  area.innerHTML = charts.map((c, i) => `
    <div class="card card-pad">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
        <div style="font-size:12.5px;font-weight:600;">${escapeHtml(c.label)}</div>
        <button class="icon-btn btn-sm" style="width:24px;height:24px" data-maximize-chart="${i}" aria-label="Maximize chart" title="Maximize"><svg style="width:13px;height:13px"><use href="#icon-maximize"/></svg></button>
      </div>
      <div class="chart-wrap" style="height:240px;"><canvas id="summary-canvas-${i}"></canvas></div>
    </div>
  `).join("");

  charts.forEach((c, i) => {
    const ctx = document.getElementById(`summary-canvas-${i}`).getContext("2d");
    summaryChartInstances.push(new Chart(ctx, buildSummaryChartJsConfig(chartType, c.labels, c.values, c.y || "Count")));
  });
}

// Shared by the small grid charts above and the "maximize" modal below, so
// the enlarged view is guaranteed to look identical to the small one, not a
// separately-maintained near-duplicate that could drift out of sync.
function buildSummaryChartJsConfig(chartType, labels, values, valueLabel) {
  const accent = cssVar("--accent") || "#e4002b";
  const gridColor = cssVar("--border") || "#e5e7eb";
  const textColor = cssVar("--muted-foreground") || "#6b7280";
  const palette = [accent, "#2563eb", "#16a34a", "#d97706", "#7c3aed", "#0891b2", "#db2777"];
  return {
    type: chartType,
    data: {
      labels,
      datasets: [{
        label: valueLabel,
        data: values,
        backgroundColor: chartType === "pie" ? palette : accent,
        borderColor: chartType === "line" ? accent : "transparent",
        borderRadius: chartType === "bar" ? 6 : 0,
        tension: 0.35,
        fill: chartType === "line" ? false : true,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: chartType === "pie", labels: { color: textColor, font: { family: "Inter" } } } },
      scales: chartType === "pie" ? {} : {
        x: { grid: { display: false }, ticks: { color: textColor, font: { family: "IBM Plex Mono", size: 10 } } },
        y: { grid: { color: gridColor }, ticks: { color: textColor, font: { family: "IBM Plex Mono", size: 10 } } },
      },
    },
  };
}

let chartMaximizeInstance = null;

function openChartMaximize(index) {
  const cfg = summaryChartConfigs[index];
  if (!cfg) return;
  document.getElementById("chart-maximize-title").textContent = cfg.title;
  const overlay = document.getElementById("chart-maximize-overlay");
  overlay.style.display = "flex";
  if (chartMaximizeInstance) chartMaximizeInstance.destroy();
  const ctx = document.getElementById("chart-maximize-canvas").getContext("2d");
  chartMaximizeInstance = new Chart(ctx, buildSummaryChartJsConfig(cfg.type, cfg.labels, cfg.values, cfg.label));
}

function closeChartMaximize() {
  document.getElementById("chart-maximize-overlay").style.display = "none";
  if (chartMaximizeInstance) { chartMaximizeInstance.destroy(); chartMaximizeInstance = null; }
}

// Delegated so it keeps working no matter how many times the chart grid
// above is re-rendered — no per-card listener ever needs re-attaching.
document.getElementById("summary-charts-area").addEventListener("click", (e) => {
  const btn = e.target.closest("[data-maximize-chart]");
  if (btn) openChartMaximize(parseInt(btn.getAttribute("data-maximize-chart"), 10));
});
document.getElementById("chart-maximize-close").addEventListener("click", closeChartMaximize);
document.getElementById("chart-maximize-overlay").addEventListener("click", (e) => {
  if (e.target.id === "chart-maximize-overlay") closeChartMaximize(); // click on the dim backdrop, not the card itself
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeChartMaximize();
});

async function renderSummaryChartsForCurrentSelection() {
  const dsValue = document.getElementById("viz-dataset").value;
  const chartType = document.getElementById("summary-chart-type").value;
  const msg = document.getElementById("summary-chart-msg");
  const area = document.getElementById("summary-charts-area");
  msg.hidden = true;
  if (!dsValue) { area.innerHTML = ""; return; }

  try {
    const result = await apiPost("/visualize/summary-charts", { dataset_id: dsValue, chart_type: chartType }, { timeoutMs: VIZ_TIMEOUT_MS });
    if (!result.available) {
      summaryChartInstances.forEach((c) => c.destroy());
      summaryChartInstances = [];
      area.innerHTML = "";
      msg.textContent = result.message;
      msg.hidden = false;
      return;
    }
    renderSummaryCharts(chartType, result.charts);
  } catch (e) {
    summaryChartInstances.forEach((c) => c.destroy());
    summaryChartInstances = [];
    area.innerHTML = "";
    msg.textContent = e.message;
    msg.hidden = false;
  }
}

document.getElementById("summary-chart-type").addEventListener("change", renderSummaryChartsForCurrentSelection);

document.getElementById("summary-render-btn").addEventListener("click", async () => {
  const dsValue = document.getElementById("viz-dataset").value;
  const area = document.getElementById("summary-area");
  const kpiArea = document.getElementById("summary-kpis");

  if (!dsValue) { showToast("Choose a data source above first", "error"); return; }

  area.innerHTML = `<p class="hint">Generating summary…</p>`;
  kpiArea.innerHTML = "";
  try {
    const result = await apiPost("/visualize/summary", { dataset_id: dsValue }, { timeoutMs: VIZ_TIMEOUT_MS });
    if (!result.available) {
      area.innerHTML = `<p class="hint" style="color:var(--destructive);">${escapeHtml(result.message)}</p>`;
    } else {
      if (result.kpis && result.kpis.length) {
        kpiArea.innerHTML = result.kpis.map((k) => `
          <div class="card card-pad" style="flex:1; min-width:140px;">
            <div style="font-size:11px;color:var(--muted-foreground);text-transform:uppercase;letter-spacing:.04em;">${escapeHtml(k.label)}</div>
            <div style="font-size:22px;font-weight:700;margin-top:4px;">${escapeHtml(k.value)}</div>
          </div>`).join("");
      }
      const rowsHtml = result.rows.map((r) => `<tr><td>${escapeHtml(r.metric)}</td><td style="text-align:right;font-weight:600;">${escapeHtml(r.value)}</td></tr>`).join("");
      area.innerHTML = `<div class="preview-table-wrap" style="max-height:360px;"><table class="preview-table"><thead><tr><th>Metric</th><th style="text-align:right;">Value</th></tr></thead><tbody>${rowsHtml}</tbody></table></div>`;
    }
  } catch (e) {
    area.innerHTML = `<p class="hint" style="color:var(--destructive);">${escapeHtml(e.message)}</p>`;
  }
  // The chart grid is independent of whether the table above succeeded — a
  // dataset can fail Summary's table for one reason but still have valid
  // charts, and vice versa.
  await renderSummaryChartsForCurrentSelection();
});

// ==========================================================================================
// APPROVALS (admin-only) — schema-change and rename requests waiting on an
// Administrator. Both change_types render as one list; approve/reject
// dispatch to the same two /admin/pending-changes/<id>/... endpoints
// regardless of which kind it is.
// ==========================================================================================

function renderApprovalsList(items) {
  const area = document.getElementById("approvals-list");
  if (items.length === 0) {
    area.innerHTML = `<div class="empty-state"><svg><use href="#icon-inbox"/></svg><h3>Nothing waiting</h3><p>Schema-change and rename requests will show up here.</p></div>`;
    return;
  }
  area.innerHTML = items.map((item) => {
    const isSchema = item.change_type === "schema_change";
    const detail = isSchema
      ? `Adds column(s): <strong>${(item.payload.new_columns || []).map(escapeHtml).join(", ")}</strong>`
      : `Rename to: <strong>${escapeHtml(item.payload.new_display_name || "")}</strong>`;
    return `
    <div class="card card-pad" style="margin-bottom:10px;">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;">
        <div>
          <div style="font-weight:600;">${isSchema ? "Schema change" : "Rename"} — ${escapeHtml(item.dataset_id)}</div>
          <div class="hint" style="margin-top:2px;">${detail}</div>
          <div class="hint" style="margin-top:2px;">Requested by ${escapeHtml(item.requested_by_username)}</div>
        </div>
        <div style="display:flex;gap:8px;">
          <button class="btn btn-primary btn-sm" data-approve="${item.id}">Approve</button>
          <button class="btn btn-secondary btn-sm" data-reject="${item.id}">Reject</button>
        </div>
      </div>
    </div>`;
  }).join("");

  area.querySelectorAll("[data-approve]").forEach((btn) => btn.addEventListener("click", async () => {
    try { await apiPost(`/admin/pending-changes/${btn.getAttribute("data-approve")}/approve`, {}); showToast("Approved"); loadApprovals(); }
    catch (e) { showToast(e.message, "error"); }
  }));
  area.querySelectorAll("[data-reject]").forEach((btn) => btn.addEventListener("click", async () => {
    try { await apiPost(`/admin/pending-changes/${btn.getAttribute("data-reject")}/reject`, {}); showToast("Rejected"); loadApprovals(); }
    catch (e) { showToast(e.message, "error"); }
  }));
}

async function loadApprovals() {
  const area = document.getElementById("approvals-list");
  area.innerHTML = `<p class="hint">Loading…</p>`;
  try {
    const items = await apiGet("/admin/pending-changes?status=pending");
    renderApprovalsList(items);
  } catch (e) {
    area.innerHTML = `<p class="hint" style="color:var(--destructive)">Could not load pending requests: ${e.message}</p>`;
  }
}

document.getElementById("approvals-refresh").addEventListener("click", loadApprovals);

// ==========================================================================================
// AI ASSISTANT — talks to /api/chat, which streams newline-delimited JSON events
// (see services/chat_agent.py). Conversation state lives only in the browser tab:
// chatHistory is resent (trimmed) with every message, nothing is stored server-side.
// ==========================================================================================

let chatHistory = [];       // [{role: "user"|"assistant", content: string}, ...] — plain text only
let chatSending = false;
let chatCharts = [];        // Chart.js instances currently in the log, for teardown/theme changes

function chatAutoGrow(el) {
  el.style.height = "auto";
  el.style.height = Math.min(el.scrollHeight, 140) + "px";
}
document.getElementById("chat-input").addEventListener("input", (e) => chatAutoGrow(e.target));

async function loadChat() {
  const banner = document.getElementById("chat-status-banner");
  try {
    const status = await apiGet("/chat/status");
    if (status.available) {
      banner.hidden = true;
    } else {
      document.getElementById("chat-status-text").textContent = status.message || "The AI assistant is currently unavailable.";
      banner.hidden = false;
    }
  } catch (e) {
    document.getElementById("chat-status-text").textContent = "Could not check the AI assistant's status — try sending a message.";
    banner.hidden = false;
  }
  document.getElementById("chat-input").focus();
}

function chatAppendRaw(role, html) {
  const log = document.getElementById("chat-log");
  const wrap = document.createElement("div");
  wrap.className = `chat-msg chat-msg-${role}`;
  wrap.innerHTML = html;
  log.appendChild(wrap);
  log.scrollTop = log.scrollHeight;
  return wrap;
}

// Deterministic per-result data: Download CSV and Chart-this never depend on
// the model calling a tool correctly — they act on data the browser already
// received. chatResultStore keeps each rendered table's raw {columns, rows}
// so the buttons under it can build a CSV or a chart on demand, client-side.
let chatResultStore = {};
let chatResultCounter = 0;

const CHAT_TOOL_LABELS = {
  list_datasets: "Looking at what datasets are available…",
  get_schema: "Inspecting the columns of", get_sample: "Fetching sample rows from",
  get_statistics: "Profiling", run_query: "Running a query…",
  bar_chart: "Building a bar chart…", line_chart: "Building a line chart…",
  scatter_plot: "Building a scatter plot…", heatmap: "Building a heatmap…",
  histogram: "Building a histogram…", boxplot: "Building a box plot…",
  correlation: "Computing correlations in", outlier_detection: "Looking for outliers in",
  missing_value_analysis: "Checking missing values in", summary_statistics: "Summarising",
  decision_tree: "Training a decision tree on", KNN: "Training a k-nearest-neighbours model on",
  linear_regression: "Fitting a linear regression on", logistic_regression: "Fitting a logistic regression on",
  KMeans: "Clustering",
};

function chatToolLabel(name, args) {
  const base = CHAT_TOOL_LABELS[name];
  if (!base) return `Using ${escapeHtml(name)}…`;
  if (base.endsWith("…")) return base;
  const ds = args && args.dataset_id ? ` <code>${escapeHtml(args.dataset_id)}</code>` : "";
  return `${base}${ds}…`;
}

function chatCsvEscape(v) {
  const s = v === null || v === undefined ? "" : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function chatDownloadCsv(resultId) {
  const result = chatResultStore[resultId];
  if (!result) return;
  const lines = [result.columns.map(chatCsvEscape).join(",")];
  for (const row of result.rows) lines.push(row.map(chatCsvEscape).join(","));
  const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `query-result-${resultId}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// Which of a table's columns look numeric enough to plot as Y (checked
// against the actual row values already in hand — no guessing from names).
function chatNumericColumns(result) {
  return result.columns.filter((_, i) => {
    const sample = result.rows.slice(0, 30);
    if (!sample.length) return false;
    const numeric = sample.filter((row) => row[i] !== null && row[i] !== "" && !isNaN(parseFloat(row[i]))).length;
    return numeric / sample.length > 0.8;
  });
}

function chatDrawManualChart(resultId, panel) {
  const result = chatResultStore[resultId];
  const chartType = panel.querySelector('[data-role="chart-type"]').value;
  const x = panel.querySelector('[data-role="chart-x"]').value;
  const y = panel.querySelector('[data-role="chart-y"]').value;
  if (!x || !y) { showToast("Pick a column for both axes", "error"); return; }
  const xi = result.columns.indexOf(x), yi = result.columns.indexOf(y);
  const labels = [], values = [];
  for (const row of result.rows.slice(0, 200)) {
    const v = parseFloat(row[yi]);
    if (isNaN(v)) continue;
    labels.push(String(row[xi] ?? ""));
    values.push(v);
  }
  if (!values.length) { showToast(`"${y}" has no numeric values to chart`, "error"); return; }
  const canvasId = `chat-manual-chart-${resultId}-${Date.now()}`;
  const holder = panel.querySelector('[data-role="chart-output"]');
  holder.innerHTML = chatRenderChart({ chart_type: chartType, title: `${y} by ${x}`, y, labels, values }, canvasId);
}

function chatRenderSqlTable(result) {
  const resultId = ++chatResultCounter;
  chatResultStore[resultId] = { columns: result.columns, rows: result.rows };
  if (!result.columns.length) return `<p class="hint">Query returned no rows.</p>`;

  const head = `<thead><tr>${result.columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>`;
  const body = `<tbody>${result.rows.map((row) => `<tr>${row.map((v) => `<td>${escapeHtml(v ?? "")}</td>`).join("")}</tr>`).join("")}</tbody>`;
  const note = result.truncated ? `<p class="hint">Showing the first ${result.rows.length} rows.</p>` : "";

  const numericCols = chatNumericColumns(result);
  const chartBtn = numericCols.length
    ? `<button class="btn btn-secondary btn-sm" data-chat-chart-toggle="${resultId}" type="button">Chart this</button>`
    : "";

  return `
    <div class="preview-table-wrap" style="max-height:320px;margin-top:6px;"><table class="preview-table">${head}${body}</table></div>
    ${note}
    <div style="display:flex;gap:8px;margin-top:8px;">
      <button class="btn btn-secondary btn-sm" data-chat-csv="${resultId}" type="button">Download CSV</button>
      ${chartBtn}
    </div>
    <div data-chart-panel="${resultId}" style="display:none;margin-top:8px;padding:10px;border:1px solid var(--border);border-radius:var(--radius);">
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
        <select class="input" style="width:auto;" data-role="chart-type"><option value="bar">Bar</option><option value="line">Line</option><option value="pie">Pie</option></select>
        <select class="input" style="width:auto;" data-role="chart-x">${result.columns.map((c) => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join("")}</select>
        <span class="hint" style="margin:0;">by</span>
        <select class="input" style="width:auto;" data-role="chart-y">${numericCols.map((c) => `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join("")}</select>
        <button class="btn btn-primary btn-sm" data-chat-chart-draw="${resultId}" type="button">Draw</button>
      </div>
      <div data-role="chart-output" style="margin-top:8px;"></div>
    </div>`;
}

function chatHexToRgb(hex) {
  const m = /^#?([0-9a-f]{6})$/i.exec((hex || "").trim());
  const n = parseInt(m ? m[1] : "e4002b", 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
}

// Chart.js config for the chart types the AI tools can produce beyond bar/line/pie.
function buildChatChartConfig(chart) {
  const accent = cssVar("--accent") || "#e4002b";
  const [r, g, b] = chatHexToRgb(accent);
  const gridColor = cssVar("--border") || "#e5e7eb";
  const textColor = cssVar("--muted-foreground") || "#6b7280";
  const tick = { color: textColor, font: { family: "IBM Plex Mono", size: 11 } };
  const axisTitle = (t) => ({ display: !!t, text: t, color: textColor });
  const base = { responsive: true, maintainAspectRatio: false };

  if (chart.chart_type === "scatter") {
    return { type: "scatter", data: { datasets: [{ label: `${chart.y} vs ${chart.x}`, data: chart.points,
        backgroundColor: `rgba(${r},${g},${b},0.55)`, pointRadius: 3 }] },
      options: { ...base, plugins: { legend: { display: false } }, scales: {
        x: { title: axisTitle(chart.x), grid: { color: gridColor }, ticks: tick },
        y: { title: axisTitle(chart.y), grid: { color: gridColor }, ticks: tick } } } };
  }
  if (chart.chart_type === "histogram") {
    return { type: "bar", data: { labels: chart.labels, datasets: [{ label: "Rows", data: chart.values,
        backgroundColor: accent, barPercentage: 1, categoryPercentage: 0.95 }] },
      options: { ...base, plugins: { legend: { display: false } }, scales: {
        x: { title: axisTitle(chart.x), grid: { display: false }, ticks: { ...tick, maxRotation: 45 } },
        y: { title: axisTitle("rows"), grid: { color: gridColor }, ticks: tick } } } };
  }
  if (chart.chart_type === "boxplot") {
    const gs = chart.groups || [];
    return { type: "bar", data: { labels: gs.map((x) => x.label), datasets: [
        { label: "Whiskers", data: gs.map((x) => [x.low, x.high]), backgroundColor: `rgba(${r},${g},${b},0.25)`, barPercentage: 0.12, grouped: false, order: 3 },
        { label: "Q1–Q3", data: gs.map((x) => [x.q1, x.q3]), backgroundColor: `rgba(${r},${g},${b},0.65)`, barPercentage: 0.55, grouped: false, order: 2 },
        { label: "Median", type: "line", data: gs.map((x) => x.median), showLine: false, pointStyle: "rectRot",
          pointRadius: 6, pointBackgroundColor: "#fff", pointBorderColor: accent, pointBorderWidth: 2, order: 1 }] },
      options: { ...base, plugins: { legend: { display: true, labels: { color: textColor, font: { family: "Inter" } } } }, scales: {
        x: { grid: { display: false }, ticks: tick },
        y: { title: axisTitle(chart.y), grid: { color: gridColor }, ticks: tick } } } };
  }
  return buildSummaryChartJsConfig(chart.chart_type, chart.labels, chart.values, chart.y);
}

// Chart.js has no heatmap, so it's an HTML table whose cell shading carries the value.
function chatHeatmapHtml(chart) {
  const [r, g, b] = chatHexToRgb(cssVar("--accent") || "#e4002b");
  const flat = chart.matrix.flat().filter((v) => v !== null && v !== undefined);
  const lo = Math.min(...flat), hi = Math.max(...flat);
  const cell = (v) => {
    if (v === null || v === undefined) return `<td style="text-align:center;color:var(--muted-foreground)">—</td>`;
    let alpha, rgb = `${r},${g},${b}`;
    if (chart.diverging) { alpha = Math.min(1, Math.abs(v)); if (v < 0) rgb = "37,99,235"; }
    else alpha = hi === lo ? 0.5 : 0.08 + 0.85 * ((v - lo) / (hi - lo));
    const txt = alpha > 0.6 ? "#fff" : "inherit";
    return `<td style="text-align:center;background:rgba(${rgb},${alpha.toFixed(2)});color:${txt};min-width:48px">${escapeHtml(v)}</td>`;
  };
  const head = `<tr><th></th>${chart.x_labels.map((l) => `<th style="font-size:11px">${escapeHtml(l)}</th>`).join("")}</tr>`;
  const rows = chart.matrix.map((row, i) => `<tr><th style="font-size:11px;text-align:left">${escapeHtml(chart.y_labels[i])}</th>${row.map(cell).join("")}</tr>`).join("");
  return `<div class="preview-table-wrap" style="max-height:360px;"><table class="preview-table">${head}${rows}</table></div>
    <p class="hint" style="margin:6px 0 0;">${escapeHtml(chart.value_label || "")}${chart.diverging ? " — red = positive, blue = negative" : ""}</p>`;
}

function chatRenderChart(chart, canvasId) {
  const titleHtml = `<div style="font-size:12.5px;font-weight:600;margin-bottom:8px;">${escapeHtml(chart.title)}</div>`;
  if (chart.chart_type === "heatmap") {
    return `<div class="card card-pad" style="margin-top:8px;">${titleHtml}${chatHeatmapHtml(chart)}</div>`;
  }
  setTimeout(() => {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    chatCharts.push(new Chart(canvas.getContext("2d"), buildChatChartConfig(chart)));
  }, 0);
  const note = chart.chart_type === "scatter" && chart.r !== undefined && chart.r !== null
    ? `<p class="hint" style="margin:6px 0 0;">Pearson r = ${escapeHtml(chart.r)} over ${escapeHtml(Number(chart.n).toLocaleString())} rows${chart.points.length < chart.n ? ` (showing a random ${chart.points.length})` : ""}</p>` : "";
  return `<div class="card card-pad" style="margin-top:8px;">${titleHtml}<div class="chart-wrap" style="height:${chart.chart_type === "boxplot" ? 260 : 220}px;"><canvas id="${canvasId}"></canvas></div>${note}</div>`;
}

// Result card for the analysis / machine-learning tools: headline numbers as chips,
// then each table (with the same Download CSV button the SQL tables have).
function chatRenderAnalysis(ev) {
  const chips = Object.entries(ev.summary || {})
    .filter(([, v]) => v !== null && v !== undefined && typeof v !== "object")
    .map(([k, v]) => `<span class="hint" style="border:1px solid var(--border);border-radius:999px;padding:2px 10px;margin:0;">${escapeHtml(k.replace(/_/g, " "))}: <b>${escapeHtml(typeof v === "number" ? +v.toFixed(4) : v)}</b></span>`)
    .join("");
  const tables = (ev.tables || []).map((t) => {
    const id = ++chatResultCounter;
    chatResultStore[id] = { columns: t.columns, rows: t.rows };
    const head = `<thead><tr>${t.columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>`;
    const body = `<tbody>${t.rows.slice(0, 60).map((row) => `<tr>${row.map((v) => `<td>${escapeHtml(v ?? "")}</td>`).join("")}</tr>`).join("")}</tbody>`;
    return `<div style="margin-top:10px;"><div class="hint" style="margin:0 0 4px;font-weight:600;">${escapeHtml(t.title || "")}</div>
      <div class="preview-table-wrap" style="max-height:300px;"><table class="preview-table">${head}${body}</table></div>
      <button class="btn btn-secondary btn-sm" data-chat-csv="${id}" type="button" style="margin-top:6px;">Download CSV</button></div>`;
  }).join("");
  const text = ev.text ? `<pre style="margin:10px 0 0;padding:8px;max-height:220px;overflow:auto;font-size:11px;background:var(--muted, #f4f4f5);border-radius:var(--radius);">${escapeHtml(ev.text)}</pre>` : "";
  return `<div class="card card-pad" style="margin-top:8px;"><div style="font-size:12.5px;font-weight:600;margin-bottom:8px;">${escapeHtml(ev.title)}</div>
    <div style="display:flex;gap:6px;flex-wrap:wrap;">${chips}</div>${tables}${text}</div>`;
}

// Delegated: table/chart buttons are inserted dynamically into #chat-log
// throughout a streaming reply, so listeners are bound once on the log
// container rather than per-button.
document.getElementById("chat-log").addEventListener("click", (e) => {
  const csvBtn = e.target.closest("[data-chat-csv]");
  if (csvBtn) { chatDownloadCsv(Number(csvBtn.getAttribute("data-chat-csv"))); return; }

  const toggleBtn = e.target.closest("[data-chat-chart-toggle]");
  if (toggleBtn) {
    const id = toggleBtn.getAttribute("data-chat-chart-toggle");
    const panel = document.querySelector(`[data-chart-panel="${id}"]`);
    if (panel) panel.style.display = panel.style.display === "none" ? "block" : "none";
    return;
  }

  const drawBtn = e.target.closest("[data-chat-chart-draw]");
  if (drawBtn) {
    const id = Number(drawBtn.getAttribute("data-chat-chart-draw"));
    const panel = document.querySelector(`[data-chart-panel="${id}"]`);
    if (panel) chatDrawManualChart(id, panel);
  }
});

async function chatSend() {
  if (chatSending) return;
  const input = document.getElementById("chat-input");
  const text = input.value.trim();
  if (!text) return;
  input.value = "";
  chatAutoGrow(input);
  chatSending = true;
  document.getElementById("chat-send-btn").disabled = true;

  chatAppendRaw("user", `<div class="chat-bubble">${escapeHtml(text)}</div>`);
  chatHistory.push({ role: "user", content: text });

  const assistantWrap = chatAppendRaw("assistant", `<div class="chat-status hint">Thinking…</div>`);
  let answerText = "";
  let sawContent = false;
  const ensureAnswerBubble = () => {
    if (!sawContent) {
      const status = assistantWrap.querySelector(".chat-status");
      if (status) status.remove();
      const bubble = document.createElement("div");
      bubble.className = "chat-bubble";
      assistantWrap.appendChild(bubble);
      sawContent = true;
    }
    return assistantWrap.querySelector(".chat-bubble");
  };

  try {
    const res = await fetch("/api/chat", {
      method: "POST", credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ messages: chatHistory.slice(-24) }),
    });
    if (res.status === 401) { showLoginOverlay(); throw new Error("Session expired — please sign in again."); }
    if (!res.ok || !res.body) {
      const data = await res.json().catch(() => null);
      throw new Error((data && data.error) || `Request failed (${res.status})`);
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let chartCounter = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let nl;
      while ((nl = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, nl).trim();
        buffer = buffer.slice(nl + 1);
        if (!line) continue;
        let event;
        try { event = JSON.parse(line); } catch (e) { continue; }

        if (event.type === "status" && !sawContent) {
          assistantWrap.querySelector(".chat-status").textContent = event.text;
        } else if (event.type === "token") {
          const bubble = ensureAnswerBubble();
          answerText += event.text;
          bubble.textContent = answerText;
        } else if (event.type === "thinking") {
          // Model reasoning — intentionally not shown; the tool-call trace below
          // already tells the person what the assistant is doing and why.
        } else if (event.type === "tool_call") {
          const el = document.createElement("div");
          el.className = "chat-tool hint";
          el.innerHTML = `<svg style="width:12px;height:12px;flex:none"><use href="#icon-database"/></svg><span>${chatToolLabel(event.name, event.arguments)}</span>`;
          assistantWrap.appendChild(el);
        } else if (event.type === "sql_result") {
          const el = document.createElement("div");
          el.innerHTML = chatRenderSqlTable(event);
          assistantWrap.appendChild(el);
        } else if (event.type === "chart") {
          const el = document.createElement("div");
          el.innerHTML = chatRenderChart(event, `chat-chart-${Date.now()}-${chartCounter++}`);
          assistantWrap.appendChild(el);
        } else if (event.type === "analysis") {
          const el = document.createElement("div");
          el.innerHTML = chatRenderAnalysis(event);
          assistantWrap.appendChild(el);
        } else if (event.type === "tool_error") {
          const el = document.createElement("div");
          el.className = "hint";
          el.style.color = "var(--destructive)";
          el.textContent = event.message;
          assistantWrap.appendChild(el);
        } else if (event.type === "error") {
          ensureAnswerBubble();
          throw new Error(event.message);
        }
        document.getElementById("chat-log").scrollTop = document.getElementById("chat-log").scrollHeight;
      }
    }

    const statusEl = assistantWrap.querySelector(".chat-status");
    if (statusEl && !sawContent) statusEl.remove();
    if (answerText) chatHistory.push({ role: "assistant", content: answerText });
  } catch (e) {
    const statusEl = assistantWrap.querySelector(".chat-status");
    if (statusEl) statusEl.remove();
    const el = document.createElement("div");
    el.className = "chat-bubble";
    el.style.color = "var(--destructive)";
    el.textContent = e.message || "Something went wrong.";
    assistantWrap.appendChild(el);
  } finally {
    chatSending = false;
    document.getElementById("chat-send-btn").disabled = false;
    document.getElementById("chat-log").scrollTop = document.getElementById("chat-log").scrollHeight;
  }
}

document.getElementById("chat-send-btn").addEventListener("click", chatSend);
document.getElementById("chat-input").addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); chatSend(); }
});
document.getElementById("chat-reset-btn").addEventListener("click", () => {
  chatCharts.forEach((c) => c.destroy());
  chatCharts = [];
  chatHistory = [];
  chatResultStore = {};
  document.getElementById("chat-log").innerHTML = `<div class="chat-msg chat-msg-assistant"><div class="chat-bubble">New conversation — ask me anything about the datasets in the lakehouse.</div></div>`;
});

// ==========================================================================================
// INIT
// ==========================================================================================

async function boot() {
  showAppShell();
  renderUserMenu();
  renderNav("dashboard");
  // Dashboard's quick-actions Merge button isn't part of NAV_ITEMS (that's
  // just the sidebar), so it needs its own admin-only gate here — otherwise
  // a non-admin sees a Merge button on the dashboard even though the /merge
  // page itself is already blocked for them.
  // Actually removed (not just `.hidden`) because `.btn` sets its own
  // `display`, which wins the cascade over the bare `[hidden]` UA rule and
  // silently keeps a "hidden" button visible. Removing the node sidesteps
  // that entirely, and control-panel-secondary's flexbox layout (see
  // style.css) automatically rebalances the remaining buttons to fill the
  // row evenly.
  if (!isAdmin()) document.getElementById("dashboard-merge-btn")?.remove();
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