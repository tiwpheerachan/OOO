(() => {
  const el = (id) => document.getElementById(id);

  // =========================================================
  // ✅ Company constants (ตามที่คุณให้มา)
  // =========================================================
  const CLIENT_RABBIT = "0105561071873";
  const CLIENT_SHD    = "0105563022918";
  const CLIENT_TOPONE = "0105565027615";

  const CLIENTS = {
    SHD:     { label: "SHD",     taxId: CLIENT_SHD,    tokens: ["shd"] },
    TOPONE:  { label: "TOPONE",  taxId: CLIENT_TOPONE, tokens: ["topone","top one","top_one","top-one"] },
    RABBIT:  { label: "RABBIT",  taxId: CLIENT_RABBIT, tokens: ["rabbit","rb","rbb"] },
    HASHTAG: { label: "HASHTAG", taxId: "",            tokens: ["hashtag","#hashtag","hash tag","hash-tag"] },
  };

  // ✅ UI labels (chips) + tokens for filename inference
  const PLATFORMS = {
    SHOPEE:   { label: "SHOPEE",   tokens: ["shopee","spay"] },
    LAZADA:   { label: "LAZADA",   tokens: ["lazada","laz"] },
    TIKTOK:   { label: "TIKTOK",   tokens: ["tiktok","tts","ttshop","tik tok","tt_"] },
    SPX:      { label: "SPX",      tokens: ["spx","shopee express","shopee-express","shopee  express"] },
    FACEBOOK: { label: "FACEBOOK", tokens: ["facebook","fb","meta"] },
    OTHER:    { label: "OTHER",    tokens: [] },
  };

  // ✅ backend expects lowercase classifier labels
  const PLATFORM_TO_BACKEND = {
    SHOPEE: "shopee",
    LAZADA: "lazada",
    TIKTOK: "tiktok",
    SPX: "spx",
    FACEBOOK: "other",
    OTHER: "other",
  };

  const state = {
    files: [],
    jobId: null,
    rows: [],
    filter: "all",
    q: "",
    backendUrl: (localStorage.getItem("peak_backend_url") || "http://localhost:8000").replace(/\/$/, ""),
    pollTimer: null,
    editMode: false,

    // ✅ pre-upload filters (multi select)
    clientFilters: new Set(),
    platformFilters: new Set(),

    // ✅ remember which filters used for each job (local)
    jobConfig: null,

    // ✅ filter strictness
    strictMode: false,

    // ✅ NEW: compute WHT (ภาษีหัก ณ ที่จ่าย)
    computeWht: true,

    // ✅ balloons guard
    balloonFiredForJob: "",
  };

  const LS_HISTORY_KEY = "peak_job_history_v1";
  const LS_EDITS_PREFIX = "peak_job_edits::";
  const LS_JOBCFG_PREFIX = "peak_job_cfg::";
  const HISTORY_MAX = 50;

  // =========================================================
  // ✅ Columns (เพิ่มคอลัมน์ ชื่อบริษัท)
  // =========================================================
  const COLUMNS = [
    ["A_seq","ลำดับที่*"],
    ["A_company_name","ชื่อบริษัท"],
    ["B_doc_date","วันที่เอกสาร"],
    ["C_reference","อ้างอิงถึง"],
    ["D_vendor_code","ผู้รับเงิน/คู่ค้า"],
    ["E_tax_id_13","เลขทะเบียน 13 หลัก"],
    ["F_branch_5","เลขสาขา 5 หลัก"],
    ["G_invoice_no","เลขที่ใบกำกับฯ"],
    ["H_invoice_date","วันที่ใบกำกับฯ"],
    ["I_tax_purchase_date","วันที่บันทึกภาษีซื้อ"],
    ["J_price_type","ประเภทราคา"],
    ["K_account","บัญชี"],
    ["L_description","คำอธิบาย"],
    ["M_qty","จำนวน"],
    ["N_unit_price","ราคาต่อหน่วย"],
    ["O_vat_rate","อัตราภาษี"],
    ["P_wht","หัก ณ ที่จ่าย"],
    ["Q_payment_method","ชำระโดย"],
    ["R_paid_amount","จำนวนเงินที่ชำระ"],
    ["S_pnd","ภ.ง.ด."],
    ["T_note","หมายเหตุ"],
    ["U_group","กลุ่มจัดประเภท"],
    ["_status","สถานะ"],
    ["_source_file","ไฟล์ต้นทาง"],
  ];

  const NON_EDITABLE = new Set(["_status","_source_file","A_company_name"]);

  // =========================================================
  // utils
  // =========================================================
  function setBackendUrl(v){
    state.backendUrl = (v || "").trim().replace(/\/$/, "");
    localStorage.setItem("peak_backend_url", state.backendUrl);
  }
  function clamp(n,a,b){ return Math.max(a, Math.min(b,n)); }
  function toPct(done,total){
    if(!total) return 0;
    return clamp(Math.round((done/total)*100), 0, 100);
  }
  function nowISO(){
    try{ return new Date().toISOString(); }catch{ return String(Date.now()); }
  }
  function jobKey(backendUrl, jobId){ return `${backendUrl}::${jobId}`; }

  function escapeHtml(s){
    return String(s ?? "")
      .replaceAll("&","&amp;")
      .replaceAll("<","&lt;")
      .replaceAll(">","&gt;")
      .replaceAll('"',"&quot;")
      .replaceAll("'","&#039;");
  }

  function formatJobMeta(job){
    if(!job) return "—";
    const done = job.processed_files || 0;
    const total = job.total_files || 0;
    const ok = job.ok_files || 0;
    const rev = job.review_files || 0;
    const err = job.error_files || 0;
    return `state=${job.state} · files ${done}/${total} · OK ${ok} · Review ${rev} · Error ${err}`;
  }

  function safeSetText(id, txt){
    const x = el(id);
    if(x) x.textContent = txt;
  }
  function safeSetDisabled(id, on){
    const x = el(id);
    if(x) x.disabled = !!on;
  }

  // =========================================================
  // ✅ Infer (filename)
  // =========================================================
  function normalizeName(s){
    return String(s || "")
      .toLowerCase()
      .replace(/[_\-\.]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function includesAnyToken(name, tokens){
    const s = normalizeName(name);
    for(const t of tokens || []){
      if(!t) continue;
      const tNorm = normalizeName(t);
      const escaped = tNorm.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const rx = new RegExp(`\\b${escaped}\\b`, "i");
      if(rx.test(s)) return true;
    }
    return false;
  }

  function inferClientTagFromFilename(filename){
    const s = normalizeName(filename);
    for(const [tag, config] of Object.entries(CLIENTS)){
      const tokens = config.tokens || [];
      if(tokens.length && includesAnyToken(s, tokens)){
        return tag;
      }
    }
    return null;
  }

  function inferPlatformFromFilename(filename){
    const s = normalizeName(filename);
    if(/\b(spx|shopee\s*express|shopee-express)\b/.test(s)) return "SPX";
    if(/\bshopee\b/.test(s)) return "SHOPEE";
    if(/\b(lazada|laz)\b/.test(s)) return "LAZADA";
    if(/\b(tiktok|tts|ttshop|tik\s*tok)\b/.test(s)) return "TIKTOK";
    if(/\b(facebook|fb|meta)\b/.test(s)) return "FACEBOOK";
    return null;
  }

  // =========================================================
  // ✅ Job config persistence
  // =========================================================
  function saveJobCfg(backendUrl, jobId, cfg){
    try{
      localStorage.setItem(LS_JOBCFG_PREFIX + jobKey(backendUrl, jobId), JSON.stringify(cfg || {}));
    }catch(_){}
  }
  function loadJobCfg(backendUrl, jobId){
    try{
      const raw = localStorage.getItem(LS_JOBCFG_PREFIX + jobKey(backendUrl, jobId));
      const obj = raw ? JSON.parse(raw) : null;
      return obj && typeof obj === "object" ? obj : null;
    }catch(_){ return null; }
  }

  function deriveCompanyNameFromRow(row){
    const clientTaxId = String(row._client_tax_id || row.client_tax_id || "").trim();
    if(clientTaxId){
      if(clientTaxId === CLIENT_RABBIT) return "RABBIT";
      if(clientTaxId === CLIENT_SHD) return "SHD";
      if(clientTaxId === CLIENT_TOPONE) return "TOPONE";
    }
    const clientTag = String(row._client_tag || row.client_tag || "").trim().toUpperCase();
    if(clientTag && CLIENTS[clientTag]) return CLIENTS[clientTag].label;

    if(state.jobConfig?.clientTags?.length === 1){
      const only = state.jobConfig.clientTags[0];
      if(CLIENTS[only]) return CLIENTS[only].label;
    }

    const src = String(row._source_file || "");
    const inferred = inferClientTagFromFilename(src);
    if(inferred && CLIENTS[inferred]) return CLIENTS[inferred].label;

    return "";
  }

  function enrichRowsForUI(){
    state.rows = (state.rows || []).map((r) => {
      const row = (r && typeof r === "object") ? r : {};
      row.A_company_name = row.A_company_name || deriveCompanyNameFromRow(row);
      return row;
    });
  }

  // =========================================================
  // ✅ Snow CSS (เดิม)
  // =========================================================
  function ensureSnowCSS(){
    if(document.getElementById("snowStyle_v1")) return;

    const css = `
.backendSnow{ position:relative; overflow:hidden; }
.backendSnow .snowBox{
  position:absolute; inset:0; pointer-events:none; z-index:0; border-radius: inherit;
}
.backendSnow > *:not(.snowBox){ position:relative; z-index:1; }
.snowflake{
  position:absolute; top:-22px; will-change: transform, opacity;
  filter: drop-shadow(0 6px 10px rgba(20,40,90,.16));
  animation-name: snowFall_v1;
  animation-timing-function: linear;
  animation-iteration-count: infinite;
}
@keyframes snowFall_v1{
  0%   { transform: translate3d(0,-10px,0); opacity: 0; }
  10%  { opacity: 1; }
  100% { transform: translate3d(var(--drift, 0px), calc(100% + 90px), 0); opacity: 0.05; }
}
.chipRow{ display:flex; gap:8px; flex-wrap:wrap; }
.chip.ghost{ opacity:.82; }
.filterDetails{
  font-size: 0.85rem;
  color: rgba(11,23,51,.72);
  margin-top: 0.6rem;
  padding: 0.55rem 0.65rem;
  background: rgba(255,255,255,.55);
  border: 1px solid rgba(20,40,90,.10);
  border-radius: 10px;
}
.filterDetails strong{ color: rgba(11,23,51,.92); }
    `.trim();

    const style = document.createElement("style");
    style.id = "snowStyle_v1";
    style.textContent = css;
    document.head.appendChild(style);
  }

  function createSnowflakes(containerId, snowflakeCount = 50){
    const snowContainer = document.getElementById(containerId);
    if(!snowContainer) return;

    ensureSnowCSS();
    snowContainer.innerHTML = "";

    const snowflakes = ["❄","❅","❆"];
    for(let i = 0; i < snowflakeCount; i++){
      const snowflake = document.createElement("div");
      snowflake.className = "snowflake";
      snowflake.textContent = snowflakes[Math.floor(Math.random() * snowflakes.length)];

      const left = Math.random() * 100;
      const delay = Math.random() * 6;
      const duration = 7 + Math.random() * 9;
      const drift = (Math.random() - 0.5) * 70;
      const size = 9 + Math.random() * 5;
      const opacity = 0.25 + Math.random() * 0.35;
      const topStart = -18 - Math.random() * 40;

      snowflake.style.left = `${left}%`;
      snowflake.style.top = `${topStart}px`;
      snowflake.style.animationDelay = `${delay}s`;
      snowflake.style.animationDuration = `${duration}s`;
      snowflake.style.setProperty("--drift", `${drift}px`);
      snowflake.style.fontSize = `${size}px`;
      snowflake.style.opacity = `${opacity}`;

      snowContainer.appendChild(snowflake);
    }
  }

  function initSnow(){
    const box = el("snowBackend");
    if(!box) return;
    createSnowflakes("snowBackend", 20);

    document.addEventListener("visibilitychange", () => {
      if(document.visibilityState === "visible"){
        createSnowflakes("snowBackend", 20);
      }
    });
  }

  // =========================================================
  // ✅ Balloon FX (เดิม)
  // =========================================================
  function ensureBalloonCSS(){
    if(document.getElementById("balloonStyle_v1")) return;
    const css = `
.balloonsLayer{
  position:fixed;
  inset:0;
  pointer-events:none;
  z-index:9999;
}
.balloon{
  position:absolute;
  bottom:-120px;
  width:74px;
  height:92px;
  border-radius: 50% 50% 48% 48%;
  box-shadow: 0 18px 30px rgba(20,40,90,.18);
  border: 1px solid rgba(255,255,255,.45);
  transform: translate3d(-50%,0,0);
  animation: balloonRise_v1 5.3s ease-in forwards;
}
.balloon::after{
  content:"";
  position:absolute;
  left:50%;
  bottom:-16px;
  width:2px;
  height:18px;
  background: rgba(20,40,90,.18);
  transform: translateX(-50%);
}
@keyframes balloonRise_v1{
  0%   { transform: translate3d(-50%, 0, 0) scale(.96); opacity: 0; }
  10%  { opacity: 1; }
  100% { transform: translate3d(-50%, -115vh, 0) scale(1.04); opacity: 0; }
}
    `.trim();
    const style = document.createElement("style");
    style.id = "balloonStyle_v1";
    style.textContent = css;
    document.head.appendChild(style);
  }

  function launchBalloonsOnce(jobId){
    const key = jobId || "nojob";
    if(state.balloonFiredForJob === key) return;
    state.balloonFiredForJob = key;

    ensureBalloonCSS();

    const layer = document.createElement("div");
    layer.className = "balloonsLayer";
    document.body.appendChild(layer);

    const lefts = [24, 52, 78];
    const colors = [
      "linear-gradient(180deg, rgba(140,210,255,.95), rgba(120,160,255,.85))",
      "linear-gradient(180deg, rgba(190,255,210,.95), rgba(120,210,170,.85))",
      "linear-gradient(180deg, rgba(255,200,230,.95), rgba(210,140,255,.85))"
    ];

    for(let i=0;i<3;i++){
      const b = document.createElement("div");
      b.className = "balloon";
      b.style.left = lefts[i] + "%";
      b.style.background = colors[i];
      b.style.animationDelay = (i * 0.35) + "s";
      layer.appendChild(b);
    }

    setTimeout(() => { try{ layer.remove(); }catch(e){} }, 5600);
  }

  // =========================================================
  // History
  // =========================================================
  function loadHistory(){
    try{
      const raw = localStorage.getItem(LS_HISTORY_KEY);
      const arr = raw ? JSON.parse(raw) : [];
      return Array.isArray(arr) ? arr : [];
    }catch(_){ return []; }
  }
  function saveHistory(arr){
    try{
      localStorage.setItem(LS_HISTORY_KEY, JSON.stringify(arr.slice(0, HISTORY_MAX)));
    }catch(_){}
  }
  function pushHistory(entry){
    const arr = loadHistory();
    const key = jobKey(entry.backendUrl, entry.jobId);
    const filtered = arr.filter(x => jobKey(x.backendUrl, x.jobId) !== key);
    filtered.unshift(entry);
    saveHistory(filtered);
    renderHistory();
  }
  function clearHistory(){
    saveHistory([]);
    renderHistory();
  }
  function fmtLocal(dtISO){
    try{
      const d = new Date(dtISO);
      return d.toLocaleString("th-TH", { dateStyle:"medium", timeStyle:"short" });
    }catch(_){ return String(dtISO || ""); }
  }

  // =========================================================
  // edits persistence
  // =========================================================
  function loadEdits(backendUrl, jobId){
    try{
      const raw = localStorage.getItem(LS_EDITS_PREFIX + jobKey(backendUrl, jobId));
      const obj = raw ? JSON.parse(raw) : null;
      return obj && typeof obj === "object" ? obj : null;
    }catch(_){ return null; }
  }
  function saveEdits(backendUrl, jobId, rows){
    try{
      const payload = { savedAt: nowISO(), rows };
      localStorage.setItem(LS_EDITS_PREFIX + jobKey(backendUrl, jobId), JSON.stringify(payload));
    }catch(_){}
  }
  function applyEditsIfAny(){
    if(!state.jobId) return;
    const edits = loadEdits(state.backendUrl, state.jobId);
    if(edits?.rows && Array.isArray(edits.rows)){
      state.rows = edits.rows;
    }
  }

  // =========================================================
  // Filter (results table)
  // =========================================================
  function matchRow(row){
    if(state.filter !== "all"){
      if(String(row._status||"").toUpperCase() !== state.filter) return false;
    }
    if(state.q){
      const q = state.q.toLowerCase();
      for(const [k] of COLUMNS){
        const v = String(row[k] ?? "");
        if(v.toLowerCase().includes(q)) return true;
      }
      return false;
    }
    return true;
  }

  // =========================================================
  // Table render
  // =========================================================
  function renderTable(){
    const thead = el("thead");
    const tbody = el("tbody");
    if(!thead || !tbody) return;

    thead.innerHTML =
      "<tr>" +
      COLUMNS.map(([k, label]) => `<th title="${escapeHtml(k)}">${escapeHtml(label)}</th>`).join("") +
      "</tr>";

    const filtered = (state.rows || []).filter(matchRow);

    const rowsHtml = filtered.map((r, idx) => {
      const cls = (String(r._status||"") === "NEEDS_REVIEW") ? "review" : "";
      const tds = COLUMNS.map(([k]) => {
        const v = (r[k] ?? "");
        const isLongField = (k === "L_description" || k === "T_note");
        const errors = Array.isArray(r._errors) ? r._errors : [];
        const editable = state.editMode && !NON_EDITABLE.has(k);

        if(editable){
          const val = String(v ?? "");
          const errHtml = (isLongField && errors.length)
            ? `<div class="err">${escapeHtml(errors.join(" · "))}</div>` : "";
          return `
            <td>
              <input class="cellEdit"
                     data-ri="${idx}"
                     data-k="${escapeHtml(k)}"
                     value="${escapeHtml(val)}" />
              ${errHtml}
            </td>
          `;
        }else{
          const errHtml = (isLongField && errors.length)
            ? `<div class="err">${escapeHtml(errors.join(" · "))}</div>` : "";
          return `
            <td>
              <span class="cell" title="${escapeHtml(String(v ?? ""))}">${escapeHtml(String(v ?? ""))}</span>
              ${errHtml}
            </td>
          `;
        }
      }).join("");

      return `<tr class="${cls}">${tds}</tr>`;
    }).join("");

    tbody.innerHTML = rowsHtml || `<tr><td colspan="${COLUMNS.length}" class="muted">ไม่มีข้อมูล</td></tr>`;
    syncHScrollGeometry();
  }

  function readEditsFromDOM(){
    const tbody = el("tbody");
    if(!tbody) return;

    const inputs = tbody.querySelectorAll("input.cellEdit[data-ri][data-k]");
    if(!inputs.length) return;

    const filtered = (state.rows || []).filter(matchRow);

    inputs.forEach((inp) => {
      const ri = Number(inp.getAttribute("data-ri"));
      const k = inp.getAttribute("data-k");
      if(!Number.isFinite(ri) || !k) return;
      const row = filtered[ri];
      if(!row) return;
      row[k] = inp.value;
    });
  }

  // =========================================================
  // Queue render (job.files)
  // =========================================================
  function renderQueue(job){
    const q = el("queue");
    if(!q) return;
    if(!job || !Array.isArray(job.files) || job.files.length === 0){
      q.innerHTML = `<div class="muted small">ยังไม่มีงาน</div>`;
      return;
    }
    q.innerHTML = job.files.map((f) => {
      const st = String(f.state || "").toLowerCase();
      const badgeCls = st === "done" ? "ok" : (st === "needs_review" ? "review" : "");
      const label = st === "needs_review" ? "review" : (st || "queued");
      const sub = [
        `platform=${f.platform || "unknown"}`,
        `rows=${f.rows_count || 0}`,
        f.message || ""
      ].filter(Boolean).join(" · ");
      return `
        <div class="qItem">
          <div class="badge ${badgeCls}">${escapeHtml(label)}</div>
          <div class="qMeta">
            <div class="qName">${escapeHtml(f.filename || "")}</div>
            <div class="qSub">${escapeHtml(sub)}</div>
          </div>
        </div>
      `;
    }).join("");
  }

  // =========================================================
  // Progress UI
  // =========================================================
  function setProgressUI(job){
    const done = job?.processed_files || 0;
    const total = job?.total_files || 0;
    const ok = job?.ok_files || 0;
    const rev = job?.review_files || 0;
    const pct = toPct(done, total);

    const ring = el("ring");
    const barFill = el("barFill");
    const progressPct = el("progressPct");
    const jobState = el("jobState");
    const kpiInWork = el("kpiInWork");
    const kpiOk = el("kpiOk");
    const kpiReview = el("kpiReview");
    const kpiMiniFiles = el("kpiMiniFiles");
    const queueHint = el("queueHint");

    if(ring) ring.style.setProperty("--p", String(pct));
    if(barFill) barFill.style.width = `${pct}%`;
    if(progressPct) progressPct.textContent = `${pct}%`;
    if(jobState) jobState.textContent = job?.state ? `state=${job.state}` : "—";

    const inWork = total ? Math.max(0, total - done) : "—";
    if(kpiInWork) kpiInWork.textContent = String(inWork);
    if(kpiOk) kpiOk.textContent = String(ok || 0);
    if(kpiReview) kpiReview.textContent = String(rev || 0);

    if(kpiMiniFiles){
      kpiMiniFiles.textContent = total ? `files ${done}/${total}` : "—";
    }
    if(queueHint){
      queueHint.textContent = total ? `ประมวลผลแล้ว ${done}/${total}` : "—";
    }
  }

  // =========================================================
  // API wrapper
  // =========================================================
  async function api(path, opts = {}){
    const url = state.backendUrl + path;
    const res = await fetch(url, opts);
    if(!res.ok){
      const txt = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText} :: ${txt}`);
    }
    return res;
  }

  // =========================================================
  // Polling
  // =========================================================
  async function pollJob(){
    if(!state.jobId) return;
    try{
      const job = await (await api(`/api/job/${state.jobId}`)).json();

      safeSetText("jobMeta", formatJobMeta(job));
      setProgressUI(job);
      renderQueue(job);

      if(job.state === "done" || job.state === "error"){
        if(state.pollTimer){
          clearInterval(state.pollTimer);
          state.pollTimer = null;
        }

        state.jobConfig = loadJobCfg(state.backendUrl, state.jobId) || null;

        const rowsRes = await (await api(`/api/job/${state.jobId}/rows`)).json();
        state.rows = rowsRes.rows || [];

        applyEditsIfAny();
        enrichRowsForUI();
        renderTable();

        safeSetDisabled("btnCsv", false);
        safeSetDisabled("btnXlsx", false);
        safeSetDisabled("btnCsvEdited", state.rows.length === 0);
        safeSetDisabled("btnXlsxEdited", state.rows.length === 0);

        pushHistory({
          jobId: state.jobId,
          backendUrl: state.backendUrl,
          finishedAt: nowISO(),
          state: job.state,
          processed_files: job.processed_files || 0,
          total_files: job.total_files || 0,
          ok_files: job.ok_files || 0,
          review_files: job.review_files || 0,
          error_files: job.error_files || 0,
          rows_count: state.rows.length,
          clientTags: state.jobConfig?.clientTags || [],
          platforms: state.jobConfig?.platforms || [],
          computeWht: !!state.jobConfig?.computeWht,
        });

        if(job.state === "done"){
          launchBalloonsOnce(state.jobId);
        }
      }
    }catch(e){
      console.warn(e);
    }
  }

  // =========================================================
  // History Modal
  // =========================================================
  function openModal(){
    const m = el("historyModal");
    if(!m) return;
    m.classList.add("isOpen");
    m.setAttribute("aria-hidden", "false");
    renderHistory();
  }
  function closeModal(){
    const m = el("historyModal");
    if(!m) return;
    m.classList.remove("isOpen");
    m.setAttribute("aria-hidden", "true");
  }

  function renderHistory(){
    const list = el("historyList");
    const count = el("historyCount");
    if(!list || !count) return;

    const arr = loadHistory();
    count.textContent = `${arr.length} รายการ (เก็บสูงสุด ${HISTORY_MAX})`;

    if(!arr.length){
      list.innerHTML = `<div class="muted small">ยังไม่มีประวัติ</div>`;
      return;
    }

    list.innerHTML = arr.map((h) => {
      const pct = toPct(h.processed_files || 0, h.total_files || 0);
      const stateLabel = h.state || "done";
      const sub = [
        `backend=${h.backendUrl}`,
        `job=${h.jobId}`,
        `files=${h.processed_files || 0}/${h.total_files || 0}`,
        `OK=${h.ok_files || 0}`,
        `Review=${h.review_files || 0}`,
        `Error=${h.error_files || 0}`,
        `rows=${h.rows_count || 0}`,
        (h.clientTags?.length ? `client=${h.clientTags.join("+")}` : ""),
        (h.platforms?.length ? `platform=${h.platforms.join("+")}` : ""),
        (typeof h.computeWht === "boolean" ? `WHT=${h.computeWht ? "on" : "off"}` : ""),
      ].filter(Boolean).join(" · ");

      return `
        <div class="hItem">
          <div class="hLeft">
            <div class="hTitle">${escapeHtml(fmtLocal(h.finishedAt))}</div>
            <div class="hSub">${escapeHtml(sub)}</div>
          </div>
          <div class="hRight">
            <span class="hPill">${escapeHtml(stateLabel)} · ${pct}%</span>
            <button class="btn" data-load="${escapeHtml(h.jobId)}" data-backend="${escapeHtml(h.backendUrl)}">โหลด</button>
            <button class="btn danger" data-del="${escapeHtml(h.jobId)}" data-backend="${escapeHtml(h.backendUrl)}">ลบ</button>
          </div>
        </div>
      `;
    }).join("");

    list.querySelectorAll("[data-load]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const jobId = btn.getAttribute("data-load");
        const backendUrl = btn.getAttribute("data-backend");
        if(!jobId || !backendUrl) return;

        setEditMode(false);
        setBackendUrl(backendUrl);

        const backendUrlInput = el("backendUrl");
        if(backendUrlInput) backendUrlInput.value = state.backendUrl;

        const backendSelect = el("backendSelect");
        if(backendSelect) backendSelect.value = backendUrl;

        state.jobId = jobId;
        state.balloonFiredForJob = "";

        safeSetText("jobMeta", `job_id=${state.jobId} · loading...`);

        safeSetDisabled("btnCsv", true);
        safeSetDisabled("btnXlsx", true);
        safeSetDisabled("btnCsvEdited", true);
        safeSetDisabled("btnXlsxEdited", true);

        try{
          state.jobConfig = loadJobCfg(state.backendUrl, state.jobId) || null;

          const job = await (await api(`/api/job/${state.jobId}`)).json();
          safeSetText("jobMeta", formatJobMeta(job));
          setProgressUI(job);
          renderQueue(job);

          const rowsRes = await (await api(`/api/job/${state.jobId}/rows`)).json();
          state.rows = rowsRes.rows || [];

          applyEditsIfAny();
          enrichRowsForUI();
          renderTable();

          safeSetDisabled("btnCsv", false);
          safeSetDisabled("btnXlsx", false);
          safeSetDisabled("btnCsvEdited", state.rows.length === 0);
          safeSetDisabled("btnXlsxEdited", state.rows.length === 0);

          if(job.state === "done") launchBalloonsOnce(state.jobId);

          closeModal();
        }catch(e){
          alert("โหลดไม่สำเร็จ (job อาจ expired): " + (e?.message || e));
        }
      });
    });

    list.querySelectorAll("[data-del]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const jobId = btn.getAttribute("data-del");
        const backendUrl = btn.getAttribute("data-backend");
        if(!jobId || !backendUrl) return;
        const arr2 = loadHistory().filter(x => jobKey(x.backendUrl, x.jobId) !== jobKey(backendUrl, jobId));
        saveHistory(arr2);
        renderHistory();
      });
    });
  }

  // =========================================================
  // Edit mode
  // =========================================================
  function setEditMode(on){
    state.editMode = !!on;
    const btnSave = el("btnSave");
    const btnEdit = el("btnEdit");
    if(btnSave) btnSave.disabled = !state.editMode;
    if(btnEdit) btnEdit.textContent = state.editMode ? "ยกเลิกแก้ไข" : "แก้ไข";
    renderTable();
  }

  function saveEditsNow(){
    if(!state.jobId) return;
    readEditsFromDOM();
    saveEdits(state.backendUrl, state.jobId, state.rows);
    setEditMode(false);
    safeSetDisabled("btnCsvEdited", state.rows.length === 0);
    safeSetDisabled("btnXlsxEdited", state.rows.length === 0);
    alert("บันทึกแล้ว ✅ (เก็บในเครื่อง + Export ได้)");
  }

  // =========================================================
  // Export edited CSV/XLSX (client-side)
  // =========================================================
  function downloadBlob(blob, filename){
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 2000);
  }

  function csvEscape(val){
    const s = String(val ?? "");
    if(/[,"\n\r]/.test(s)){
      return `"${s.replaceAll('"','""')}"`;
    }
    return s;
  }

  function exportEditedCSV(){
    const header = COLUMNS.map(([,label]) => label).join(",");
    const lines = (state.rows || []).map((r) => COLUMNS.map(([k]) => csvEscape(r[k] ?? "")).join(","));
    const csv = [header, ...lines].join("\r\n");
    const blob = new Blob(["\ufeff", csv], { type: "text/csv;charset=utf-8" });
    downloadBlob(blob, `peak_AU_edited_${state.jobId || "nojob"}.csv`);
  }

  function exportEditedXLSX(){
    const cols = COLUMNS.map(([k,label]) => ({ k, label }));
    const rowsHtml = (state.rows || []).map((r) => {
      const tds = cols.map(c => `<td>${escapeHtml(String(r[c.k] ?? ""))}</td>`).join("");
      return `<tr>${tds}</tr>`;
    }).join("");

    const html =
`<html>
<head><meta charset="utf-8" /></head>
<body>
<table border="1">
<thead><tr>${cols.map(c => `<th>${escapeHtml(c.label)}</th>`).join("")}</tr></thead>
<tbody>${rowsHtml}</tbody>
</table>
</body></html>`;

    const blob = new Blob([html], { type: "application/vnd.ms-excel;charset=utf-8" });
    downloadBlob(blob, `peak_AU_edited_${state.jobId || "nojob"}.xls`);
  }

  // =========================================================
  // Horizontal dock sync (เดิม)
  // =========================================================
  let hsyncLock = false;

  function syncHScrollGeometry(){
    const wrap = el("tableWrap");
    const dock = el("hScrollDock");
    const bar = el("hScrollBar");
    const inner = el("hScrollInner");
    const table = el("resultTable");
    if(!wrap || !dock || !bar || !inner || !table) return;

    const scrollW = wrap.scrollWidth;
    const clientW = wrap.clientWidth;

    const need = scrollW > clientW + 2;
    dock.style.display = need ? "flex" : "none";
    if(!need) return;

    inner.style.width = `${scrollW}px`;

    if(!hsyncLock){
      hsyncLock = true;
      bar.scrollLeft = wrap.scrollLeft;
      hsyncLock = false;
    }
  }

  function bindHScrollSync(){
    const wrap = el("tableWrap");
    const bar = el("hScrollBar");
    if(!wrap || !bar) return;

    wrap.addEventListener("scroll", () => {
      if(hsyncLock) return;
      hsyncLock = true;
      bar.scrollLeft = wrap.scrollLeft;
      hsyncLock = false;
    }, { passive: true });

    bar.addEventListener("scroll", () => {
      if(hsyncLock) return;
      hsyncLock = true;
      wrap.scrollLeft = bar.scrollLeft;
      hsyncLock = false;
    }, { passive: true });

    window.addEventListener("resize", () => syncHScrollGeometry());
  }

  // =========================================================
  // Pre-upload filter controls
  // =========================================================
  function toggleChip(btn, isOn){
    if(!btn) return;
    btn.classList.toggle("active", !!isOn);
  }

  function syncFilterChipsUI(){
    document.querySelectorAll("[data-client]").forEach((b) => {
      const tag = String(b.getAttribute("data-client") || "").toUpperCase();
      toggleChip(b, state.clientFilters.has(tag));
    });
    document.querySelectorAll("[data-platform]").forEach((b) => {
      const p = String(b.getAttribute("data-platform") || "").toUpperCase();
      toggleChip(b, state.platformFilters.has(p));
    });

    const strictEl = el("strictMode");
    if(strictEl && strictEl.type === "checkbox"){
      strictEl.checked = !!state.strictMode;
    }

    const whtEl = el("computeWht");
    if(whtEl && whtEl.type === "checkbox"){
      whtEl.checked = !!state.computeWht;
    }
  }

  function setButtonsEnabled(hasFiles){
    const up = el("btnUpload");
    const cl = el("btnClear");
    if(up) up.disabled = !hasFiles;
    if(cl) cl.disabled = !hasFiles;
  }

  function setUploadInfo(extraNote = ""){
    const info = el("uploadInfo");
    if(!info) return;

    if(!state.files.length){
      info.textContent = "ยังไม่ได้เลือกไฟล์";
      return;
    }

    const size = state.files.reduce((a,f) => a + (f.size||0), 0);
    const base = `${state.files.length} ไฟล์ · ${(size/1024/1024).toFixed(2)} MB`;
    info.textContent = extraNote ? `${base} · ${extraNote}` : base;
  }

  function prefilterFilesBeforeUpload(files){
    const doClient = state.clientFilters.size > 0;
    const doPlat = state.platformFilters.size > 0;

    if(!doClient && !doPlat){
      return { kept: files, skipped: [], details: [], note: "" };
    }

    const kept = [];
    const skipped = [];
    const details = [];

    for(const f of files){
      const fname = f?.name || "";
      const clientTag = inferClientTagFromFilename(fname);
      const platformTag = inferPlatformFromFilename(fname);

      let passClient = true;
      let passPlatform = true;
      let reason = "";

      if(doClient){
        if(clientTag){
          if(state.clientFilters.has(clientTag)){
            passClient = true;
          }else{
            passClient = false;
            reason = `client=${clientTag} (ไม่อยู่ใน filter)`;
          }
        }else{
          if(state.strictMode){
            passClient = false;
            reason = "client=unknown (strict mode)";
          }else{
            passClient = true;
          }
        }
      }

      if(doPlat){
        if(platformTag){
          if(state.platformFilters.has(platformTag)){
            passPlatform = true;
          }else{
            passPlatform = false;
            reason = reason
              ? `${reason}, platform=${platformTag} (ไม่อยู่ใน filter)`
              : `platform=${platformTag} (ไม่อยู่ใน filter)`;
          }
        }else{
          if(state.strictMode){
            passPlatform = false;
            reason = reason
              ? `${reason}, platform=unknown (strict mode)`
              : "platform=unknown (strict mode)";
          }else{
            passPlatform = true;
          }
        }
      }

      const pass = passClient && passPlatform;

      if(pass){
        kept.push(f);
        details.push({
          filename: fname,
          status: "KEEP",
          client: clientTag || "unknown",
          platform: platformTag || "unknown",
        });
      }else{
        skipped.push(f);
        details.push({
          filename: fname,
          status: "SKIP",
          client: clientTag || "unknown",
          platform: platformTag || "unknown",
          reason: reason || "ไม่ตรง filter",
        });
      }
    }

    let note = "";
    if(skipped.length > 0) note = `ข้าม ${skipped.length} ไฟล์ (ไม่ตรง filter)`;
    if(kept.length === 0 && files.length > 0) note = "⚠️ ทุกไฟล์ถูกข้าม - ลองปรับ filter";

    return { kept, skipped, details, note };
  }

  function currentJobCfgFromFilters(){
    const clientTags = Array.from(state.clientFilters);
    const platformsUI = Array.from(state.platformFilters);

    const clientTaxIds = clientTags
      .map(t => CLIENTS[t]?.taxId)
      .filter(Boolean);

    const platforms = platformsUI
      .map(p => PLATFORM_TO_BACKEND[p] || "")
      .filter(Boolean);

    const uniq = (arr) => {
      const s = new Set();
      const out = [];
      for(const x of arr){
        if(!x || s.has(x)) continue;
        s.add(x);
        out.push(x);
      }
      return out;
    };

    return {
      clientTags: uniq(clientTags),
      clientTaxIds: uniq(clientTaxIds),
      platforms: uniq(platforms),
      strictMode: !!state.strictMode,
      computeWht: !!state.computeWht,
      savedAt: nowISO()
    };
  }

  function showFilterDetails(details){
    const container = el("filterDetails");
    if(!container) return;

    if(!details || details.length === 0){
      container.style.display = "none";
      return;
    }

    const kept = details.filter(d => d.status === "KEEP");
    const skipped = details.filter(d => d.status === "SKIP");

    let html = `<div class="filterDetails">`;
    html += `<strong>รายละเอียดการกรอง:</strong><br>`;
    html += `✅ เก็บ: ${kept.length} ไฟล์<br>`;
    html += `❌ ข้าม: ${skipped.length} ไฟล์<br>`;

    if(skipped.length > 0){
      html += `<br><strong>ตัวอย่างไฟล์ที่ข้าม:</strong><br>`;
      skipped.slice(0, 10).forEach(d => {
        html += `• ${escapeHtml(d.filename)} (${escapeHtml(d.reason || "")})<br>`;
      });
      if(skipped.length > 10){
        html += `… และอีก ${skipped.length - 10} ไฟล์<br>`;
      }
    }

    html += `</div>`;
    container.innerHTML = html;
    container.style.display = "block";
  }

  // =========================================================
  // Bind UI
  // =========================================================
  function bind(){
    const backendUrlInput = el("backendUrl");
    const backendSelect = el("backendSelect");

    const DEFAULT_BACKEND = "https://ooo-taen.onrender.com";
    const saved = localStorage.getItem("peak_backend_url");
    setBackendUrl(saved || DEFAULT_BACKEND);

    if(backendUrlInput) backendUrlInput.value = state.backendUrl;
    if(backendSelect) backendSelect.value = state.backendUrl;

    backendSelect?.addEventListener("change", () => {
      setBackendUrl(backendSelect.value);
      if(backendUrlInput) backendUrlInput.value = state.backendUrl;
    });

    backendUrlInput?.addEventListener("change", (e) => {
      setBackendUrl(e.target.value);
      if(backendSelect){
        const v = state.backendUrl;
        if(v === DEFAULT_BACKEND || v === "http://localhost:8000"){
          backendSelect.value = v;
        }
      }
    });

    const strictEl = el("strictMode");
    if(strictEl && strictEl.type === "checkbox"){
      strictEl.addEventListener("change", () => {
        state.strictMode = !!strictEl.checked;
      });
    }

    // ✅ NEW: WHT toggle
    const whtEl = el("computeWht");
    if(whtEl && whtEl.type === "checkbox"){
      whtEl.addEventListener("change", () => {
        state.computeWht = !!whtEl.checked;
      });
    }

    document.querySelectorAll("[data-client]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const tag = String(btn.getAttribute("data-client") || "").toUpperCase();
        if(!tag) return;
        if(state.clientFilters.has(tag)) state.clientFilters.delete(tag);
        else state.clientFilters.add(tag);
        syncFilterChipsUI();
      });
    });

    document.querySelectorAll("[data-platform]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const p = String(btn.getAttribute("data-platform") || "").toUpperCase();
        if(!p) return;
        if(state.platformFilters.has(p)) state.platformFilters.delete(p);
        else state.platformFilters.add(p);
        syncFilterChipsUI();
      });
    });

    el("btnClientAll")?.addEventListener("click", () => {
      Object.keys(CLIENTS).forEach(k => state.clientFilters.add(k));
      syncFilterChipsUI();
    });
    el("btnClientClear")?.addEventListener("click", () => {
      state.clientFilters.clear();
      syncFilterChipsUI();
    });

    el("btnPlatformAll")?.addEventListener("click", () => {
      Object.keys(PLATFORMS).forEach(k => state.platformFilters.add(k));
      syncFilterChipsUI();
    });
    el("btnPlatformClear")?.addEventListener("click", () => {
      state.platformFilters.clear();
      syncFilterChipsUI();
    });

    // default UI sync
    syncFilterChipsUI();

    // File picker
    el("btnPick")?.addEventListener("click", () => el("file")?.click());

    const fileInput = el("file");
    fileInput?.addEventListener("change", () => {
      state.files = Array.from(fileInput.files || []);
      setButtonsEnabled(state.files.length > 0);
      setUploadInfo();
    });

    // Drag drop
    const drop = el("drop");
    drop?.addEventListener("dragover", (e) => { e.preventDefault(); drop.classList.add("isOver"); });
    drop?.addEventListener("dragleave", () => drop.classList.remove("isOver"));
    drop?.addEventListener("drop", (e) => {
      e.preventDefault();
      drop.classList.remove("isOver");
      const files = Array.from(e.dataTransfer?.files || []);
      state.files = files;

      try{
        const dt = new DataTransfer();
        files.forEach(f => dt.items.add(f));
        fileInput.files = dt.files;
      }catch(_){}

      setButtonsEnabled(files.length > 0);
      setUploadInfo();
    });

    // Upload
    el("btnUpload")?.addEventListener("click", async () => {
      if(!state.files.length) return;

      setEditMode(false);
      safeSetDisabled("btnUpload", true);

      state.jobId = null;
      state.rows = [];
      state.balloonFiredForJob = "";
      renderTable();

      const { kept, details, note } = prefilterFilesBeforeUpload(state.files);
      showFilterDetails(details);

      if(!kept.length){
        alert(
          "⚠️ ไม่มีไฟล์ที่ตรงกับ Filter ที่เลือก (ทุกไฟล์ถูกข้ามหมด)\n\n" +
          "แนะนำ:\n" +
          "• กด Clear ที่ Client/Platform filter\n" +
          "• ปิด Strict Mode\n" +
          "• ตรวจชื่อไฟล์ให้มี keyword เช่น shopee / tiktok / shd / rabbit\n"
        );
        safeSetDisabled("btnUpload", false);
        return;
      }

      setUploadInfo(note);

      safeSetText("jobMeta", "uploading...");
      setProgressUI({ processed_files: 0, total_files: kept.length, ok_files: 0, review_files: 0, state: "uploading" });

      const fd = new FormData();
      kept.forEach((f) => fd.append("files", f, f.name));

      // cfg -> backend (ต้องส่งครบ!)
      const cfg = currentJobCfgFromFilters();
      fd.append("client_tags", (cfg.clientTags || []).join(","));
      fd.append("client_tax_ids", (cfg.clientTaxIds || []).join(","));
      fd.append("platforms", (cfg.platforms || []).join(","));
      fd.append("strict_mode", cfg.strictMode ? "1" : "0");
      fd.append("compute_wht", cfg.computeWht ? "1" : "0");

      try{
        const res = await (await api("/api/upload", { method: "POST", body: fd })).json();
        state.jobId = res.job_id;

        saveJobCfg(state.backendUrl, state.jobId, cfg);
        state.jobConfig = cfg;

        const extra = [];
        if(typeof res.files_added === "number") extra.push(`added=${res.files_added}`);
        if(typeof res.files_skipped === "number") extra.push(`skipped=${res.files_skipped}`);
        safeSetText("jobMeta", `job_id=${state.jobId} · processing... ${extra.length ? "(" + extra.join(", ") + ")" : ""}`);

        safeSetDisabled("btnCsv", true);
        safeSetDisabled("btnXlsx", true);
        safeSetDisabled("btnCsvEdited", true);
        safeSetDisabled("btnXlsxEdited", true);

        if(state.pollTimer) clearInterval(state.pollTimer);
        state.pollTimer = setInterval(pollJob, 1200);
        pollJob();
      }catch(e){
        alert("Upload error: " + (e?.message || e));
      }finally{
        safeSetDisabled("btnUpload", false);
      }
    });

    // Clear
    el("btnClear")?.addEventListener("click", () => {
      setEditMode(false);
      state.files = [];
      state.jobId = null;
      state.rows = [];
      state.balloonFiredForJob = "";
      if(fileInput) fileInput.value = "";
      setButtonsEnabled(false);
      setUploadInfo();
      safeSetText("jobMeta", "—");
      safeSetDisabled("btnCsv", true);
      safeSetDisabled("btnXlsx", true);
      safeSetDisabled("btnCsvEdited", true);
      safeSetDisabled("btnXlsxEdited", true);
      const q = el("queue");
      if(q) q.innerHTML = `<div class="muted small">ยังไม่มีงาน</div>`;
      setProgressUI(null);
      renderTable();

      const detailsEl = el("filterDetails");
      if(detailsEl) detailsEl.style.display = "none";
    });

    // Results filter chips
    document.querySelectorAll(".chip[data-filter]").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".chip[data-filter]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        state.filter = btn.dataset.filter || "all";
        renderTable();
      });
    });

    // Search
    el("q")?.addEventListener("input", (e) => {
      state.q = e.target.value.trim();
      renderTable();
    });

    // Export raw (backend)
    el("btnCsv")?.addEventListener("click", () => {
      if(!state.jobId) return;
      window.open(`${state.backendUrl}/api/export/${state.jobId}.csv`, "_blank");
    });
    el("btnXlsx")?.addEventListener("click", () => {
      if(!state.jobId) return;
      window.open(`${state.backendUrl}/api/export/${state.jobId}.xlsx`, "_blank");
    });

    // Export edited (frontend)
    el("btnCsvEdited")?.addEventListener("click", () => {
      if(state.editMode) readEditsFromDOM();
      exportEditedCSV();
    });
    el("btnXlsxEdited")?.addEventListener("click", () => {
      if(state.editMode) readEditsFromDOM();
      exportEditedXLSX();
    });

    // Edit mode
    el("btnEdit")?.addEventListener("click", () => {
      if(!state.rows.length){
        alert("ยังไม่มีข้อมูลให้แก้ไข");
        return;
      }
      setEditMode(!state.editMode);
    });

    el("btnSave")?.addEventListener("click", () => {
      if(!state.editMode) return;
      saveEditsNow();
    });

    // History
    el("btnHistory")?.addEventListener("click", openModal);
    el("btnClearHistory")?.addEventListener("click", () => {
      if(confirm("ล้างประวัติทั้งหมด?")) clearHistory();
    });
    el("historyModal")?.addEventListener("click", (e) => {
      const t = e.target;
      if(t && t.getAttribute && t.getAttribute("data-close") === "1") closeModal();
    });
    window.addEventListener("keydown", (e) => { if(e.key === "Escape") closeModal(); });

    bindHScrollSync();

    renderTable();
    setButtonsEnabled(false);
    setUploadInfo();
    setProgressUI(null);
    renderHistory();
    syncHScrollGeometry();
  }

  // =========================================================
  // Start
  // =========================================================
  document.addEventListener("DOMContentLoaded", () => {
    bind();
    initSnow();
  });

})();
