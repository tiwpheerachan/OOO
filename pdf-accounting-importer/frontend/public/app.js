(() => {
  const el = (id) => document.getElementById(id);

  const state = {
    files: [],
    jobId: null,
    rows: [],
    filter: "all",
    q: "",
    backendUrl: localStorage.getItem("peak_backend_url") || "http://localhost:8000",
    pollTimer: null,
    editMode: false,
  };

  const LS_HISTORY_KEY = "peak_job_history_v1";
  const LS_EDITS_PREFIX = "peak_job_edits::";
  const HISTORY_MAX = 50;

  const COLUMNS = [
    ["A_seq","ลำดับที่*"],
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

  const NON_EDITABLE = new Set(["_status","_source_file"]);

  // ---- utils ----
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

  // ---- history ----
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

  // ---- edits persistence ----
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

  // ---- filter ----
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

  // ---- table render ----
  function renderTable(){
    const thead = el("thead");
    const tbody = el("tbody");
    if(!thead || !tbody) return;

    thead.innerHTML =
      "<tr>" +
      COLUMNS.map(([k, label]) => `<th title="${escapeHtml(k)}">${escapeHtml(label)}</th>`).join("") +
      "</tr>";

    const filtered = state.rows.filter(matchRow);

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

    // ✅ after rendering: update horizontal dock sizing
    syncHScrollGeometry();
  }

  function readEditsFromDOM(){
    const tbody = el("tbody");
    if(!tbody) return;

    const inputs = tbody.querySelectorAll("input.cellEdit[data-ri][data-k]");
    if(!inputs.length) return;

    const filtered = state.rows.filter(matchRow);

    inputs.forEach((inp) => {
      const ri = Number(inp.getAttribute("data-ri"));
      const k = inp.getAttribute("data-k");
      if(!Number.isFinite(ri) || !k) return;
      const row = filtered[ri];
      if(!row) return;
      row[k] = inp.value;
    });
  }

  // ---- queue ----
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

  // ---- progress UI ----
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

  // ---- api ----
  async function api(path, opts = {}){
    const url = state.backendUrl + path;
    const res = await fetch(url, opts);
    if(!res.ok){
      const txt = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText} :: ${txt}`);
    }
    return res;
  }

  // ---- polling ----
  async function pollJob(){
    if(!state.jobId) return;
    try{
      const job = await (await api(`/api/job/${state.jobId}`)).json();

      el("jobMeta").textContent = formatJobMeta(job);
      setProgressUI(job);
      renderQueue(job);

      if(job.state === "done" || job.state === "error"){
        clearInterval(state.pollTimer);
        state.pollTimer = null;

        const rowsRes = await (await api(`/api/job/${state.jobId}/rows`)).json();
        state.rows = rowsRes.rows || [];

        applyEditsIfAny();
        renderTable();

        el("btnCsv").disabled = false;
        el("btnXlsx").disabled = false;
        el("btnCsvEdited").disabled = state.rows.length === 0;
        el("btnXlsxEdited").disabled = state.rows.length === 0;

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
        });
      }
    }catch(e){
      console.warn(e);
    }
  }

  // ---- History Modal ----
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
        `rows=${h.rows_count || 0}`
      ].join(" · ");

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
        el("backendUrl").value = state.backendUrl;

        state.jobId = jobId;
        el("jobMeta").textContent = `job_id=${state.jobId} · loading...`;

        el("btnCsv").disabled = true;
        el("btnXlsx").disabled = true;
        el("btnCsvEdited").disabled = true;
        el("btnXlsxEdited").disabled = true;

        try{
          const job = await (await api(`/api/job/${state.jobId}`)).json();
          el("jobMeta").textContent = formatJobMeta(job);
          setProgressUI(job);
          renderQueue(job);

          const rowsRes = await (await api(`/api/job/${state.jobId}/rows`)).json();
          state.rows = rowsRes.rows || [];

          applyEditsIfAny();
          renderTable();

          el("btnCsv").disabled = false;
          el("btnXlsx").disabled = false;
          el("btnCsvEdited").disabled = state.rows.length === 0;
          el("btnXlsxEdited").disabled = state.rows.length === 0;

          closeModal();
        }catch(e){
          alert("โหลดไม่สำเร็จ (job อาจ expired): " + e.message);
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

  // ---- edit mode ----
  function setEditMode(on){
    state.editMode = !!on;
    el("btnSave").disabled = !state.editMode;
    el("btnEdit").textContent = state.editMode ? "ยกเลิกแก้ไข" : "แก้ไข";
    renderTable();
  }

  function saveEditsNow(){
    if(!state.jobId) return;
    readEditsFromDOM();
    saveEdits(state.backendUrl, state.jobId, state.rows);
    setEditMode(false);
    el("btnCsvEdited").disabled = state.rows.length === 0;
    el("btnXlsxEdited").disabled = state.rows.length === 0;
    alert("บันทึกแล้ว ✅ (เก็บในเครื่อง + Export ได้)");
  }

  // ---- export edited CSV/XLSX ----
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
    const lines = state.rows.map((r) => COLUMNS.map(([k]) => csvEscape(r[k] ?? "")).join(","));
    const csv = [header, ...lines].join("\r\n");
    const blob = new Blob(["\ufeff", csv], { type: "text/csv;charset=utf-8" });
    downloadBlob(blob, `peak_AU_edited_${state.jobId || "nojob"}.csv`);
  }

  function exportEditedXLSX(){
    const cols = COLUMNS.map(([k,label]) => ({ k, label }));
    const rowsHtml = state.rows.map((r) => {
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

  // ---- horizontal dock sync (สำคัญ) ----
  let hsyncLock = false;

  function syncHScrollGeometry(){
    const wrap = el("tableWrap");
    const dock = el("hScrollDock");
    const bar = el("hScrollBar");
    const inner = el("hScrollInner");
    const table = el("resultTable");

    if(!wrap || !dock || !bar || !inner || !table) return;

    // compute scrollable width
    const scrollW = wrap.scrollWidth;
    const clientW = wrap.clientWidth;

    // show dock only if horizontally scrollable
    const need = scrollW > clientW + 2;
    dock.style.display = need ? "flex" : "none";
    if(!need) return;

    // set inner width so bar has same scroll range
    inner.style.width = `${scrollW}px`;

    // sync bar scroll position with wrap
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

  // ---- UI helpers ----
  function setButtonsEnabled(hasFiles){
    el("btnUpload").disabled = !hasFiles;
    el("btnClear").disabled = !hasFiles;
  }

  function setUploadInfo(){
    const info = el("uploadInfo");
    if(!info) return;
    if(!state.files.length){
      info.textContent = "ยังไม่ได้เลือกไฟล์";
    }else{
      const size = state.files.reduce((a,f) => a + (f.size||0), 0);
      info.textContent = `${state.files.length} ไฟล์ · ${(size/1024/1024).toFixed(2)} MB`;
    }
  }

  // ---- bind ----
  function bind(){
    el("backendUrl").value = state.backendUrl;
    el("backendUrl").addEventListener("change", (e) => setBackendUrl(e.target.value));

    el("btnPick").addEventListener("click", () => el("file").click());

    const fileInput = el("file");
    fileInput.addEventListener("change", () => {
      state.files = Array.from(fileInput.files || []);
      setButtonsEnabled(state.files.length > 0);
      setUploadInfo();
    });

    const drop = el("drop");
    drop.addEventListener("dragover", (e) => { e.preventDefault(); drop.classList.add("isOver"); });
    drop.addEventListener("dragleave", () => drop.classList.remove("isOver"));
    drop.addEventListener("drop", (e) => {
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

    el("btnUpload").addEventListener("click", async () => {
      if(!state.files.length) return;

      setEditMode(false);

      el("btnUpload").disabled = true;
      state.jobId = null;
      state.rows = [];
      renderTable();

      el("jobMeta").textContent = "uploading...";
      setProgressUI({ processed_files: 0, total_files: state.files.length, ok_files: 0, review_files: 0, state: "uploading" });

      const fd = new FormData();
      state.files.forEach((f) => fd.append("files", f, f.name));

      try{
        const res = await (await api("/api/upload", { method: "POST", body: fd })).json();
        state.jobId = res.job_id;

        el("jobMeta").textContent = `job_id=${state.jobId} · processing...`;
        el("btnCsv").disabled = true;
        el("btnXlsx").disabled = true;
        el("btnCsvEdited").disabled = true;
        el("btnXlsxEdited").disabled = true;

        if(state.pollTimer) clearInterval(state.pollTimer);
        state.pollTimer = setInterval(pollJob, 1200);
        pollJob();
      }catch(e){
        alert("Upload error: " + e.message);
      }finally{
        el("btnUpload").disabled = false;
      }
    });

    el("btnClear").addEventListener("click", () => {
      setEditMode(false);
      state.files = [];
      state.jobId = null;
      state.rows = [];
      fileInput.value = "";
      setButtonsEnabled(false);
      setUploadInfo();
      el("jobMeta").textContent = "—";
      el("btnCsv").disabled = true;
      el("btnXlsx").disabled = true;
      el("btnCsvEdited").disabled = true;
      el("btnXlsxEdited").disabled = true;
      el("queue").innerHTML = `<div class="muted small">ยังไม่มีงาน</div>`;
      setProgressUI(null);
      renderTable();
    });

    document.querySelectorAll(".chip").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".chip").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        state.filter = btn.dataset.filter || "all";
        renderTable();
      });
    });

    el("q").addEventListener("input", (e) => {
      state.q = e.target.value.trim();
      renderTable();
    });

    el("btnCsv").addEventListener("click", () => {
      if(!state.jobId) return;
      window.open(`${state.backendUrl}/api/export/${state.jobId}.csv`, "_blank");
    });
    el("btnXlsx").addEventListener("click", () => {
      if(!state.jobId) return;
      window.open(`${state.backendUrl}/api/export/${state.jobId}.xlsx`, "_blank");
    });

    el("btnCsvEdited").addEventListener("click", () => {
      if(state.editMode) readEditsFromDOM();
      exportEditedCSV();
    });
    el("btnXlsxEdited").addEventListener("click", () => {
      if(state.editMode) readEditsFromDOM();
      exportEditedXLSX();
    });

    el("btnEdit").addEventListener("click", () => {
      if(!state.rows.length){
        alert("ยังไม่มีข้อมูลให้แก้ไข");
        return;
      }
      setEditMode(!state.editMode);
    });

    el("btnSave").addEventListener("click", () => {
      if(!state.editMode) return;
      saveEditsNow();
    });

    // history
    el("btnHistory").addEventListener("click", openModal);
    el("btnClearHistory").addEventListener("click", () => {
      if(confirm("ล้างประวัติทั้งหมด?")) clearHistory();
    });
    el("historyModal").addEventListener("click", (e) => {
      const t = e.target;
      if(t && t.getAttribute && t.getAttribute("data-close") === "1") closeModal();
    });
    window.addEventListener("keydown", (e) => { if(e.key === "Escape") closeModal(); });

    // horizontal dock sync
    bindHScrollSync();

    // initial
    renderTable();
    setButtonsEnabled(false);
    setUploadInfo();
    setProgressUI(null);
    renderHistory();
    syncHScrollGeometry();
  }

  bind();
})();
