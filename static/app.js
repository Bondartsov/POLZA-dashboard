// Polza.AI Dashboard v3 — Main Application

// ─── State ─────────────────────────────────────────────────────────────────────
const S = {
  items: [], filtered: [], page: 1, limit: 50, total: 0, totalPages: 0,
  sortBy: 'createdAt', sortOrder: 'desc',
  serverSortBy: 'createdAt', serverSortOrder: 'desc',
  loading: false, allLoaded: false,
  groupMode: 'flat',
  keys: [], pastedKeys: [],
  autoRefresh: true, refreshTimer: null, lastUpdate: null,
  keyNames: new Set(),
  balance: null,
  sessions: [], sessionsLoading: false, expandedSession: null,
};
const KEY_COLORS = ['#6c7bf0','#4ade80','#f87171','#fbbf24','#60a5fa','#c084fc','#fb923c','#22d3ee','#f472b6','#a3e635','#f97316','#14b8a6'];
const charts = {};

// ─── Helpers ───────────────────────────────────────────────────────────────────
const fmt = {
  date: iso => { if (!iso) return '—'; const d = new Date(iso); return d.toLocaleDateString('ru-RU',{day:'2-digit',month:'2-digit',year:'2-digit',hour:'2-digit',minute:'2-digit'}); },
  dateShort: iso => { if (!iso) return '—'; const d = new Date(iso); return d.toLocaleDateString('ru-RU',{day:'2-digit',month:'2-digit'}); },
  num: n => n == null ? '—' : Number(n).toLocaleString('ru-RU'),
  cost: c => { if (c == null) return '—'; const v = parseFloat(c); if (isNaN(v)) return c; if (v === 0) return '0'; if (v < 0.01) return v.toFixed(4); return v.toLocaleString('ru-RU', {minimumFractionDigits:2, maximumFractionDigits:2}); },
  costShort: c => { if (c == null) return '—'; const v = parseFloat(c); if (isNaN(v)) return c; if (v < 1000) return v.toFixed(2); if (v < 1e6) return (v/1e3).toFixed(1) + ' тыс'; return (v/1e6).toFixed(2) + ' млн'; },
  time: ms => { if (ms == null) return '—'; return ms < 1000 ? ms.toLocaleString('ru-RU') + ' мс' : (ms / 1000).toFixed(1) + ' с'; },
  pct: (v, t) => t > 0 ? Math.round(v / t * 100) : 0,
};
const fmtDate = fmt.date, fmtNum = fmt.num, fmtCost = fmt.cost, fmtTime = fmt.time;
const statusIcon = s => ({completed:'✅',failed:'❌',pending:'⏳'}[s] || '❓');
function getCacheInfo(it) {
  const d = it.usage?.prompt_tokens_details;
  const cached = d?.cached_tokens || 0;
  const total = it.usage?.prompt_tokens || 0;
  return { read: cached, write: total - cached, total };
}
function tryJSON(s) { if (!s) return ''; try { return JSON.stringify(JSON.parse(s), null, 2); } catch { return s; } }
function esc(s) { return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// ─── Theme ──────────────────────────────────────────────────────────────────────
function initTheme() {
  const saved = localStorage.getItem('polza-theme');
  if (saved === 'light') {
    document.documentElement.classList.add('light');
    document.getElementById('themeSwitch').checked = true;
  }
}
function toggleTheme() {
  const isLight = document.getElementById('themeSwitch').checked;
  document.documentElement.classList.toggle('light', isLight);
  localStorage.setItem('polza-theme', isLight ? 'light' : 'dark');
  // Re-render charts with new colors
  if (S.items.length) renderCharts();
}

// ─── Quick dates ────────────────────────────────────────────────────────────────
function setQuickDate(range) {
  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  let from;
  if (range === 'today') from = today;
  else if (range === 'week') { const d = new Date(now); d.setDate(d.getDate() - 7); from = d.toISOString().slice(0, 10); }
  else if (range === 'month') { const d = new Date(now); d.setMonth(d.getMonth() - 1); from = d.toISOString().slice(0, 10); }
  document.getElementById('dateFrom').value = from || '';
  document.getElementById('dateTo').value = today;
  applyFilters();
}

// ─── Balance ────────────────────────────────────────────────────────────────────
async function loadBalance() {
  try {
    const r = await fetch('/api/balance');
    if (!r.ok) return;
    const data = await r.json();
    S.balance = data;
    const box = document.getElementById('balanceBox');
    const val = document.getElementById('balanceValue');
    const amount = data.balance ?? data.amount ?? data.total ?? null;
    if (amount != null) {
      val.textContent = fmtCost(amount) + ' ₽';
      val.className = 'balance-value' + (parseFloat(amount) < 100 ? ' low' : '');
      box.style.display = 'flex';
    }
  } catch(e) {}
}

// ─── Init ───────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  initTheme();
  loadBalance();

  try {
    const c = await (await fetch('/api/config')).json();
    S.keys = c.keys || [];
    renderKeyList();
  } catch(e) {}

  document.querySelectorAll('thead th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.sort, type = th.dataset.type;
      if (S.sortBy === key) S.sortOrder = S.sortOrder === 'desc' ? 'asc' : 'desc';
      else { S.sortBy = key; S.sortOrder = 'desc'; }
      document.querySelectorAll('thead th').forEach(t => t.classList.remove('sorted','asc'));
      th.classList.add('sorted');
      if (S.sortOrder === 'asc') th.classList.add('asc');
      if (type === 'server') { S.serverSortBy = key; S.serverSortOrder = S.sortOrder; if (!S.allLoaded) { S.page = 1; loadPage(); return; } }
      clientSideUpdate();
    });
  });

  let searchTO;
  document.getElementById('searchInput').addEventListener('input', () => { clearTimeout(searchTO); searchTO = setTimeout(clientSideUpdate, 200); });
  document.getElementById('filterKey').addEventListener('change', () => clientSideUpdate());
  document.getElementById('modalOverlay').addEventListener('click', e => { if (e.target === e.currentTarget) closeModal(); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') { closeModal(); closeSidebar(); } });

  await loadPage();
  startAutoRefresh();
});

// ─── Auto-refresh ───────────────────────────────────────────────────────────────
function startAutoRefresh() {
  if (S.refreshTimer) clearInterval(S.refreshTimer);
  S.refreshTimer = setInterval(() => {
    if (S.autoRefresh && !S.loading) loadPage(true);
  }, 60000);
}
function toggleAutoRefresh() {
  S.autoRefresh = document.getElementById('setAutoRefresh').checked;
  if (S.autoRefresh) startAutoRefresh(); else if (S.refreshTimer) clearInterval(S.refreshTimer);
}
function updateTimestamp() {
  S.lastUpdate = new Date();
  const el = document.getElementById('updateInfo');
  el.innerHTML = `Обновлено: <span class="val">${S.lastUpdate.toLocaleTimeString('ru-RU')}</span>${S.autoRefresh ? ' · 🔄 60с' : ''}`;
}

// ─── Sidebar ────────────────────────────────────────────────────────────────────
function toggleSidebar() {
  document.getElementById('sidebar').classList.toggle('open');
  document.getElementById('sidebarBackdrop').classList.toggle('open');
}
function closeSidebar() {
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('sidebarBackdrop').classList.remove('open');
}

function renderKeyList() {
  const el = document.getElementById('keyList');
  const all = [...S.keys, ...S.pastedKeys];
  if (!all.length) { el.innerHTML = '<div style="font-size:11px;color:var(--text2)">Откройте ⚙ и вставьте ключи</div>'; return; }
  el.innerHTML = all.map((k, i) => {
    const c = KEY_COLORS[i % KEY_COLORS.length];
    return `<div class="key-item"><span class="key-dot" style="background:${c}"></span><span>${k.isPrimary ? '⭐' : '👤'} ${k.name}</span><span style="color:var(--text2);font-size:10px;margin-left:auto">...${(k.key||'').slice(-4)}</span></div>`;
  }).join('');
}

function parsePasted() {
  const text = document.getElementById('keysPaste').value.trim();
  if (!text) return [];
  const keys = [];
  for (const line of text.split('\n')) {
    const t = line.trim();
    if (!t) continue;
    let name = '', key = '';
    if (t.includes('\t')) {
      const parts = t.split('\t');
      const idx = parts.findIndex(p => p.trim().startsWith('pza_') || p.trim().startsWith('sk-'));
      if (idx >= 0) { key = parts[idx].trim(); name = parts.slice(0, idx).join(' ').trim() || key.slice(-6); }
      else { name = parts[0].trim(); key = parts[parts.length - 1].trim(); }
    } else if (t.includes('pza_')) { const i = t.indexOf('pza_'); name = t.slice(0, i).trim(); key = t.slice(i).trim(); }
    else if (t.includes('sk-')) { const i = t.indexOf('sk-'); name = t.slice(0, i).trim(); key = t.slice(i).trim(); }
    else continue;
    if (key && (key.startsWith('pza_') || key.startsWith('sk-'))) keys.push({ key, name: name || key.slice(-6) });
  }
  return keys;
}

async function loadPastedKeys() {
  const keys = parsePasted();
  if (!keys.length) { document.getElementById('parsedInfo').innerHTML = '<span class="err">❌ Вставьте ключи</span>'; return; }
  document.getElementById('keysPaste').value = '';
  const btn = document.getElementById('btnLoadKeys');
  btn.disabled = true; btn.textContent = '⏳ Синхронизация...';
  showProgress('Регистрация ' + keys.length + ' ключей...', 10);
  try {
    const reg = await fetch('/api/keys', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({keys}) });
    if (!reg.ok) throw new Error('Ошибка регистрации ключей');
    showProgress('Синхронизация с Polza.AI API...', 30);
    const sync = await fetch('/api/sync/run', { method: 'POST' });
    if (!sync.ok) throw new Error('Ошибка синхронизации');
    const syncData = await sync.json();
    const totalNew = syncData.totalNew || 0;
    showProgress(`✅ +${totalNew.toLocaleString('ru-RU')} новых записей синхронизировано`, 100);
    S.pastedKeys = keys;
    renderKeyList();
    updateTimestamp(); await loadPage();
    setTimeout(hideProgress, 3000);
  } catch(e) {
    showProgress(`❌ ${e.message}`, 0);
  } finally { btn.disabled = false; btn.textContent = '🔄 Загрузить по всем'; }
}

// ─── API ────────────────────────────────────────────────────────────────────────
async function apiGet(url) { const r = await fetch(url); if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`); return r.json(); }

function getFilterParams() {
  const p = new URLSearchParams();
  const df = document.getElementById('dateFrom').value, dt = document.getElementById('dateTo').value;
  if (df) p.set('dateFrom', df + 'T00:00:00Z');
  if (dt) p.set('dateTo', dt + 'T23:59:59Z');
  const ft = document.getElementById('filterType').value; if (ft) p.set('requestType', ft);
  const fs = document.getElementById('filterStatus').value; if (fs) p.set('status', fs);
  const fk = document.getElementById('filterKey').value; if (fk) p.set('keyName', fk);
  return p;
}

async function loadPage(silent = false) {
  if (S.loading) return;
  S.loading = true;
  if (!silent) renderTable();
  try {
    const params = getFilterParams();
    params.set('sortBy', S.serverSortBy); params.set('sortOrder', S.serverSortOrder);
    const data = await apiGet('/api/db/all?' + params);
    S.items = data.items || []; S.total = S.items.length; S.allLoaded = true; S.totalPages = 1; S.page = 1;
    S.loading = false;
    updateTimestamp(); rebuildKeyFilter(); clientSideUpdate();
  } catch(e) {
    S.loading = false;
    if (!silent) document.getElementById('tableBody').innerHTML = `<tr><td colspan="10" style="color:var(--red);padding:16px">❌ ${e.message}</td></tr>`;
  }
}

// ─── Client-side sort / filter / group ──────────────────────────────────────────
function clientSideUpdate() { sortItems(); filterItems(); renderSummary(); renderCharts(); renderTable(); renderPagination(); }

function sortItems() {
  const key = S.sortBy, dir = S.sortOrder === 'desc' ? -1 : 1;
  S.items.sort((a, b) => {
    let va, vb;
    switch (key) {
      case 'totalTokens': va = a.usage?.prompt_tokens||0; vb = b.usage?.prompt_tokens||0; break;
      case 'cacheRead': va = getCacheInfo(a).read; vb = getCacheInfo(b).read; break;
      case 'cacheWrite': va = getCacheInfo(a).write; vb = getCacheInfo(b).write; break;
      case 'apiKeyName': va = (a.apiKeyName||a.apiKeyShort||a._sourceKey||'').toLowerCase(); vb = (b.apiKeyName||b.apiKeyShort||b._sourceKey||'').toLowerCase(); break;
      case 'generationTimeMs': va = a.generationTimeMs||0; vb = b.generationTimeMs||0; break;
      case 'modelDisplayName': va = (a.modelDisplayName||'').toLowerCase(); vb = (b.modelDisplayName||'').toLowerCase(); break;
      case 'requestType': va = a.requestType||''; vb = b.requestType||''; break;
      case 'status': va = a.status||''; vb = b.status||''; break;
      case 'cost': va = parseFloat(a.cost)||0; vb = parseFloat(b.cost)||0; break;
      default: va = a.createdAt||''; vb = b.createdAt||'';
    }
    if (va < vb) return -1 * dir; if (va > vb) return 1 * dir; return 0;
  });
}

function filterItems() {
  const q = document.getElementById('searchInput').value.toLowerCase();
  const fk = document.getElementById('filterKey').value;
  S.filtered = S.items.filter(it => {
    if (fk) { const itKey = it.apiKeyName || it._sourceKey || it.apiKeyShort || '?'; if (itKey !== fk) return false; }
    if (q) {
      return (it.modelDisplayName||'').toLowerCase().includes(q) || (it.model||'').toLowerCase().includes(q) ||
        (it.id||'').toLowerCase().includes(q) || (it.apiKeyName||'').toLowerCase().includes(q) ||
        (it._sourceKey||'').toLowerCase().includes(q) || (it.apiKeyShort||'').toLowerCase().includes(q);
    }
    return true;
  });
}

function setGroup(mode, btn) {
  S.groupMode = mode;
  S.expandedSession = null;
  document.querySelectorAll('.group-tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  if (mode === 'session') {
    document.getElementById('tableWrap').style.display = 'none';
    document.getElementById('pagination').style.display = 'none';
    document.getElementById('sessionsWrap').style.display = 'block';
    loadSessions();
  } else {
    document.getElementById('tableWrap').style.display = '';
    document.getElementById('pagination').style.display = '';
    document.getElementById('sessionsWrap').style.display = 'none';
    renderTable();
  }
}

// ─── Render: Summary ────────────────────────────────────────────────────────────
function renderSummary() {
  const items = S.filtered;
  if (!items.length) { document.getElementById('summaryCards').innerHTML = ''; return; }
  const totalCost = items.reduce((s,i) => s + (parseFloat(i.cost)||0), 0);
  const totalPrompt = items.reduce((s,i) => s + (i.usage?.prompt_tokens||0), 0);
  const totalCompletion = items.reduce((s,i) => s + (i.usage?.completion_tokens||0), 0);
  const totalCached = items.reduce((s,i) => s + (i.usage?.prompt_tokens_details?.cached_tokens||0), 0);
  const totalWritten = totalPrompt - totalCached;
  const totalReasoning = items.reduce((s,i) => s + (i.usage?.completion_tokens_details?.reasoning_tokens||0), 0);
  const failed = items.filter(i => i.status==='failed').length;
  const completed = items.filter(i => i.status==='completed').length;
  const cachePct = fmt.pct(totalCached, totalPrompt);
  const avgTime = items.reduce((s,i) => s + (i.generationTimeMs||0), 0) / items.length;
  const uniqueKeys = new Set(items.map(i => i.apiKeyName || i._sourceKey || '?')).size;

  document.getElementById('summaryTitle').innerHTML = `Суммарно по <b>${fmtNum(items.length)}</b> записям | <b>${uniqueKeys}</b> API ключей | Период: ${document.getElementById('dateFrom').value || 'все'} — ${document.getElementById('dateTo').value || 'сейчас'}`;
  document.getElementById('summaryCards').innerHTML = `
    <div class="summary-card"><div class="label">Запросов</div><div class="value">${fmtNum(items.length)}</div><div class="sub">✅ ${fmtNum(completed)} · ❌ ${fmtNum(failed)}</div></div>
    <div class="summary-card"><div class="label">Стоимость</div><div class="value">${fmtCost(totalCost)} ₽</div><div class="sub">~${fmtCost(totalCost/items.length)} ₽/запрос</div></div>
    <div class="summary-card"><div class="label">Входящие токены (input)</div><div class="value">${fmtNum(totalPrompt)}</div>
      <div class="cache-bar"><div class="read" style="width:${cachePct}%"></div><div class="write" style="width:${100-cachePct}%"></div></div>
      <div class="sub" style="margin-top:3px">Прочитано из кэша: <span style="color:var(--cache-read)">${fmtNum(totalCached)}</span> (${cachePct}%) · Записано в кэш: <span style="color:var(--cache-write)">${fmtNum(totalWritten)}</span></div></div>
    <div class="summary-card"><div class="label">Исходящие токены (output)</div><div class="value">${fmtNum(totalCompletion)}</div><div class="sub">Reasoning: ${fmtNum(totalReasoning)} (${totalCompletion>0?Math.round(totalReasoning/totalCompletion*100):0}%)</div></div>
    <div class="summary-card"><div class="label">Всего токенов</div><div class="value">${fmtNum(totalPrompt + totalCompletion)}</div><div class="sub">↓${fmtNum(totalPrompt)} вход → ↑${fmtNum(totalCompletion)} выход</div></div>
    <div class="summary-card"><div class="label">Среднее время</div><div class="value">${fmtTime(avgTime)}</div></div>
  `;
  document.getElementById('topStats').innerHTML = `<span>Записей: <span class="val">${fmtNum(items.length)}</span></span><span>Токены: <span class="val">${fmtNum(totalPrompt + totalCompletion)}</span></span><span>Сумма: <span class="val">${fmtCost(totalCost)} ₽</span></span>`;
}

// ─── Render: Charts ─────────────────────────────────────────────────────────────
function chartTextColor() { return getComputedStyle(document.documentElement).getPropertyValue('--chart-text').trim() || '#9da2b8'; }

function renderCharts() {
  const items = S.filtered;
  if (items.length < 2) { document.getElementById('chartsWrap').style.display = 'none'; return; }
  document.getElementById('chartsWrap').style.display = 'grid';
  const tc = chartTextColor();

  // 1) Cost by day
  const byDay = {};
  items.forEach(i => { const d = (i.createdAt||'').slice(0,10); if (!byDay[d]) byDay[d] = {cost:0, count:0}; byDay[d].cost += parseFloat(i.cost)||0; byDay[d].count++; });
  const days = Object.keys(byDay).sort();
  renderChart('chartCost', 'bar', {
    labels: days.map(d => fmt.dateShort(d+'T00:00:00Z')),
    datasets: [{ label: '₽', data: days.map(d => +byDay[d].cost.toFixed(2)), backgroundColor: 'rgba(108,123,240,.6)', borderRadius: 4 }]
  }, { plugins: { legend: { display: false } } });

  // 2) By model
  const byModel = {};
  items.forEach(i => { const m = i.modelDisplayName||'?'; if (!byModel[m]) byModel[m] = 0; byModel[m]++; });
  const topModels = Object.entries(byModel).sort((a,b) => b[1]-a[1]).slice(0,8);
  renderChart('chartModels', 'doughnut', {
    labels: topModels.map(m => m[0]),
    datasets: [{ data: topModels.map(m => m[1]), backgroundColor: topModels.map((_,i) => KEY_COLORS[i % KEY_COLORS.length]) }]
  }, { plugins: { legend: { position: 'right', labels: { boxWidth: 10, font: { size: 10 }, color: tc } } } });

  // 3) Cache: прочитано vs записано
  const cacheByDay = {};
  items.forEach(i => {
    const d = (i.createdAt||'').slice(0,10);
    if (!cacheByDay[d]) cacheByDay[d] = {read:0, write:0};
    const ci = getCacheInfo(i);
    cacheByDay[d].read += ci.read;
    cacheByDay[d].write += ci.write;
  });
  const cacheDays = Object.keys(cacheByDay).sort();
  renderChart('chartCache', 'bar', {
    labels: cacheDays.map(d => fmt.dateShort(d+'T00:00:00Z')),
    datasets: [
      { label: 'Прочитано из кэша', data: cacheDays.map(d => cacheByDay[d].read), backgroundColor: 'rgba(52,211,153,.7)', borderRadius: 4 },
      { label: 'Записано в кэш', data: cacheDays.map(d => cacheByDay[d].write), backgroundColor: 'rgba(244,114,182,.7)', borderRadius: 4 },
    ]
  }, { plugins: { legend: { labels: { font: { size: 10 }, color: tc } } }, scales: { x: { stacked: true, ticks: { color: tc } }, y: { stacked: true, ticks: { color: tc } } } });

  // 4) Cost by key
  const byKey = {};
  items.forEach(i => { const k = i.apiKeyName || i._sourceKey || '?'; if (!byKey[k]) byKey[k] = 0; byKey[k] += parseFloat(i.cost)||0; });
  const keyEntries = Object.entries(byKey).sort((a,b) => b[1]-a[1]).slice(0,10);
  renderChart('chartKeys', 'bar', {
    labels: keyEntries.map(k => k[0].length > 20 ? k[0].slice(0,18)+'…' : k[0]),
    datasets: [{ label: '₽', data: keyEntries.map(k => +k[1].toFixed(2)), backgroundColor: keyEntries.map((_,i) => KEY_COLORS[i % KEY_COLORS.length]), borderRadius: 4 }]
  }, { indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { ticks: { color: tc } }, y: { ticks: { color: tc, font: { size: 10 } } } } });
}

function renderChart(id, type, data, opts = {}) {
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id)?.getContext('2d');
  if (!ctx) return;
  charts[id] = new Chart(ctx, {
    type, data,
    options: {
      responsive: true, maintainAspectRatio: true,
      ...opts,
      ...(type === 'bar' || type === 'line' ? { scales: { ...(opts.scales || {}), ...(opts.scales?.x ? {} : { x: { ticks: { color: chartTextColor() } } }), ...(opts.scales?.y ? {} : { y: { ticks: { color: chartTextColor() } } }) } } : {}),
    }
  });
}

// ─── Render: Table ──────────────────────────────────────────────────────────────
function renderTable() {
  const tbody = document.getElementById('tableBody');
  if (S.loading) { tbody.innerHTML = '<tr><td colspan="10" class="loading"><div class="spinner"></div><br>Загрузка...</td></tr>'; return; }
  const items = S.filtered;
  if (!items.length) { tbody.innerHTML = '<tr><td colspan="10" style="padding:16px;text-align:center;color:var(--text2)">Нет данных</td></tr>'; return; }
  let pageItems = S.allLoaded ? items.slice((S.page-1)*S.limit, S.page*S.limit) : items;
  let html = '';
  if (S.groupMode === 'flat') { html += renderRows(pageItems); }
  else {
    const groups = groupItems(pageItems);
    for (const g of groups) { html += `<tr class="group-row"><td colspan="10">${groupLabel(g)}</td></tr>`; html += renderRows(g.items); }
  }
  tbody.innerHTML = html;
}

function renderRows(items) {
  return items.map(it => {
    const ci = getCacheInfo(it), cost = parseFloat(it.cost)||0;
    const prompt = it.usage?.prompt_tokens || 0;
    return `<tr onclick="openDetail('${it.id}')">
      <td>${fmtDate(it.createdAt)}</td>
      <td title="${it.model||''}">${it.modelDisplayName||it.model||'—'}</td>
      <td><span class="badge badge-${it.requestType}">${it.requestType||'?'}</span></td>
      <td><span class="badge badge-${it.status}">${statusIcon(it.status)} ${it.status}</span></td>
      <td class="cost ${cost>10?'cost-high':cost===0?'cost-zero':''}">${fmtCost(cost)}</td>
      <td>${fmtNum(prompt)}</td>
      <td><span class="cache-read ${ci.read===0?'zero':''}">${ci.read > 0 ? fmtNum(ci.read) : '—'}</span></td>
      <td><span class="cache-write ${ci.write===0?'zero':''}">${ci.write > 0 ? fmtNum(ci.write) : '—'}</span></td>
      <td>${fmtTime(it.generationTimeMs)}</td>
      <td>${it.apiKeyName||it.apiKeyShort||it._sourceKey||'—'}</td>
    </tr>`;
  }).join('');
}

function groupItems(items) {
  if (S.groupMode === 'flat') return [{ label: '', items }];
  const groups = new Map();
  for (const it of items) {
    let key;
    if (S.groupMode === 'day') key = (it.createdAt||'').slice(0,10);
    else if (S.groupMode === 'model') key = it.modelDisplayName||it.model||'?';
    else if (S.groupMode === 'key') key = it.apiKeyName||it._sourceKey||it.apiKeyShort||'?';
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(it);
  }
  const result = [...groups.entries()].map(([label, gItems]) => ({ label, items: gItems }));
  result.sort((a,b) => { const da = a.items[0]?.createdAt||'', db = b.items[0]?.createdAt||''; return S.sortOrder==='desc'?db.localeCompare(da):da.localeCompare(db); });
  return result;
}

function groupLabel(g) {
  const items = g.items, tc = items.reduce((s,i)=>s+(parseFloat(i.cost)||0),0), tt = items.reduce((s,i)=>s+(i.usage?.prompt_tokens||0),0);
  const ch = items.reduce((s,i)=>s+(i.usage?.prompt_tokens_details?.cached_tokens||0),0), pr = items.reduce((s,i)=>s+(i.usage?.prompt_tokens||0),0);
  const cp = fmt.pct(ch, pr), fl = items.filter(i=>i.status==='failed').length;
  const fe = fl>0?` <span style="color:var(--red)">❌${fl}</span>`:'';
  if (S.groupMode==='day') { const d=new Date(g.label); return `📅 ${d.toLocaleDateString('ru-RU',{weekday:'short',day:'numeric',month:'short'})} — ${items.length} зап., ${fmtCost(tc)} ₽, кэш прочитано ${cp}%${fe}`; }
  if (S.groupMode==='model') return `🤖 ${g.label} — ${items.length} зап., ${fmtCost(tc)} ₽${fe}`;
  if (S.groupMode==='key') return `👤 ${g.label} — ${items.length} зап., ${fmtCost(tc)} ₽, ${fmtNum(tt)} токенов, кэш ${cp}%${fe}`;
  return g.label;
}

function renderPagination() {
  const el = document.getElementById('pagination');
  const tp = S.allLoaded ? Math.ceil(S.filtered.length / S.limit) : S.totalPages;
  const total = S.allLoaded ? S.filtered.length : S.total;
  if (tp <= 1) { el.innerHTML = `<span class="info">Всего: ${fmtNum(total)}</span>`; return; }
  const btns = [];
  btns.push(`<button class="btn" ${S.page<=1?'disabled':''} onclick="goPage(1)">⏮</button>`);
  btns.push(`<button class="btn" ${S.page<=1?'disabled':''} onclick="goPage(${S.page-1})">◀</button>`);
  let s = Math.max(1,S.page-2), e = Math.min(tp,S.page+2);
  if (s>1) btns.push('<span class="info">…</span>');
  for (let p=s;p<=e;p++) btns.push(`<button class="btn ${p===S.page?'active':''}" onclick="goPage(${p})">${p}</button>`);
  if (e<tp) btns.push('<span class="info">…</span>');
  btns.push(`<button class="btn" ${S.page>=tp?'disabled':''} onclick="goPage(${S.page+1})">▶</button>`);
  btns.push(`<button class="btn" ${S.page>=tp?'disabled':''} onclick="goPage(${tp})">⏭</button>`);
  el.innerHTML = btns.join('') + `<span class="info">Стр. ${S.page}/${tp} · ${fmtNum(total)}</span>`;
}

function goPage(p) { S.page = p; if (S.allLoaded) { renderTable(); renderPagination(); } else loadPage(); window.scrollTo({top:0,behavior:'smooth'}); }

// ─── Actions ────────────────────────────────────────────────────────────────────
function applyFilters() { S.page = 1; if (S.allLoaded) clientSideUpdate(); else loadPage(); }
function resetFilters() {
  document.getElementById('dateFrom').value = '';
  document.getElementById('dateTo').value = '';
  document.getElementById('filterType').value = '';
  document.getElementById('filterStatus').value = '';
  document.getElementById('filterKey').value = '';
  document.getElementById('searchInput').value = '';
  S.page = 1;
  if (S.allLoaded) clientSideUpdate(); else loadPage();
}

// ─── Detail Modal ──────────────────────────────────────────────────────────────
async function openDetail(genId) {
  const overlay = document.getElementById('modalOverlay'), body = document.getElementById('modalBody');
  overlay.classList.add('open');
  body.innerHTML = '<div class="loading"><div class="spinner"></div><br>Загрузка...</div>';
  document.getElementById('modalTitle').textContent = genId;
  try {
    const [detail, log] = await Promise.all([
      apiGet('/api/generations/'+genId),
      apiGet('/api/generations/'+genId+'/log').catch(()=>null)
    ]);
    const li = S.items.find(i=>i.id===genId)||{};
    let h = '<div class="detail-grid">';
    h += dc('Модель', li.modelDisplayName||detail.finalEndpointSlug||'—');
    h += dc('Провайдер', detail.finalEndpointSlug||li.provider||'—');
    h += dc('Тип', `<span class="badge badge-${detail.requestType||li.requestType}">${detail.requestType||li.requestType} ${detail.apiType||''}</span>`);
    h += dc('Статус', `<span class="badge badge-${detail.status}">${statusIcon(detail.status)} ${detail.status}</span>`);
    h += dc('Finish', detail.finishReason||'—');
    h += dc('Режим', detail.responseMode||'—');
    h += dc('Стоимость', `<span class="cost ${(parseFloat(detail.clientCost)||0)>10?'cost-high':''}">${fmtCost(detail.clientCost)} ₽</span>`);
    h += dc('Время', fmtTime(detail.generationTimeMs));
    h += dc('Latency', fmtTime(detail.latencyMs));
    h += dc('Создано', fmtDate(detail.createdAt||li.createdAt));
    h += dc('Завершено', fmtDate(detail.completedAt));
    h += dc('Попыток', detail.attemptsCount||1);
    h += dc('API Ключ', li.apiKeyName||li.apiKeyShort||detail.apiKeyId||'—');
    h += dc('Log', detail.hasLog?'✅':'❌');
    h += '</div>';

    const usage = detail.usage||li.usage;
    if (usage) {
      const cached = usage.prompt_tokens_details?.cached_tokens||0;
      const prompt = usage.prompt_tokens||0;
      const written = prompt - cached;
      const reasoning = usage.completion_tokens_details?.reasoning_tokens||0;
      const completion = usage.completion_tokens||0, total = usage.total_tokens||prompt+completion;
      const cachePct = fmt.pct(cached, prompt);
      h += '<div class="section-title">🔢 Токены</div>';
      h += '<div class="usage-grid">';
      h += ui(fmtNum(prompt),'Вход (input)');
      h += ui(fmtNum(completion),'Выход (output)');
      h += ui(fmtNum(total),'Всего');
      h += ui(`<span style="color:var(--cache-read)">${fmtNum(cached)}</span>`,'Прочитано из кэша');
      h += ui(`<span style="color:var(--cache-write)">${fmtNum(written)}</span>`,'Записано в кэш');
      h += ui(fmtNum(reasoning),'Reasoning');
      h += ui(fmtNum(usage.prompt_tokens_details?.audio_tokens||0),'Audio');
      h += ui(fmtNum(usage.prompt_tokens_details?.video_tokens||0),'Video');
      h += '</div>';
      h += `<div style="margin-bottom:14px;font-size:11px;color:var(--text2)">
        <b>Кэш промптов:</b> из ${fmtNum(prompt)} входящих токенов — <span style="color:var(--cache-read)">прочитано из кэша: ${fmtNum(cached)}</span> (${cachePct}%) · <span style="color:var(--cache-write)">записано в кэш: ${fmtNum(written)}</span>
        <div class="cache-bar" style="height:8px;margin-top:4px"><div class="read" style="width:${cachePct}%"></div><div class="write" style="width:${100-cachePct}%"></div></div>
      </div>`;
    }

    if (log) {
      const req = log.request||{}, msgs = req.messages||[];
      h += `<div class="section-title">📤 Запрос (${msgs.length} сообщений)</div>`;
      h += `<div style="font-size:11px;color:var(--text2);margin-bottom:6px">Модель: ${req.model||'—'} · Stream: ${req.stream?'Да':'Нет'}</div>`;
      if (req.tools?.length) {
        h += `<details style="margin-bottom:8px"><summary style="cursor:pointer;color:var(--accent2);font-size:12px">🔧 Tools (${req.tools.length})</summary><div style="margin-top:4px">`;
        req.tools.forEach(t => { h += `<div class="tool-call"><span class="fn-name">${t.function?.name||'?'}</span> <span style="color:var(--text2);font-size:11px">${(t.function?.description||'').slice(0,80)}</span></div>`; });
        h += '</div></details>';
      }
      h += renderMessages(msgs);
      h += '<div class="section-title">📥 Ответ</div>';
      const resp = log.response||{}, choices = resp.choices||[];
      choices.forEach((ch,ci) => {
        const msg = ch.message||{};
        h += `<div style="font-size:11px;color:var(--text2);margin-bottom:4px">Choice ${ci} · finish: ${ch.finish_reason||'—'}</div>`;
        if (msg.tool_calls?.length) msg.tool_calls.forEach(tc => { h += `<div class="tool-call"><div class="fn-name">🔧 ${tc.function?.name||tc.type||'?'}</div><pre>${tryJSON(tc.function?.arguments)}</pre></div>`; });
        if (msg.content) h += `<div class="msg msg-assistant"><div class="msg-header" onclick="toggleMsg(this)"><span class="role">ASSISTANT</span>${msg.content.length} chars<span class="toggle">▼</span></div><div class="msg-body">${esc(msg.content)}</div></div>`;
      });
    } else h += '<div style="padding:16px;color:var(--text2);text-align:center">Лог недоступен</div>';
    body.innerHTML = h;
  } catch(e) { body.innerHTML = `<div style="padding:16px;color:var(--red)">❌ ${e.message}</div>`; }
}

function renderMessages(msgs) {
  return msgs.map(m => {
    const role = m.role||'unknown', content = typeof m.content==='string'?m.content:JSON.stringify(m.content,null,2), len = content.length;
    let extra = '';
    if (m.tool_calls?.length) extra += m.tool_calls.map(tc => `<div class="tool-call"><div class="fn-name">🔧 ${tc.function?.name||'?'}</div><pre>${tryJSON(tc.function?.arguments)}</pre></div>`).join('');
    if (m.tool_call_id) extra += `<div style="font-size:10px;color:var(--text2)">tool_call_id: ${m.tool_call_id}</div>`;
    if (m.name) extra += `<div style="font-size:10px;color:var(--text2)">name: ${m.name}</div>`;
    return `<div class="msg msg-${role}"><div class="msg-header" onclick="toggleMsg(this)"><span class="role">${role.toUpperCase()}</span>${len?len+' chars':''}${m.tool_calls?m.tool_calls.length+' tc':''}<span class="toggle">▼</span></div><div class="msg-body">${esc(content)}${extra}</div></div>`;
  }).join('');
}

function dc(l,v) { return `<div class="detail-card"><div class="label">${l}</div><div class="value">${v}</div></div>`; }
function ui(n,l) { return `<div class="usage-item"><div class="num">${n}</div><div class="lbl">${l}</div></div>`; }
function toggleMsg(el) { const b=el.nextElementSibling; b.classList.toggle('open'); el.querySelector('.toggle').textContent=b.classList.contains('open')?'▲':'▼'; }
function closeModal() { document.getElementById('modalOverlay').classList.remove('open'); }

// ─── Progress Banner ──────────────────────────────────────────────────────────
function showProgress(text, pct) {
  const banner = document.getElementById('progressBanner');
  document.getElementById('progressText').textContent = text;
  document.getElementById('progressFill').style.width = pct + '%';
  banner.style.display = 'block';
}
function hideProgress() {
  document.getElementById('progressBanner').style.display = 'none';
}

// ─── Key Filter Dropdown ──────────────────────────────────────────────────────
function rebuildKeyFilter() {
  const sel = document.getElementById('filterKey');
  const current = sel.value;
  const names = new Set();
  S.items.forEach(it => {
    const n = it.apiKeyName || it._sourceKey || it.apiKeyShort;
    if (n) names.add(n);
  });
  S.keyNames = names;
  sel.innerHTML = '<option value="">Все ключи</option>';
  [...names].sort().forEach(n => {
    sel.innerHTML += `<option value="${n}"${n===current?' selected':''}>${n}</option>`;
  });
}

// ─── Sessions ──────────────────────────────────────────────────────────────────
async function loadSessions() {
  S.sessionsLoading = true;
  renderSessionsView();
  try {
    const params = getFilterParams();
    const data = await apiGet('/api/db/sessions?' + params);
    S.sessions = data.sessions || [];
    S.sessionsLoading = false;
    renderSessionsView();
  } catch(e) {
    S.sessionsLoading = false;
    renderSessionsView();
  }
}

function renderSessionsView() {
  const el = document.getElementById('sessionsWrap');
  if (S.sessionsLoading) {
    el.innerHTML = '<div style="text-align:center;padding:32px"><div class="spinner"></div><br>Загрузка сессий...</div>';
    return;
  }
  if (!S.sessions.length) {
    el.innerHTML = '<div style="text-align:center;padding:32px;color:var(--text2)">Нет данных о сессиях.<br><small>Сессии доступны для ключей, использующих чат-клиенты (Cursor, Claude Code и т.д.)</small><br><button class="btn" style="margin-top:12px" onclick="runBackfill()">🔄 Заполнить metadata</button></div>';
    return;
  }

  const totalCost = S.sessions.reduce((s,x) => s + x.totalCost, 0);
  const totalReqs = S.sessions.reduce((s,x) => s + x.totalCount, 0);
  const uniqueKeys = new Set(S.sessions.map(s => s.sourceKey)).size;

  let html = `<div class="sessions-header">
    <span>💬 <b>${fmtNum(S.sessions.length)}</b> сессий · <b>${fmtNum(totalReqs)}</b> запросов · <b>${fmtCost(totalCost)}</b> ₽ · <b>${uniqueKeys}</b> сотрудников</span>
    <button class="btn" onclick="runBackfill()" style="font-size:11px">🔄 Заполнить metadata</button>
  </div>`;

  for (const s of S.sessions) {
    const shortId = s.sessionId.slice(0, 8) + '…';
    const first = new Date(s.firstAt), last = new Date(s.lastAt);
    const durationMs = last - first;
    const duration = durationMs > 3600000 ? (durationMs/3600000).toFixed(1) + ' ч' :
                     durationMs > 60000 ? (durationMs/60000).toFixed(0) + ' мин' : '< 1 мин';
    const cacheBar = s.cachePct > 0 ?
      `<div class="cache-bar" style="height:6px;margin-top:4px"><div class="read" style="width:${s.cachePct}%"></div><div class="write" style="width:${100-s.cachePct}%"></div></div>` : '';

    html += `<div class="session-card ${S.expandedSession===s.sessionId?'expanded':''}" onclick="toggleSession('${s.sessionId}')">
      <div class="session-main">
        <div class="session-id">💬 ${shortId}</div>
        <div class="session-user">👤 ${esc(s.sourceKey||'?')}</div>
        <div class="session-stats">
          <span>${fmtNum(s.totalCount)} зап.</span>
          <span class="cost">${fmtCost(s.totalCost)} ₽</span>
          <span>${fmtNum(s.totalPrompt + s.totalCompletion)} токенов</span>
        </div>
        <div class="session-time">
          <span>${fmtDate(s.firstAt)}</span>
          <span style="color:var(--text2)">→ ${fmtDate(s.lastAt)}</span>
          <span class="session-duration">${duration}</span>
        </div>
        <div class="session-models">${(s.models||[]).map(m=>'<span class="badge">'+esc(m)+'</span>').join(' ')}</div>
        ${cacheBar ? '<div style="margin-top:2px;font-size:10px;color:var(--text2)">Кэш: '+s.cachePct+'% прочитано</div>'+cacheBar : ''}
      </div>
      <div class="session-detail" id="session-${s.sessionId.slice(0,8)}" style="display:${S.expandedSession===s.sessionId?'block':'none'}">
        ${S.expandedSession===s.sessionId ? '<div class="loading"><div class="spinner"></div><br>Загрузка...</div>' : ''}
      </div>
    </div>`;
  }
  el.innerHTML = html;
}

async function toggleSession(sessionId) {
  if (S.expandedSession === sessionId) {
    S.expandedSession = null;
    renderSessionsView();
    return;
  }
  S.expandedSession = sessionId;
  renderSessionsView();

  // Load generations for this session from client-side data
  const items = S.items.filter(i => i._sessionId === sessionId);
  const detailEl = document.getElementById('session-' + sessionId.slice(0, 8));
  if (!detailEl) return;

  if (!items.length) {
    detailEl.innerHTML = '<div style="padding:12px;color:var(--text2);font-size:12px">Нет загруженных записей для этой сессии. Попробуйте обновить страницу.</div>';
    return;
  }

  let html = '<table style="width:100%;font-size:12px"><thead><tr><th>Время</th><th>Модель</th><th>Тип</th><th>Стоимость</th><th>Вход</th><th>Кэш</th><th>Время</th></tr></thead><tbody>';
  for (const it of items) {
    const ci = getCacheInfo(it), cost = parseFloat(it.cost)||0;
    html += `<tr onclick="openDetail('${it.id}')" style="cursor:pointer">
      <td>${fmtDate(it.createdAt)}</td>
      <td>${it.modelDisplayName||'—'}</td>
      <td><span class="badge badge-${it.requestType}">${it.requestType||'?'}</span></td>
      <td class="cost">${fmtCost(cost)}</td>
      <td>${fmtNum(it.usage?.prompt_tokens||0)}</td>
      <td><span style="color:var(--cache-read)">${ci.read>0?fmtNum(ci.read):'—'}</span></td>
      <td>${fmtTime(it.generationTimeMs)}</td>
    </tr>`;
  }
  html += '</tbody></table>';
  detailEl.innerHTML = html;
}

async function runBackfill() {
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '⏳ Заполнение...';
  showProgress('Заполнение metadata сессий...', 10);
  try {
    let totalEnriched = 0, iteration = 0;
    while (iteration < 100) {
      iteration++;
      const r = await fetch('/api/sessions/backfill?limit=50', { method: 'POST' });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      totalEnriched += data.enriched || 0;
      const pct = data.done ? 100 : Math.min(90, 10 + iteration * 5);
      showProgress(`Заполнение metadata: +${totalEnriched} enriched, осталось ~${data.remaining}`, pct);
      if (data.done || data.remaining === 0) break;
      await new Promise(r => setTimeout(r, 500));
    }
    showProgress(`✅ Metadata заполнено: ${totalEnriched} сессий enriched`, 100);
    setTimeout(hideProgress, 3000);
    await loadSessions();
  } catch(e) {
    showProgress('❌ ' + e.message, 0);
  } finally {
    btn.disabled = false;
    btn.textContent = '🔄 Заполнить metadata';
  }
}
