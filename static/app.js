// Polza.AI Dashboard v3 — Main Application

// ─── State ─────────────────────────────────────────────────────────────────────
const S = {
  items: [], filtered: [], page: 1, limit: 50, total: 0, totalPages: 0,
  sortBy: 'createdAt', sortOrder: 'desc',
  serverSortBy: 'createdAt', serverSortOrder: 'desc',
  loading: false, allLoaded: false,
  groupMode: 'flat',
  keys: [], pastedKeys: [],
  autoRefresh: true, refreshTimer: null, refreshInterval: 60000, lastUpdate: null,
  keyNames: new Set(),
  balance: null,
  provider: 'ollama',  // current AI provider: 'ollama' | 'anthropic'
  providerConfig: null, // full config from /api/provider/config
  analyzeAllRunning: false, // true when analyze-all background worker is active
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
  S.allLoaded = false;
  loadPage();
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
  loadProviderConfig();

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
  pollBackfillStatus(); // Check if backfill is running from previous session
  checkAnalyzeAllStatus(); // Check if analyze-all is running from previous session
});

// ─── Auto-refresh ───────────────────────────────────────────────────────────────
function startAutoRefresh() {
  if (S.refreshTimer) clearInterval(S.refreshTimer);
  S.refreshTimer = setInterval(() => {
    if (S.autoRefresh && !S.loading) loadPage(true);
  }, S.refreshInterval);
}
function toggleAutoRefresh() {
  S.autoRefresh = document.getElementById('setAutoRefresh').checked;
  if (S.autoRefresh) startAutoRefresh(); else if (S.refreshTimer) clearInterval(S.refreshTimer);
  updateTimestamp();
}
function applyRefreshInterval() {
  const val = parseInt(document.getElementById('setRefreshInterval').value, 10) * 1000;
  S.refreshInterval = val;
  if (S.autoRefresh) startAutoRefresh();
  updateTimestamp();
}
function updateTimestamp() {
  S.lastUpdate = new Date();
  const el = document.getElementById('updateInfo');
  const sec = S.refreshInterval / 1000;
  const label = sec < 60 ? `${sec}с` : `${Math.round(sec/60)} мин`;
  el.innerHTML = `Обновлено: <span class="val">${S.lastUpdate.toLocaleTimeString('ru-RU')}</span>${S.autoRefresh ? ` · 🔄 ${label}` : ''}`;
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

// ─── Provider ──────────────────────────────────────────────────────────────────
async function loadProviderConfig() {
  try {
    const r = await fetch('/api/provider/config');
    if (!r.ok) return;
    const cfg = await r.json();
    S.providerConfig = cfg;
    S.provider = cfg.provider;

    // 1. LLM Provider dropdown
    const llmDd = document.getElementById('llmProviderDropdown');
    if (llmDd) {
      if (cfg.provider === 'openrouter' && cfg.openrouter?.model) {
        let suffix = '';
        if (cfg.openrouter.model.includes('nano')) suffix = 'nemotron-nano';
        else if (cfg.openrouter.model.includes('gemma')) suffix = 'gemma';
        else suffix = 'nemotron-super';
        llmDd.value = 'openrouter-or-' + suffix;
      } else {
        llmDd.value = cfg.provider;
      }
      const llmInfo = document.getElementById('llmModelInfo');
      if (llmInfo) {
        const sel = llmDd.options[llmDd.selectedIndex];
        llmInfo.textContent = sel ? sel.textContent.replace(/^[^\s]+ /, '') : '';
      }
    }

    // 2. Embedding provider dropdown
    const embDd = document.getElementById('embeddingDropdown');
    if (embDd && cfg.embedding) {
      embDd.value = cfg.embedding.provider;
      const embInfo = document.getElementById('embeddingInfo');
      if (embInfo) {
        const sel = embDd.options[embDd.selectedIndex];
        embInfo.textContent = sel ? sel.textContent.replace(/^[^\s]+ /, '') : '';
      }
    }

    // 3. RAG Chat model dropdown
    const ragDd = document.getElementById('ragChatDropdown');
    if (ragDd && cfg.ragChat) {
      ragDd.value = cfg.ragChat.model;
      const ragInfo = document.getElementById('ragChatInfo');
      if (ragInfo) {
        const sel = ragDd.options[ragDd.selectedIndex];
        ragInfo.textContent = sel ? sel.textContent.replace(/^[^\s]+ /, '') : '';
      }
    }

    // Auto-analyze checkbox
    const aa = document.getElementById('autoAnalyzeSwitch');
    if (aa) aa.checked = cfg.autoAnalyze === true;
  } catch(e) { console.warn('provider config load failed:', e); }
}

// === 1. LLM Provider ===
async function setLLMProvider(val) {
  try {
    let provider, orModel;
    if (val.startsWith('openrouter-or-')) {
      provider = 'openrouter';
      const shortName = val.replace('openrouter-or-', '');
      if (shortName === 'nemotron-super') orModel = 'nvidia/nemotron-3-super-120b-a12b:free';
      else if (shortName === 'gemma') orModel = 'google/gemma-4-31b-it:free';
      else if (shortName === 'nemotron-nano') orModel = 'nvidia/nemotron-3-nano-30b-a3b:free';
      else orModel = 'nvidia/nemotron-3-super-120b-a12b:free';
    } else {
      provider = val;
    }

    const body = {provider};
    if (orModel) body.openrouterModel = orModel;

    const r = await fetch('/api/provider/set', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    if (!r.ok) return;
    S.provider = provider;
    if (S.providerConfig) {
      S.providerConfig.provider = provider;
      if (orModel && S.providerConfig.openrouter) S.providerConfig.openrouter.model = orModel;
    }
    const llmInfo = document.getElementById('llmModelInfo');
    const dd = document.getElementById('llmProviderDropdown');
    if (llmInfo && dd) {
      const sel = dd.options[dd.selectedIndex];
      llmInfo.textContent = '✅ ' + (sel ? sel.textContent.replace(/^[^\s]+ /, '') : provider);
    }
  } catch(e) { console.warn('LLM provider switch failed:', e); }
}

async function saveDefaultLLM() {
  try {
    await fetch('/api/provider/set', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({saveDefault: true}),
    });
    const info = document.getElementById('llmModelInfo');
    if (info) info.textContent += ' 📌';
  } catch(e) { console.warn('save default LLM failed:', e); }
}

// === 2. Embedding Provider ===
async function setEmbeddingProvider(provider) {
  try {
    const r = await fetch('/api/provider/set', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({embeddingProvider: provider}),
    });
    if (!r.ok) return;
    if (S.providerConfig && S.providerConfig.embedding) S.providerConfig.embedding.provider = provider;
    const info = document.getElementById('embeddingInfo');
    const dd = document.getElementById('embeddingDropdown');
    if (info && dd) {
      const sel = dd.options[dd.selectedIndex];
      info.textContent = '✅ ' + (sel ? sel.textContent.replace(/^[^\s]+ /, '') : provider);
    }
  } catch(e) { console.warn('Embedding switch failed:', e); }
}

async function saveDefaultEmbedding() {
  try {
    await fetch('/api/provider/set', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({saveEmbeddingDefault: true}),
    });
    const info = document.getElementById('embeddingInfo');
    if (info) info.textContent += ' 📌';
  } catch(e) { console.warn('save default embedding failed:', e); }
}

// === 3. RAG Chat Model ===
async function setRagChatModel(model) {
  try {
    const r = await fetch('/api/provider/set', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ragChatModel: model}),
    });
    if (!r.ok) return;
    if (S.providerConfig && S.providerConfig.ragChat) S.providerConfig.ragChat.model = model;
    const info = document.getElementById('ragChatInfo');
    const dd = document.getElementById('ragChatDropdown');
    if (info && dd) {
      const sel = dd.options[dd.selectedIndex];
      info.textContent = '✅ ' + (sel ? sel.textContent.replace(/^[^\s]+ /, '') : model);
    }
  } catch(e) { console.warn('RAG chat switch failed:', e); }
}

async function saveDefaultRAGChat() {
  try {
    await fetch('/api/provider/set', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({saveRagChatDefault: true}),
    });
    const info = document.getElementById('ragChatInfo');
    if (info) info.textContent += ' 📌';
  } catch(e) { console.warn('save RAG chat default failed:', e); }
}

async function toggleAutoAnalyze() {
  const checked = document.getElementById('autoAnalyzeSwitch').checked;
  try {
    const r = await fetch('/api/provider/set', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({autoAnalyze: checked}),
    });
    if (!r.ok) return;
    if (S.providerConfig) S.providerConfig.autoAnalyze = checked;
    console.log('Auto-analyze:', checked);
  } catch(e) { console.warn('auto-analyze toggle failed:', e); }
}

function getProviderLabel() {
  const llmDd = document.getElementById('llmProviderDropdown');
  let llmName = S.provider || 'LLM';
  if (llmDd && llmDd.selectedIndex >= 0) {
    // Format: "🏠 Ollama On-Prem (Qwen 3.5 · free)" -> "Ollama On-Prem"
    let text = llmDd.options[llmDd.selectedIndex].text;
    text = text.replace(/^[^\s]+ /, ''); // remove emoji
    llmName = text.split(' (')[0];
  }

  const embDd = document.getElementById('embeddingDropdown');
  let embName = S.providerConfig?.embedding?.provider || 'Emb';
  if (embDd && embDd.selectedIndex >= 0) {
    // Format: "⚡ Qwen 3 Embed 8B (0.88₽/M · ~100ms)" -> "Qwen 3 Embed 8B"
    let text = embDd.options[embDd.selectedIndex].text;
    text = text.replace(/^[^\s]+ /, ''); // remove emoji
    embName = text.split(' (')[0];
  }

  return `${llmName} + ${embName}`;
}

function getProviderEstimate() {
  if (S.provider === 'ollama') return '5-10s';
  if (S.provider === 'openrouter') return '5-15s';
  return '2-3s';
}
  return 'Haiku · ~$0.002';
}

function getProviderEstimate() {
  if (S.provider === 'ollama') return '~5-10 сек';
  if (S.provider === 'openrouter') return '~5-15 сек';
  return '~2-3 сек';
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
    // Prefetch cached summaries for visible page in background.
    prefetchSummaries();
  } catch(e) {
    S.loading = false;
    if (!silent) document.getElementById('tableBody').innerHTML = `<tr><td colspan="11" style="color:var(--red);padding:16px">❌ ${e.message}</td></tr>`;
  }
}

// ─── START_BLOCK_SUMMARY_PREFETCH
// Batch-fetch cached AI summaries for visible rows so the 🧠 button is replaced
// by a badge immediately on page load (no re-query needed).
async function prefetchSummaries() {
  try {
    const visible = S.allLoaded
      ? S.filtered.slice((S.page - 1) * S.limit, S.page * S.limit)
      : S.filtered;
    const ids = visible.map(it => it.id).filter(Boolean);
    if (!ids.length) return;
    // Don't refetch ones already in state.
    const missing = ids.filter(id => !S[`summary_${id}`]);
    if (!missing.length) return;
    const r = await fetch('/api/generation-summaries', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ids: missing}),
    });
    if (!r.ok) return;
    const data = await r.json();
    const found = data.summaries || {};
    let any = false;
    for (const id of Object.keys(found)) {
      S[`summary_${id}`] = found[id];
      any = true;
    }
    if (any) renderTable();
  } catch(e) { /* silent */ }
}
// ─── END_BLOCK_SUMMARY_PREFETCH

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
      case 'category':
        // Sort: personal first, then flagged, then work, then empty
        va = _categorySortKey(a); vb = _categorySortKey(b); break;
      case 'aiTopic':
        va = (S[`summary_${a.id}`]?.topic||'').toLowerCase();
        vb = (S[`summary_${b.id}`]?.topic||'').toLowerCase(); break;
      default: va = a.createdAt||''; vb = b.createdAt||'';
    }
    if (va < vb) return -1 * dir; if (va > vb) return 1 * dir; return 0;
  });
}

function _categorySortKey(it) {
  const sum = S[`summary_${it.id}`];
  if (!sum) return 'zzz';
  if (sum.isWork === false) return '1';  // personal first
  const flags = Array.isArray(sum.riskFlags) ? sum.riskFlags : [];
  if (flags.length > 0) return '2';  // flagged
  return '3';  // work
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
  document.querySelectorAll('.group-tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  renderTable();
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
  if (S.loading) { tbody.innerHTML = '<tr><td colspan="12" class="loading"><div class="spinner"></div><br>Загрузка...</td></tr>'; return; }
  const items = S.filtered;
  if (!items.length) { tbody.innerHTML = '<tr><td colspan="12" style="padding:16px;text-align:center;color:var(--text2)">Нет данных</td></tr>'; return; }
  let pageItems = S.allLoaded ? items.slice((S.page-1)*S.limit, S.page*S.limit) : items;
  let html = '';
  if (S.groupMode === 'flat') { html += renderRows(pageItems); }
  else {
    const groups = groupItems(pageItems);
    for (const g of groups) { html += `<tr class="group-row"><td colspan="12">${groupLabel(g)}</td></tr>`; html += renderRows(g.items); }
  }
  tbody.innerHTML = html;
}

function renderRows(items) {
  return items.map(it => {
    const ci = getCacheInfo(it), cost = parseFloat(it.cost)||0;
    const prompt = it.usage?.prompt_tokens || 0;
    const catCell = renderCategoryCell(it);
    const aiCell = renderAiCell(it);
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
      <td>${catCell}</td>
      <td>${aiCell}</td>
    </tr>`;
  }).join('');
}

// ─── START_BLOCK_CATEGORY_CELL
function renderCategoryCell(it) {
  const sum = S[`summary_${it.id}`];
  if (!sum) {
    // Mirror AI cell status: queued / waiting / not analyzed
    if (S.analyzeAllRunning) return '<span style="color:var(--yellow);font-size:11px">🔄</span>';
    if (S.providerConfig?.autoAnalyze) return '<span style="color:var(--blue);font-size:11px">⏳</span>';
    return '<span style="color:var(--text2)">—</span>';
  }
  const isWork = sum.isWork !== false;
  const flags = Array.isArray(sum.riskFlags) ? sum.riskFlags : [];
  // Main category badge
  let badge = '';
  if (isWork && flags.length === 0) {
    badge = '<span class="cat-badge cat-work">✅ Рабочий</span>';
  } else if (!isWork) {
    badge = '<span class="cat-badge cat-personal">⚠️ Личное</span>';
  } else {
    // Work but with flags — show primary flag
    const f = flags[0];
    const labels = { personal: '👤 Личное', sensitive: '🔒 Секрет', high_cost: '💸 Дорого', unusual_model: '🤔 Модель' };
    badge = `<span class="cat-badge cat-flagged">${labels[f] || '⚠️ ' + f}</span>`;
  }
  return badge;
}
// ─── END_BLOCK_CATEGORY_CELL

// ─── START_BLOCK_AI_CELL
// Status indicators:
//   ✅ done (has summary) — green topic
//   🔄 In queue (analyze-all running) — yellow
//   ⏳ Waiting (auto-analyze ON, will process on next sync) — blue
//   🧠 Not analyzed (nothing active) — grey
function renderAiCell(it) {
  const sum = S[`summary_${it.id}`];
  if (!sum) {
    if (S.analyzeAllRunning) {
      return `<span class="ai-cell ai-cell-queued" title="В очереди на анализ">🔄 В очереди</span>`;
    }
    if (S.providerConfig?.autoAnalyze) {
      return `<span class="ai-cell ai-cell-waiting" title="Авто-анализ включён — будет обработано">⏳ Ожидает</span>`;
    }
    return `<span class="ai-cell ai-cell-empty" title="Не проанализировано — откройте детали">🧠</span>`;
  }
  const isPersonal = sum.isWork === false;
  const flags = Array.isArray(sum.riskFlags) ? sum.riskFlags : [];
  const flagIcons = [
    isPersonal ? '⚠️' : '',
    flags.includes('personal') ? '👤' : '',
    flags.includes('sensitive') ? '🔒' : '',
    flags.includes('high_cost') ? '💸' : '',
  ].filter(Boolean).join('');
  const topic = sum.topic || 'Готово';
  const tooltip = `${topic}\n\n${sum.summary || ''}`;
  return `<span class="ai-cell ai-cell-done ${isPersonal ? 'ai-cell-personal' : ''}" title="${esc(tooltip)}">
    ${flagIcons ? `<span class="ai-cell-flags">${flagIcons}</span>` : '<span class="ai-cell-icon">🧠</span>'}
    <span class="ai-cell-topic">${esc(topic)}</span>
  </span>`;
}
// ─── END_BLOCK_AI_CELL

function groupItems(items) {
  if (S.groupMode === 'flat') return [{ label: '', items }];
  const groups = new Map();
  const unanalyzedKey = '__unanalyzed__';
  for (const it of items) {
    let key;
    if (S.groupMode === 'day') key = (it.createdAt||'').slice(0,10);
    else if (S.groupMode === 'model') key = it.modelDisplayName||it.model||'?';
    else if (S.groupMode === 'key') key = it.apiKeyName||it._sourceKey||it.apiKeyShort||'?';
    else if (S.groupMode === 'topic') {
      const sum = S[`summary_${it.id}`];
      key = sum && sum.topic ? sum.topic : unanalyzedKey;
    }
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(it);
  }
  const result = [...groups.entries()].map(([label, gItems]) => ({ label, items: gItems }));
  // Topic grouping: sort by group size desc, unanalyzed last
  if (S.groupMode === 'topic') {
    result.sort((a, b) => {
      if (a.label === unanalyzedKey) return 1;
      if (b.label === unanalyzedKey) return -1;
      return b.items.length - a.items.length;
    });
  } else {
    result.sort((a,b) => { const da = a.items[0]?.createdAt||'', db = b.items[0]?.createdAt||''; return S.sortOrder==='desc'?db.localeCompare(da):da.localeCompare(db); });
  }
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
  if (S.groupMode==='topic') {
    if (g.label === '__unanalyzed__') {
      const ids = items.map(i => i.id).join('|');
      return `
        <div class="topic-group-header topic-group-unanalyzed">
          <div class="topic-group-main">
            🤖 <b>Без AI-анализа</b> — ${items.length} запросов, ${fmtCost(tc)} ₽
            <div class="topic-group-hint">Нажмите, чтобы проанализировать всю группу через Claude Haiku (~$0.002/запрос)</div>
          </div>
          <button class="btn-group-analyze" onclick="event.stopPropagation();bulkAnalyze(this,'${ids}')">🧠 Проанализировать (${items.length})</button>
        </div>`;
    }
    // Analyzed topic — group header shows topic + stats
    const sampleIds = items.slice(0, 3).map(i => i.id);
    // Check isWork flag from summaries in the group (majority vote for display)
    let personalCount = 0;
    for (const it of items) {
      const s = S[`summary_${it.id}`];
      if (s && s.isWork === false) personalCount++;
    }
    const personalBadge = personalCount > 0
      ? `<span class="topic-personal-badge" title="${personalCount} из ${items.length} помечены как возможно личное">⚠️ ${personalCount}</span>`
      : '';
    return `
      <div class="topic-group-header">
        <div class="topic-group-main">
          🧠 <b>${esc(g.label)}</b> — ${items.length} запросов, ${fmtCost(tc)} ₽, кэш ${cp}%${fe} ${personalBadge}
        </div>
      </div>`;
  }
  return g.label;
}

// Bulk-analyze: kick off summarize requests in parallel with a small concurrency cap.
async function bulkAnalyze(btnEl, idsStr) {
  const ids = idsStr.split('|').filter(Boolean);
  if (!ids.length) return;
  const total = ids.length;
  btnEl.disabled = true;
  let done = 0;
  const updateBtn = () => { btnEl.textContent = `⏳ ${done}/${total}`; };
  updateBtn();

  const CONCURRENCY = 3;
  let idx = 0;
  async function worker() {
    while (idx < ids.length) {
      const id = ids[idx++];
      try {
        const r = await fetch('/api/generation/summarize', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({generationId: id}),
        });
        if (r.ok) {
          const data = await r.json();
          S[`summary_${id}`] = data;
        }
      } catch(e) {}
      done++;
      updateBtn();
    }
  }
  const workers = [];
  for (let i = 0; i < Math.min(CONCURRENCY, total); i++) workers.push(worker());
  await Promise.all(workers);

  btnEl.textContent = `✅ Готово (${done}/${total})`;
  renderTable();
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
    let h = '';

    // AI Analysis section goes on top (collapsible, manual trigger)
    h += renderAiSection(genId, li);

    h += '<div class="detail-grid">';
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
      h += `<details class="log-section"><summary class="log-section-toggle">📤 Запрос (${msgs.length} сообщений) · Модель: ${req.model||'—'} · Stream: ${req.stream?'Да':'Нет'}</summary>`;
      if (req.tools?.length) {
        h += `<details style="margin-bottom:8px"><summary style="cursor:pointer;color:var(--accent2);font-size:12px">🔧 Tools (${req.tools.length})</summary><div style="margin-top:4px">`;
        req.tools.forEach(t => { h += `<div class="tool-call"><span class="fn-name">${t.function?.name||'?'}</span> <span style="color:var(--text2);font-size:11px">${(t.function?.description||'').slice(0,80)}</span></div>`; });
        h += '</div></details>';
      }
      h += renderMessages(msgs);
      h += '</details>';
      h += `<details class="log-section"><summary class="log-section-toggle">📥 Ответ</summary>`;
      const resp = log.response||{}, choices = resp.choices||[];
      choices.forEach((ch,ci) => {
        const msg = ch.message||{};
        h += `<div style="font-size:11px;color:var(--text2);margin-bottom:4px">Choice ${ci} · finish: ${ch.finish_reason||'—'}</div>`;
        if (msg.tool_calls?.length) msg.tool_calls.forEach(tc => { h += `<div class="tool-call"><div class="fn-name">🔧 ${tc.function?.name||tc.type||'?'}</div><pre>${tryJSON(tc.function?.arguments)}</pre></div>`; });
        if (msg.content) h += `<div class="msg msg-assistant"><div class="msg-header" onclick="toggleMsg(this)"><span class="role">ASSISTANT</span>${msg.content.length} chars<span class="toggle">▼</span></div><div class="msg-body">${esc(msg.content)}</div></div>`;
      });
      h += '</details>';
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

// ─── Backfill (server-side background) ────────────────────────────────────────

let _backfillPollTimer = null;

function startBackfillPoll() {
  if (_backfillPollTimer) return;
  _backfillPollTimer = setInterval(pollBackfillStatus, 2000);
  pollBackfillStatus();
}

function stopBackfillPoll() {
  if (_backfillPollTimer) { clearInterval(_backfillPollTimer); _backfillPollTimer = null; }
}

async function pollBackfillStatus() {
  try {
    const data = await (await fetch('/api/sessions/backfill/status')).json();
    renderBackfillIndicator(data);
    // If running, keep polling. If stopped and we're on sessions tab, refresh sessions
    if (!data.running) {
      stopBackfillPoll();
      if (S.groupMode === 'session' && data.enriched > 0) loadSessions();
    }
  } catch(e) { /* ignore */ }
}

function renderBackfillIndicator(d) {
  const el = document.getElementById('backfillIndicator');
  const icon = document.getElementById('backfillIcon');
  const text = document.getElementById('backfillText');
  const time = document.getElementById('backfillTime');

  if (!d.running && !d.errorMsg && d.remaining === 0 && d.enriched === 0) {
    el.style.display = 'none';
    return;
  }

  el.style.display = 'flex';
  const pct = d.total > 0 ? Math.round((d.enriched + d.noData) / d.total * 100) : 0;

  if (d.running) {
    icon.textContent = '⏳';
    text.innerHTML = `<b>Backfill:</b> ${fmtNum(d.enriched)} enriched, осталось ${fmtNum(d.remaining)} <small>(${pct}%)</small>`;
    time.textContent = d.lastUpdate ? fmtDate(d.lastUpdate) : '';
  } else if (d.errorMsg) {
    icon.textContent = '❌';
    text.innerHTML = `<b>Backfill ошибка:</b> ${esc(d.errorMsg)} <button class="btn" style="font-size:10px;margin-left:6px" onclick="backfillRetry()">🔄 Повторить</button> <button class="btn" style="font-size:10px" onclick="backfillStop()">✕ Закрыть</button>`;
    time.textContent = d.lastUpdate ? fmtDate(d.lastUpdate) : '';
  } else if (d.remaining === 0 && d.enriched > 0) {
    icon.textContent = '✅';
    text.innerHTML = `<b>Backfill завершён:</b> ${fmtNum(d.enriched)} enriched, ${fmtNum(d.errors)} ошибок`;
    time.textContent = d.lastUpdate ? fmtDate(d.lastUpdate) : '';
    setTimeout(() => { el.style.display = 'none'; }, 8000);
  } else if (d.enriched > 0) {
    icon.textContent = '⏸';
    text.innerHTML = `<b>Backfill приостановлен:</b> ${fmtNum(d.enriched)} enriched, ${fmtNum(d.remaining)} осталось`;
    time.textContent = d.lastUpdate ? fmtDate(d.lastUpdate) : '';
  } else {
    el.style.display = 'none';
  }
}

async function runBackfill() {
  const r = await fetch('/api/sessions/backfill/start', { method: 'POST' });
  if (!r.ok) { const e = await r.json(); alert('Ошибка: ' + (e.error||r.status)); return; }
  startBackfillPoll();
}

async function backfillRetry() {
  const r = await fetch('/api/sessions/backfill/retry', { method: 'POST' });
  if (!r.ok) { const e = await r.json(); alert('Ошибка: ' + (e.error||r.status)); return; }
  startBackfillPoll();
}

async function backfillStop() {
  await fetch('/api/sessions/backfill/stop', { method: 'POST' });
  stopBackfillPoll();
  const data = await (await fetch('/api/sessions/backfill/status')).json();
  renderBackfillIndicator(data);
}

// ─── Analyze ALL (persistent, DB-backed) ─────────────────────────────────────

let _analyzeAllPollTimer = null;

// Load analysis stats on every page load — always visible
async function loadAnalysisStats() {
  try {
    const r = await fetch('/api/analysis-stats');
    if (!r.ok) return;
    const data = await r.json();
    S.analyzeAllRunning = data.job && (data.job.status === 'running' || data.job.status === 'paused');
    renderAnalysisStats(data);
    // If job is running/paused, start polling
    if (S.analyzeAllRunning) startAnalyzeAllPoll();
  } catch(e) { console.warn('analysis-stats load failed:', e); }
}

function renderAnalysisStats(data) {
  const el = document.getElementById('analysisStats');
  const text = document.getElementById('analysisStatsText');
  if (!el || !text) return;

  const total = data.total || 0;
  const analyzed = data.analyzed || 0;
  const remaining = data.remaining || 0;
  const job = data.job || {};
  const isRunning = job.status === 'running';
  const isPaused = job.status === 'paused';

  // Always show if there are records
  if (total === 0) { el.style.display = 'none'; return; }
  el.style.display = 'flex';
  el.classList.toggle('active', isRunning || isPaused);

  const pct = total > 0 ? Math.round(analyzed / total * 100) : 0;

  if (isRunning || isPaused) {
    const label = isPaused ? '⏸' : '⏳';
    const eta = (!isPaused && job.done > 0) ? _estimateEta(job) : '';
    text.innerHTML = `${label} <span class="stats-done">${analyzed}</span>/${total} (<span class="stats-pct">${pct}%</span>) <span class="stats-remaining">−${remaining}</span> ${eta}`;
    // Update button
    const btn = document.getElementById('btnAnalyzeAll');
    if (btn) { btn.textContent = isRunning ? '⏸ Пауза' : '▶ Продолжить'; }
  } else if (remaining > 0) {
    text.innerHTML = `📊 <span class="stats-done">${analyzed}</span>/${total} (<span class="stats-pct">${pct}%</span>) <span class="stats-remaining">−${remaining} не анал.</span>`;
    const btn = document.getElementById('btnAnalyzeAll');
    if (btn) { btn.textContent = '🧠 Анализ всех'; }
  } else {
    text.innerHTML = `✅ <span class="stats-done">${analyzed}</span>/${total} — все проанализированы`;
    const btn = document.getElementById('btnAnalyzeAll');
    if (btn) { btn.textContent = '🧠 Анализ всех'; }
  }
}

async function startAnalyzeAll() {
  const btn = document.getElementById('btnAnalyzeAll');
  // If running → pause
  if (S.analyzeAllRunning && !document.getElementById('analysisStats')?.classList.contains('paused-state')) {
    await fetch('/api/analyze-all/pause', { method: 'POST' });
    return;
  }
  // Check if paused → resume
  try {
    const stats = await (await fetch('/api/analysis-stats')).json();
    if (stats.job?.status === 'paused') {
      btn.disabled = true;
      btn.textContent = '⏳ Возобновление...';
      await fetch('/api/analyze-all/start', { method: 'POST' });
      btn.disabled = false;
      startAnalyzeAllPoll();
      return;
    }
  } catch(e) {}

  if (!confirm(`Запустить AI-анализ всех неанализированных записей?\n\nПровайдер: ${getProviderLabel()}\nЭто может занять значительное время.`)) return;
  btn.disabled = true;
  btn.textContent = '⏳ Запуск...';
  const r = await fetch('/api/analyze-all/start', { method: 'POST' });
  const data = await r.json();
  btn.disabled = false;
  if (data.status === 'already_running') {
    alert('Анализ уже запущен');
  }
  startAnalyzeAllPoll();
}

function startAnalyzeAllPoll() {
  if (_analyzeAllPollTimer) return;
  _analyzeAllPollTimer = setInterval(pollAnalyzeAllStatus, 2000);
  pollAnalyzeAllStatus();
}

function stopAnalyzeAllPoll() {
  if (_analyzeAllPollTimer) { clearInterval(_analyzeAllPollTimer); _analyzeAllPollTimer = null; }
}

async function pollAnalyzeAllStatus() {
  try {
    const data = await (await fetch('/api/analysis-stats')).json();
    S.analyzeAllRunning = data.job && (data.job.status === 'running' || data.job.status === 'paused');
    renderAnalysisStats(data);

    const banner = document.getElementById('progressBanner');
    const job = data.job || {};
    const processed = (job.done || 0) + (job.skipped || 0) + (job.errors || 0);
    const total = job.total || data.total || 1;
    const pct = Math.round(processed / total * 100);

    if (job.status === 'running') {
      const eta = job.done > 0 ? _estimateEta(job) : '';
      const prov = getProviderLabel();
      showProgress(
        `🧠 AI-анализ (${prov}): ${job.done} готово / ${data.remaining} осталось (${job.errors} ош.) ${eta}`,
        pct
      );
      const pt = document.getElementById('progressText');
      pt.innerHTML = `🧠 AI-анализ (<b>${prov}</b>): <b>${job.done}</b>/${total} (${pct}%) · ${data.remaining} осталось ${eta}
        <button class="btn btn-small" style="margin-left:8px;padding:2px 10px" onclick="toggleAnalyzeAllPause()">⏸ Пауза</button>
        <button class="btn btn-small btn-danger" style="margin-left:4px;padding:2px 10px" onclick="stopAnalyzeAll()">⏹ Стоп</button>`;
      // Refresh table periodically
      if ((job.done || 0) > 0 && (job.done % 3 === 0)) {
        await prefetchSummaries();
        renderTable();
      }
    } else if (job.status === 'paused') {
      const prov = getProviderLabel();
      showProgress(`⏸ AI-анализ (${prov}) на паузе: ${job.done} готово, ${data.remaining} осталось`, pct);
      const pt = document.getElementById('progressText');
      pt.innerHTML = `⏸ AI-анализ (<b>${prov}</b>) на паузе: <b>${job.done}</b>/${total} (${pct}%) · ${data.remaining} осталось
        <button class="btn btn-small" style="margin-left:8px;padding:2px 10px" onclick="resumeAnalyzeAll()">▶ Продолжить</button>
        <button class="btn btn-small btn-danger" style="margin-left:4px;padding:2px 10px" onclick="stopAnalyzeAll()">⏹ Стоп</button>`;
    } else {
      // Not running
      hideProgress();
      stopAnalyzeAllPoll();
      if (data.remaining > 0 && data.analyzed > 0) {
        // Some analyzed, some remaining — show completion toast
        showProgress(`📊 Проанализировано ${data.analyzed} из ${data.total} (${data.remaining} осталось)`, Math.round(data.analyzed / data.total * 100));
        setTimeout(hideProgress, 5000);
      }
      await prefetchSummaries();
      renderTable();
    }
  } catch(e) { /* ignore */ }
}

async function toggleAnalyzeAllPause() {
  await fetch('/api/analyze-all/pause', { method: 'POST' });
}

async function resumeAnalyzeAll() {
  await fetch('/api/analyze-all/start', { method: 'POST' });
}

async function stopAnalyzeAll() {
  await fetch('/api/analyze-all/stop', { method: 'POST' });
}

function _estimateEta(d) {
  if (!d.startedAt || (d.done || 0) < 1) return '';
  const start = new Date(d.startedAt).getTime();
  const now = d.updatedAt ? new Date(d.updatedAt).getTime() : Date.now();
  const elapsed = (now - start) / 1000;
  const perItem = elapsed / d.done;
  const remaining = ((d.total || 0) - (d.done || 0) - (d.skipped || 0) - (d.errors || 0)) * perItem;
  if (remaining < 60) return `~${Math.round(remaining)}с`;
  if (remaining < 3600) return `~${Math.round(remaining / 60)} мин`;
  return `~${(remaining / 3600).toFixed(1)}ч`;
}

// Also check analyze-all status on page load (legacy compat)
async function checkAnalyzeAllStatus() {
  await loadAnalysisStats();
}

// ─── START_BLOCK_AI_SUMMARIZE
// AI analysis is rendered INSIDE the detail modal (not as a separate modal).
// Triggered manually by the user via "🧠 Проанализировать" button.

const FLAG_LABELS = {
  personal: '👤 Личное использование',
  off_hours: '🌙 Вне рабочего времени',
  unusual_model: '🤔 Нестандартная модель',
  high_cost: '💸 Высокая стоимость',
  sensitive: '🔒 Чувствительные данные',
};

function renderAiSection(genId, li) {
  const sum = S[`summary_${genId}`];
  const sessionId = li && li._sessionId ? li._sessionId : '';
  const sessSum = sessionId ? S[`sessSum_${sessionId}`] : null;

  // Header with collapse arrow
  let html = `<div class="ai-block" id="aiBlock_${genId}">`;

  if (!sum) {
    // Not yet analyzed — show call-to-action
    html += `
      <div class="ai-block-header">
        <div class="ai-block-title">🧠 AI-анализ запроса</div>
        <div class="ai-block-status">не выполнен</div>
      </div>
      <div class="ai-block-empty">
        <p>Модель проанализирует промпт и выдаст: тему, описание задачи, предполагаемый проект, флаги рисков.</p>
        <button class="btn btn-primary" onclick="runGenAnalysis('${genId}')">🧠 Проанализировать</button>
        <span class="ai-block-hint">${getProviderLabel()} · ${getProviderEstimate()}</span>
      </div>`;
  } else {
    const isPersonal = sum.isWork === false;
    const flags = Array.isArray(sum.riskFlags) ? sum.riskFlags : [];
    const flagsHtml = flags.length
      ? `<div class="summary-flags">${flags.map(f => `<span class="flag">${esc(FLAG_LABELS[f] || f)}</span>`).join('')}</div>`
      : '';
    const projectHtml = sum.projectGuess
      ? `<div class="ai-field"><span class="ai-field-label">🗂 Проект:</span> <span class="ai-field-value">${esc(sum.projectGuess)}</span></div>`
      : '';
    const metaHtml = `
      <div class="ai-meta">
        ${sum.cached ? '✅ из кеша БД' : '🆕 только что'}
        ${sum.provider === 'ollama' ? ' · 🏠 On-Prem' : sum.provider === 'openrouter' ? ' · 🌐 Cloud Free' : ' · ☁️ Cloud'}
        ${sum.llmModel ? ` · ${esc(sum.llmModel)}` : ''}
        ${sum.updatedAt ? ` · ${fmtDate(sum.updatedAt)}` : ''}
        ${sum.llmCost != null ? ` · $${Number(sum.llmCost).toFixed(6)}` : ''}
        ${sum.inputTokens ? ` · in ${fmtNum(sum.inputTokens)} / out ${fmtNum(sum.outputTokens||0)}` : ''}
        ${sum.cacheReadTokens ? ` · cache_read ${fmtNum(sum.cacheReadTokens)}` : ''}
        ${sum.vectorStored ? ' · 📐 вектор' : ''}
      </div>`;

    html += `
      <div class="ai-block-header">
        <div class="ai-block-title">🧠 AI-анализ запроса</div>
        <div class="summary-work-badge ${isPersonal ? 'personal' : 'work'}">
          ${isPersonal ? '⚠️ Возможно личное' : '✅ Рабочий запрос'}
        </div>
      </div>
      <div class="ai-block-body">
        <div class="ai-topic-big">${esc(sum.topic || '—')}</div>
        ${flagsHtml}
        <div class="ai-summary-text">${esc(sum.summary || '')}</div>
        ${projectHtml}
        ${metaHtml}
        <div class="ai-block-actions">
          <button class="btn btn-small" onclick="runGenAnalysis('${genId}', true)">🔄 Пересуммаризировать</button>
        </div>
      </div>`;
  }

  // Session block (if generation has session_id)
  if (sessionId) {
    html += `<div class="ai-session-block" id="aiSessBlock_${genId}">`;
    if (!sessSum) {
      html += `
        <div class="ai-session-header">
          <div>
            <div class="ai-session-title">💬 Часть сессии</div>
            <div class="ai-session-sid" title="${esc(sessionId)}">ID: ${esc(sessionId.slice(0, 32))}…</div>
          </div>
          <button class="btn btn-small btn-primary" onclick="runSessionAnalysis('${esc(sessionId)}', '${genId}')">🧠 Анализ сессии целиком</button>
        </div>`;
    } else {
      const isPersonal = sessSum.isWork === false;
      html += `
        <div class="ai-session-header">
          <div>
            <div class="ai-session-title">💬 Сессия: ${esc(sessSum.topic || '—')}</div>
            <div class="ai-session-sid" title="${esc(sessionId)}">ID: ${esc(sessionId.slice(0, 32))}…</div>
          </div>
          <div class="summary-work-badge ${isPersonal ? 'personal' : 'work'}" style="font-size:11px">
            ${isPersonal ? '⚠️ Личная' : '✅ Рабочая'}
          </div>
        </div>
        <div class="ai-session-body">${esc(sessSum.summary || '')}</div>`;
    }
    html += `</div>`;
  }

  html += `</div>`;
  return html;
}

async function runGenAnalysis(genId, force = false) {
  const block = document.getElementById(`aiBlock_${genId}`);
  const providerName = S.provider === 'ollama' ? 'Qwen' : S.provider === 'openrouter' ? (document.getElementById('orModelDropdown')?.options[document.getElementById('orModelDropdown').selectedIndex]?.text || 'Cloud Free') : 'Claude Haiku';
  if (block) {
    block.innerHTML = `
      <div class="ai-block-header">
        <div class="ai-block-title">🧠 AI-анализ запроса</div>
        <div class="ai-block-status">⏳ анализирую...</div>
      </div>
      <div class="ai-block-loading"><div class="spinner"></div> ${esc(providerName)} обрабатывает промпт...</div>`;
  }
  try {
    const r = await fetch('/api/generation/summarize', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({generationId: genId, force}),
    });
    const data = await r.json();
    if (!r.ok) {
      if (block) block.innerHTML = `<div class="ai-block-error">❌ ${esc(data.error || 'Ошибка анализа')}</div>`;
      return;
    }
    S[`summary_${genId}`] = data;
    // Re-render section and table row badge
    const li = S.items.find(i => i.id === genId) || {};
    if (block) block.outerHTML = renderAiSection(genId, li);
    renderTable();
  } catch(e) {
    if (block) block.innerHTML = `<div class="ai-block-error">❌ ${esc(e.message)}</div>`;
  }
}

async function runSessionAnalysis(sessionId, genId) {
  const sessBlock = document.getElementById(`aiSessBlock_${genId}`);
  if (sessBlock) {
    sessBlock.innerHTML = `<div class="ai-block-loading"><div class="spinner"></div> Анализ сессии целиком...</div>`;
  }
  try {
    const r = await fetch('/api/session/summarize', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sessionId}),
    });
    const data = await r.json();
    if (!r.ok) {
      if (sessBlock) sessBlock.innerHTML = `<div class="ai-block-error">❌ ${esc(data.error || 'Ошибка')}</div>`;
      return;
    }
    S[`sessSum_${sessionId}`] = data;
    // Re-render session block
    const li = S.items.find(i => i.id === genId) || {};
    const full = renderAiSection(genId, li);
    const aiBlock = document.getElementById(`aiBlock_${genId}`);
    if (aiBlock) aiBlock.outerHTML = full;
  } catch(e) {
    if (sessBlock) sessBlock.innerHTML = `<div class="ai-block-error">❌ ${esc(e.message)}</div>`;
  }
}

// ─── END_BLOCK_AI_SUMMARIZE
