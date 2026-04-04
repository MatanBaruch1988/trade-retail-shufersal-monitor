// ═══════════════════════════════════════════════════════════════
//  Tempo Price Monitor - Dashboard JS
// ═══════════════════════════════════════════════════════════════

const API = '';  // same origin
let currentAlertId = null;
let presenceData = null;
let alertsData = [];

// ── Tabs ──────────────────────────────────────────────────────
const TAB_LABELS = {
  'summary': 'התראות', 'presence': 'מפת כיסוי', 'gaps': 'פערי תמחור',
  'history': 'מגמות מחיר', 'data-quality': 'כיסוי נתונים', 'audit': 'יומן מערכת',
  'manage': 'ניהול',
  'guide': 'מדריך',
};

function showTab(name) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
  document.getElementById('tab-' + name).classList.remove('hidden');
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.toggle('tab-active', btn.dataset.tab === name);
    btn.classList.toggle('tab-inactive', btn.dataset.tab !== name);
  });
  // Mobile title + sheet active state
  const mobileTitle = document.getElementById('mobileTabTitle');
  if (mobileTitle) mobileTitle.textContent = TAB_LABELS[name] || name;
  document.querySelectorAll('.tab-sheet-btn').forEach(btn => {
    btn.classList.toggle('sheet-active', btn.dataset.sheetTab === name);
  });
  if (name === 'presence') { if (!presenceData) loadPresence(); else renderPresence(presenceData); }
  if (name === 'gaps') loadGaps();
  if (name === 'history') loadHistory();
  if (name === 'data-quality') loadDataQuality();
  if (name === 'audit') loadAudit();
  if (name === 'manage') loadManage();
}

// ── Guide accordion ───────────────────────────────────────────
// ⚠️  LIVING DOCUMENT: עדכן את tab-guide ב-index.html בכל שינוי בפרויקט
// (agents, סכמות, לוח זמנים, גיליונות, לוגיקת סינון)
function toggleGuide(id) {
  const body = document.getElementById(id);
  const arrow = document.getElementById(id + '-arrow');
  const isOpen = !body.classList.contains('hidden');
  body.classList.toggle('hidden', isOpen);
  if (arrow) arrow.style.transform = isOpen ? '' : 'rotate(180deg)';
}

// ── Status bar ────────────────────────────────────────────────
async function loadStatus() {
  try {
    const r = await fetch(API + '/api/status');
    const d = await r.json();
    const badge = document.getElementById('statusBadge');
    if (d.last_refresh_at) {
      const dt = new Date(parseFloat(d.last_refresh_at) * 1000);
      const today = new Date();
      const isToday = dt.toDateString() === today.toDateString();
      badge.textContent = 'עודכן: ' + (isToday ? 'היום ' : '') + dt.toLocaleTimeString('he-IL', {hour:'2-digit', minute:'2-digit'});
      badge.className = d.is_stale ? 'text-xs text-red-500 font-medium' : 'text-xs text-green-600 font-medium';
    } else {
      badge.textContent = 'לא עודכן עדיין';
      badge.className = 'text-xs text-gray-400';
    }
  } catch (e) {
    console.error('status error', e);
    showToast('שגיאה בטעינת סטטוס', 'error');
  }
}

// ── Action Queue ──────────────────────────────────────────────
async function loadActionQueue() {
  try {
    const r = await fetch(API + '/api/action-queue');
    alertsData = await r.json();

    // Update header badge (red alerts)
    const redCount = alertsData.filter(a => a.severity === 'red').length;
    const badge = document.getElementById('alertBadge');
    const count = document.getElementById('alertCount');
    if (redCount > 0) {
      badge.classList.remove('hidden');
      badge.classList.add('flex');
      count.textContent = redCount;
    } else {
      badge.classList.add('hidden');
    }

    // Update tile counts
    ['high_gap', 'no_promo', 'single_format', 'promo_mismatch'].forEach(type => {
      const el = document.getElementById('tileCount_' + type);
      if (el) el.textContent = alertsData.filter(a => a.alert_type === type).length;
    });
  } catch (e) { console.error('action queue error', e); }
}

function renderAlertCard(a) {
  return `<div class="severity-${a.severity} rounded-xl px-4 py-3 flex items-center justify-between gap-3 cursor-pointer hover:opacity-90 transition-opacity"
       onclick="openModal(${a.id}, '${esc(a.product_name)}', '${esc(a.issue)}', '${esc(a.recommended_action)}', '${a.severity}')">
    <div class="flex items-center gap-3 min-w-0">
      <span class="text-lg flex-shrink-0">${severityIcon(a.severity)}</span>
      <div class="min-w-0">
        <div class="font-semibold text-sm truncate">${esc(a.product_name)}</div>
        <div class="text-xs opacity-80 truncate">${esc(a.issue)}</div>
      </div>
    </div>
    <button class="flex-shrink-0 bg-white/70 hover:bg-white px-3 py-1.5 rounded-lg text-xs font-semibold border border-current/20 transition-colors">פעל</button>
  </div>`;
}

const drillDownLabels = {
  high_gap:       '🔴 פערי מחיר',
  no_promo:       '🟡 ללא מבצע',
  single_format:  '🟠 כיסוי חסר',
  promo_mismatch: '🔵 מבצע חלקי',
};

function openDrillDown(type) {
  const filtered = alertsData.filter(a => a.alert_type === type);
  document.getElementById('drillDownTitle').textContent =
    `${drillDownLabels[type] || type} - ${filtered.length} מוצרים`;
  document.getElementById('drillDownBody').innerHTML = filtered.length
    ? filtered.map(renderAlertCard).join('')
    : '<div class="text-center text-gray-400 py-4 text-sm">אין התראות בקטגוריה זו</div>';
  const panel = document.getElementById('drillDownPanel');
  panel.classList.remove('hidden');
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function closeDrillDown() {
  document.getElementById('drillDownPanel').classList.add('hidden');
}

function severityIcon(s) {
  return s === 'red' ? '🔴' : s === 'yellow' ? '🟡' : s === 'blue' ? '🔵' : '🟢';
}

// ── Modal ─────────────────────────────────────────────────────
function openModal(id, name, issue, action, severity) {
  currentAlertId = id;
  document.getElementById('modalProduct').textContent = name;
  document.getElementById('modalIssue').textContent = issue;

  // Split recommended_action into steps (split by period or newline)
  const steps = action.split(/[.\n]/).map(s => s.trim()).filter(Boolean);
  document.getElementById('modalSteps').innerHTML = steps.map((s, i) =>
    `<div class="flex items-start gap-2">
       <span class="flex-shrink-0 w-5 h-5 rounded-full bg-gray-200 text-gray-600 text-xs flex items-center justify-center font-bold">${i+1}</span>
       <span>${s}</span>
     </div>`
  ).join('');

  document.getElementById('actionModal').classList.remove('hidden');
}

function closeModal() {
  document.getElementById('actionModal').classList.add('hidden');
  currentAlertId = null;
}

document.getElementById('actionModal').addEventListener('click', closeModal);

async function resolveAlert() {
  if (!currentAlertId) return;
  try {
    await fetch(API + '/api/action/' + currentAlertId + '/resolve', { method: 'POST' });
    closeModal();
    closeDrillDown();
    loadActionQueue();
  } catch (e) { showToast('שגיאה: ' + e.message, 'error'); }
}

// ── Insights ──────────────────────────────────────────────────
async function loadInsights() {
  try {
    const r = await fetch(API + '/api/insights');
    const d = await r.json();
    const ins = d.insights || {};
    const kpis = d.kpis || {};

    document.getElementById('narrativeSummary').textContent = ins.narrative_summary || 'לחץ ריענון לטעינת נתונים';
    const top3 = document.getElementById('top3Actions');
    if (ins.top_3_actions && ins.top_3_actions.length) {
      top3.innerHTML = '<div class="text-xs font-semibold text-blue-700 mb-1">המלצות:</div>' +
        ins.top_3_actions.map((a, i) => `<div class="text-xs text-blue-800">📌 ${a}</div>`).join('');
    }

    document.getElementById('kpiTotal').textContent = kpis.total_products ?? '-';
    document.getElementById('kpiWithPromo').textContent = kpis.products_with_promo ?? '-';
    document.getElementById('kpiNoPromo').textContent = kpis.products_without_promo ?? '-';
    document.getElementById('kpiAvgDisc').textContent = kpis.avg_discount_pct ? kpis.avg_discount_pct + '%' : '-';

  } catch (e) { console.error('insights error', e); }
}


// ── Presence Matrix ────────────────────────────────────────────
async function loadPresence() {
  try {
    const r = await fetch(API + '/api/presence');
    presenceData = await r.json();
    renderPresence(presenceData);
  } catch (e) {
    console.error('presence error', e);
    showToast('שגיאה בטעינת מפת נוכחות', 'error');
  }
}

window._presenceStats = {};

const _fmtDate = d => {
  if (!d) return '';
  if (d.length === 10 && d[4] === '-') return `${d.slice(8)}/${d.slice(5,7)}/${d.slice(0,4)}`;
  if (d.length === 8) return `${d.slice(6)}/${d.slice(4,6)}/${d.slice(0,4)}`;
  return d;
};

function showPresenceTooltip(event, key) {
  const stats = window._presenceStats[key];
  if (!stats) return;
  const tip = document.getElementById('presenceTooltip');

  // Build promo list section (under badge)
  let promoHeader = '';
  const promoList = stats.promo_list || [];
  if (stats.type === 'promo' && promoList.length > 0) {
    promoHeader = `<div class="mt-2 space-y-1.5">`;
    for (const pr of promoList) {
      promoHeader += `<div class="border-t border-gray-700 pt-1 first:border-0 first:pt-0">
        <div class="text-gray-100 text-xs">${esc(pr.desc)}</div>
        <div class="text-green-300 text-xs font-medium text-right">₪${fmt(pr.unit_price)} ליחידה</div>
        ${pr.start_date || pr.end_date ? `<div class="text-gray-500 text-[10px] text-right">${_fmtDate(pr.end_date)} – ${_fmtDate(pr.start_date)}</div>` : ''}
      </div>`;
    }
    promoHeader += `</div>`;
  }

  // Stats section — show ranges when multiple promos
  const priceRange = Math.abs((stats.price_max || 0) - (stats.price_min || 0));
  const uniformPrice = priceRange < 0.001;
  const _priceLabel = uniformPrice
    ? `₪${fmt(stats.price_min)}`
    : `₪${fmt(stats.price_max)}–₪${fmt(stats.price_min)}`;
  const typeBadge = stats.type === 'promo'
    ? `<span class="inline-block mt-1 px-2 py-0.5 rounded bg-green-700 text-green-100 text-xs font-semibold" dir="ltr">מבצע: ${_priceLabel}</span>`
    : `<span class="inline-block mt-1 px-2 py-0.5 rounded bg-blue-700 text-blue-100 text-xs font-semibold" dir="ltr">קטלוגי: ${_priceLabel}</span>`;
  const statsSection = uniformPrice ? '' : `
  <div class="font-bold text-yellow-300 mb-2">סטטיסטיקות</div>
  <div class="grid grid-cols-2 gap-x-3 gap-y-0.5 mb-2">
    <span class="text-gray-300">מינימום:</span><span dir="ltr">₪${fmt(stats.price_min)}</span>
    <span class="text-gray-300">מקסימום:</span><span dir="ltr">₪${fmt(stats.price_max)}</span>
    <span class="text-gray-300">ממוצע:</span><span dir="ltr">₪${fmt(stats.price_avg)}</span>
    <span class="text-gray-300">חציון:</span><span dir="ltr">₪${fmt(stats.price_median)}</span>
  </div>`;

  let html = `<div class="border-b border-gray-600 pb-2 mb-2">
    <div class="font-bold text-white text-sm leading-snug">${esc(stats.name)}</div>
    <div class="text-gray-400 text-xs mt-0.5">${esc(stats.format_name)}</div>
    ${typeBadge}
    ${promoHeader}
  </div>
  ${statsSection}`;

  html += `<div class="${uniformPrice ? '' : 'border-t border-gray-600 '}pt-2">
    <div class="font-bold text-yellow-300 mb-1">פירוט סניפים <span class="text-gray-400 font-normal text-[10px]">(${stats.store_count})</span></div>
    <div class="space-y-0.5">`;
  for (const s of stats.stores) {
    const label = s.store_name ? `${s.store_name}${s.city ? ' – ' + s.city : ''}` : s.store_id;
    const priceStr = !uniformPrice && s.price != null ? `<span class="font-medium whitespace-nowrap" dir="ltr">₪${fmt(s.price)}</span>` : '';
    html += `<div class="flex items-center gap-2">
      <span class="text-gray-300 truncate flex-1 min-w-0">${esc(label)}</span>
      ${priceStr}
    </div>`;
  }
  html += `</div></div>`;

  tip.innerHTML = html;
  tip.classList.remove('hidden');
  _positionPresenceTooltip(event);
}

function hidePresenceTooltip() {
  document.getElementById('presenceTooltip').classList.add('hidden');
}

function _positionPresenceTooltip(event) {
  const tip = document.getElementById('presenceTooltip');
  const margin = 12;
  const vw = window.innerWidth, vh = window.innerHeight;
  const tw = tip.offsetWidth || 260, th = tip.offsetHeight || 200;
  let x = event.clientX + margin, y = event.clientY + margin;
  if (x + tw > vw - margin) x = event.clientX - tw - margin;
  if (y + th > vh - margin) y = event.clientY - th - margin;
  tip.style.left = Math.max(margin, x) + 'px';
  tip.style.top  = Math.max(margin, y) + 'px';
}

function renderPresence(data) {
  if (!data) return;
  const { formats, products } = data;
  window._presenceStats = {};

  // Header
  document.getElementById('presenceHead').innerHTML =
    '<tr>' +
    '<th class="px-3 py-2 text-right sticky right-0 bg-gray-50 z-10 w-32">ברקוד</th>' +
    '<th class="px-3 py-2 text-right sticky right-32 bg-gray-50 z-10">מוצר</th>' +
    formats.map(f => `<th class="px-3 py-2 text-center text-xs font-semibold">${esc(f)}</th>`).join('') +
    '<th class="px-3 py-2 text-center">סה"כ</th></tr>';

  // Body
  const search = document.getElementById('presenceSearch').value.toLowerCase();
  const filtered = products.filter(p => p.name.toLowerCase().includes(search));

  document.getElementById('presenceBody').innerHTML = filtered.map(p => {
    const cells = formats.map(fmtName => {
      const cell = p.formats[fmtName] || {};
      const { price_min, price_max, price_avg, price_median, store_count, stores,
              promo, promo_list,
              promo_min, promo_max, promo_avg, promo_median, promo_store_count, promo_stores } = cell;

      const hasCatalog = price_min != null;
      const hasPromo   = promo != null;

      if (!hasCatalog && !hasPromo) {
        return `<td class="px-2 py-2 text-center"><span class="text-gray-300 text-sm">-</span></td>`;
      }

      let badges = '';

      if (hasCatalog) {
        const keyC = `${p.barcode}__${fmtName}__c`;
        window._presenceStats[keyC] = { name: p.name, format_name: fmtName, type: 'catalog', price_min, price_max, price_avg, price_median, store_count, stores };
        const rangeStr = price_min === price_max
          ? `₪${fmt(price_min)}`
          : `₪${fmt(price_min)}–₪${fmt(price_max)}`;
        badges += `<span class="inline-flex flex-col items-center text-xs px-1 py-0.5 rounded bg-blue-100 text-blue-700 cursor-help"
            data-skey="${keyC}"
            onmouseenter="showPresenceTooltip(event,'${keyC}')"
            onmouseleave="hidePresenceTooltip()"
            onmousemove="_positionPresenceTooltip(event)">
            <span>קטלוגי</span><span class="font-medium" dir="ltr">${rangeStr}</span>
          </span>`;
      }

      if (hasPromo) {
        const keyP = `${p.barcode}__${fmtName}__p`;
        const pMin = promo_min ?? promo;
        const pMax = promo_max ?? promo;
        window._presenceStats[keyP] = {
          name: p.name, format_name: fmtName, type: 'promo',
          price_min:    pMin,
          price_max:    pMax,
          price_avg:    promo_avg ?? promo,
          price_median: promo_median ?? promo,
          store_count:  promo_store_count ?? 1,
          stores:       promo_stores || [],
          promo_list:   promo_list || [],
        };
        const promoRange = pMin === pMax
          ? `₪${fmt(pMin)}`
          : `₪${fmt(pMin)}–₪${fmt(pMax)}`;
        badges += `<span class="inline-flex flex-col items-center text-xs px-1 py-0.5 rounded bg-green-100 text-green-700 cursor-help"
            data-skey="${keyP}"
            onmouseenter="showPresenceTooltip(event,'${keyP}')"
            onmouseleave="hidePresenceTooltip()"
            onmousemove="_positionPresenceTooltip(event)">
            <span>מבצע</span><span class="font-semibold" dir="ltr">${promoRange}</span>
          </span>`;
      }

      return `<td class="px-2 py-2 text-center"><div class="inline-flex flex-col gap-0.5">${badges}</div></td>`;
    }).join('');

    const count = formats.filter(f => {
      const c = p.formats[f] || {};
      return c.price_min != null || c.promo != null;
    }).length;

    return `<tr class="hover:bg-gray-50 border-t border-gray-100">
      <td class="px-3 py-2 sticky right-0 bg-white z-10 w-32 text-xs text-gray-400 font-mono whitespace-nowrap">${esc(p.barcode)}</td>
      <td class="px-3 py-2 sticky right-32 bg-white z-10 max-w-xs truncate font-medium text-gray-800 text-sm">${esc(p.name)}</td>
      ${cells}
      <td class="px-3 py-2 text-center font-semibold text-gray-700">${count}/${formats.length}</td>
    </tr>`;
  }).join('');
}

function filterPresence() {
  if (presenceData) renderPresence(presenceData);
}

// ── Price Gaps ────────────────────────────────────────────────
let _gapsData = { catalog: [], promo: [] };
const _gapsSort = {
  catalog: { field: 'gap_ils', dir: -1 },
  promo:   { field: 'gap_ils', dir: -1 },
};

function _renderSortedGaps(table) {
  const { field, dir } = _gapsSort[table];
  const sorted = [..._gapsData[table]].sort((a, b) => {
    const av = a[field] ?? '', bv = b[field] ?? '';
    return typeof av === 'number' ? (av - bv) * dir : String(av).localeCompare(String(bv), 'he') * dir;
  });
  const bodyId = table === 'catalog' ? 'gapsBody' : 'gapsBodyPromo';
  document.getElementById(bodyId).innerHTML = _renderGapRows(sorted);
  document.querySelectorAll(`th[data-table="${table}"]`).forEach(th => {
    const icon = th.querySelector('.sort-icon');
    if (!icon) return;
    const active = th.dataset.field === field;
    icon.textContent = active ? (dir === 1 ? '↑' : '↓') : '↕';
    icon.className = 'sort-icon ' + (active ? 'text-blue-600' : 'text-gray-400');
  });
}

document.addEventListener('click', e => {
  const th = e.target.closest('th[data-table][data-field]');
  if (!th) return;
  const table = th.dataset.table, field = th.dataset.field;
  _gapsSort[table].dir = _gapsSort[table].field === field ? -_gapsSort[table].dir : -1;
  _gapsSort[table].field = field;
  _renderSortedGaps(table);
});

function _renderGapRows(gaps) {
  return gaps.map(g => {
    const cls = g.gap_pct >= 30 ? 'text-red-600 font-bold' : g.gap_pct >= 15 ? 'text-yellow-600 font-semibold' : 'text-gray-700';
    return `<tr class="hover:bg-gray-50">
      <td class="px-4 py-2 text-xs text-gray-400 font-mono whitespace-nowrap">${esc(g.barcode || '')}</td>
      <td class="px-4 py-2 font-medium text-gray-800 max-w-xs truncate whitespace-nowrap">${esc(g.name)}</td>
      <td class="px-4 py-2 text-green-700 whitespace-nowrap" dir="ltr">₪${fmt(g.min_price)}</td>
      <td class="px-4 py-2 text-xs text-gray-500 whitespace-nowrap">${esc(g.min_format || '')}</td>
      <td class="px-4 py-2 text-red-700 whitespace-nowrap" dir="ltr">₪${fmt(g.max_price)}</td>
      <td class="px-4 py-2 text-xs text-gray-500 whitespace-nowrap">${esc(g.max_format || '')}</td>
      <td class="px-4 py-2 ${cls} whitespace-nowrap" dir="ltr">₪${fmt(g.gap_ils)}</td>
      <td class="px-4 py-2 ${cls} whitespace-nowrap">${g.gap_pct}%</td>
    </tr>`;
  }).join('') || '<tr><td colspan="8" class="text-center py-6 text-gray-400">אין נתוני פערים</td></tr>';
}

function _renderGapCards(gaps) {
  return gaps.map(g => {
    const cls = g.gap_pct >= 30 ? 'text-red-600 font-bold' : g.gap_pct >= 15 ? 'text-yellow-600 font-semibold' : 'text-gray-700';
    const border = g.gap_pct >= 30 ? 'border-red-300' : g.gap_pct >= 15 ? 'border-yellow-300' : 'border-gray-200';
    return `<div class="bg-white rounded-xl border ${border} p-3 shadow-sm">
      <div class="font-medium text-sm text-gray-800 mb-2 truncate">${esc(g.name)}</div>
      <div class="flex justify-between text-xs mb-1">
        <span class="text-gray-500">📉 ${esc(g.min_format || '')}</span>
        <span class="text-green-700 font-semibold" dir="ltr">₪${fmt(g.min_price)}</span>
      </div>
      <div class="flex justify-between text-xs mb-2">
        <span class="text-gray-500">📈 ${esc(g.max_format || '')}</span>
        <span class="text-red-700 font-semibold" dir="ltr">₪${fmt(g.max_price)}</span>
      </div>
      <div class="flex justify-between text-sm font-bold border-t pt-2 mt-1">
        <span class="text-gray-600">פער</span>
        <span class="${cls}" dir="ltr">₪${fmt(g.gap_ils)} (${g.gap_pct}%)</span>
      </div>
    </div>`;
  }).join('') || '<div class="text-center py-6 text-gray-400 text-sm">אין נתוני פערים</div>';
}

async function loadGaps() {
  try {
    const r = await fetch(API + '/api/price-gaps');
    const d = await r.json();
    _gapsData.catalog = d.catalog || [];
    _gapsData.promo   = d.promo   || [];
    _renderSortedGaps('catalog');
    _renderSortedGaps('promo');
    const catalogCards = document.getElementById('price-gaps-cards');
    if (catalogCards) catalogCards.innerHTML = _renderGapCards(_gapsData.catalog);
    const promoCards = document.getElementById('price-gaps-cards-promo');
    if (promoCards) promoCards.innerHTML = _renderGapCards(_gapsData.promo);
  } catch (e) {
    console.error('gaps error', e);
    showToast('שגיאה בטעינת פערי מחירים', 'error');
  }
}

// ── Data Quality ─────────────────────────────────────────────
function coverageCls(pct, hasData) {
  if (!hasData) return 'text-gray-400';
  return pct >= 80 ? 'text-green-600 font-medium' : pct >= 50 ? 'text-yellow-600 font-medium' : 'text-red-600 font-medium';
}

async function loadDataQuality() {
  try {
    const r = await fetch(API + '/api/data-quality');
    const d = await r.json();

    // Day Completeness Banner
    const banner = document.getElementById('dqDayBanner');
    if (banner) {
      const pct = d.day_completeness_pct ?? 0;
      const bannerCls = pct >= 80 ? 'bg-green-50 border-green-200' : pct >= 50 ? 'bg-yellow-50 border-yellow-200' : 'bg-red-50 border-red-200';
      const pctCls   = pct >= 80 ? 'text-green-600' : pct >= 50 ? 'text-yellow-600' : 'text-red-600';
      const icon = pct >= 80 ? '✅' : pct >= 50 ? '🟡' : '🔴';
      banner.className = `rounded-xl border p-4 mb-4 flex flex-wrap items-center justify-between gap-3 ${bannerCls}`;
      banner.innerHTML = `
        <div class="flex items-center gap-2 text-sm font-medium text-gray-700">
          <span>📅</span><span>יום נוכחי: ${esc(d.today_display || '')}</span>
        </div>
        <div class="flex items-center gap-3">
          <span class="text-sm font-semibold text-gray-800">${icon} ${d.formats_today ?? 0}/${d.total_known_formats ?? 0} פורמטים עודכנו להיום</span>
          <span class="text-2xl font-bold ${pctCls}">${pct}%</span>
        </div>`;
    }

    // Format freshness table
    const statusCfg = {
      today:     { icon: '✅', label: 'היום',   cls: 'text-green-700 bg-green-50' },
      yesterday: { icon: '⚠️', label: 'אתמול',  cls: 'text-yellow-700 bg-yellow-50' },
      stale:     { icon: '🔴', label: 'ישן',    cls: 'text-red-700 bg-red-50' },
      missing:   { icon: '❌', label: 'חסר',    cls: 'text-gray-500 bg-gray-100' },
    };
    const ff = d.format_freshness || [];
    document.getElementById('dqFormatFreshnessBody').innerHTML = ff.length
      ? ff.map(f => {
          const s = statusCfg[f.status] || statusCfg.missing;
          return `<tr class="border-t border-gray-100 hover:bg-gray-50">
            <td class="px-4 py-2 font-medium text-gray-800">${esc(f.format_name)}</td>
            <td class="px-4 py-2 text-center font-mono text-xs text-gray-500">${esc(f.display_date)}</td>
            <td class="px-4 py-2 text-center font-mono text-xs">${f.branch_count_db ?? '-'}</td>
            <td class="px-4 py-2 text-center font-mono text-xs text-gray-400">${f.branch_count_site || '-'}</td>
            <td class="px-4 py-2 text-center text-xs ${coverageCls(f.branch_coverage_pct, !!f.branch_count_site)}">${f.branch_count_site ? f.branch_coverage_pct + '%' : '-'}</td>
            <td class="px-4 py-2 text-center font-mono text-xs">${f.bc_count}</td>
            <td class="px-4 py-2 text-center font-mono text-xs text-gray-400">${f.bc_count_site ?? '-'}</td>
            <td class="px-4 py-2 text-center text-xs ${coverageCls(f.coverage_pct, f.bc_count > 0)}">${f.bc_count > 0 ? f.coverage_pct + '%' : '-'}</td>
            <td class="px-4 py-2 text-center"><span class="px-2 py-0.5 rounded-full text-xs font-medium ${s.cls}">${s.icon} ${s.label}</span></td>
          </tr>`;
        }).join('')
      : '<tr><td colspan="9" class="text-center py-4 text-gray-400 text-sm">הרץ ריענון לעדכון</td></tr>';

    // ברקודים
    document.getElementById('dqTotal').textContent = d.total_user_barcodes;
    document.getElementById('dqFound').textContent = d.found_barcodes_count;
    document.getElementById('dqMissingBC').textContent = d.missing_barcodes_count;

    const bcCardsEl = document.getElementById('dqMissingBCCards');
    if (bcCardsEl) {
      bcCardsEl.innerHTML = d.missing_barcodes.length
        ? d.missing_barcodes.map(b => `
            <div class="py-2 px-1">
              <div class="text-sm font-medium text-gray-800">${esc(b.name)}</div>
              <div class="text-xs text-gray-400 flex gap-2 mt-0.5">
                <span class="font-mono">${b.barcode}</span>
              </div>
            </div>`).join('')
        : '<div class="text-center py-4 text-green-600 text-sm font-medium">✓ כל הברקודים נמצאו</div>';
    }

    document.getElementById('dqMissingBCBody').innerHTML = d.missing_barcodes.length
      ? d.missing_barcodes.map(b => `<tr class="hover:bg-gray-50">
          <td class="px-4 py-2 text-gray-800">${esc(b.name)}</td>
          <td class="px-4 py-2 font-mono text-xs text-gray-400">${b.barcode}</td>
        </tr>`).join('')
      : '<tr><td colspan="2" class="text-center py-6 text-green-600 font-medium">✓ כל הברקודים נמצאו</td></tr>';

  } catch (e) {
    console.error('data quality error', e);
    showToast('שגיאה בטעינת בקרת נתונים', 'error');
  }
}

// ── History ───────────────────────────────────────────────────
let historyBarcodesCache = null;
let historyCharts = [];
let historyDays = 30;

function onHistoryDaysChange(days) {
  historyDays = days;
  [7, 14, 30].forEach(d => {
    const btn = document.getElementById(`historyDayBtn${d}`);
    if (!btn) return;
    if (d === days) {
      btn.className = 'px-3 py-1.5 bg-tempo text-white font-medium transition-colors';
    } else {
      btn.className = d === 14
        ? 'px-3 py-1.5 text-gray-500 hover:bg-gray-50 border-x border-gray-200 transition-colors'
        : 'px-3 py-1.5 text-gray-500 hover:bg-gray-50 transition-colors';
    }
  });
  const barcode = document.getElementById('historyBarcode')?.value;
  if (barcode) onHistoryBarcodeChange();
}

async function loadHistory() {
  try {
    if (!historyBarcodesCache) {
      const r = await fetch(API + '/api/barcodes');
      historyBarcodesCache = await r.json();
    }
    const sel = document.getElementById('historyBarcode');
    if (!sel) return;
    const current = sel.value;
    sel.innerHTML = '<option value="">בחר מוצר לצפייה בהיסטוריה...</option>' +
      historyBarcodesCache.filter(b => b.active).map(b =>
        `<option value="${esc(b.barcode)}"${b.barcode === current ? ' selected' : ''}>${esc(b.name)} (${esc(b.barcode)})</option>`
      ).join('');
    if (current) await onHistoryBarcodeChange();
  } catch(e) {
    console.error('history load error', e);
    showToast('שגיאה בטעינת רשימת מוצרים', 'error');
  }
}

async function onHistoryBarcodeChange() {
  const barcode = document.getElementById('historyBarcode').value;
  const content = document.getElementById('historyContent');
  const kpiEl   = document.getElementById('historyKpi');
  if (!barcode) {
    historyCharts.forEach(c => c.destroy()); historyCharts = [];
    if (kpiEl) kpiEl.classList.add('hidden');
    content.innerHTML = `<div class="bg-white rounded-xl border border-gray-200 p-6 text-center text-gray-400">
      <p class="text-lg">📈 בחר מוצר כדי לראות היסטוריית מחירים</p>
      <p class="text-sm mt-2">כל ריצת pipeline שומרת snapshot - 30 ימים אחורה</p>
    </div>`;
    return;
  }
  content.innerHTML = `<div class="p-6 text-center text-gray-400 text-sm">טוען...</div>`;
  try {
    const r = await fetch(API + `/api/history/${encodeURIComponent(barcode)}/chart?days=${historyDays}`);
    const data = await r.json();
    renderHistoryCharts(data);
  } catch(e) {
    console.error('history fetch error', e);
    document.getElementById('historyContent').innerHTML =
      `<div class="bg-white rounded-xl border border-gray-200 p-6 text-center text-red-400">שגיאה בטעינת היסטוריה</div>`;
  }
}

function renderHistoryCharts(data) {
  const content = document.getElementById('historyContent');
  const kpiEl   = document.getElementById('historyKpi');
  historyCharts.forEach(c => c.destroy()); historyCharts = [];

  const formats = (data.formats || []).filter(f => f.min_price.some(v => v !== null));
  if (!formats.length) {
    if (kpiEl) kpiEl.classList.add('hidden');
    content.innerHTML = `<div class="bg-white rounded-xl border border-gray-200 p-6 text-center text-gray-400">
      <p class="text-lg">📊 אין היסטוריה עדיין</p>
      <p class="text-sm mt-2">נתונים יצטברו לאחר מספר ריצות pipeline</p>
    </div>`;
    return;
  }

  // Ensure tooltip overlay exists
  if (!document.getElementById('historyTooltip')) {
    const tip = document.createElement('div');
    tip.id = 'historyTooltip';
    tip.style.cssText = 'position:fixed;z-index:9999;pointer-events:none;display:none;background:#1f2937;color:#f9fafb;border-radius:12px;padding:12px 16px;font-size:13px;min-width:180px;max-width:280px;box-shadow:0 8px 30px rgba(0,0,0,0.4);line-height:1.7;direction:rtl';
    document.body.appendChild(tip);
  }

  const shortDate = d => { const [,m,day] = d.split('-'); return `${day}/${m}`; };
  const lastNonNull = arr => { for (let i = arr.length - 1; i >= 0; i--) if (arr[i] != null) return arr[i]; return null; };

  // Build global KPI bar
  const kpiItems = formats.map(f => {
    const cur = lastNonNull(f.avg_price);
    const promo = lastNonNull(f.promo_min);
    return { name: f.format_name, cur, promo };
  }).filter(x => x.cur != null);

  if (kpiEl && kpiItems.length) {
    const allPrices = kpiItems.map(x => x.cur);
    const minPrice = Math.min(...allPrices);
    const maxPrice = Math.max(...allPrices);
    const cheapest = kpiItems.find(x => x.cur === minPrice);
    const priciest = kpiItems.find(x => x.cur === maxPrice);
    const bestPromo = kpiItems.filter(x => x.promo != null).sort((a,b) => a.promo - b.promo)[0];

    const div = '<div class="self-stretch border-r border-gray-100"></div>';
    kpiEl.innerHTML = `<div class="bg-white rounded-xl border border-gray-200 p-4">
      <div class="flex flex-wrap gap-6 items-start">
        ${minPrice !== maxPrice ? `<div>
          <div class="text-xs text-gray-400 mb-0.5">טווח מחירים</div>
          <div class="text-xl font-semibold text-gray-700" dir="ltr">₪${minPrice.toFixed(2)} – ₪${maxPrice.toFixed(2)}</div>
          <div class="text-xs text-gray-500 mt-0.5">פער של ₪${(maxPrice - minPrice).toFixed(2)}</div>
        </div>${div}` : ''}
        <div>
          <div class="text-xs text-gray-400 mb-0.5">הזול ביותר</div>
          <div class="text-2xl font-bold text-green-600" dir="ltr">₪${minPrice.toFixed(2)}</div>
          <div class="text-xs text-gray-500 mt-0.5">${esc(cheapest.name)}</div>
        </div>
        ${minPrice !== maxPrice ? `${div}<div>
          <div class="text-xs text-gray-400 mb-0.5">היקר ביותר</div>
          <div class="text-2xl font-bold text-red-500" dir="ltr">₪${maxPrice.toFixed(2)}</div>
          <div class="text-xs text-gray-500 mt-0.5">${esc(priciest.name)}</div>
        </div>` : ''}
        ${bestPromo ? `${div}<div>
          <div class="text-xs text-gray-400 mb-0.5">מבצע טוב ביותר</div>
          <div class="text-2xl font-bold text-purple-600" dir="ltr">₪${bestPromo.promo.toFixed(2)}</div>
          <div class="text-xs text-gray-500 mt-0.5">${esc(bestPromo.name)} · חיסכון ${Math.round((bestPromo.cur - bestPromo.promo) / bestPromo.cur * 100)}%</div>
        </div>` : ''}
      </div>
    </div>`;
    kpiEl.classList.remove('hidden');
  }

  const cards = formats.map((f, idx) => {
    const maxBc    = Math.max(...(f.branch_count.filter(Boolean)));
    const hasPromo = f.promo_min.some(v => v !== null);
    const curPrice = lastNonNull(f.avg_price);
    const curPromo = lastNonNull(f.promo_min);

    // Trend: compare last price to value ~7 snapshots back
    let trendHtml = '';
    if (curPrice != null) {
      const validIdxs = f.avg_price.map((v,i) => v != null ? i : -1).filter(i => i >= 0);
      const lastIdx = validIdxs[validIdxs.length - 1];
      const prevIdx = validIdxs[Math.max(0, validIdxs.length - 8)]; // ~7 days back
      const prevPrice = f.avg_price[prevIdx];
      if (prevPrice != null && prevIdx !== lastIdx) {
        const diff = curPrice - prevPrice;
        const pct  = (diff / prevPrice * 100).toFixed(1);
        if (Math.abs(diff) > 0.005) {
          const up   = diff > 0;
          const clr  = up ? '#ef4444' : '#10b981';
          const sign = up ? '↑' : '↓';
          trendHtml = `<span style="color:${clr};font-size:13px;font-weight:600">${sign} ${Math.abs(pct)}%</span>`;
        } else {
          trendHtml = `<span style="color:#9ca3af;font-size:12px">ללא שינוי</span>`;
        }
      }
    }

    const promoBadge = (hasPromo && curPromo != null && curPrice != null)
      ? `<span class="text-xs font-medium px-2 py-0.5 rounded-full" style="background:#f3e8ff;color:#7c3aed">מבצע ₪${curPromo.toFixed(2)} · ${Math.round((curPrice - curPromo) / curPrice * 100)}% הנחה</span>`
      : '';

    return `<div class="bg-white rounded-xl border border-gray-200 p-4">
      <div class="flex justify-between items-start mb-3">
        <h3 class="font-semibold text-gray-800">${esc(f.format_name)}</h3>
        <span class="text-xs text-gray-400 pt-0.5">${maxBc > 1 ? `עד ${maxBc} סניפים` : ''}</span>
      </div>
      ${curPrice != null ? `<div class="flex items-end gap-3 mb-3">
        <div>
          <div class="text-xs text-gray-400 mb-0.5">מחיר נוכחי</div>
          <div class="text-2xl font-bold text-gray-900 leading-none" dir="ltr">₪${curPrice.toFixed(2)}</div>
        </div>
        ${trendHtml ? `<div class="mb-0.5">${trendHtml}</div>` : ''}
        ${promoBadge ? `<div class="mr-auto">${promoBadge}</div>` : ''}
      </div>` : ''}
      <div style="height:220px;position:relative">
        <canvas id="hchart-${idx}"></canvas>
      </div>
    </div>`;
  }).join('');

  content.innerHTML = `<div class="grid grid-cols-1 md:grid-cols-2 gap-4">${cards}</div>`;

  formats.forEach((f, idx) => {
    const canvas = document.getElementById(`hchart-${idx}`);
    if (!canvas) return;
    const labels   = f.dates.map(shortDate);
    const hasPromo = f.promo_min.some(v => v !== null);
    const datasets = [
      { label: 'מינ׳',  data: f.min_price, borderColor: '#10b981', borderWidth: 1.5,
        fill: '+1', backgroundColor: 'rgba(16,185,129,0.08)', tension: 0.35,
        pointRadius: 2, pointHoverRadius: 5, spanGaps: true },
      { label: 'מקס׳',  data: f.max_price, borderColor: '#f87171', borderWidth: 1.5,
        fill: false, tension: 0.35, pointRadius: 2, pointHoverRadius: 5, spanGaps: true },
      { label: 'ממוצע', data: f.avg_price, borderColor: '#3b82f6', borderWidth: 2.5,
        fill: false, tension: 0.35, pointRadius: 3, pointHoverRadius: 6, spanGaps: true },
    ];
    if (hasPromo) {
      datasets.push({ label: 'מבצע', data: f.promo_min, borderColor: '#a855f7',
        borderDash: [5, 3], borderWidth: 2, fill: false, tension: 0.35,
        pointRadius: 4, pointHoverRadius: 7, pointStyle: 'circle', spanGaps: true });
    }

    const chart = new Chart(canvas, {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            position: 'bottom', rtl: true,
            labels: { font: { size: 11 }, boxWidth: 16, usePointStyle: true, padding: 12 },
          },
          tooltip: { enabled: false, external: _makeHistoryTooltip(f.daily_detail, f.dates) },
        },
        scales: {
          x: {
            ticks: { font: { size: 9 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 10 },
            grid: { color: 'rgba(0,0,0,0.04)' },
          },
          y: {
            ticks: { font: { size: 11 }, callback: v => v != null ? '₪' + v.toFixed(2) : '', maxTicksLimit: 6 },
            grid: { color: 'rgba(0,0,0,0.06)' },
            grace: '8%',
          },
        },
      },
    });
    historyCharts.push(chart);
  });
}

function _makeHistoryTooltip(dailyDetail, dates) {
  return function(context) {
    const tip = document.getElementById('historyTooltip');
    if (!tip) return;
    if (context.tooltip.opacity === 0) { tip.style.display = 'none'; return; }

    const idx = context.tooltip.dataPoints?.[0]?.dataIndex;
    if (idx == null) { tip.style.display = 'none'; return; }

    const dateIso = dates[idx];
    const [,m,day] = dateIso.split('-');
    const rows = (dailyDetail[dateIso] || []);
    if (!rows.length) { tip.style.display = 'none'; return; }

    const sep = '<div style="border-top:1px solid #374151;margin:7px 0"></div>';
    const parts = rows.map(r => {
      const p = r.price != null ? r.price.toFixed(2) : null;
      if (!p) return '';
      let html = `<div style="display:flex;justify-content:space-between;gap:16px;align-items:center">
        <span style="color:#9ca3af;font-size:11px">קטלוגי</span>
        <span dir="ltr" style="font-weight:600">₪${p}</span>
      </div>`;
      if (r.promo_price != null) {
        const saving     = (r.price - r.promo_price).toFixed(2);
        const discStr    = r.discount_pct != null ? `${r.discount_pct}%` : '';
        html += `${sep}<div style="display:flex;justify-content:space-between;gap:16px;align-items:center">
          <span style="color:#c4b5fd;font-size:11px">מבצע</span>
          <span dir="ltr" style="color:#c4b5fd;font-weight:600">₪${r.promo_price.toFixed(2)}</span>
        </div>
        <div style="display:flex;justify-content:space-between;gap:16px;font-size:11px;color:#6b7280">
          <span>חיסכון</span>
          <span dir="ltr">₪${saving}${discStr ? ` | ${discStr}` : ''}</span>
        </div>`;
      }
      return html;
    }).filter(Boolean);

    tip.innerHTML = `<div style="font-weight:700;font-size:14px;margin-bottom:9px;color:#93c5fd">${day}/${m}</div>
      ${parts.join(sep)}`;

    const rect = context.chart.canvas.getBoundingClientRect();
    const x    = rect.left + context.tooltip.caretX;
    const y    = rect.top  + context.tooltip.caretY;
    const w    = tip.offsetWidth || 200;
    tip.style.display = 'block';
    tip.style.left    = Math.min(x + 14, window.innerWidth - w - 16) + 'px';
    tip.style.top     = Math.max(y - 24, 8) + 'px';
  };
}

// ── Audit Calendar ────────────────────────────────────────────
function renderAuditCalendar(data) {
  const el = document.getElementById('auditCalendarGrid');
  if (!el) return;
  const grid       = data.calendar_grid || {};
  const dates      = data.calendar_dates || [];
  const formats    = data.calendar_formats || [];
  const norm       = data.calendar_format_norm || {};
  const timestamps = data.calendar_timestamps || {};
  const today      = data.today || '';

  if (!dates.length || !formats.length) {
    el.innerHTML = '<div class="text-center py-6 text-gray-400 text-sm">אין נתוני היסטוריה - נתונים יצטברו לאחר מספר ריצות pipeline</div>';
    return;
  }

  const shortDates = dates.map(d => { const [,m,day] = d.split('-'); return `${day}/${m}`; });

  const headerCells = shortDates.map((sd, i) => {
    const isToday = dates[i] === today;
    return `<th class="px-1 py-1.5 text-center text-xs font-medium border-r border-gray-100${isToday ? ' bg-blue-50 text-blue-600 font-bold' : ' text-gray-500'}">${sd}</th>`;
  }).join('');

  const bodyRows = formats.map(fmt => {
    const fmtData  = grid[fmt] || {};
    const fmtTs    = timestamps[fmt] || {};
    const normalCount = norm[fmt] || 0;
    const cells = dates.map(d => {
      const count   = fmtData[d] || 0;
      const isToday = d === today;
      let cell, title;
      if (count === 0) {
        if (isToday) { cell = '🔄'; title = 'ממתין לעדכון היום'; }
        else         { cell = '❌'; title = 'לא הגיע'; }
      } else if (normalCount > 0 && count < normalCount * 0.3) {
        cell = '⚠️'; title = `${count} ברקודים (חלקי)`;
      } else {
        cell = '✅'; title = `${count} ברקודים`;
      }
      const tsLabel = fmtTs[d] || '';
      const tsHtml  = tsLabel ? `<div class="text-gray-400 text-[10px] leading-none mt-0.5">${tsLabel}</div>` : '';
      return `<td class="px-1 py-1 text-center text-sm border-r border-gray-100${isToday ? ' bg-blue-50' : ''}" title="${title}"><div>${cell}</div>${tsHtml}</td>`;
    }).join('');
    return `<tr class="border-t border-gray-100 hover:bg-gray-50">
      <td class="px-3 py-1.5 text-xs font-medium text-gray-700 whitespace-nowrap sticky right-0 bg-white">${esc(fmt)}</td>
      ${cells}
    </tr>`;
  }).join('');

  el.innerHTML = `<table class="w-full text-sm" style="min-width:600px">
    <thead class="bg-gray-50">
      <tr>
        <th class="px-3 py-1.5 text-right text-xs font-medium text-gray-500 sticky right-0 bg-gray-50">פורמט</th>
        ${headerCells}
      </tr>
    </thead>
    <tbody>${bodyRows}</tbody>
  </table>`;
}

// ── Audit ─────────────────────────────────────────────────────
async function loadAudit() {
  try {
    const resp = await fetch(API + '/api/audit?limit=50');
    const d = await resp.json();
    const runs = Array.isArray(d) ? d : (d.runs || []);

    // KPIs
    document.getElementById('auditTotalRuns').textContent = runs.length;

    if (runs.length > 0) {
      const latest = runs[0];
      const latestDate = new Date(latest.started_at * 1000);
      const diffMin = Math.round((Date.now() / 1000 - latest.started_at) / 60);
      document.getElementById('auditLastRun').textContent =
        diffMin < 60 ? `לפני ${diffMin} דק'` :
        diffMin < 1440 ? `לפני ${Math.round(diffMin/60)} שע'` :
        latestDate.toLocaleDateString('he-IL');
    }

    const last10 = runs.slice(0, 10);
    const okCount = last10.filter(r => r.status === 'ok' || r.status === 'partial' || r.status === 'skipped').length;
    document.getElementById('auditSuccessRate').textContent = last10.length
      ? Math.round(okCount / last10.length * 100) + '%' : '-';

    const weekCutoff = Date.now() / 1000 - 7 * 86400;
    const weekFailed = runs.filter(r => r.started_at > weekCutoff).reduce((s, r) => s + (r.files_failed || 0), 0);
    document.getElementById('auditFailedFiles').textContent = weekFailed;

    // Calendar grid
    renderAuditCalendar(d);

    // Mobile cards
    const auditCardsEl = document.getElementById('auditCards');
    if (auditCardsEl) {
      auditCardsEl.innerHTML = runs.length ? runs.map(run => {
        const dt = new Date(run.started_at * 1000);
        const timeStr = dt.toLocaleDateString('he-IL') + ' ' + dt.toLocaleTimeString('he-IL', {hour:'2-digit', minute:'2-digit'});
        const statusInfo = {
          ok:      { label: run.new_data ? '✅ עודכן' : '⚪ ללא שינוי', cls: run.new_data ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500' },
          partial: { label: '⚠️ חלקי',  cls: 'bg-yellow-100 text-yellow-700' },
          failed:  { label: '❌ כשל',    cls: 'bg-red-100 text-red-700' },
          skipped: { label: '⏭ דולג',   cls: 'bg-gray-100 text-gray-500' },
          running: { label: '⏳ רץ',     cls: 'bg-blue-100 text-blue-700' },
        };
        const si = statusInfo[run.status] || { label: run.status, cls: 'bg-gray-100 text-gray-500' };
        const trigger = run.trigger === 'manual' ? '👆 ידני' : run.trigger === 'startup' ? '🚀 הפעלה' : '⏰ מתוזמן';
        const delta = (run.products_after != null && run.products_before != null)
          ? run.products_after - run.products_before : null;
        const deltaStr = delta == null ? '' : delta > 0 ? `+${delta}` : `${delta}`;
        return `<div class="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <div class="flex items-center justify-between mb-2">
            <span class="text-xs text-gray-500">${timeStr}</span>
            <span class="px-2 py-0.5 rounded-full text-xs font-medium ${si.cls}">${si.label}</span>
          </div>
          <div class="flex flex-wrap gap-2 text-xs text-gray-500">
            <span>${trigger}</span>
            ${run.shufersal_date && run.shufersal_date !== '-' ? `<span>📅 ${run.shufersal_date}</span>` : ''}
            ${run.products_after != null ? `<span>📦 ${run.products_after} מוצרים${deltaStr ? ` (${deltaStr})` : ''}</span>` : ''}
            ${run.files_attempted ? `<span>📂 ${run.files_ok||0}/${run.files_attempted} קבצים</span>` : ''}
            ${run.duration_s != null ? `<span>⏱ ${run.duration_s}שנ'</span>` : ''}
          </div>
          ${(run.errors && run.errors.length) ? `
            <div class="mt-2 text-xs text-red-600">
              ${run.errors.map(e => `<div class="truncate max-w-full">${esc(e.file)}: ${esc(e.error)}</div>`).join('')}
            </div>` : ''}
        </div>`;
      }).join('') : '<div class="text-center py-6 text-gray-400 text-sm">אין נתוני ריצות - הרץ ריענון ראשון</div>';
    }

    // Table
    document.getElementById('auditBody').innerHTML = runs.length ? runs.map(run => {
      const dt = new Date(run.started_at * 1000);
      const timeStr = dt.toLocaleDateString('he-IL') + ' ' + dt.toLocaleTimeString('he-IL', {hour:'2-digit', minute:'2-digit'});

      const statusInfo = {
        ok:      { label: run.new_data ? '✅ עודכן' : '⚪ ללא שינוי', cls: run.new_data ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500' },
        partial: { label: '⚠️ חלקי',  cls: 'bg-yellow-100 text-yellow-700' },
        failed:  { label: '❌ כשל',    cls: 'bg-red-100 text-red-700' },
        skipped: { label: '⏭ דולג',   cls: 'bg-gray-100 text-gray-500' },
        running: { label: '⏳ רץ',     cls: 'bg-blue-100 text-blue-700' },
      };
      const si = statusInfo[run.status] || { label: run.status, cls: 'bg-gray-100 text-gray-500' };
      const trigger = run.trigger === 'manual' ? '👆 ידני' : run.trigger === 'startup' ? '🚀 הפעלה' : '⏰ מתוזמן';

      const filesStr = run.files_attempted != null
        ? `${run.files_ok || 0}/${run.files_attempted}${run.files_failed ? ` <span class="text-red-500">(${run.files_failed} נכשלו)</span>` : ''}`
        : '-';

      const errorsHtml = (run.errors && run.errors.length)
        ? `<button onclick="this.nextElementSibling.classList.toggle('hidden')" class="text-xs text-red-600 underline">${run.errors.length} שגיאות ▼</button>
           <div class="hidden mt-1 text-xs text-red-700 bg-red-50 rounded p-2 space-y-0.5">
             ${run.errors.map(e => `<div class="truncate max-w-full">${esc(e.file)}: ${esc(e.error)}</div>`).join('')}
           </div>`
        : '<span class="text-gray-300">-</span>';

      return `<tr class="hover:bg-gray-50 align-top">
        <td class="px-4 py-2 text-xs text-gray-500 whitespace-nowrap">${timeStr}</td>
        <td class="px-4 py-2"><span class="px-2 py-0.5 rounded-full text-xs font-medium ${si.cls}">${si.label}</span></td>
        <td class="px-4 py-2 text-xs text-gray-500">${trigger}</td>
        <td class="px-4 py-2 text-center text-xs font-mono text-gray-500">${run.shufersal_date ?? '-'}</td>
        <td class="px-4 py-2 text-center text-xs font-mono">${run.products_before ?? '-'}</td>
        <td class="px-4 py-2 text-center text-xs font-mono">${run.products_after ?? '-'}</td>
        <td class="px-4 py-2 text-center text-xs">${filesStr}</td>
        <td class="px-4 py-2 text-xs">${errorsHtml}</td>
        <td class="px-4 py-2 text-center text-xs font-mono text-gray-500">${run.duration_s != null ? run.duration_s : '-'}</td>
      </tr>`;
    }).join('') : '<tr><td colspan="9" class="text-center py-8 text-gray-400">אין נתוני ריצות - הרץ ריענון ראשון</td></tr>';
  } catch (e) {
    console.error('audit error', e);
    showToast('שגיאה בטעינת audit', 'error');
  }
}

// ── Tab Bottom Sheet ──────────────────────────────────────────
function openTabSheet() {
  const sheet = document.getElementById('tabSheet');
  const panel = document.getElementById('tabSheetPanel');
  const trigger = document.getElementById('tabSheetTrigger');
  sheet.classList.remove('hidden');
  sheet.removeAttribute('aria-hidden');
  if (trigger) trigger.setAttribute('aria-expanded', 'true');
  requestAnimationFrame(() => {
    panel.classList.remove('translate-y-full');
  });
}

function closeTabSheet() {
  const sheet = document.getElementById('tabSheet');
  const panel = document.getElementById('tabSheetPanel');
  const trigger = document.getElementById('tabSheetTrigger');
  panel.classList.add('translate-y-full');
  if (trigger) trigger.setAttribute('aria-expanded', 'false');
  setTimeout(() => {
    sheet.classList.add('hidden');
    sheet.setAttribute('aria-hidden', 'true');
  }, 300);
}

document.addEventListener('keydown', e => { if (e.key === 'Escape') closeTabSheet(); });

// ── Toast ─────────────────────────────────────────────────────
function showToast(msg, type = 'info') {
  const colors = { success: 'bg-green-600', error: 'bg-red-600', info: 'bg-gray-800' };
  const el = document.createElement('div');
  el.className = `pointer-events-auto px-4 py-2.5 rounded-xl text-white text-sm font-medium shadow-lg ${colors[type] || colors.info} transition-opacity duration-300`;
  el.textContent = msg;
  document.getElementById('toastContainer').appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 3000);
}

// ── Helpers ───────────────────────────────────────────────────
function esc(str) {
  if (!str) return '';
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

function fmt(n) {
  if (n == null) return '-';
  return parseFloat(n).toFixed(2);
}

// ══════════════════════════════════════════════════════════════
//  Manage Tab
// ══════════════════════════════════════════════════════════════

let _manageBarcodes = [];   // cached list from server

async function loadManage() {
  await Promise.all([loadManageBarcodes(), loadManageFormats()]);
}

// ── Barcodes ──────────────────────────────────────────────────
async function loadManageBarcodes() {
  try {
    const resp = await fetch(API + '/api/barcodes');
    _manageBarcodes = await resp.json();
    renderBarcodesTable(_manageBarcodes);
  } catch (e) {
    console.error('manage barcodes error', e);
    showToast('שגיאה בטעינת ברקודים', 'error');
  }
}

function renderBarcodesTable(barcodes) {
  // Update count badge
  const countEl = document.getElementById('manageBarcodCount');
  if (countEl) {
    countEl.textContent = `${barcodes.length} ברקודים`;
    countEl.classList.remove('hidden');
  }

  // Detect barcodes with no real name (name == barcode)
  const noName = barcodes.filter(b => !b.name || b.name === b.barcode);
  const noNameAlert = document.getElementById('noNameAlert');
  const noNameList  = document.getElementById('noNameList');
  if (noName.length > 0) {
    noNameList.textContent = ' ' + noName.map(b => b.barcode).join(', ');
    noNameAlert.classList.remove('hidden');
  } else {
    noNameAlert.classList.add('hidden');
  }

  // Desktop table
  const tbody = document.getElementById('barcodesTableBody');
  if (tbody) {
    tbody.innerHTML = barcodes.length
      ? barcodes.map(b => {
          const hasName = b.name && b.name !== b.barcode;
          const nameCell = hasName
            ? `<span>${esc(b.name)}</span>`
            : `<span class="text-amber-600">⚠️ ${esc(b.name || b.barcode)} <span class="text-xs text-gray-400">(לא ידוע)</span></span>`;
          return `<tr class="border-t border-gray-100 hover:bg-gray-50" id="bc-row-${b.id}">
            <td class="px-4 py-2.5 text-xs font-mono text-gray-600">${esc(b.barcode)}</td>
            <td class="px-4 py-2.5 text-sm text-gray-800">${nameCell}</td>
            <td class="px-4 py-2.5 text-center">
              <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${b.active ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}">
                ${b.active ? 'פעיל' : 'מושבת'}
              </span>
            </td>
            <td class="px-4 py-2.5 text-center">
              <button onclick="deleteBarcode(${b.id}, '${esc(b.barcode)}')"
                      class="text-red-500 hover:text-red-700 text-xs px-2 py-1 rounded hover:bg-red-50 transition-colors">
                מחק
              </button>
            </td>
          </tr>`;
        }).join('')
      : '<tr><td colspan="4" class="text-center py-8 text-gray-400">אין ברקודים מוגדרים</td></tr>';
  }

  // Mobile cards
  const cards = document.getElementById('barcodesCards');
  if (cards) {
    cards.innerHTML = barcodes.length
      ? barcodes.map(b => {
          const hasName = b.name && b.name !== b.barcode;
          return `<div class="bg-white border border-gray-200 rounded-xl px-4 py-3 flex items-center justify-between gap-3">
            <div class="min-w-0">
              <div class="text-xs font-mono text-gray-500">${esc(b.barcode)}</div>
              <div class="text-sm font-medium text-gray-800 truncate ${!hasName ? 'text-amber-600' : ''}">
                ${!hasName ? '⚠️ ' : ''}${esc(b.name || b.barcode)}
              </div>
            </div>
            <button onclick="deleteBarcode(${b.id}, '${esc(b.barcode)}')"
                    class="flex-shrink-0 text-red-500 hover:text-red-700 text-xs border border-red-200 px-2 py-1 rounded-lg">
              מחק
            </button>
          </div>`;
        }).join('')
      : '<div class="text-center py-8 text-gray-400 text-sm">אין ברקודים מוגדרים</div>';
  }
}

async function lookupAndFillName() {
  const bcInput = document.getElementById('newBarcodeInput');
  const nameInput = document.getElementById('newBarcodeNameInput');
  const errEl = document.getElementById('addBarcodeError');
  const barcode = bcInput.value.trim();
  if (!barcode) { showBarcodeError('יש להזין ברקוד'); return; }
  if (!/^\d{4,}$/.test(barcode)) { showBarcodeError('ברקוד חייב להכיל לפחות 4 ספרות'); return; }
  errEl.classList.add('hidden');
  nameInput.value = 'מחפש...';
  try {
    const r = await fetch(API + `/api/barcodes/lookup/${encodeURIComponent(barcode)}`);
    const d = await r.json();
    nameInput.value = (d.name && d.name !== barcode) ? d.name : '';
    nameInput.placeholder = (d.name && d.name !== barcode) ? '' : 'שם לא נמצא בנתוני שופרסל';
  } catch {
    nameInput.value = '';
    nameInput.placeholder = 'שגיאה בחיפוש';
  }
}

function clearBarcodeNamePreview() {
  // Name is read-only — only clear the error message when barcode input changes
  document.getElementById('addBarcodeError').classList.add('hidden');
}

function showBarcodeError(msg) {
  const el = document.getElementById('addBarcodeError');
  el.textContent = msg;
  el.classList.remove('hidden');
}

async function submitAddBarcode() {
  const bcInput   = document.getElementById('newBarcodeInput');
  const nameInput = document.getElementById('newBarcodeNameInput');
  const barcode   = bcInput.value.trim();
  const name      = nameInput.value.trim();

  if (!barcode) { showBarcodeError('יש להזין ברקוד'); return; }
  if (!/^\d{4,}$/.test(barcode)) { showBarcodeError('ברקוד חייב להכיל לפחות 4 ספרות'); return; }

  // Check for duplicate
  if (_manageBarcodes.some(b => b.barcode === barcode)) {
    showBarcodeError('ברקוד זה כבר קיים ברשימה');
    return;
  }

  try {
    const resp = await fetch(API + '/api/barcodes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ barcode, name, active: true }),
    });
    const d = await resp.json();
    if (!resp.ok) { showBarcodeError(d.detail || 'שגיאה בהוספה'); return; }

    showToast(`ברקוד ${barcode} נוסף${d.name && d.name !== barcode ? ' - ' + d.name : ''}`, 'success');
    bcInput.value = '';
    nameInput.value = '';
    document.getElementById('addBarcodeError').classList.add('hidden');
    await loadManageBarcodes();
  } catch (e) {
    showBarcodeError('שגיאת רשת');
  }
}

async function deleteBarcode(id, barcode) {
  if (!confirm(`למחוק את הברקוד ${barcode}?`)) return;
  try {
    const resp = await fetch(API + `/api/barcodes/${id}`, { method: 'DELETE' });
    if (!resp.ok) { showToast('שגיאה במחיקה', 'error'); return; }
    showToast(`ברקוד ${barcode} נמחק`, 'success');
    await loadManageBarcodes();
  } catch {
    showToast('שגיאת רשת', 'error');
  }
}

async function handleExcelImport(input) {
  const file = input.files[0];
  if (!file) return;
  const label = document.getElementById('excelLabel');
  label.innerHTML = `<span class="text-gray-500 text-sm">טוען...</span>`;

  const formData = new FormData();
  formData.append('file', file);

  try {
    const resp = await fetch(API + '/api/barcodes/import', {
      method: 'POST',
      body: formData,
    });
    const d = await resp.json();
    if (!resp.ok) {
      showToast(d.detail || 'שגיאה בטעינת הקובץ', 'error');
    } else {
      const noNameCount = d.no_name ? d.no_name.length : 0;
      // Primary toast: how many loaded + how many names auto-filled
      showToast(`יובאו ${d.imported} ברקודים - שמות נמצאו אוטומטית ל-${d.names_found} מוצרים`, 'success');
      // Secondary toast if some names are missing
      if (noNameCount > 0) {
        setTimeout(() => {
          showToast(`${noNameCount} ברקודים ימולאו אוטומטית בריצת pipeline הבאה`, 'info');
        }, 800);
      }
      await loadManageBarcodes();
    }
  } catch (e) {
    showToast('שגיאת רשת בטעינת הקובץ', 'error');
  } finally {
    // Reset file input + label
    input.value = '';
    label.innerHTML = `<svg class="w-4 h-4 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/>
    </svg>
    טעינה מ-Excel
    <input type="file" accept=".xlsx,.xls" class="hidden" id="excelFileInput" onchange="handleExcelImport(this)">`;
  }
}

// ── Formats ───────────────────────────────────────────────────
async function loadManageFormats() {
  try {
    const resp = await fetch(API + '/api/formats');
    const formats = await resp.json();
    renderFormatsTable(formats);
  } catch (e) {
    console.error('manage formats error', e);
    showToast('שגיאה בטעינת פורמטים', 'error');
  }
}

function _toggleBtnHtml(fmtName, active) {
  // Absolute-positioned knob is RTL-safe (translate-x goes physical right regardless of dir)
  const bg   = active ? 'bg-tempo' : 'bg-gray-300';
  const knob = active ? 'left-6' : 'left-1';
  return `<button onclick="toggleFormat('${esc(fmtName)}', this)"
                  data-active="${active}"
                  class="relative inline-block h-6 w-11 rounded-full transition-colors focus:outline-none ${bg}">
            <span class="absolute top-1 w-4 h-4 bg-white rounded-full shadow transition-all ${knob}"></span>
          </button>`;
}

function renderFormatsTable(formats) {
  const tbody = document.getElementById('formatsTableBody');
  if (!tbody) return;
  const sorted = [...formats].sort((a, b) => (b.store_count || 0) - (a.store_count || 0));
  tbody.innerHTML = sorted.length
    ? sorted.map(f => `
        <tr class="border-t border-gray-100 hover:bg-gray-50">
          <td class="px-4 py-3 text-sm font-medium text-gray-800">${esc(f.name)}</td>
          <td class="px-4 py-3 text-center text-sm text-gray-600">${f.store_count || '-'}</td>
          <td class="px-4 py-3 text-center">${_toggleBtnHtml(f.name, f.active)}</td>
        </tr>`)
      .join('')
    : '<tr><td colspan="3" class="text-center py-8 text-gray-400">אין פורמטים ידועים - הרץ pipeline ראשון</td></tr>';
}

async function toggleFormat(fmtName, btn) {
  try {
    const resp = await fetch(API + `/api/formats/${encodeURIComponent(fmtName)}/toggle`, {
      method: 'PUT',
    });
    const d = await resp.json();
    if (!resp.ok) { showToast('שגיאה', 'error'); return; }
    // Update button in-place
    btn.dataset.active = String(d.active);
    btn.className = `relative inline-block h-6 w-11 rounded-full transition-colors focus:outline-none ${d.active ? 'bg-tempo' : 'bg-gray-300'}`;
    btn.querySelector('span').className = `absolute top-1 w-4 h-4 bg-white rounded-full shadow transition-all ${d.active ? 'left-6' : 'left-1'}`;
    showToast(`${fmtName} ${d.active ? 'הופעל' : 'הושבת'}`, 'success');
  } catch {
    showToast('שגיאת רשת', 'error');
  }
}

// ── Init ──────────────────────────────────────────────────────
(async () => {
  await Promise.all([loadStatus(), loadActionQueue()]);
})();
