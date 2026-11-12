/*
 * captain-mode.js — localhost decoration layer for docs/app.htm.
 *
 * The public deploy of this script is structurally inert: it tries to reach
 * http://127.0.0.1:8000/health on load. If the local roster_api.py is not
 * running (the case on precincts.info), the script exits silently and the
 * page behaves exactly as it does today. No banner, no click handlers, no
 * leak surface.
 *
 * If the backend IS reachable (a self-host captain running the OSS stack):
 *
 *   1. Inject a top banner: "Self-hosted — real voter data".
 *   2. Watch #center-pane for chart elements. When the precinct view renders,
 *      attach click handlers to the cohort ribbon, the doughnut, and the
 *      generation chart.
 *   3. Each click opens a slide-in roster panel filtered to that cohort or
 *      generation. Single-facet (replace), not composing — by design.
 *   4. The panel reshapes itself responsively:
 *        - Desktop (>=881px): side-pane, dense table-style rows.
 *        - Mobile  (<=880px): full-screen overlay, accordion per voter.
 *      Both presentations carry from the original captain prototypes.
 *
 * Endpoints used (served by serve/roster_api.py):
 *   GET /health
 *   GET /roster?level=precinct&id=<name>&county=<2-digit>&cohort=<SLUG>
 *   GET /roster?level=precinct&id=<name>&county=<2-digit>&generation=<bucket>
 *   GET /export?... &format=csv|xlsx
 */
(function () {
  'use strict';

  // The host page is hardcoded on this constant. If a captain ever runs the
  // backend on a non-default port they can override via ?captainApi=http://...
  const DEFAULT_API = 'http://127.0.0.1:8000';
  const API = new URLSearchParams(location.search).get('captainApi') || DEFAULT_API;

  // Must match COHORT_SPEC in serve/roster_api.py AND COHORT_LABELS in v2.js;
  // tools/tests/test_cohort_lean.py pins the slug order to COHORT_SLICES.
  // Order is the partisan-spectrum left-to-right that the ribbon segments use,
  // so .hero-ribbon-seg index N maps to COHORTS[N] directly.
  const COHORTS = [
    { slug: 'PURE_R',         label: 'Solid Republican',  color: '#ef4444' },
    { slug: 'UNC_LAPSED_R',   label: 'Lapsed Republican', color: '#fca5a5' },
    { slug: 'MIXED_ACTIVE',   label: 'Mixed — Active',    color: '#f59e0b' },
    { slug: 'MIXED_LAPSED',   label: 'Mixed — Lapsed',    color: '#a78bfa' },
    { slug: 'MINOR_PARTY',    label: 'Minor Party',       color: '#14b8a6' },
    { slug: 'UNC_NONPARTISAN', label: 'Issues Only (No Party Ballot)', color: '#c4b5cd' },
    { slug: 'UNC_NO_PRIMARY', label: 'No Primary History', color: '#9ca3af' },
    { slug: 'UNC_LAPSED_D',   label: 'Lapsed Democrat',   color: '#93c5fd' },
    { slug: 'PURE_D',         label: 'Solid Democrat',    color: '#3b82f6' },
  ];

  // The pipeline pre-computes a `Generation` column with these exact strings.
  // Don't change these without also updating pipeline/voter_data_cleaner.py —
  // the JS click handler matches on label, and the API filter matches on value.
  const GEN_COLORS = {
    'Silent/Greatest': '#94a3b8',
    'Baby Boomers':    '#a78bfa',
    'Gen X':           '#60a5fa',
    'Millennials':     '#34d399',
    'Gen Z':           '#f59e0b',
  };

  let mounted = false;
  let panel = null;
  let currentScope = null;   // { county, precinct, precinctSlug }
  let currentFilter = null;  // { kind: 'cohort'|'generation', value, label, color }
  let pageOffset = 0;
  const PAGE_SIZE = 100;

  // Session-1 additions for captain notes + walk lists.
  let captain = null;        // { id, display_name, email, phone, ... } once set
  let walkList = null;       // { id, filter_kind, filter_value, ... } for the open panel
  let walkStatuses = {};     // sos_voterid -> 'queued'|'done'|'skip' for the open list
  let touchesByVoter = {};   // sos_voterid -> [touch, ...] for the open list
  const TOUCH_KINDS = [
    ['visit',         'Visited at door'],
    ['literature',    'Dropped literature'],
    ['phone',         'Phone call'],
    ['text',          'Text message'],
    ['no_answer',     'No answer'],
    ['refused',       'Refused / not interested'],
    ['moved',         'Voter has moved'],
    ['wrong_address', 'Wrong address'],
  ];
  const OUTCOMES = [
    ['yes',   'Yes',   '#16a34a'],
    ['maybe', 'Maybe', '#f59e0b'],
    ['no',    'No',    '#dc2626'],
  ];

  // ─── boot ────────────────────────────────────────────────────────────────
  async function boot() {
    // Only attempt localhost connection if we are on localhost, file://, or explicitly requested.
    // This prevents Android Chrome from showing a "wants to access local devices" prompt.
    const isLocal = location.hostname === 'localhost' || location.hostname === '127.0.0.1' || location.protocol === 'file:';
    const isExplicit = new URLSearchParams(location.search).has('captainApi');
    if (!isLocal && !isExplicit) return;

    let alive = false;
    let cacheReady = false;
    try {
      const r = await fetch(API + '/health', { mode: 'cors' });
      alive = r.ok;
      if (alive) {
        const h = await r.json();
        cacheReady = !!h.cache_exists;
      }
    } catch (_) { /* unreachable -> public mode */ }
    if (!alive) return;
    if (!cacheReady) {
      // API is running but the enriched parquet is missing. Show a clear
      // warning instead of activating click handlers that would 500.
      injectAssets();
      mountWarningBanner();
      return;
    }
    injectAssets();
    document.body.classList.add('captain-mode');
    mountBanner();
    mountPanel();
    observeCenterPane();
    syncScopeFromUrl();
    // Captain identity: fetch /captain/me. If null, the picker shows on the
    // first chart click (no point blocking the page until they actually need
    // to write something).
    fetchCaptain();
    window.addEventListener('popstate', syncScopeFromUrl);
    // v2.js mutates location via history.replaceState (no popstate). Poll.
    let lastSearch = location.search;
    setInterval(() => {
      if (location.search !== lastSearch) {
        lastSearch = location.search;
        syncScopeFromUrl();
      }
    }, 300);
    mounted = true;
  }

  function injectAssets() {
    if (document.getElementById('captain-mode-css')) return;
    const link = document.createElement('link');
    link.id = 'captain-mode-css';
    link.rel = 'stylesheet';
    link.href = 'captain/captain-mode.css';
    document.head.appendChild(link);
  }

  function mountBanner() {
    const b = document.createElement('div');
    b.className = 'captain-banner';
    b.innerHTML =
      '<span class="dot"></span>' +
      '<span><b>Self-hosted</b> &middot; real voter data active</span>' +
      '<span class="grow"></span>' +
      '<span class="hint">Click any cohort or generation bar to generate a roster</span>';
    document.body.appendChild(b);
  }

  function mountWarningBanner() {
    const b = document.createElement('div');
    b.className = 'captain-banner captain-banner-warn';
    b.innerHTML =
      '<span class="dot"></span>' +
      '<span><b>Captain mode unavailable</b> &middot; enriched parquet missing</span>' +
      '<span class="grow"></span>' +
      '<span class="hint">Run <code>python pipeline/ohio_voter_pipeline.py</code> (option 1), then restart the API.</span>';
    document.body.appendChild(b);
  }

  function mountPanel() {
    panel = document.createElement('aside');
    panel.className = 'captain-roster-panel';
    panel.setAttribute('aria-hidden', 'true');
    panel.innerHTML =
      '<header class="captain-roster-head">' +
        '<span class="swatch"></span>' +
        '<span class="title">Roster</span>' +
        '<button class="close" type="button">Close</button>' +
      '</header>' +
      '<div class="captain-roster-meta"></div>' +
      '<div class="captain-progress"></div>' +
      '<div class="captain-roster-actions">' +
        '<button class="primary" data-action="xlsx">Export XLSX</button>' +
        '<button data-action="csv">Export CSV</button>' +
        '<button class="map" data-action="map" disabled title="Walk-list maps are planned — KML/GeoJSON routing by street order so a canvasser walks a sensible path, not an alphabetical one.">Map view</button>' +
      '</div>' +
      '<div class="captain-roster-body"></div>' +
      '<footer class="captain-roster-foot">' +
        '<span class="status"></span>' +
        '<span><button data-action="more">Load more</button></span>' +
      '</footer>';
    document.body.appendChild(panel);
    panel.querySelector('.close').addEventListener('click', closePanel);
    panel.addEventListener('click', onPanelClick);
  }

  function closePanel() {
    panel.classList.remove('is-open');
    panel.setAttribute('aria-hidden', 'true');
  }

  function onPanelClick(e) {
    // Done checkbox is an <input>, handled separately
    const cb = e.target.closest('input[data-action="toggle-done"]');
    if (cb) {
      const row = cb.closest('.captain-row');
      const sos = row && row.dataset.sos;
      if (sos) toggleDone(row, sos, cb.checked);
      return;
    }
    const btn = e.target.closest('button[data-action]');
    if (btn) {
      const action = btn.dataset.action;
      if (action === 'csv' || action === 'xlsx') return doExport(action);
      if (action === 'more')      { pageOffset += PAGE_SIZE; return loadRoster({ append: true }); }
      if (action === 'log-visit') return openVisitForm(btn.closest('.captain-row'));
      if (action === 'submit-visit') return submitVisitForm(btn.closest('.captain-row'));
      if (action === 'cancel-visit') return closeVisitForm(btn.closest('.captain-row'));
      return;
    }
    // No actionable target: mobile accordion behavior.
    const row = e.target.closest('.captain-row');
    if (row) toggleAccordion(row);
  }

  async function toggleDone(row, sos, checked) {
    if (!walkList) return;
    const status = checked ? 'done' : 'queued';
    try {
      await fetch(API + '/walk-list/' + walkList.id + '/voter/' + encodeURIComponent(sos) + '/status', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status }),
      });
      walkStatuses[sos] = status;
      row.classList.toggle('is-done', checked);
      refreshProgress();
    } catch (_) { /* leave checkbox state — the next render will reconcile */ }
  }

  function openVisitForm(row) {
    if (!row) return;
    const slot = row.querySelector('.captain-visit-form');
    if (!slot) return;
    if (!slot.hidden) { slot.hidden = true; return; }
    slot.innerHTML =
      '<div class="vf-row"><label>What happened?' +
        '<select name="kind">' +
          TOUCH_KINDS.map(([k, label]) => '<option value="' + esc(k) + '">' + esc(label) + '</option>').join('') +
        '</select></label></div>' +
      '<div class="vf-row"><span class="vf-label">Outcome</span>' +
        '<div class="vf-outcomes">' +
          OUTCOMES.map(([v, label, color]) =>
            '<label class="out-pill" style="--c:' + color + '"><input type="radio" name="outcome" value="' + esc(v) + '"><span>' + esc(label) + '</span></label>'
          ).join('') +
          '<label class="out-pill out-skip"><input type="radio" name="outcome" value=""><span>Skip</span></label>' +
        '</div>' +
      '</div>' +
      '<div class="vf-row"><label>Notes (optional)<textarea name="notes" rows="2" placeholder="Cares about schools…"></textarea></label></div>' +
      '<div class="vf-actions">' +
        '<button type="button" class="primary" data-action="submit-visit">Log it</button>' +
        '<button type="button" data-action="cancel-visit">Cancel</button>' +
      '</div>';
    slot.hidden = false;
  }

  function closeVisitForm(row) {
    if (!row) return;
    const slot = row.querySelector('.captain-visit-form');
    if (slot) { slot.hidden = true; slot.innerHTML = ''; }
  }

  async function submitVisitForm(row) {
    if (!row || !walkList || !captain) return;
    const sos = row.dataset.sos;
    const slot = row.querySelector('.captain-visit-form');
    const kind = slot.querySelector('select[name="kind"]').value;
    const outcomeInput = slot.querySelector('input[name="outcome"]:checked');
    const outcome = outcomeInput ? outcomeInput.value : '';
    const notes = slot.querySelector('textarea[name="notes"]').value.trim();
    const body = {
      sos_voterid: sos,
      captain_id: captain.id,
      precinct_county: currentScope.county || '',
      precinct_name: currentScope.precinct || '',
      kind,
      outcome: outcome || null,
      notes: notes || null,
    };
    try {
      const r = await fetch(API + '/touches', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      if (!r.ok || !data.touch) { alert(data.error || 'Could not save touch.'); return; }
      (touchesByVoter[sos] = touchesByVoter[sos] || []).unshift(data.touch);
      closeVisitForm(row);
      // Re-render this row's touch list inline without redrawing the whole
      // panel. Build the new list via the shared helper, drop the existing
      // node, and parse the fragment with DOMParser so we can append a real
      // Element (avoids touching innerHTML/insertAdjacentHTML on this path).
      const oldList = row.querySelector('.captain-touches');
      if (oldList) oldList.remove();
      const fragment = new DOMParser()
        .parseFromString(buildTouchesHtml(touchesByVoter[sos]), 'text/html')
        .body.firstChild;
      if (fragment) slot.parentNode.insertBefore(fragment, slot);
      refreshProgress();
    } catch (e) { alert('Network error: ' + e); }
  }

  function toggleAccordion(row) {
    // One open at a time (mobile-only effect — desktop CSS ignores .is-expanded).
    const all = panel.querySelectorAll('.captain-row.is-expanded');
    const wasOpen = row.classList.contains('is-expanded');
    all.forEach(r => r.classList.remove('is-expanded'));
    if (!wasOpen) row.classList.add('is-expanded');
  }

  // ─── URL → scope ─────────────────────────────────────────────────────────
  // v2.js URL shape (see docs/app.htm + v2.js parseSlot): ?level=precinct&id=
  // The precinct id is the slugified precinct name. We need (a) county number
  // and (b) the un-slugified precinct name to query the roster API.
  // The displayed precinct name appears in the page's breadcrumb after render.
  function syncScopeFromUrl() {
    const p = new URLSearchParams(location.search);
    if (p.get('level') !== 'precinct') {
      currentScope = null;
      return;
    }
    // v2.js writeState() uses ?level=precinct&id=<precinct_slug>&county=<county_slug>
    // (see writeState calls around v2.js:754 / :827).
    const precinctSlug = p.get('id') || '';
    const countySlug = p.get('county') || '';
    currentScope = {
      countySlug,
      precinctSlug,
      // Display name: read from the hierarchy row v2.js renders. That row's
      // data-precinct-name attribute holds the exact parquet name (e.g.
      // "KETTERING 1-A") including any hyphens, apostrophes, or slashes that
      // the slug erases. Fall back to a best-effort slug-to-display if the row
      // is not in the DOM yet (race on cold-load); the API is slug-agnostic so
      // roster queries work regardless of what's in `precinct`.
      precinct: precinctDisplayFromSlug(precinctSlug),
      county: null,     // 2-digit number resolved via manifest alphabetical index
      countyName: null, // human-readable name (e.g. "Montgomery") for UI display
    };
    resolveCountyNumber();
  }

  function precinctDisplayFromSlug(slug) {
    if (!slug) return '';
    const row = document.querySelector(
      '.hier-row[data-precinct="' + cssEscape(slug) + '"]'
    );
    const name = row && row.getAttribute('data-precinct-name');
    if (name) return name;
    // Fallback: slug-to-display is lossy (collapses hyphens/spaces) but better
    // than showing nothing while the hierarchy is still rendering.
    return String(slug).replace(/_/g, ' ').toUpperCase();
  }

  function cssEscape(s) {
    return String(s).replace(/["\\]/g, '\\$&');
  }

  // The roster API needs COUNTY_NUMBER (2-digit). The dashboard knows the
  // county slug from the URL; the manifest maps slugs to numbers. We scrape
  // it from v2.js's already-cached manifest if available, or from the
  // precinct's chart data filename pattern (data/<county_slug>_precinct_...).
  // For prototype simplicity: ask the API itself to resolve by precinct name.
  // (Future: expose v2.js's manifest as window._captainManifest.)
  async function resolveCountyNumber() {
    if (!currentScope) return;
    // The county_slug is in the URL when present; map to a 2-digit number
    // by reading the manifest exposed at docs/data/_manifest.json.
    if (!currentScope.countySlug) {
      // Look at active jurisdiction row in left pane — its data attribute carries the county.
      const row = document.querySelector('.hierarchy .is-active[data-county]');
      if (row) currentScope.countySlug = row.dataset.county;
    }
    if (currentScope.countySlug && !currentScope.county) {
      try {
        const m = await fetchManifest();
        const list = m.counties || m.processedCounties || [];
        // List is alphabetical names like ["Adams","Allen",...]; the parquet's
        // COUNTY_NUMBER is the 1-based alphabetical position, zero-padded.
        const idx = list.findIndex(c =>
          countyToSlug(typeof c === 'string' ? c : (c.name || '')) === currentScope.countySlug
        );
        if (idx >= 0) {
          currentScope.county = countyIndexToNumber(idx);
          const entry = list[idx];
          currentScope.countyName = typeof entry === 'string' ? entry : (entry.name || '');
        }
      } catch (_) { /* manifest unavailable — fall back when user clicks */ }
    }
  }

  let _manifestCache = null;
  async function fetchManifest() {
    if (_manifestCache) return _manifestCache;
    const r = await fetch('manifest.json');
    _manifestCache = await r.json();
    return _manifestCache;
  }

  function countyToSlug(name) {
    return String(name).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
  }

  // Ohio county numbers are 1..88 in alphabetical order (Adams=01,
  // Wyandot=88). The dashboard's manifest.counties array is sorted
  // alphabetically, so a county's number IS its 1-based index. Verified
  // against the enriched parquet: Montgomery is the 57th county in the
  // list, and the parquet's COUNTY_NUMBER for Kettering 1-A is "57".
  function countyIndexToNumber(idx) {
    return String(idx + 1).padStart(2, '0');
  }

  // ─── captain identity + picker ───────────────────────────────────────────
  async function fetchCaptain() {
    try {
      const r = await fetch(API + '/captain/me');
      const data = await r.json();
      captain = data.captain || null;
    } catch (_) { captain = null; }
  }

  // Returns true once a captain exists (after picker submit if needed).
  async function ensureCaptain() {
    if (captain) return true;
    return new Promise(resolve => showPickerModal(ok => resolve(ok)));
  }

  function showPickerModal(done) {
    const overlay = document.createElement('div');
    overlay.className = 'captain-picker-overlay';
    // Pre-fill precinct from the current scope so the captain doesn't retype it.
    const pCounty = (currentScope && currentScope.county) || '';
    const pCountyName = (currentScope && currentScope.countyName) || '';
    const pName = (currentScope && currentScope.precinct) || '';
    // County name displayed read-only; the 2-digit number is what the API needs,
    // carried in a hidden input so form serialization at submit time is unchanged.
    const countyRow = pCountyName
      ? '<div class="captain-readonly-row"><span class="rl-label">County</span><span class="rl-value">' + esc(pCountyName) + '</span></div>' +
        '<input type="hidden" name="precinct_county" value="' + esc(pCounty) + '">'
      : '<label>County number<input name="precinct_county" required value="' + esc(pCounty) + '" placeholder="e.g. 57"></label>';
    overlay.innerHTML =
      '<div class="captain-picker">' +
        '<h2>Set up your captain account</h2>' +
        '<p class="sub">Used to attribute your notes and identify you as your precinct\'s point of contact.</p>' +
        '<form>' +
          '<label>Your name<input name="display_name" required autocomplete="name"></label>' +
          '<label>Email<input name="email" type="email" required autocomplete="email"></label>' +
          '<label>Phone<input name="phone" type="tel" inputmode="tel" required autocomplete="tel"></label>' +
          '<label>Precinct<input name="precinct_name" required value="' + esc(pName) + '"></label>' +
          countyRow +
          '<div class="captain-picker-actions">' +
            '<button type="submit" class="primary">Save</button>' +
            '<button type="button" class="cancel">Cancel</button>' +
          '</div>' +
          '<p class="privacy">This information stays on your machine in <code>local/captain.db</code>. It is not sent anywhere.</p>' +
        '</form>' +
      '</div>';
    document.body.appendChild(overlay);
    const form = overlay.querySelector('form');
    overlay.querySelector('.cancel').addEventListener('click', () => {
      overlay.remove(); done(false);
    });
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(form);
      const body = {};
      ['display_name','email','phone','precinct_name','precinct_county'].forEach(k => {
        body[k] = (fd.get(k) || '').toString().trim();
      });
      try {
        const r = await fetch(API + '/captain', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const data = await r.json();
        if (!r.ok || !data.captain) {
          alert(data.error || 'Could not save captain.');
          return;
        }
        captain = data.captain;
        overlay.remove();
        done(true);
      } catch (e) {
        alert('Network error: ' + e);
      }
    });
    setTimeout(() => form.querySelector('input[name="display_name"]').focus(), 50);
  }

  // ─── walk-list lifecycle ─────────────────────────────────────────────────
  async function ensureWalkList() {
    // Find-or-create the walk list for (captain, scope, filter). The backend
    // is idempotent on (captain_id, precinct, filter_kind, filter_value), so
    // re-opening the same filter resumes the same list with preserved statuses.
    if (!captain || !currentScope || !currentFilter) return null;
    const body = {
      captain_id: captain.id,
      precinct_county: currentScope.county || '',
      precinct_name: currentScope.precinct || '',
      filter_label: currentFilter.label,
    };
    if (currentFilter.kind === 'cohort')      body.cohort = currentFilter.value;
    else                                       body.generation = currentFilter.value;
    try {
      const r = await fetch(API + '/walk-list', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      if (!r.ok || !data.walk_list) return null;
      walkList = data.walk_list;
      const [statuses, touches] = await Promise.all([
        fetch(API + '/walk-list/' + walkList.id + '/statuses').then(r => r.json()),
        fetch(API + '/walk-list/' + walkList.id + '/touches').then(r => r.json()),
      ]);
      walkStatuses = statuses.statuses || {};
      touchesByVoter = touches.touches || {};
      return walkList;
    } catch (_) {
      return null;
    }
  }

  async function refreshProgress() {
    if (!walkList) return;
    try {
      const r = await fetch(API + '/walk-list/' + walkList.id + '/progress');
      const p = await r.json();
      const el = panel.querySelector('.captain-progress');
      if (!el) return;
      const last = p.last_touched_at ? ' &middot; last touched ' + esc(p.last_touched_at.slice(11, 16)) : '';
      el.innerHTML = '<b>' + p.done + '</b> of <b>' + p.total + '</b> done' +
        (p.skip ? ' &middot; ' + p.skip + ' skipped' : '') + last;
    } catch (_) { /* non-fatal */ }
  }

  // ─── chart decoration ────────────────────────────────────────────────────
  function observeCenterPane() {
    const cp = document.getElementById('center-pane');
    if (!cp) return;
    const decorate = () => {
      decorateRibbon(cp);
      decorateChartCards(cp);
    };
    decorate();
    new MutationObserver(decorate).observe(cp, { childList: true, subtree: true });
  }

  function decorateRibbon(root) {
    const segs = root.querySelectorAll('.hero-ribbon-seg');
    segs.forEach((seg, i) => {
      if (seg.dataset.captainBound) return;
      seg.dataset.captainBound = '1';
      seg.addEventListener('click', () => openFilter({
        kind: 'cohort', value: COHORTS[i].slug,
        label: COHORTS[i].label, color: COHORTS[i].color,
      }));
    });
  }

  // For Chart.js canvases, click → cohort/generation derived from the segment
  // index. We hook by reading the existing Chart instance attached to the
  // canvas (Chart.js exposes Chart.getChart(canvas)).
  function decorateChartCards(root) {
    const cards = root.querySelectorAll('.chart-card[data-chart-id]');
    cards.forEach(card => {
      if (card.dataset.captainBound) return;
      const canvas = card.querySelector('canvas');
      if (!canvas) return;
      const id = card.dataset.chartId;
      const handler = chartIdToFacet(id);
      if (!handler) return;
      card.dataset.captainBound = '1';
      canvas.addEventListener('click', (e) => {
        const Chart = window.Chart;
        if (!Chart) return;
        const chart = Chart.getChart(canvas);
        if (!chart) return;
        const pts = chart.getElementsAtEventForMode(e, 'nearest', { intersect: true }, true);
        if (!pts.length) return;
        const idx = pts[0].index;
        const labels = chart.data.labels || [];
        const label = String(labels[idx] || '').split(' — ')[0];
        const filter = handler(idx, label);
        if (filter) openFilter(filter);
      });
    });
  }

  function chartIdToFacet(id) {
    if (id === 'chart-party') {
      return (idx, _label) => COHORTS[idx] ? {
        kind: 'cohort', value: COHORTS[idx].slug,
        label: COHORTS[idx].label, color: COHORTS[idx].color,
      } : null;
    }
    if (id === 'chart-gen') {
      return (_idx, label) => label ? {
        kind: 'generation', value: label, label: label + ' generation',
        color: GEN_COLORS[label] || '#64748b',
      } : null;
    }
    // chart-party-gen / chart-party-decade compose two facets, but we ship
    // single-facet only for this iteration. Treat them as cohort-only by
    // reading the dataset label (Pure R / Pure D etc.).
    if (id === 'chart-party-gen' || id === 'chart-party-decade') {
      return (_idx, label) => {
        const c = COHORTS.find(x => x.label.toLowerCase().includes(label.toLowerCase())
          || label.toLowerCase().includes(x.label.toLowerCase()));
        return c ? { kind: 'cohort', value: c.slug, label: c.label, color: c.color } : null;
      };
    }
    return null;
  }

  // ─── city roster (export-only) ────────────────────────
  // City views do not generate an on-screen roster or notes (precinct-scoped by
  // design). A cohort/generation click downloads that cohort for the whole city
  // as CSV. The /export endpoint aggregates across all counties the city spans
  // via the CITY-column slug match, so a cross-county city (e.g. Kettering in
  // Greene + Montgomery) exports as one list. No county param is sent.
  function exportCityRoster(filter, citySlug) {
    if (!citySlug) return;
    // The CITY column stores "<NAME> CITY" (e.g. "KETTERING CITY"), which the
    // API slugifies to "kettering_city". The URL carries the bare slug
    // ("kettering"), so append "_city" to match — same suffix convention as the
    // precomputed data/city/ files. Idempotent if the slug already has it.
    const fs = citySlug.endsWith('_city') ? citySlug : `${citySlug}_city`;
    const params = { level: 'city', id: fs, format: 'csv' };
    if (filter.kind === 'cohort') params.cohort = filter.value;
    else params.generation = filter.value;
    const url = buildUrl('/export', params);
    // Stream the attachment via a transient <a download>; setting location on a
    // single-page app can interrupt page state, so we use an anchor instead.
    const a = document.createElement('a');
    a.href = url.toString();
    a.download = '';
    document.body.appendChild(a);
    a.click();
    a.remove();
  }

  // ─── roster loading ──────────────────────────────────────────────────────
  async function openFilter(filter) {
    // City-level views are an export-only surface: the roster/notes UI is
    // precinct-scoped by design (one precinct, one captain). At the city level
    // a cohort click downloads the cohort across the whole city instead of
    // opening the notes panel. The backend supports level=city on /export.
    const _p = new URLSearchParams(location.search);
    if (_p.get('level') === 'city') {
      exportCityRoster(filter, _p.get('id') || '');
      return;
    }
    if (!currentScope) {
      alert('Pick a precinct from the hierarchy first.');
      return;
    }
    // First click ever: gate on the picker so we can attribute notes.
    const haveCaptain = await ensureCaptain();
    if (!haveCaptain) return;
    currentFilter = filter;
    pageOffset = 0;
    walkList = null;
    walkStatuses = {};
    touchesByVoter = {};
    panel.classList.add('is-open');
    panel.setAttribute('aria-hidden', 'false');
    panel.querySelector('.swatch').style.background = filter.color;
    panel.querySelector('.title').textContent = filter.label + ' — ' + (currentScope.precinct || '');
    panel.querySelector('.captain-roster-meta').innerHTML =
      '<span><b>Precinct:</b> ' + esc(currentScope.precinct || '?') + '</span>' +
      '<span><b>County:</b> ' + esc(currentScope.county || '?') + '</span>' +
      '<span><b>Filter:</b> ' + esc(filter.label) + '</span>' +
      '<span><b>Captain:</b> ' + esc(captain.display_name) + '</span>';
    panel.querySelector('.captain-progress').innerHTML = 'Loading walk list…';
    // Create the walk list first so seeds + statuses + touches are ready when
    // the roster body renders. Roster load can run in parallel with the
    // progress refresh.
    ensureWalkList().then(() => { refreshProgress(); loadRoster({ append: false }); });
  }

  async function loadRoster({ append }) {
    const body = panel.querySelector('.captain-roster-body');
    if (!append) {
      body.innerHTML = '<div class="loading">Loading roster…</div>';
    }
    if (!currentScope.county) {
      // Last-resort lookup if we haven't resolved the 2-digit number yet.
      await resolveCountyNumber();
    }
    const url = buildUrl('/roster', {
      level: 'precinct',
      // Send the URL slug, not the display name. The API normalizes both sides
      // ("kettering_1_a" vs "KETTERING 1-A") so punctuation is irrelevant.
      id: currentScope.precinctSlug,
      county: currentScope.county || '',
      limit: PAGE_SIZE,
      offset: pageOffset,
    });
    if (currentFilter.kind === 'cohort') {
      url.searchParams.set('cohort', currentFilter.value);
    } else {
      url.searchParams.set('generation', currentFilter.value);
    }
    try {
      const r = await fetch(url);
      const data = await r.json();
      if (!r.ok || data.error) {
        body.innerHTML = '<div class="error">' + esc(data.error || ('HTTP ' + r.status)) + '</div>';
        return;
      }
      renderRoster(data, append);
    } catch (e) {
      body.innerHTML = '<div class="error">Request failed: ' + esc(String(e)) + '</div>';
    }
  }

  function renderRoster(data, append) {
    const body = panel.querySelector('.captain-roster-body');
    const head =
      '<div class="captain-table-head">' +
        '<div>Name</div><div>Address</div><div>Last Primary</div><div>Last General</div>' +
      '</div>';
    const rowsHtml = (data.rows || []).map(r => {
      const sos = r.sos_voterid || '';
      const j = r.jurisdictions || {};
      const jurChips =
        '<div class="juris">' +
          chip('Precinct', j.precinct_name) +
          chip('Ward', j.ward) +
          chip('Township', j.township) +
          chip('Village', j.village) +
          chip('US House', j.congressional_district) +
          chip('State Senate', j.state_senate_district) +
          chip('State House', j.state_representative_district) +
          chip('School', j.local_school_district) +
        '</div>';
      const status = walkStatuses[sos] || 'queued';
      const isDone = status === 'done';
      const touchHtml = buildTouchesHtml(touchesByVoter[sos]);
      return (
        '<div class="captain-row' + (isDone ? ' is-done' : '') + '" data-sos="' + esc(sos) + '">' +
          '<div class="name">' + esc((r.last || '') + ', ' + (r.first || '')) + '</div>' +
          '<div class="addr">' +
            esc(r.address || '') +
            '<span class="city">' + esc((r.city || '') + (r.zip ? ' ' + r.zip : '')) + '</span>' +
          '</div>' +
          '<div class="last-primary">' + esc(r.last_primary || '—') + '</div>' +
          '<div class="last-general">' + esc(r.last_general || '—') + '</div>' +
          '<div class="accordion-toggle">Tap to see jurisdictions</div>' +
          '<div class="accordion-content"><div class="inner">' + jurChips + '</div></div>' +
          '<div class="row-actions">' +
            '<button type="button" class="log-visit" data-action="log-visit">Log visit</button>' +
            '<label class="done-toggle"><input type="checkbox" data-action="toggle-done"' + (isDone ? ' checked' : '') + '> Done</label>' +
          '</div>' +
          touchHtml +
          '<div class="captain-visit-form" hidden></div>' +
        '</div>'
      );
    }).join('');

    if (append) {
      // Append rows, leave head + footer alone.
      const tmp = document.createElement('div');
      tmp.innerHTML = rowsHtml;
      while (tmp.firstChild) body.appendChild(tmp.firstChild);
    } else {
      body.innerHTML = head + (data.total === 0
        ? '<div class="empty">No voters match this filter.</div>'
        : rowsHtml);
    }
    const shown = Math.min(pageOffset + (data.rows || []).length, data.total);
    panel.querySelector('.captain-roster-foot .status').textContent =
      'Showing ' + shown.toLocaleString() + ' of ' + (data.total || 0).toLocaleString();
    const more = panel.querySelector('button[data-action="more"]');
    more.disabled = shown >= data.total;
  }

  function chip(label, val) {
    if (val == null || val === '') return '';
    return '<span><b>' + esc(label) + '</b>' + esc(String(val)) + '</span>';
  }

  // Single source of truth for the per-voter touch list markup. Both initial
  // render and post-submit re-render call into this so the template can only
  // change in one place. Every interpolated value flows through esc().
  function buildTouchesHtml(touches) {
    if (!touches || !touches.length) return '';
    const rows = touches.map(t => {
      const when = (t.touched_at || '').slice(0, 16);
      const out = t.outcome
        ? ' <span class="outcome out-' + esc(t.outcome) + '">' + esc(t.outcome) + '</span>'
        : '';
      const note = t.notes ? ' &middot; ' + esc(t.notes) : '';
      return '<div class="t-row">' + esc(when) + ' &middot; ' + esc(t.kind) + out + note + '</div>';
    }).join('');
    return '<div class="captain-touches">' + rows + '</div>';
  }

  // ─── exports ─────────────────────────────────────────────────────────────
  function doExport(fmt) {
    if (!currentFilter || !currentScope) return;
    const url = buildUrl('/export', {
      level: 'precinct',
      id: currentScope.precinctSlug,
      county: currentScope.county || '',
      format: fmt,
    });
    if (currentFilter.kind === 'cohort') {
      url.searchParams.set('cohort', currentFilter.value);
    } else {
      url.searchParams.set('generation', currentFilter.value);
    }
    window.location.href = url.toString();
  }

  function buildUrl(path, params) {
    const u = new URL(API + path);
    Object.entries(params).forEach(([k, v]) => {
      if (v != null && v !== '') u.searchParams.set(k, v);
    });
    return u;
  }

  // XSS escape — every server-supplied value that lands in innerHTML MUST
  // flow through this. Trust boundary: the SWVF parquet may contain arbitrary
  // text in name/address fields (it's government-typed data, not validated).
  // Covers element-text and quoted-attribute contexts, which is all we use.
  // Do NOT interpolate raw values into <script>, href="javascript:...", or
  // unquoted attributes anywhere downstream.
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
