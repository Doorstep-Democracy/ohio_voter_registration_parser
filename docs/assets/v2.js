/* ============================================================
   Precincts.info V2 — Main app
   - URL state with GA-style event emission
   - Data fetching (manifest + per-jurisdiction chartConfigs)
   - Hierarchy tree (Geography ↔ Districts toggle)
   - Hero: big number + cohort spark ribbon + doughnut
   - Chart cards via Chart.js (reused chartConfig from data files)
   - Hex map of 88 counties, color by party lean
   - Compare mode (two jurisdictions side-by-side)
   - Tweaks panel (style, theme, layout, density, view)
   - Chart export: JSON + PNG
   ============================================================ */
(function () {
  'use strict';

  // ── Canonical cohort colors (must match data files) ────────
  const COHORT_COLORS = ['#ef4444', '#fca5a5', '#f59e0b', '#a78bfa', '#9ca3af', '#93c5fd', '#3b82f6'];
  const COHORT_LABELS = ['Pure R', 'UNC – Lapsed R', 'Mixed – Active', 'Mixed – Lapsed', 'UNC – No Primary', 'UNC – Lapsed D', 'Pure D'];
  // Cohort label -> partisan side. Mirrors COHORT_SIDE in pipeline/cohort_lean.py;
  // tools/tests/test_cohort_lean.py pins the two (and COHORT_SLICES) together.
  const COHORT_SIDE = {
    'Pure R':                    'R',
    'UNC – Lapsed R':            'R',
    'Mixed – Active':            'UNC',
    'Mixed – Lapsed':            'UNC',
    'UNC – Non-partisan Ballot': 'UNC',
    'UNC – No Primary':          'UNC',
    'UNC – Lapsed D':            'D',
    'Pure D':                    'D',
  };
  // Party-lean buckets and the shape/projection helpers live in
  // assets/geo-map.js (window.GeoMap), shared with the landing map.
  // This file previously carried a second copy that had to be hand-synced.
  const GeoMap = window.GeoMap;
  const PARTY_LEAN_BUCKETS = GeoMap.PARTY_LEAN_BUCKETS;
  // Levels that live inside a single county's tree (as opposed to 'county'
  // itself or cross-county/statewide levels like unified 'city' + 'district').
  // 'city' is included because a single-county city is still reached by
  // drilling into its county tree, even though its data view is unified.
  const SUBCOUNTY_LEVELS = ['city', 'township', 'village', 'ward'];

  // The county shown when a URL names no jurisdiction, and the fallback any
  // code path uses when it needs *some* county to report on (there is no
  // statewide aggregate data file for all 88 -- see the select-state handler).
  //
  // This was 'hamilton', which arrived as an arbitrary placeholder in the
  // original V2 prototype import (5b2d4a89fd) and was never a data decision:
  // Hamilton is not the first county (Adams is), not the largest (Franklin
  // is), and carries nothing the others do not. Montgomery is the one county
  // with real BoE precinct geometry, so defaulting here opens on the deepest
  // view the site can render. Declared once so the default is a single edit,
  // not 13 scattered string literals.
  const DEFAULT_COUNTY = 'montgomery';

  // ── In-memory caches ───────────────────────────────────────
  const cache = { manifest: null, byUrl: {} };
  async function fetchJSON(url) {
    if (cache.byUrl[url]) return cache.byUrl[url];
    const res = await fetch(url);
    if (!res.ok) {
      emit('exception', { description: 'Failed to fetch ' + url, fatal: false });
      throw new Error('Failed: ' + url);
    }
    const data = await res.json();
    cache.byUrl[url] = data;
    return data;
  }

  // Cross-county city map: { "CITY NAME": ["county_slug", ...] }.
  // Built by the pipeline from every precinct index. Cached after first load;
  // null if absent (older deploys) so callers can fall back to single-county.
  let _cityCountyMap;
  async function loadCityCountyMap() {
    if (_cityCountyMap !== undefined) return _cityCountyMap;
    try { _cityCountyMap = await fetchJSON('data/city_county_map.json'); }
    catch (e) { _cityCountyMap = null; }
    return _cityCountyMap;
  }

  // ── URL state ──────────────────────────────────────────────
  // Old precincts.info URL params (?county=&geo=&precinct=&jurType=&jurName=)
  // get translated to the new scheme before any other code reads state, so
  // existing deep links keep working.
  function legacyShim() {
    const p = new URLSearchParams(location.search);
    // If already on new scheme, leave it alone.
    if (p.has('level') || p.has('compare')) return false;
    if (!p.has('county') && !p.has('geo') && !p.has('jurType')) return false;

    const oldCounty   = p.get('county');
    const oldGeo      = p.get('geo');
    const oldPrecinct = p.get('precinct');
    const jurType     = p.get('jurType');
    const jurName     = p.get('jurName');

    const slugify = s => String(s || '').toLowerCase()
      .replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');

    const next = new URLSearchParams();
    // Preserve unrelated query params (utm_*, etc.) for GA
    p.forEach((v, k) => {
      if (!['county','geo','precinct','jurType','jurCounty','jurName'].includes(k)) next.set(k, v);
    });

    if (jurType === 'state_senate_district' || jurType === 'state_representative_district' || jurType === 'congressional_district') {
      next.set('level', 'district');
      next.set('type', jurType);
      next.set('id', String(jurName || '01').padStart(2, '0'));
    } else if (oldGeo === 'precinct-detail' || (oldGeo === 'precinct' && oldPrecinct)) {
      next.set('level', 'precinct');
      next.set('county', slugify(oldCounty || DEFAULT_COUNTY));
      next.set('id', slugify(oldPrecinct || ''));
    } else if (oldCounty) {
      next.set('level', 'county');
      next.set('id', slugify(oldCounty));
    }
    history.replaceState(null, '', location.pathname + '?' + next.toString());
    return true;
  }

  function readState() {
    legacyShim();
    const p = new URLSearchParams(location.search);
    const compare = p.get('compare');
    return {
      style:   p.get('style')   || 'editorial',
      theme:   p.get('theme')   || 'dark',
      layout:  p.get('layout')  || '3col',
      density: p.get('density') || 'comfortable',
      view:    p.get('view')    || 'geo',
      level:   p.get('level')   || 'county',
      id:      p.get('id')      || DEFAULT_COUNTY,
      county:  p.get('county')  || null,
      district_type: p.get('type') || null,
      city:    p.get('city')    || null,
      compare: compare ? compare.split(',').slice(0, 2) : null
    };
  }
  function writeState(patch, replace) {
    const p = new URLSearchParams(location.search);
    Object.keys(patch).forEach(k => {
      const v = patch[k];
      if (v === null || v === undefined || v === '') p.delete(k);
      else p.set(k, Array.isArray(v) ? v.join(',') : String(v));
    });
    const url = location.pathname + (p.toString() ? '?' + p.toString() : '');
    if (replace) history.replaceState(null, '', url);
    else history.pushState(null, '', url);
  }
  function emit(name, params) {
    // GA stub — real site replaces with gtag('event', name, params)
    if (typeof window.gtag === 'function') window.gtag('event', name, params || {});
    console.debug('[gtag]', name, params || {});
  }

  // ── State container ────────────────────────────────────────
  const S = readState();

  // ── Compare slot encoding ──────────────────────────────────
  // A slot can be a county slug (bare) or "district:<dtype>:<id>".
  // Returns { kind, id, county?, dtype? }
  function parseSlot(raw) {
    if (!raw) return null;
    const parts = String(raw).split(':');
    if (parts.length >= 3 && parts[0] === 'district') {
      return { kind: 'district', dtype: parts[1], id: parts[2] };
    }
    if (parts.length === 2 && parts[0] === 'precinct') {
      // precinct:<countySlug>:<precinctSlug>  (separator is ':' between parts[1] and parts[2])
      // For simplicity we'll require 3 parts for precinct too:
      return { kind: 'precinct', id: parts[1] };
    }
    if (parts.length >= 3 && parts[0] === 'precinct') {
      return { kind: 'precinct', county: parts[1], id: parts[2] };
    }
    return { kind: 'county', id: raw };
  }
  function slotLabel(slot) {
    if (!slot) return '—';
    if (slot.kind === 'district') {
      const t = slot.dtype.replace('_district', '').replace(/_/g, ' ');
      return t.replace(/\b\w/g, c => c.toUpperCase()) + ' D' + slot.id;
    }
    if (slot.kind === 'precinct') return (slot.id || '').toUpperCase().replace(/_/g, ' ');
    return slugToCountyName(slot.id);
  }
  function slotIsLeanable(slot) {
    return slot && slot.kind === 'county';
  }

  // ── DOM ────────────────────────────────────────────────────
  const html = document.documentElement;
  const $ = (id) => document.getElementById(id);

  // ── XSS trust boundary ────────────────────────────────────────
  // esc() is the single escape funnel: every data-derived string (names
  // and narrative text from data/ JSON, URL params) passes through it
  // before being interpolated into innerHTML. Attribute reads via
  // getAttribute() see the decoded original, so round-tripped values
  // (slugs, display names) are unchanged.
  function esc(v) {
    return String(v == null ? '' : v)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  // On mobile the left/right panes are fixed-position overlays (see @media
  // max-width:880px in v2.css). After a terminal selection the center pane
  // updates *underneath* the still-open drawer, so the user sees no change.
  // Dismiss any open drawer + backdrop so the result becomes visible.
  function closeDrawers() {
    document.querySelectorAll('.left-pane.is-open, .right-pane.is-open')
      .forEach(p => p.classList.remove('is-open'));
    const bd = document.querySelector('.drawer-backdrop');
    if (bd) bd.classList.remove('is-visible');
  }

  function applyChrome() {
    html.dataset.style   = S.style;
    html.dataset.theme   = S.theme;
    html.dataset.layout  = S.layout;
    html.dataset.density = S.density;
    html.dataset.view    = S.view;
    html.dataset.compare = S.compare ? 'on' : 'off';
  }

  // ── Manifest helpers ───────────────────────────────────────
  function manifestCounties() {
    return cache.manifest ? (cache.manifest.processedCounties || cache.manifest.counties || []) : [];
  }
  function countyToSlug(name) {
    return String(name).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
  }
  function slugToCountyName(slug) {
    const list = manifestCounties();
    const norm = slug.replace(/_/g, ' ');
    return list.find(c => c.toLowerCase() === norm.toLowerCase()) || (slug[0].toUpperCase() + slug.slice(1));
  }
  // City slug helpers. city_county_map.json is keyed by DISPLAY NAME (uppercase)
  // -> [county slugs]; the slug shape matches countyToSlug / the pipeline's
  // _precinct_safe_name. A city is addressed in the URL by its own slug
  // (?level=city&id=kettering), independent of any county.
  function cityNameToSlug(name) { return countyToSlug(name); }
  async function cityNameFromSlug(slug) {
    const map = await loadCityCountyMap();
    if (map) {
      for (const display of Object.keys(map)) {
        if (cityNameToSlug(display) === slug) return display;
      }
    }
    return slug.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  }
  // Counties a city spans (all of them, unioned) — or [] if unknown.
  async function citySpanCounties(cityName) {
    const map = await loadCityCountyMap();
    const upper = (cityName || '').toUpperCase();
    return (map && map[upper] && map[upper].length) ? map[upper] : [];
  }

  // Township/village/ward index lookups. Entries carry `county_slug`
  // (township/village) or `county_slugs[]` (ward, which can span counties
  // the way a city does — e.g. Alliance's ward 2/3 spans Mahoning+Stark).
  async function placeIndexEntry(type, slug) {
    try {
      const idx = await fetchJSON(`data/${type}/index.json`);
      return (idx || []).find(e => e.slug === slug) || null;
    } catch (e) { return null; }
  }
  async function placeDisplayName(type, slug) {
    if (type === 'city') return await cityNameFromSlug(slug);
    const entry = await placeIndexEntry(type, slug);
    return (entry && entry.name) || slug.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  }
  // Deep-link recovery: a township/village/ward URL may omit ?county= (shared
  // link, old history entry). Look the id up in its index.json to backfill
  // the county slug the hierarchy tree and breadcrumb need. Undecidable from
  // the id alone otherwise — county isn't encoded in the slug for cities, and
  // wards can span counties.
  async function resolvePlaceCounty(level, id) {
    const entry = await placeIndexEntry(level, id);
    if (!entry) return null;
    if (level === 'ward') return (entry.county_slugs && entry.county_slugs[0]) || null;
    return entry.county_slug || null;
  }

  // ── Data: load all chartConfigs for a jurisdiction ─────────
  // Returns: { total, party, decade, partyDecade, gen, partyGen, participationGen, uncShadow, citySummary, precinctIndex, missing[] }
  async function loadJurisdiction(level, id, county) {
    const bag = { level, id, county, missing: [] };
    const tries = [];
    function add(key, url) { tries.push({ key, url }); }

    if (level === 'county') {
      const s = id;
      add('party',       `data/${s}_party_affiliation.json`);
      add('decade',      `data/${s}_decade_distribution.json`);
      add('gen',         `data/${s}_generation_distribution.json`);
      add('partyDecade', `data/${s}_party_by_decade.json`);
      add('partyGen',    `data/${s}_party_by_generation.json`);
      add('participationGen', `data/${s}_participation_by_generation.json`);
      add('uncShadow',   `data/${s}_unc_shadow.json`);
      add('citySummary', `data/${s}_city_summary.json`);
      add('precinctIndex',`data/${s}_precinct_index.json`);
      add('narrative',   `data/${s}_narrative.json`);
    } else if (level === 'precinct') {
      const cs = countyToSlug(county || S.county || '');
      const ps = id;
      add('party',       `data/${cs}_precinct_${ps}_party.json`);
      add('decade',      `data/${cs}_precinct_${ps}_decade.json`);
      add('gen',         `data/${cs}_precinct_${ps}_generation.json`);
      add('partyDecade', `data/${cs}_precinct_${ps}_party_by_decade.json`);
      add('partyGen',    `data/${cs}_precinct_${ps}_party_by_generation.json`);
      add('uncShadow',   `data/${cs}_precinct_${ps}_unc.json`);
      // Narrative card: silently 404s for precincts not yet generated;
      // renderNarrative() hides the card when bag.narrative is absent.
      add('narrative',   `data/${cs}_precinct_${ps}_narrative.json`);
    } else if (level === 'city') {
      // id is a CITY slug. The view is the unified all-county report. Chart data
      // comes from the pipeline's PRECOMPUTED city aggregates (correct, derived
      // straight from the voter file's CITY column) — NOT re-summed from whole
      // precinct files, which over-counts partial border precincts. Flat-file
      // pattern, same as districts.
      bag.cityName = await cityNameFromSlug(id);
      bag.spanCounties = await citySpanCounties(bag.cityName);
      // Cache by URL slug so renderMapForState() can light this city's
      // counties without re-deriving the span (it has no access to `bag`).
      // One derivation -- citySpanCounties() -- two consumers.
      window._citySpan = window._citySpan || {};
      window._citySpan[id] = bag.spanCounties;
      // Pipeline writes city files with the jurisdiction-type suffix baked in
      // (CITY value "KETTERING CITY" -> slug "kettering_city"), matching the
      // township/village/district trees. The URL id is the bare city slug
      // ("kettering"), so append "_city" to address the precomputed files.
      const fs = id.endsWith('_city') ? id : `${id}_city`;
      add('party',       `data/city/${fs}_party_affiliation.json`);
      add('decade',      `data/city/${fs}_decade_distribution.json`);
      add('gen',         `data/city/${fs}_generation_distribution.json`);
      add('partyDecade', `data/city/${fs}_party_by_decade.json`);
      add('partyGen',    `data/city/${fs}_party_by_generation.json`);
      add('uncShadow',   `data/city/${fs}_unc_shadow.json`);
      add('narrative',   `data/city/${fs}_narrative.json`);
      // Primary county's precinct index drives the left-nav precinct list.
      const cs = (bag.spanCounties && bag.spanCounties[0]) || countyToSlug(S.county || '');
      bag.primaryCounty = cs;
      add('precinctIndex', `data/${cs}_precinct_index.json`);
    } else if (level === 'township' || level === 'village') {
      // id is a county-scoped place slug (e.g. montgomery_washington_township).
      // Single-county — townships/villages don't span counties the way cities do.
      add('party',       `data/${level}/${id}_party_affiliation.json`);
      add('decade',      `data/${level}/${id}_decade_distribution.json`);
      add('partyDecade', `data/${level}/${id}_party_by_decade.json`);
      // Not generated for townships/villages (bundle has no generation-level
      // data); tried anyway so they land in bag.missing[] and render the same
      // graceful placeholder as an unprocessed district.
      add('gen',         `data/${level}/${id}_generation_distribution.json`);
      add('partyGen',    `data/${level}/${id}_party_by_generation.json`);
      add('uncShadow',   `data/${level}/${id}_unc_shadow.json`);
      add('narrative',   `data/${level}/${id}_narrative.json`);
      const cs = county || S.county;
      bag.primaryCounty = cs;
      if (cs) add('precinctIndex', `data/${cs}_precinct_index.json`);
    } else if (level === 'ward') {
      // id is a municipality-scoped ward slug (e.g. kettering_city_kettering_ward_2).
      add('party',       `data/ward/${id}_party_affiliation.json`);
      add('decade',      `data/ward/${id}_decade_distribution.json`);
      add('partyDecade', `data/ward/${id}_party_by_decade.json`);
      add('gen',         `data/ward/${id}_generation_distribution.json`);
      add('partyGen',    `data/ward/${id}_party_by_generation.json`);
      add('uncShadow',   `data/ward/${id}_unc_shadow.json`);
      add('narrative',   `data/ward/${id}_narrative.json`);
      const cs = county || S.county;
      bag.primaryCounty = cs;
      if (cs) add('precinctIndex', `data/${cs}_precinct_index.json`);
    } else if (level === 'district') {
      const t = S.district_type || 'state_senate_district';
      add('party',       `data/${t}/${id}_party_affiliation.json`);
      add('decade',      `data/${t}/${id}_decade_distribution.json`);
      add('partyDecade', `data/${t}/${id}_party_by_decade.json`);
      add('uncShadow',   `data/${t}/${id}_unc_shadow.json`);
      // Narrative card: partial rollout safe — card hides when file absent.
      add('narrative',   `data/${t}/${id}_narrative.json`);
    }

    await Promise.all(tries.map(async t => {
      try { bag[t.key] = await fetchJSON(t.url); }
      catch (e) { bag.missing.push(t.key); }
    }));

    if (bag.party && bag.party.chartConfig) {
      bag.total = bag.party.chartConfig.datasets[0].data.reduce((a, b) => a + Number(b || 0), 0);
    }

    // City precinct count: the per-county city_summary files carry the precinct
    // count (row index 5) but their voter totals (index 4) double-count partial
    // border precincts, so we take ONLY the precinct count and sum it across the
    // spanning counties. The voter total stays sourced from bag.party (the
    // corrected city aggregate). For Kettering: 2 (Greene) + 41 (Montgomery) = 43.
    if (level === 'city' && bag.spanCounties && bag.spanCounties.length) {
      const nameUpper = String(bag.cityName || id.replace(/_city$/, '').replace(/_/g, ' ')).toUpperCase();
      let pc = 0, found = false;
      await Promise.all(bag.spanCounties.map(async cslug => {
        try {
          const cs = await fetchJSON(`data/${cslug}_city_summary.json`);
          const row = (cs.rows || []).find(r => String(r[1]).toUpperCase() === nameUpper);
          if (row) { pc += Number(String(row[5]).replace(/[^0-9]/g, '')) || 0; found = true; }
        } catch (e) { /* missing county summary: skip, count stays partial */ }
      }));
      if (found) bag.cityPrecinctCount = pc;
    }

    // Ward's parent place (city/township/village), for the 4-level breadcrumb
    // (Ohio › County › City › Ward). Never parsed from the ward name itself
    // (CINTI/HUBER HTS/FIRST WARD all break name-based parsing) — read from
    // the entity the pipeline already resolved via the place resolver.
    if (level === 'ward') {
      const entry = await placeIndexEntry('ward', id);
      if (entry && entry.parent_place_slug && entry.parent_type) {
        bag.wardParentSlug = entry.parent_place_slug;
        bag.wardParentType = entry.parent_type;
        bag.wardParentName = await placeDisplayName(entry.parent_type, entry.parent_place_slug);
      }
    }
    return bag;
  }

  // ── County / district map ──────────────────────────────────
  // Real geometry from data/state_map/*.geojson, rendered through
  // window.GeoMap. The old stylized hex grid and jittered district cells
  // are gone -- they carried no geographic information.
  //
  // `count` is still read by preloadDistrictLean (iterates 1..count to fetch
  // lean JSON) and `label` by the map eyebrow, so the table stays. The old
  // `cols`/`rows` grid geometry is gone with the renderer that used it.
  const DISTRICT_LAYOUTS = {
    congressional_district:        { count: 15, label: 'congressional districts' },
    state_senate_district:         { count: 33, label: 'state senate districts' },
    state_representative_district: { count: 99, label: 'state house districts' }
  };

  // dtype -> geojson layer file. The `properties.id` join is direct: each
  // file's ids are zero-padded 2-char and contiguous (house '01'-'99',
  // senate '01'-'33', congress '01'-'15'), byte-identical to the ids the
  // callers pass. Do not strip or re-pad -- that would break the join.
  // Districts have no statewide aggregate file, so a district view must always
  // name one. Ids are zero-padded 2-char in both the geojson and the data
  // filenames ('01'-'99'), and every type starts at 01.
  const DEFAULT_DISTRICT_ID = '01';

  const DISTRICT_GEO_FILES = {
    congressional_district:        'data/state_map/congress.geojson',
    state_senate_district:         'data/state_map/senate.geojson',
    state_representative_district: 'data/state_map/house.geojson'
  };
  const COUNTY_GEO_FILE = 'data/state_map/counties.geojson';

  // ── Map render generation ──────────────────────────────────
  // Every map renderer is async (geojson fetch) and they all write into the
  // SAME #map node, so the existing `$('map') !== root` guard cannot separate
  // them -- it only detects the container being replaced, not a second
  // renderer landing in it. Without a generation token the slowest fetch
  // wins: selecting Montgomery painted the precinct layer, then a still
  // in-flight county render overwrote it with the 88-county map.
  //
  // The token identifies the VIEW, not the call. Several dispatches can
  // legitimately target one view (first paint, refreshView, then the lean
  // preload landing); those must share a token or they cancel each other.
  // It advances only when the view signature actually changes, so a genuine
  // navigation invalidates in-flight renders from the previous view.
  let _mapGen = 0;
  let _mapGenKey = null;
  function mapViewKey() {
    return [S.view, S.level, S.id, S.county, S.district_type,
            S.compare ? S.compare.join('|') : ''].join('');
  }
  function nextMapGen() {
    const k = mapViewKey();
    if (k !== _mapGenKey) { _mapGenKey = k; _mapGen++; }
    return _mapGen;
  }

  // Sum a party-affiliation chart into {r, unc, d, total} by cohort label, never by
  // array position (the cohort order changed once and position-based sums silently
  // went wrong). Returns null, and logs, unless the chart carries exactly the
  // canonical cohort labels; callers render a dash rather than a wrong number.
  function cohortSides(chartConfig) {
    if (!chartConfig || !chartConfig.labels || !chartConfig.datasets) return null;
    const labels = chartConfig.labels.map(l => String(l).split(' \u2014 ')[0].trim());
    const data = chartConfig.datasets[0].data;
    const expected = Object.keys(COHORT_SIDE);
    const seen = new Set(labels);
    if (labels.length !== expected.length || seen.size !== expected.length ||
        !expected.every(k => seen.has(k))) {
      console.error('cohortSides: unexpected cohort labels', labels);
      return null;
    }
    const out = { r: 0, unc: 0, d: 0, total: 0 };
    labels.forEach((l, i) => {
      const v = Number(data[i] || 0);
      const side = COHORT_SIDE[l];
      if (side === 'R') out.r += v;
      else if (side === 'D') out.d += v;
      else out.unc += v;
      out.total += v;
    });
    return out;
  }

  function partyLean(partyData) {
    if (!partyData || !partyData.chartConfig) return null;
    const s = cohortSides(partyData.chartConfig);
    if (!s || !s.total) return null;
    return (s.d - s.r) / s.total;
  }
  const leanColor = GeoMap.leanColor;

  async function preloadCountyLean() {
    // Only loads counties whose data files exist (others stay neutral)
    const leans = {};
    const counties = manifestCounties();
    await Promise.all(counties.map(async name => {
      const slug = countyToSlug(name);
      try {
        const p = await fetchJSON(`data/${slug}_party_affiliation.json`);
        leans[name] = partyLean(p);
      } catch (e) { leans[name] = null; }
    }));
    return leans;
  }

  // ── Selection inset ────────────────────────────────────────
  // At statewide extent most jurisdictions are too small to read: the median
  // house district is 0.52% of the panel area and the smallest (HD-03) is
  // 0.05%. The statewide map answers "where"; this inset answers "what shape".
  //
  // Zoom here is not a transform -- it is a second projection fitted to the
  // selected feature's own bbox (GeoMap.featureBounds -> computeProjection).
  // Neighbours inside that frame are drawn dimmed for context; clipBounds
  // skips everything outside it so we build ~3 paths, not 99.
  //
  // INSET_MAX_FRAC: selections already occupying more than this fraction of
  // the panel are legible on their own, so the inset would be redundant --
  // large counties and CD-scale districts suppress it automatically. This is
  // a geometry test, never a hardcoded jurisdiction list, so every level
  // (and every future precinct layer) gets the same treatment.
  const INSET_MAX_FRAC = 0.04;
  let _insetDismissed = false;

  function hideInset() {
    const box = $('map-inset');
    if (box) { box.hidden = true; while (box.firstChild) box.removeChild(box.firstChild); }
    const btn = $('inset-restore');
    if (btn) btn.hidden = true;
  }

  function ensureRestoreBtn(onRestore) {
    let btn = $('inset-restore');
    if (!btn) {
      const frame = document.querySelector('.map-frame');
      if (!frame) return null;
      btn = document.createElement('button');
      btn.id = 'inset-restore';
      btn.className = 'inset-restore';
      btn.type = 'button';
      btn.textContent = 'Detail';
      btn.title = 'Show zoomed detail of the selection';
      frame.appendChild(btn);
    }
    btn.onclick = () => { _insetDismissed = false; onRestore(); };
    return btn;
  }

  // gj/key identify the selected feature; label captions it.
  function renderInset(gj, keyProp, key, label) {
    const box = $('map-inset');
    if (!box) return;
    if (!key) { hideInset(); return; }
    const feat = gj.features.find(f => (f.properties || {})[keyProp] === key);
    if (!feat) { hideInset(); return; }

    // Suppress when the selection is already big enough to read statewide.
    const full = gj.bounds;
    const own = GeoMap.featureBounds(feat.geometry, 0);
    if (!own) { hideInset(); return; }
    const frac = ((own[2] - own[0]) * (own[3] - own[1])) /
                 (((full[2] - full[0]) * (full[3] - full[1])) || 1);
    if (frac > INSET_MAX_FRAC) { hideInset(); return; }

    if (_insetDismissed) {
      hideInset();
      const rb = ensureRestoreBtn(() => renderInset(gj, keyProp, key, label));
      if (rb) rb.hidden = false;
      return;
    }
    const rb0 = $('inset-restore');
    if (rb0) rb0.hidden = true;

    const bounds = GeoMap.featureBounds(feat.geometry, 0.18);
    box.hidden = false;
    GeoMap.render({
      container: box,
      geojson: gj,
      proj: GeoMap.computeProjection(bounds),
      clipBounds: bounds,
      mode: 'selected',
      keyProp: keyProp,
      selectedKey: key,
      className: 'geo-shape',
      ariaLabel: 'Zoomed detail of ' + label,
      nameFor: p => p.name || String(p[keyProp])
    });

    const cap = document.createElement('span');
    cap.className = 'inset-cap';
    cap.textContent = label;
    box.appendChild(cap);

    const close = document.createElement('button');
    close.className = 'inset-close';
    close.type = 'button';
    close.setAttribute('aria-label', 'Hide zoomed detail');
    close.title = 'Hide zoomed detail';
    close.textContent = '\u00d7';
    close.onclick = () => {
      _insetDismissed = true;
      hideInset();
      const rb = ensureRestoreBtn(() => renderInset(gj, keyProp, key, label));
      if (rb) rb.hidden = false;
    };
    box.appendChild(close);
  }

  // Signature unchanged -- both call sites (refreshView, and the lean-preload
  // callback) pass the same four arguments.
  //
  // `leans` is kept because callers still populate and pass it, but coloring
  // reads properties.lean straight out of the geojson we are already
  // fetching: measured 2026-07-22 across all 88 counties, the two agree to
  // within 5e-05 (pure 4dp rounding in build_state_map.py), because
  // partyLean() here and party_lean() there both read cohorts by label through
  // the same COHORT_SIDE table (pipeline/cohort_lean.py). Using the embedded value avoids a second network round trip.
  function renderDistrictMap(dtype, selectedId, compareIds, leans, gen) {
    const root = $('map');
    if (!root) return;
    const layout = DISTRICT_LAYOUTS[dtype] || DISTRICT_LAYOUTS.state_senate_district;
    const file = DISTRICT_GEO_FILES[dtype] || DISTRICT_GEO_FILES.state_senate_district;
    GeoMap.loadGeoJSON(file).then(gj => {
      // The view may have changed while the fetch was in flight.
      if ((gen !== undefined && gen !== _mapGen) || ($('map')) !== root) return;
      // With nothing selected there is nothing to contrast against, so
      // 'selected' mode would dim every shape and the map would read as
      // uniformly dark -- which is exactly what switching to Districts used
      // to show. Fall back to 'all' until the user picks something.
      const anySel = !!(selectedId || (compareIds && compareIds.some(Boolean)));
      GeoMap.render({
        container: root,
        geojson: gj,
        mode: anySel ? 'selected' : 'all',
        keyProp: 'id',
        selectedKey: selectedId || null,
        compareKeys: compareIds || null,
        className: 'geo-shape',
        ariaLabel: 'Ohio ' + layout.label + ' map',
        nameFor: p => 'District ' + p.id,
        onClick: (id) => { selectDistrict(dtype, id); }
      });
      renderInset(gj, 'id', selectedId || null,
        layout.label.replace(/s$/, '').replace(/\b\w/g, c => c.toUpperCase()) + ' ' + (selectedId || ''));
    }).catch(e => console.warn('[map] district geometry load failed', e));
  }

  async function preloadDistrictLean(dtype) {
    const layout = DISTRICT_LAYOUTS[dtype];
    if (!layout) return {};
    const leans = {};
    await Promise.all(Array.from({ length: layout.count }, (_, k) => k + 1).map(async n => {
      const id = String(n).padStart(2, '0');
      try {
        const p = await fetchJSON('data/' + dtype + '/' + id + '_party_affiliation.json');
        leans[id] = partyLean(p);
      } catch (e) { leans[id] = null; }
    }));
    return leans;
  }

  async function selectDistrict(dtype, id) {
    if (S.compare) {
      const arr = S.compare.slice();
      const slot = window._focusedSlot === 'b' ? 1 : 0;
      arr[slot] = 'district:' + dtype + ':' + id;
      S.compare = arr;
      writeState({ compare: arr });
      emit('compare_update', { slot: slot === 0 ? 'a' : 'b', value: arr[slot] });
    } else {
      S.level = 'district'; S.id = id; S.district_type = dtype;
      writeState({ level: 'district', id, type: dtype });
      emit('select_jurisdiction', { level: 'district', type: dtype, id });
    }
    await refreshView();
  }

  // Signature unchanged -- all 5 call sites keep passing display names.
  // selectedName/compareNames arrive as display names ("Montgomery"); the
  // geojson keys on properties.slug, so names are mapped through the
  // existing countyToSlug() (build_state_map.py's county_slug() is
  // byte-identical to it, so the keys join).
  //
  // City and precinct views fall back to highlighting the CONTAINING county:
  // sub-county polygons exist for Montgomery only so far. The caller decides
  // which county to pass, so each county upgrades automatically as its own
  // shapes land -- nothing here branches on a county name.
  // activeSlugs: county slugs to keep lit without the selected/compare stroke
  // -- used for a multi-county city, which highlights every county it spans.
  function renderHexMap(leans, selectedName, compareNames, gen, activeSlugs) {
    const root = $('map');
    if (!root) return;
    const selSlug = selectedName ? countyToSlug(selectedName) : null;
    const cmpSlugs = compareNames ? compareNames.filter(Boolean).map(countyToSlug) : null;
    GeoMap.loadGeoJSON(COUNTY_GEO_FILE).then(gj => {
      if ((gen !== undefined && gen !== _mapGen) || ($('map')) !== root) return;
      // See renderDistrictMap: with no selection, 'selected' mode dims all 88
      // and the statewide overview reads as a dark blank.
      const anySel = !!(selSlug || (cmpSlugs && cmpSlugs.length) ||
                        (activeSlugs && activeSlugs.length));
      GeoMap.render({
        container: root,
        geojson: gj,
        mode: anySel ? 'selected' : 'all',
        keyProp: 'slug',
        selectedKey: selSlug,
        compareKeys: cmpSlugs,
        activeKeys: (activeSlugs && activeSlugs.length) ? activeSlugs : null,
        className: 'geo-shape',
        ariaLabel: 'Ohio counties map',
        nameFor: p => p.name + ' County',
        onClick: (slug, p) => { selectCounty(p.name); }
      });
      // Compare mode highlights two counties; an inset can only frame one,
      // so it is suppressed rather than arbitrarily picking slot A.
      const insetKey = (cmpSlugs && cmpSlugs.length) ? null : selSlug;
      renderInset(gj, 'slug', insetKey, (selectedName || '') + ' County');
    }).catch(e => console.warn('[map] county geometry load failed', e));
  }

  // ── Precinct choropleth (county drill-down) ────────────────
  // Montgomery is the only county whose BoE precinct polygons we have so
  // far (1 of 88 -- see DATA_QUALITY.md `precinct_geometry`). Nothing here
  // names it: we probe for the file and fall back to the county map on a
  // 404, so each county upgrades the moment its shapes land. Branching on
  // observed availability rather than a county list is CLAUDE.md section 5.
  const PRECINCT_GEO_DIR = 'data/state_map/precincts_';
  const _precinctGeoMiss = {};   // countySlug -> true once probed and absent

  function precinctGeoFile(countySlug) { return PRECINCT_GEO_DIR + countySlug + '.geojson'; }

  // Finer-tolerance geometry for the zoomed selection inset only (Handoff 6).
  // The overview file above simplifies at ~85m to hold the statewide 184 KB
  // payload, which reads as faceted rectangles once one precinct fills the
  // inset panel. Probed the same way as precinctGeoFile: a 404 just falls
  // back to the overview gj already in hand, so nothing here hardcodes which
  // county has a detail layer.
  function precinctDetailGeoFile(countySlug) { return PRECINCT_GEO_DIR + countySlug + '_detail.geojson'; }

  // Which precincts should stay coloured for the current selection.
  // Membership comes from the pipeline-stamped place_slug / ward_slug in the
  // precinct index -- never by re-aggregating precinct files at runtime,
  // which is what caused the Kettering over-count (frontend rule).
  // Returns null for "colour everything" (whole-county view).
  function precinctActiveKeys(index) {
    const rows = (index && index.precincts) || [];
    if (S.level === 'precinct') return new Set([S.id]);
    if (S.level === 'ward') return new Set(rows.filter(p => p.ward_slug === S.id).map(p => p.safe_name));
    if (SUBCOUNTY_LEVELS.includes(S.level) || S.level === 'city') {
      return new Set(rows.filter(p => p.place_slug === S.id).map(p => p.safe_name));
    }
    return null;
  }

  // countySlug: which county's geometry to draw. Resolves through the same
  // index the hierarchy uses, so names never have to be un-slugged.
  function renderPrecinctMap(countySlug, gen) {
    const root = $('map');
    if (!root) return Promise.resolve(false);
    const file = precinctGeoFile(countySlug);
    return Promise.all([
      GeoMap.loadGeoJSON(file),
      fetchJSON('data/' + countySlug + '_precinct_index.json')
    ]).then(([gj, index]) => {
      // Superseded by a newer view: report success so the caller does NOT
      // fall back and repaint over whatever is now current.
      if (gen !== _mapGen || ($('map')) !== root) return true;
      const active = precinctActiveKeys(index);
      GeoMap.render({
        container: root,
        geojson: gj,
        mode: active ? 'selected' : 'all',
        keyProp: 'safe_name',
        activeKeys: active,
        selectedKey: S.level === 'precinct' ? S.id : null,
        className: 'geo-shape',
        ariaLabel: slugToCountyName(countySlug) + ' County precinct map',
        nameFor: p => p.name,
        // Mirrors the [data-action="select-precinct"] hierarchy handler's
        // state transition; there is no shared selectPrecinct() to call.
        onClick: async (safeName, props) => {
          S.level = 'precinct'; S.id = safeName; S.county = countySlug; S.city = null;
          writeState({ level: 'precinct', id: safeName, county: countySlug, city: null, type: null });
          emit('select_jurisdiction', { level: 'precinct', id: safeName, county: countySlug, name: props.name });
          await refreshView();
        }
      });
      // One precinct is far below INSET_MAX_FRAC, so the inset carries the
      // actual shape reading here; a whole-county view has no single feature.
      if (S.level === 'precinct') {
        const row = (index.precincts || []).find(p => p.safe_name === S.id);
        const label = (row && row.name) || S.id;
        // Prefer the finer-tolerance detail layer; fall back to the overview
        // gj already loaded above on any failure (404 or otherwise) so the
        // inset never goes blank waiting on a file that may not exist yet.
        GeoMap.loadGeoJSON(precinctDetailGeoFile(countySlug))
          .then(detailGj => renderInset(detailGj, 'safe_name', S.id, label))
          .catch(() => renderInset(gj, 'safe_name', S.id, label));
      } else {
        hideInset();
      }
      return true;
    }).catch(() => {
      _precinctGeoMiss[countySlug] = true;    // absent (404) or malformed
      return false;
    });
  }

  function renderMapSelection(bag) {
    const root = $('map-selection');
    if (!root) return;
    if (!bag) { root.innerHTML = '<span class="eyebrow">Selection</span><div class="mname muted">—</div>'; return; }
    const name = bag.displayName || bag.id;
    const totalFmt = bag.total != null ? bag.total.toLocaleString() : '—';
    let leanText = '', leanBars = '';
    const sides = (bag.party && bag.party.chartConfig) ? cohortSides(bag.party.chartConfig) : null;
    if (bag.party && bag.party.chartConfig && !sides) leanText = '\u2014';
    if (sides) {
      const total = bag.total || 1;
      const r = sides.r, dd = sides.d, unc = sides.unc;
      const leanPct = ((dd - r) / total * 100);
      leanText = (leanPct > 0 ? '+' : '') + leanPct.toFixed(1) + '% D−R · UNC ' + (unc / total * 100).toFixed(0) + '%';
      const segR = (r / total * 100).toFixed(1);
      const segU = (unc / total * 100).toFixed(1);
      const segD = (dd / total * 100).toFixed(1);
      leanBars = '<div class="mlean">' +
        '<div class="mlean-seg" style="width:' + segR + '%;background:var(--c-rep)"></div>' +
        '<div class="mlean-seg" style="width:' + segU + '%;background:var(--c-unc)"></div>' +
        '<div class="mlean-seg" style="width:' + segD + '%;background:var(--c-dem)"></div>' +
      '</div>';
    }
    root.innerHTML =
      '<span class="eyebrow">Selection</span>' +
      '<div class="mname">' + esc(name) + '</div>' +
      '<div class="mstat">' + totalFmt + ' registered · ' + leanText + '</div>' +
      leanBars;
  }

  // ── Hierarchy tree ─────────────────────────────────────────
  async function renderHierarchy() {
    const root = $('hierarchy');
    if (!root) return;
    root.innerHTML = '';

    if (S.view === 'geo') {
      const counties = manifestCounties();
      const html = ['<div class="hier-section">',
        '<span class="eyebrow">Statewide</span>',
        '<div class="hier-row depth-0" data-action="select-state"><span class="twirl"></span><span class="label">Ohio</span><span class="count">' + counties.length + '</span></div>',
        '</div>'];

      // Cities spanning multiple counties: the unified, county-independent view.
      // (Single-county cities remain reachable by drilling into their county.)
      const cmap = await loadCityCountyMap();
      if (cmap) {
        const multi = Object.keys(cmap)
          .filter(name => (cmap[name] || []).length > 1)
          .sort();
        if (multi.length) {
          html.push('<div class="hier-section"><span class="eyebrow">Cities spanning counties (' + multi.length + ')</span>');
          multi.forEach(name => {
            const slug = cityNameToSlug(name);
            const isSel = S.level === 'city' && !S.county && S.id === slug;
            const nC = cmap[name].length;
            html.push('<div class="hier-row depth-0 ' + (isSel ? 'is-selected' : '') +
              '" data-action="select-city" data-city-slug="' + esc(slug) + '" data-city-name="' + esc(name) + '">' +
              '<span class="twirl"></span><span class="label">' + esc(name) + '</span>' +
              '<span class="count">' + nC + ' co.</span></div>');
          });
          html.push('</div>');
        }
      }

      html.push('<div class="hier-section"><span class="eyebrow">Counties (88)</span>');
      counties.forEach(c => {
        const slug = countyToSlug(c);
        const isSel = S.level === 'county' && S.id === slug;
        const isExpanded = isSel ||
          (SUBCOUNTY_LEVELS.includes(S.level) && countyToSlug(S.county || '') === slug) ||
          (S.level === 'precinct' && countyToSlug(S.county || '') === slug);
        html.push('<div class="hier-row depth-0 ' + (isSel ? 'is-selected' : '') + '" data-action="select-county" data-county="' + esc(slug) + '" data-county-name="' + esc(c) + '"><span class="twirl">▸</span><span class="label">' + esc(c) + '</span><span class="count">—</span></div>');
        if (isExpanded) {
          html.push('<div class="hier-children is-open" data-county-children="' + esc(slug) + '"></div>');
        }
      });
      html.push('</div>');
      root.innerHTML = html.join('');

      // If a county is open, lazy-load its place/ward/precinct list
      if (S.level === 'county' || S.level === 'precinct' || SUBCOUNTY_LEVELS.includes(S.level)) {
        const cs = (S.level === 'precinct' || SUBCOUNTY_LEVELS.includes(S.level)) ? countyToSlug(S.county || '') : S.id;
        await populateCountyChildren(cs);
      }
    } else {
      // Districts view
      const html = [
        '<div class="hier-section"><span class="eyebrow">Federal</span>',
        '<div class="hier-row depth-0 ' + (S.district_type === 'congressional_district' ? 'is-open' : '') + '" data-action="toggle-dtype" data-dtype="congressional_district"><span class="twirl">▸</span><span class="label">Congressional</span><span class="count">16</span></div>',
        '<div class="hier-children ' + (S.district_type === 'congressional_district' ? 'is-open' : '') + '" data-dtype-children="congressional_district"></div>',
        '</div>',
        '<div class="hier-section"><span class="eyebrow">State</span>',
        '<div class="hier-row depth-0 ' + (S.district_type === 'state_senate_district' ? 'is-open' : '') + '" data-action="toggle-dtype" data-dtype="state_senate_district"><span class="twirl">▸</span><span class="label">State Senate</span><span class="count">33</span></div>',
        '<div class="hier-children ' + (S.district_type === 'state_senate_district' ? 'is-open' : '') + '" data-dtype-children="state_senate_district"></div>',
        '<div class="hier-row depth-0 ' + (S.district_type === 'state_representative_district' ? 'is-open' : '') + '" data-action="toggle-dtype" data-dtype="state_representative_district"><span class="twirl">▸</span><span class="label">State House</span><span class="count">99</span></div>',
        '<div class="hier-children ' + (S.district_type === 'state_representative_district' ? 'is-open' : '') + '" data-dtype-children="state_representative_district"></div>',
        '</div>'
      ];
      root.innerHTML = html.join('');
      if (S.district_type) await populateDistrictChildren(S.district_type);
    }
    wireHierarchyEvents();
  }

  const PLACE_TYPE_LABEL = { city: 'City', village: 'Village', township: 'Township' };

  async function populateCountyChildren(countySlug) {
    const wrap = document.querySelector('[data-county-children="' + countySlug + '"]');
    if (!wrap) return;
    wrap.innerHTML = '<div class="hier-row depth-1 muted"><span class="label">Loading…</span></div>';

    const precIdx = await fetchJSON('data/' + countySlug + '_precinct_index.json').catch(() => null);
    const precincts = (precIdx && precIdx.precincts) ? precIdx.precincts : [];

    if (precincts.length === 0) {
      wrap.innerHTML = '<div class="hier-row depth-1 muted"><span class="label">Not yet processed</span></div>';
      return;
    }

    // Group precincts by the pipeline-stamped place_slug/place_type/place_name
    // (single resolver — every precinct resolves to exactly one place; no
    // name-matching heuristics, no "Other precincts" bucket). A precinct
    // without a place_slug is a pipeline defect the validation gate catches;
    // it still gets its own depth-1 row rather than reviving a synthetic bucket.
    const places = {};
    const placeless = [];
    precincts.forEach(p => {
      if (!p.place_slug) { placeless.push(p); return; }
      const g = (places[p.place_slug] = places[p.place_slug] || {
        slug: p.place_slug, type: p.place_type, name: p.place_name, precincts: []
      });
      g.precincts.push(p);
    });

    const placeList = Object.keys(places).map(k => places[k]);
    placeList.forEach(pl => { pl.total = pl.precincts.reduce((sum, p) => sum + (Number(p.total) || 0), 0); });
    placeList.sort((a, b) => b.total - a.total);

    function precinctRow(p, depth) {
      const isSel = S.level === 'precinct' && S.id === p.safe_name;
      return '<div class="hier-row depth-' + depth + ' ' + (isSel ? 'is-selected' : '') + '" data-action="select-precinct" data-county="' + esc(countySlug) + '" data-precinct="' + esc(p.safe_name) + '" data-precinct-name="' + esc(p.name) + '">' +
        '<span class="twirl"></span>' +
        '<span class="label">' + esc(p.name) + '</span>' +
        '<span class="count">' + (p.total ? p.total.toLocaleString() : '—') + '</span>' +
      '</div>';
    }

    const html = [];
    placeList.forEach(place => {
      // place_slug IS the routing id (bare-name slug for cities, matching the
      // frontend's own cityNameToSlug; county-prefixed for village/township —
      // see PLAN_SUBCOUNTY_JURISDICTIONS.md Part 2). Never recomputed here.
      const isPlaceSel = S.level === place.type && S.id === place.slug;
      html.push(
        '<div class="hier-row depth-1 ' + (isPlaceSel ? 'is-selected' : '') + '" data-action="toggle-place" data-place-slug="' + esc(place.slug) + '" data-place-type="' + esc(place.type) + '" data-place-name="' + esc(place.name) + '" data-county="' + esc(countySlug) + '">' +
          '<span class="twirl">▸</span>' +
          '<span class="label">' + esc(place.name) + '</span>' +
          '<span class="place-type-badge">' + esc(PLACE_TYPE_LABEL[place.type] || place.type) + '</span>' +
          '<span class="count">' + place.precincts.length + '</span>' +
        '</div>',
        '<div class="hier-children" data-place-children="' + esc(place.slug) + '">'
      );

      // Nest precincts under wards when the place has any ward-holding
      // precincts; otherwise keep the flat place > precinct shape (no empty
      // ward layer for at-large cities/townships/villages).
      const wards = {};
      const flat = [];
      place.precincts.forEach(p => {
        if (p.ward_slug) {
          const w = (wards[p.ward_slug] = wards[p.ward_slug] || { slug: p.ward_slug, name: p.ward_name, precincts: [] });
          w.precincts.push(p);
        } else {
          flat.push(p);
        }
      });

      Object.keys(wards).map(k => wards[k]).sort((a, b) => b.precincts.length - a.precincts.length).forEach(ward => {
        const isWardSel = S.level === 'ward' && S.id === ward.slug;
        html.push(
          '<div class="hier-row depth-2 ' + (isWardSel ? 'is-selected' : '') + '" data-action="toggle-ward" data-ward-slug="' + esc(ward.slug) + '" data-ward-name="' + esc(ward.name) + '" data-county="' + esc(countySlug) + '">' +
            '<span class="twirl">▸</span>' +
            '<span class="label">' + esc(ward.name) + '</span>' +
            '<span class="count">' + ward.precincts.length + '</span>' +
          '</div>',
          '<div class="hier-children" data-ward-children="' + esc(ward.slug) + '">'
        );
        ward.precincts.forEach(p => html.push(precinctRow(p, 3)));
        html.push('</div>');
      });
      flat.forEach(p => html.push(precinctRow(p, 2)));

      html.push('</div>'); // close data-place-children
    });

    placeless.forEach(p => html.push(precinctRow(p, 1)));

    wrap.innerHTML = html.join('');

    // Toggle handlers on place rows: expand tree AND navigate to the place's view.
    wrap.querySelectorAll('[data-action="toggle-place"]').forEach(el => {
      el.onclick = async (e) => {
        e.stopPropagation();
        const pslug = el.getAttribute('data-place-slug');
        const ptype = el.getAttribute('data-place-type');
        const pname = el.getAttribute('data-place-name');
        const cs = el.getAttribute('data-county');
        const isOpen = el.classList.toggle('is-open');
        const children = wrap.querySelector('[data-place-children="' + pslug + '"]');
        if (children) children.classList.toggle('is-open', isOpen);

        wrap.querySelectorAll('[data-action="toggle-place"].is-selected').forEach(r => r.classList.remove('is-selected'));
        el.classList.add('is-selected');

        S.level = ptype; S.id = pslug; S.county = cs; S.city = ptype === 'city' ? pname : null;
        writeState({ level: ptype, id: pslug, county: cs, city: null, type: null });
        emit('select_jurisdiction', { level: ptype, id: pslug, county: cs, name: pname });
        closeDrawers();
        await refreshView();
      };
    });

    // Toggle handlers on ward rows: expand tree AND navigate to the ward's view.
    wrap.querySelectorAll('[data-action="toggle-ward"]').forEach(el => {
      el.onclick = async (e) => {
        e.stopPropagation();
        const wslug = el.getAttribute('data-ward-slug');
        const wname = el.getAttribute('data-ward-name');
        const cs = el.getAttribute('data-county');
        const isOpen = el.classList.toggle('is-open');
        const children = wrap.querySelector('[data-ward-children="' + wslug + '"]');
        if (children) children.classList.toggle('is-open', isOpen);

        wrap.querySelectorAll('[data-action="toggle-ward"].is-selected').forEach(r => r.classList.remove('is-selected'));
        el.classList.add('is-selected');

        S.level = 'ward'; S.id = wslug; S.county = cs; S.city = null;
        writeState({ level: 'ward', id: wslug, county: cs, city: null, type: null });
        emit('select_jurisdiction', { level: 'ward', id: wslug, county: cs, name: wname });
        closeDrawers();
        await refreshView();
      };
    });

    // Wire precinct click handlers (depth-2/3 rows are excluded from wireHierarchyEvents)
    wrap.querySelectorAll('[data-action="select-precinct"]').forEach(el => {
      el.onclick = async (e) => {
        e.stopPropagation();
        const county = el.getAttribute('data-county');
        const precinct = el.getAttribute('data-precinct');
        const name = el.getAttribute('data-precinct-name');
        S.level = 'precinct'; S.id = precinct; S.county = county; S.city = null;
        writeState({ level: 'precinct', id: precinct, county: county, city: null, type: null });
        emit('select_jurisdiction', { level: 'precinct', id: precinct, county, name });
        closeDrawers();   // mobile: terminal selection, reveal center pane
        await refreshView();
      };
    });

    // Auto-expand every hier-children wrapper between the selected precinct
    // and the county root — 1 level for a flat place, 2 when ward-nested.
    if (S.level === 'precinct') {
      const selRow = wrap.querySelector('[data-action="select-precinct"].is-selected');
      if (selRow) {
        let node = selRow.parentNode;
        while (node && node !== wrap) {
          if (node.classList && node.classList.contains('hier-children')) {
            node.classList.add('is-open');
            const placeKey = node.getAttribute('data-place-children');
            const wardKey = node.getAttribute('data-ward-children');
            const toggle = placeKey
              ? wrap.querySelector('[data-place-slug="' + placeKey + '"]')
              : wrap.querySelector('[data-ward-slug="' + wardKey + '"]');
            if (toggle) toggle.classList.add('is-open');
          }
          node = node.parentNode;
        }
      }
    }
    // Auto-expand the active place when at city/township/village view.
    if (S.level === 'city' || S.level === 'township' || S.level === 'village') {
      const placeRow = wrap.querySelector('[data-place-slug="' + S.id + '"]');
      if (placeRow) {
        const children = wrap.querySelector('[data-place-children="' + S.id + '"]');
        placeRow.classList.add('is-open');
        if (children) children.classList.add('is-open');
      }
    }
    // Auto-expand the active ward, and its enclosing place, at ward view.
    if (S.level === 'ward') {
      const wardRow = wrap.querySelector('[data-ward-slug="' + S.id + '"]');
      if (wardRow) {
        wardRow.classList.add('is-open');
        const wardChildren = wrap.querySelector('[data-ward-children="' + S.id + '"]');
        if (wardChildren) wardChildren.classList.add('is-open');
        let node = wardRow.parentNode;
        while (node && node !== wrap) {
          if (node.classList && node.classList.contains('hier-children') && node.hasAttribute('data-place-children')) {
            node.classList.add('is-open');
            const placeKey = node.getAttribute('data-place-children');
            const toggle = wrap.querySelector('[data-place-slug="' + placeKey + '"]');
            if (toggle) toggle.classList.add('is-open');
            break;
          }
          node = node.parentNode;
        }
      }
    }

    scrollSelectedIntoView();
  }

  // Bring the selected hierarchy row into view inside the left pane.
  //
  // The tree is 88 counties deep, so arriving at a county page (from the
  // landing map, a shared link, or search) left the selection scrolled far
  // below the fold with no indication where it was. Scrolls only the
  // hierarchy's own scroll container -- scrollIntoView() would also scroll
  // the page/right pane, moving the map out from under the user.
  //
  // 'auto' rather than 'smooth': this fires on every render, and an animated
  // scroll on each refresh reads as jitter.
  function scrollSelectedIntoView() {
    const pane = document.querySelector('.left-pane .hierarchy') || $('hierarchy');
    if (!pane) return;
    const row = pane.querySelector('.hier-row.is-selected');
    if (!row) { pane.scrollTop = 0; return; }
    const pr = pane.getBoundingClientRect();
    const rr = row.getBoundingClientRect();
    if (rr.top >= pr.top && rr.bottom <= pr.bottom) return;   // already visible
    // Park it ~1/4 down the pane so following rows (a county's places, a
    // district's siblings) stay on screen instead of sitting at the edge.
    //
    // Computed from live rects rather than offsetTop: the pane is
    // position:static, so row.offsetParent is BODY and offsetTop is measured
    // against the document, not this scroll container. Clamped so the target
    // cannot land outside the scrollable range.
    const target = pane.scrollTop + (rr.top - pr.top) - pane.clientHeight / 4;
    pane.scrollTop = Math.max(0, Math.min(target, pane.scrollHeight - pane.clientHeight));
  }

  async function populateDistrictChildren(dtype) {
    const wrap = document.querySelector('[data-dtype-children="' + dtype + '"]');
    if (!wrap) return;
    wrap.innerHTML = '<div class="hier-row depth-1 muted"><span class="label">Loading…</span></div>';
    try {
      const idx = await fetchJSON(`data/${dtype}/index.json`);
      const list = Array.isArray(idx) ? idx : (idx.districts || []);
      const html = list.map(d => {
        const isSel = S.level === 'district' && S.district_type === dtype && S.id === d.slug;
        const label = d.display_name || d.name || d.slug;
        return '<div class="hier-row depth-1 ' + (isSel ? 'is-selected' : '') + '" data-action="select-district" data-dtype="' + esc(dtype) + '" data-id="' + esc(d.slug) + '" data-district-name="' + esc(label) + '"><span class="twirl"></span><span class="label">District ' + esc(label) + '</span><span class="count">' + (d.voter_count ? d.voter_count.toLocaleString() : '—') + '</span></div>';
      });
      wrap.innerHTML = html.join('');
      // These rows arrive after renderHierarchy() has already run, so the
      // selected district would otherwise stay below the fold (99 house rows).
      scrollSelectedIntoView();
    } catch (e) {
      wrap.innerHTML = '<div class="hier-row depth-1 muted"><span class="label">Not yet processed</span></div>';
    }
  }

  function wireHierarchyEvents() {
    document.querySelectorAll('[data-action]').forEach(el => {
      // Skip rows whose handlers are owned by populateCountyChildren (place
      // toggles, ward toggles, and precincts nested under either). Otherwise
      // this would overwrite their onclick and the tree expansion would die
      // silently. All select-precinct rows are rendered exclusively inside
      // populateCountyChildren (at depth 1/2/3 depending on ward nesting), so
      // it's skipped unconditionally rather than by a specific depth class.
      const action = el.getAttribute('data-action');
      if (action === 'toggle-place' || action === 'toggle-ward') return;
      if (action === 'select-precinct') return;
      el.onclick = async () => {
        if (action === 'select-state') {
          // No state-level data file exists for all 88; just zoom out the breadcrumb
          S.level = 'county'; S.id = DEFAULT_COUNTY; S.county = null;
          writeState({ level: 'county', id: DEFAULT_COUNTY, compare: null, county: null });
          emit('select_jurisdiction', { level: 'state', id: 'ohio' });
        } else if (action === 'select-county') {
          const slug = el.getAttribute('data-county');
          const name = el.getAttribute('data-county-name');
          S.level = 'county'; S.id = slug; S.county = null; S.district_type = null;
          writeState({ level: 'county', id: slug, county: null, type: null });
          emit('select_jurisdiction', { level: 'county', id: slug, name });
        } else if (action === 'select-city') {
          // Unified, county-independent city view (no county param).
          const slug = el.getAttribute('data-city-slug');
          const name = el.getAttribute('data-city-name');
          S.level = 'city'; S.id = slug; S.city = name; S.county = null; S.district_type = null;
          writeState({ level: 'city', id: slug, county: null, city: null, type: null });
          emit('select_jurisdiction', { level: 'city', id: slug, name });
        } else if (action === 'select-precinct') {
          const county = el.getAttribute('data-county');
          const precinct = el.getAttribute('data-precinct');
          const name = el.getAttribute('data-precinct-name');
          S.level = 'precinct'; S.id = precinct; S.county = county;
          writeState({ level: 'precinct', id: precinct, county: county });
          emit('select_jurisdiction', { level: 'precinct', id: precinct, county, name });
        } else if (action === 'toggle-dtype') {
          // Expanding a district type is also a MAP change: the map shows one
          // district type at a time, so picking "State Senate" must repaint it
          // with all 33. This used to renderHierarchy() and return early,
          // skipping refreshView() below -- the tree expanded but the map
          // stayed on whatever type was previously shown.
          const dtype = el.getAttribute('data-dtype');
          const collapsing = S.district_type === dtype;
          S.district_type = collapsing ? null : dtype;
          // District ids are per-type, so an id carried over from the previous
          // type would point at an unrelated district. Land on District 1 of
          // the newly-opened type rather than clearing to null: there is no
          // all-districts aggregate file, so a null id renders an empty pane.
          if (!collapsing) {
            S.level = 'district'; S.id = DEFAULT_DISTRICT_ID;
            writeState({ level: 'district', id: DEFAULT_DISTRICT_ID, type: dtype });
          } else {
            writeState({ type: null });
          }
          await renderHierarchy();
          await refreshView();
          return;
        } else if (action === 'select-district') {
          const dtype = el.getAttribute('data-dtype');
          const id = el.getAttribute('data-id');
          const name = el.getAttribute('data-district-name');
          S.level = 'district'; S.id = id; S.district_type = dtype;
          writeState({ level: 'district', id, type: dtype });
          emit('select_jurisdiction', { level: 'district', type: dtype, id, name });
        }
        closeDrawers();   // mobile: reveal the updated center pane (no-op on desktop)
        await refreshView();
      };
    });
  }

  // ── Selection drivers ──────────────────────────────────────
  async function selectCounty(name) {
    const slug = countyToSlug(name);
    if (S.compare) {
      const slot = window._focusedSlot === 'b' ? 1 : 0;
      const arr = S.compare.slice();
      arr[slot] = slug;
      S.compare = arr;
      writeState({ compare: arr });
      emit('compare_update', { slot: slot === 0 ? 'a' : 'b', id: slug, name });
    } else {
      S.level = 'county'; S.id = slug; S.county = null; S.district_type = null;
      writeState({ level: 'county', id: slug, county: null, type: null });
      emit('select_jurisdiction', { level: 'county', id: slug, name });
    }
    closeDrawers();   // mobile: reveal center pane after map/county selection
    await refreshView();
  }

  // ── Charts ─────────────────────────────────────────────────
  const chartInstances = {};
  function destroyAll() {
    Object.keys(chartInstances).forEach(k => {
      try { chartInstances[k].destroy(); } catch (e) {}
      delete chartInstances[k];
    });
  }
  function destroyChart(key) {
    if (chartInstances[key]) {
      try { chartInstances[key].destroy(); } catch (e) {}
      delete chartInstances[key];
    }
  }

  function commonOptions() {
    const cs = getComputedStyle(html);
    const ink = cs.getPropertyValue('--ink').trim();
    const muted = cs.getPropertyValue('--muted').trim();
    const rule = cs.getPropertyValue('--rule').trim();
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: cs.getPropertyValue('--surface').trim(),
          titleColor: ink,
          bodyColor: ink,
          borderColor: rule,
          borderWidth: 1,
          padding: 10,
          cornerRadius: 6,
          titleFont: { weight: '500' },
          bodyFont: { size: 12 }
        }
      },
      scales: {
        x: { ticks: { color: muted, font: { size: 10 } }, grid: { color: rule, drawTicks: false }, border: { color: rule } },
        y: { ticks: { color: muted, font: { size: 10 } }, grid: { color: rule, drawTicks: false }, border: { color: rule }, beginAtZero: true }
      }
    };
  }

  function renderChart(canvasId, type, dataObj, opts) {
    destroyChart(canvasId);
    const c = $(canvasId);
    if (!c) return;
    const base = commonOptions();
    const merged = JSON.parse(JSON.stringify(base));
    if (type === 'doughnut') {
      merged.scales = undefined;
      merged.cutout = '60%';
    }
    if (opts && opts.stacked) {
      merged.scales.x.stacked = true;
      merged.scales.y.stacked = true;
    }
    if (opts && opts.percentY) {
      merged.scales.y.max = 100;
      merged.scales.y.ticks.callback = (v) => v + '%';
    }
    if (!merged.onClick) {
      merged.onClick = (e, elements) => {
        if (elements && elements.length > 0) {
          const index = elements[0].index;
          const label = dataObj.labels && dataObj.labels[index] ? dataObj.labels[index] : 'unknown';
          emit('select_content', { content_type: 'chart_bar', item_id: canvasId, description: String(label) });
        }
      };
    }
    chartInstances[canvasId] = new Chart(c.getContext('2d'), {
      type,
      data: dataObj,
      options: merged
    });
  }

  function renderHero(container, bag) {
    if (bag && bag.level === 'city') {
      const cityRow = bag.citySummary && bag.citySummary.rows
        ? bag.citySummary.rows.find(r => r[1] === bag.displayName) : null;
      // Precinct count is summed across spanning counties in loadJurisdiction
      // (cross-county-aware); fall back to the single-county row, then '?'.
      const precinctCount = (bag.cityPrecinctCount != null)
        ? bag.cityPrecinctCount
        : (cityRow ? cityRow[5] : '?');
      if (bag.party && bag.party.chartConfig) {
        // Full hero with ribbon (same layout as county)
        const labels = bag.party.chartConfig.labels.map(l => esc(String(l).split(' \u2014 ')[0]));
        const data   = bag.party.chartConfig.datasets[0].data;
        const colors = bag.party.chartConfig.datasets[0].backgroundColor || COHORT_COLORS;
        const total  = data.reduce((a, b) => a + Number(b || 0), 0);
        const sides = cohortSides(bag.party.chartConfig);
        const r   = sides ? sides.r : 0;
        const unc = sides ? sides.unc : 0;
        const dd  = sides ? sides.d : 0;
        const share = v => sides ? (v / total * 100).toFixed(1) + '%' : '\u2014';
        container.innerHTML =
          '<div class="hero-headline">' +
            '<div class="eyebrow">Total registered voters</div>' +
            '<div class="hero-number">' + total.toLocaleString() + '</div>' +
            '<div class="hero-subtitle">' +
              esc(bag.displayName) + ' \u00b7 ' + esc(precinctCount) + ' precincts \u00b7 ' +
              '<b>' + share(r) + '</b> Republican-leaning, ' +
              '<b>' + share(unc) + '</b> unaffiliated, ' +
              '<b>' + share(dd) + '</b> Democratic-leaning' +
            '</div>' +
          '</div>' +
          '<div class="hero-ribbon-wrap">' +
            '<div class="hero-ribbon-label"><span>Partisan spectrum</span></div>' +
            '<div class="hero-ribbon">' + labels.map((l, i) => {
              const pct = (data[i] / total * 100).toFixed(2);
              return '<div class="hero-ribbon-seg" style="width:' + pct + '%;background:' + esc(colors[i]) + '" title="' + l + ': ' + data[i].toLocaleString() + ' (' + pct + '%)"></div>';
            }).join('') + '</div>' +
            '<div class="hero-legend">' + labels.map((l, i) => {
              return '<div class="legend-item"><span class="sw" style="background:' + esc(colors[i]) + '"></span><span class="lbl">' + l + '</span><span class="val">' + (data[i] / total * 100).toFixed(1) + '%</span></div>';
            }).join('') + '</div>' +
          '</div>';
      } else if (cityRow) {
        container.innerHTML =
          '<div class="hero-headline">' +
            '<div class="eyebrow">Total registered voters</div>' +
            '<div class="hero-number">' + esc(cityRow[4]) + '</div>' +
            '<div class="hero-subtitle">' + esc(bag.displayName) + ' \u00b7 ' +
              esc(precinctCount) + ' precincts \u00b7 ' +
              esc(cityRow[2]) + ' active, ' + esc(cityRow[3]) + ' confirmation' +
            '</div>' +
          '</div>' +
          '<div class="hero-ribbon-wrap">' +
            '<div class="hero-ribbon-label"><span>Aggregating precinct data\u2026</span></div>' +
          '</div>';
      } else {
        container.innerHTML = '<div class="hero-headline"><div class="eyebrow">' + esc(bag.displayName) + '</div><div class="hero-number">\u2014</div><div class="hero-subtitle muted">No summary data found.</div></div><div></div>';
      }
      return;
    }
    if (!bag || !bag.party || !bag.party.chartConfig) {
      container.innerHTML = '<div class="hero-headline"><div class="eyebrow">Total registered voters</div><div class="hero-number">—</div><div class="hero-subtitle muted">Data has not yet been processed for this jurisdiction.</div></div><div></div>';
      return;
    }
    const labels = bag.party.chartConfig.labels.map(l => esc(String(l).split(' — ')[0]));
    const data = bag.party.chartConfig.datasets[0].data;
    const colors = bag.party.chartConfig.datasets[0].backgroundColor || COHORT_COLORS;
    const total = bag.total || data.reduce((a, b) => a + Number(b || 0), 0);
    const sides = cohortSides(bag.party.chartConfig);
    const r = sides ? sides.r : 0;
    const unc = sides ? sides.unc : 0;
    const dd = sides ? sides.d : 0;
    const share = v => sides ? (v / total * 100).toFixed(1) + '%' : '\u2014';

    container.innerHTML = '' +
      '<div class="hero-headline">' +
        '<div class="eyebrow">Total registered voters</div>' +
        '<div class="hero-number">' + total.toLocaleString() + '</div>' +
        '<div class="hero-subtitle">' +
          esc(bag.displayName) + ' · ' +
          '<b>' + share(r) + '</b> Republican-leaning, ' +
          '<b>' + share(unc) + '</b> unaffiliated, ' +
          '<b>' + share(dd) + '</b> Democratic-leaning' +
        '</div>' +
      '</div>' +
      '<div class="hero-ribbon-wrap">' +
        '<div class="hero-ribbon-label"><span>Partisan spectrum</span><span class="mono">' + esc(bag.party.updated || '') + '</span></div>' +
        '<div class="hero-ribbon">' + labels.map((l, i) => {
          const pct = (data[i] / total * 100).toFixed(2);
          return '<div class="hero-ribbon-seg" style="width:' + pct + '%;background:' + esc(colors[i]) + '" title="' + l + ': ' + data[i].toLocaleString() + ' (' + pct + '%)"></div>';
        }).join('') + '</div>' +
        '<div class="hero-legend">' + labels.map((l, i) => {
          return '<div class="legend-item"><span class="sw" style="background:' + esc(colors[i]) + '"></span><span class="lbl">' + l + '</span><span class="val">' + (data[i] / total * 100).toFixed(1) + '%</span></div>';
        }).join('') + '</div>' +
      '</div>';
  }

  function renderDoughnutCard(bag) {
    if (!bag.party || !bag.party.chartConfig) { setPlaceholder('chart-party-wrap', 'No party data'); return; }
    renderChart('chart-party', 'doughnut', bag.party.chartConfig);
    // inline legend
    const lg = $('chart-party-legend');
    if (lg) {
      const labels = bag.party.chartConfig.labels.map(l => esc(String(l).split(' — ')[0]));
      const data = bag.party.chartConfig.datasets[0].data;
      const colors = bag.party.chartConfig.datasets[0].backgroundColor || COHORT_COLORS;
      const total = data.reduce((a, b) => a + Number(b || 0), 0);
      lg.innerHTML = labels.map((l, i) =>
        '<div class="legend-item"><span class="sw" style="background:' + esc(colors[i]) + '"></span><span class="lbl">' + l + '</span><span class="val">' + data[i].toLocaleString() + '</span></div>'
      ).join('');
    }
  }
  function setPlaceholder(wrapId, msg) {
    const w = $(wrapId);
    if (w) w.innerHTML = '<div class="placeholder">' + msg + '</div>';
  }
  function ensureCanvas(wrapId, canvasId) {
    const w = $(wrapId);
    if (!w) return;
    w.innerHTML = '<canvas id="' + canvasId + '"></canvas>';
  }

  function renderCharts(bag) {
    // Doughnut + legend
    if (bag.party && bag.party.chartConfig) {
      ensureCanvas('chart-party-wrap', 'chart-party');
      renderDoughnutCard(bag);
    } else { setPlaceholder('chart-party-wrap', 'Party data not yet processed'); }

    if (bag.decade && bag.decade.chartConfig) {
      ensureCanvas('chart-decade-wrap', 'chart-decade');
      renderChart('chart-decade', 'bar', bag.decade.chartConfig);
    } else { setPlaceholder('chart-decade-wrap', 'Decade data not yet processed'); }

    if (bag.partyDecade && bag.partyDecade.chartConfig) {
      ensureCanvas('chart-party-decade-wrap', 'chart-party-decade');
      renderChart('chart-party-decade', 'bar', bag.partyDecade.chartConfig, { stacked: true });
    } else { setPlaceholder('chart-party-decade-wrap', 'Party × decade not yet processed'); }

    if (bag.gen && bag.gen.chartConfig) {
      ensureCanvas('chart-gen-wrap', 'chart-gen');
      renderChart('chart-gen', 'bar', bag.gen.chartConfig);
    } else { setPlaceholder('chart-gen-wrap', 'Generation data not yet processed for this scope'); }

    if (bag.partyGen && bag.partyGen.chartConfig) {
      ensureCanvas('chart-party-gen-wrap', 'chart-party-gen');
      renderChart('chart-party-gen', 'bar', bag.partyGen.chartConfig, { stacked: true });
    } else { setPlaceholder('chart-party-gen-wrap', 'Party × generation not yet processed for this scope'); }

    if (bag.participationGen && bag.participationGen.chartConfig) {
      ensureCanvas('chart-participation-gen-wrap', 'chart-participation-gen');
      renderChart('chart-participation-gen', 'bar', bag.participationGen.chartConfig, { percentY: true });
    } else { setPlaceholder('chart-participation-gen-wrap', 'Participation data not yet processed for this scope'); }

    if (bag.uncShadow && bag.uncShadow.chartConfig) {
      ensureCanvas('chart-unc-wrap', 'chart-unc');
      renderChart('chart-unc', 'bar', bag.uncShadow.chartConfig, { stacked: true });
    } else { setPlaceholder('chart-unc-wrap', 'UNC shadow not yet processed'); }

    if (bag.level === 'city' || bag.level === 'township' || bag.level === 'village' || bag.level === 'ward') {
      setPlaceholder('city-table-wrap', 'Precinct-level charts above \u2014 select a precinct for detailed data');
    } else if (bag.citySummary && bag.citySummary.rows) {
      renderTable($('city-table-wrap'), bag.citySummary);
    } else if ($('city-table-wrap')) {
      setPlaceholder('city-table-wrap', S.level === 'county' ? 'City/township summary not yet processed' : 'Not applicable at this scope');
    }
  }

  // Sortable table for citySummary
  const tableState = { sortKey: null, sortDir: 'desc' };
  function renderTable(wrap, ds) {
    if (!wrap || !ds.rows) return;
    const headers = ds.headers || [];
    function parseNum(s) { return Number(String(s).replace(/,/g, '')); }
    let rows = ds.rows.slice();
    if (tableState.sortKey !== null) {
      const idx = tableState.sortKey;
      rows.sort((a, b) => {
        const av = a[idx], bv = b[idx];
        const an = parseNum(av), bn = parseNum(bv);
        const numeric = !isNaN(an) && !isNaN(bn);
        if (numeric) return tableState.sortDir === 'asc' ? an - bn : bn - an;
        return tableState.sortDir === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
      });
    }
    const ths = headers.map((h, i) => {
      const isAct = tableState.sortKey === i;
      const ind = isAct ? (tableState.sortDir === 'asc' ? '▲' : '▼') : '↕';
      return '<th data-col="' + i + '" class="' + (isAct ? 'sort-active' : '') + '">' + esc(h) + '<span class="sort-ind">' + ind + '</span></th>';
    }).join('');
    const tbody = rows.map(r =>
      '<tr>' + r.map((c, i) => '<td>' + esc(c) + '</td>').join('') + '</tr>'
    ).join('');
    wrap.innerHTML = '<table class="data-table"><thead><tr>' + ths + '</tr></thead><tbody>' + tbody + '</tbody></table>';
    wrap.querySelectorAll('th[data-col]').forEach(th => {
      th.onclick = () => {
        const col = Number(th.getAttribute('data-col'));
        if (tableState.sortKey === col) {
          tableState.sortDir = tableState.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          tableState.sortKey = col;
          tableState.sortDir = 'desc';
        }
        renderTable(wrap, ds);
      };
    });
  }

  // ── Narrative card ─────────────────────────────────────────
  function renderNarrative(bag) {
    const el = $('narrative-card');
    if (!el) return;
    if (!bag || !bag.narrative || !bag.narrative.narrative) {
      el.style.display = 'none';
      return;
    }
    el.style.display = '';
    el.innerHTML =
      '<div class="eyebrow" style="margin-bottom:6px">Jurisdiction overview</div>' +
      '<p class="narrative-text">' + esc(bag.narrative.narrative) + '</p>' +
      '<div class="narrative-meta">Data as of ' + esc(bag.narrative.updated || '') +
      ' &middot; generated by ' + esc(bag.narrative.generated_by || 'AI') + '</div>';
  }

  // ── Breadcrumb ─────────────────────────────────────────────
  function renderBreadcrumb(bag) {
    const root = $('breadcrumb');
    if (!root) return;
    const parts = [{ label: 'Ohio', action: 'state' }];
    if (bag.level === 'county') {
      parts.push({ label: bag.displayName, here: true });
    } else if (bag.level === 'city') {
      // Unified city: Ohio › City (no intermediate county — it may span several).
      // The span, if multi-county, is shown as a subtitle label by the hero.
      parts.push({ label: bag.displayName + (bag.citySpanLabel ? ' (' + bag.citySpanLabel + ')' : ''), here: true });
    } else if (bag.level === 'township' || bag.level === 'village') {
      parts.push({ label: slugToCountyName(bag.county), action: 'county', county: bag.county });
      parts.push({ label: bag.displayName, here: true });
    } else if (bag.level === 'ward') {
      parts.push({ label: slugToCountyName(bag.county), action: 'county', county: bag.county });
      // Parent municipality segment (City/Township/Village), if the ward
      // entity's parent was resolved. Absent (e.g. parent index lookup
      // failed) -> skip straight to the ward, still under the county.
      if (bag.wardParentSlug && bag.wardParentType && bag.wardParentName) {
        parts.push({ label: bag.wardParentName, action: 'place', placeType: bag.wardParentType, placeId: bag.wardParentSlug });
      }
      parts.push({ label: bag.displayName, here: true });
    } else if (bag.level === 'precinct') {
      parts.push({ label: slugToCountyName(bag.county), action: 'county', county: bag.county });
      parts.push({ label: bag.displayName, here: true });
    } else if (bag.level === 'district') {
      parts.push({ label: (S.district_type || '').replace(/_/g, ' ').replace('district', '').trim().replace(/\b\w/g, c => c.toUpperCase()) || 'Districts', action: 'dtype', dtype: S.district_type });
      parts.push({ label: 'District ' + bag.displayName, here: true });
    }
    root.innerHTML = parts.map((p, i) => {
      const sep = i > 0 ? '<span class="sep">›</span> ' : '';
      if (p.here) return sep + '<span class="here">' + esc(p.label) + '</span>';
      return sep + '<a data-bc="' + (p.action || '') + '" data-county="' + esc(p.county || '') + '" data-dtype="' + esc(p.dtype || '') + '" data-place-type="' + esc(p.placeType || '') + '" data-place-id="' + esc(p.placeId || '') + '">' + esc(p.label) + '</a>';
    }).join(' ');
    root.querySelectorAll('a[data-bc]').forEach(a => {
      a.onclick = async () => {
        const action = a.getAttribute('data-bc');
        if (action === 'state') {
          S.level = 'county'; S.id = DEFAULT_COUNTY;
          writeState({ level: 'county', id: DEFAULT_COUNTY });
        } else if (action === 'county') {
          const slug = a.getAttribute('data-county');
          S.level = 'county'; S.id = countyToSlug(slug);
          writeState({ level: 'county', id: S.id });
        } else if (action === 'dtype') {
          S.level = 'county'; S.id = DEFAULT_COUNTY; S.district_type = null;
          writeState({ level: 'county', id: DEFAULT_COUNTY, type: null });
        } else if (action === 'place') {
          const ptype = a.getAttribute('data-place-type');
          const pid = a.getAttribute('data-place-id');
          S.level = ptype; S.id = pid;
          writeState({ level: ptype, id: pid });
        }
        await refreshView();
      };
    });
  }

  // ── Center pane skeleton ───────────────────────────────────
  function chartCardHTML(id, title, sub) {
    return '' +
    '<div class="chart-card" data-chart-id="' + id + '">' +
      '<header>' +
        '<div>' +
          '<h3>' + esc(title) + '</h3>' +
          (sub ? '<div class="card-sub">' + esc(sub) + '</div>' : '') +
        '</div>' +
        '<div class="card-actions">' +
          '<button class="icon-btn" data-menu-trigger="' + id + '" title="Chart actions">⋯</button>' +
          '<div class="menu-pop" data-menu-pop="' + id + '">' +
            '<button data-export="png" data-cid="' + id + '">Export PNG</button>' +
            '<button data-export="json" data-cid="' + id + '">Export JSON</button>' +
          '</div>' +
        '</div>' +
      '</header>' +
      '<div class="chart-canvas-wrap" id="' + id + '-wrap"><canvas id="' + id + '"></canvas></div>' +
      (id === 'chart-party' ? '<div class="chart-legend-inline" id="chart-party-legend"></div>' : '') +
    '</div>';
  }

  function buildCenterPaneSingle() {
    const cp = $('center-pane');
    cp.innerHTML = '' +
      '<div class="breadcrumb" id="breadcrumb"></div>' +
      '<div class="hero" id="hero"></div>' +
      '<div class="narrative-card" id="narrative-card" style="display:none"></div>' +
      '<div class="charts-grid">' +
        chartCardHTML('chart-party',         'Party Affiliation', 'Partisan spectrum') +
        chartCardHTML('chart-decade',        'Voter Age by Birth Decade') +
        chartCardHTML('chart-party-decade',  'Party × Birth Decade') +
        chartCardHTML('chart-gen',           'Generation Distribution') +
        chartCardHTML('chart-party-gen',     'Party × Generation') +
        chartCardHTML('chart-participation-gen', 'Primary Participation Rate by Generation', 'Regular primaries + generals since registering; specials excluded') +
        chartCardHTML('chart-unc',           'UNC Voter Behavior', 'Inferred from primary ballot history') +
        '<div class="chart-card chart-card--wide" data-chart-id="city-table">' +
          '<header><div><h3>Registration by City / Township</h3><div class="card-sub">Sortable; rows are precincts grouped by name prefix</div></div>' +
            '<div class="card-actions"><button class="icon-btn" data-menu-trigger="city-table">⋯</button>' +
              '<div class="menu-pop" data-menu-pop="city-table">' +
                '<button data-export="json" data-cid="city-table">Export JSON</button>' +
              '</div>' +
            '</div>' +
          '</header>' +
          '<div id="city-table-wrap" class="chart-canvas-wrap"></div>' +
        '</div>' +
      '</div>';
  }

  function buildCenterPaneCompare(slotA, slotB) {
    const cp = $('center-pane');
    const aRaw = S.compare && S.compare[0];
    const bRaw = S.compare && S.compare[1];
    slotA = slotA || parseSlot(aRaw);
    slotB = slotB || parseSlot(bRaw);
    const a = slotA ? slotLabel(slotA) : '—';
    const b = slotB ? slotLabel(slotB) : '—';
    cp.innerHTML = '' +
      '<div class="breadcrumb full" id="breadcrumb"></div>' +
      '<section class="compare-pane" data-slot="a">' +
        '<div class="compare-pane-header"><span>Slot A</span><span class="name">' + esc(a) + '</span><button data-action="swap-slot" data-slot="a">change</button></div>' +
        '<div class="hero" id="hero-a"></div>' +
      '</section>' +
      '<section class="compare-pane" data-slot="b">' +
        '<div class="compare-pane-header"><span>Slot B</span><span class="name">' + esc(b) + '</span><button data-action="swap-slot" data-slot="b">change</button></div>' +
        '<div class="hero" id="hero-b"></div>' +
      '</section>' +
      '<section class="compare-pane" data-slot="a">' +
        chartCardHTML('chart-party-a',        'Party · ' + a, 'Partisan spectrum') +
      '</section>' +
      '<section class="compare-pane" data-slot="b">' +
        chartCardHTML('chart-party-b',        'Party · ' + b, 'Partisan spectrum') +
      '</section>' +
      '<section class="compare-pane" data-slot="a">' +
        chartCardHTML('chart-decade-a',       'Age · ' + a) +
      '</section>' +
      '<section class="compare-pane" data-slot="b">' +
        chartCardHTML('chart-decade-b',       'Age · ' + b) +
      '</section>' +
      '<section class="compare-pane" data-slot="a">' +
        chartCardHTML('chart-party-decade-a', 'Party × Age · ' + a) +
      '</section>' +
      '<section class="compare-pane" data-slot="b">' +
        chartCardHTML('chart-party-decade-b', 'Party × Age · ' + b) +
      '</section>' +
      '<section class="compare-pane" data-slot="a">' +
        chartCardHTML('chart-unc-a',          'UNC · ' + a) +
      '</section>' +
      '<section class="compare-pane" data-slot="b">' +
        chartCardHTML('chart-unc-b',          'UNC · ' + b) +
      '</section>';
  }

  // ── Compare rendering ──────────────────────────────────────
  async function refreshCompareView() {
    if (!S.compare || S.compare.length === 0) return;
    const slotA = parseSlot(S.compare[0]) || { kind: 'county', id: DEFAULT_COUNTY };
    const slotB = parseSlot(S.compare[1]) || { kind: 'county', id: 'franklin' };
    buildCenterPaneCompare(slotA, slotB);

    async function loadSlot(slot) {
      if (slot.kind === 'district') {
        S.district_type = slot.dtype;
        return await loadJurisdiction('district', slot.id, null);
      }
      if (slot.kind === 'precinct') {
        return await loadJurisdiction('precinct', slot.id, slot.county);
      }
      return await loadJurisdiction('county', slot.id, null);
    }

    const [aBag, bBag] = await Promise.all([loadSlot(slotA), loadSlot(slotB)]);
    aBag.displayName = slotLabel(slotA);
    bBag.displayName = slotLabel(slotB);

    renderHero($('hero-a'), aBag);
    renderHero($('hero-b'), bBag);

    if (aBag.party && aBag.party.chartConfig) renderChart('chart-party-a', 'doughnut', aBag.party.chartConfig);
    if (bBag.party && bBag.party.chartConfig) renderChart('chart-party-b', 'doughnut', bBag.party.chartConfig);
    if (aBag.decade && aBag.decade.chartConfig) renderChart('chart-decade-a', 'bar', aBag.decade.chartConfig);
    if (bBag.decade && bBag.decade.chartConfig) renderChart('chart-decade-b', 'bar', bBag.decade.chartConfig);
    if (aBag.partyDecade && aBag.partyDecade.chartConfig) renderChart('chart-party-decade-a', 'bar', aBag.partyDecade.chartConfig, { stacked: true });
    if (bBag.partyDecade && bBag.partyDecade.chartConfig) renderChart('chart-party-decade-b', 'bar', bBag.partyDecade.chartConfig, { stacked: true });
    if (aBag.uncShadow && aBag.uncShadow.chartConfig) renderChart('chart-unc-a', 'bar', aBag.uncShadow.chartConfig, { stacked: true });
    if (bBag.uncShadow && bBag.uncShadow.chartConfig) renderChart('chart-unc-b', 'bar', bBag.uncShadow.chartConfig, { stacked: true });

    // Breadcrumb shows comparison context
    const root = $('breadcrumb');
    const ctx = (slotA.kind === 'district' || slotB.kind === 'district') ? 'Districts' : 'Geography';
    root.innerHTML =
      '<span class="here">Compare</span> <span class="sep">·</span> ' +
      '<a data-bc="state">' + ctx + '</a> <span class="sep">›</span> ' +
      esc(aBag.displayName) + ' <b style="color:var(--accent);margin:0 4px">⇄</b> ' + esc(bBag.displayName);
    root.querySelectorAll('a[data-bc]').forEach(a => a.onclick = () => { S.compare = null; writeState({ compare: null }); refreshView(); });

    // Wire slot swap buttons — open a scrollable picker popover instead of a prompt.
    // Clicking the button also "focuses" that slot, so subsequent map clicks
    // update the correct slot.
    document.querySelectorAll('[data-action="swap-slot"]').forEach(btn => {
      btn.onclick = async (e) => {
        e.stopPropagation();
        const slot = btn.getAttribute('data-slot');
        window._focusedSlot = slot;
        document.querySelectorAll('.compare-pane-header').forEach(h => h.classList.toggle('is-focused', h.closest('[data-slot="' + slot + '"]') !== null && h.parentNode.getAttribute('data-slot') === slot));
        const current = slot === 'a' ? slotA : slotB;
        const dtype = current.kind === 'district' ? current.dtype : null;
        await openSlotPicker(btn, slot, dtype, current);
      };
    });

    wireMenus();
    return [aBag, bBag];
  }

  async function listCompareChoices(dtype) {
    if (dtype) {
      try {
        const idx = await fetchJSON('data/' + dtype + '/index.json');
        const list = Array.isArray(idx) ? idx : (idx.districts || []);
        return list.map(d => ({ id: d.slug, label: 'District ' + d.slug, sub: (d.voter_count || 0).toLocaleString() + ' voters' }));
      } catch (e) { return []; }
    }
    return manifestCounties().map(name => ({ id: countyToSlug(name), label: name, sub: '' }));
  }

  async function openSlotPicker(anchorBtn, slot, dtype, current) {
    // Remove any open picker
    document.querySelectorAll('.slot-picker-pop').forEach(el => el.remove());
    const choices = await listCompareChoices(dtype);
    const pop = document.createElement('div');
    pop.className = 'slot-picker-pop';
    pop.innerHTML =
      '<div class="spp-search"><input type="search" placeholder="Filter…" autofocus></div>' +
      '<div class="spp-hint">' + (dtype ? 'or click a district on the map' : 'or click a county on the map') + '</div>' +
      '<div class="spp-list">' +
        choices.map(c => '<button class="spp-item" data-id="' + esc(c.id) + '"><span class="spp-name">' + esc(c.label) + '</span><span class="spp-sub">' + esc(c.sub) + '</span></button>').join('') +
      '</div>';
    document.body.appendChild(pop);

    // Position below the anchor button
    const rect = anchorBtn.getBoundingClientRect();
    pop.style.left = Math.max(8, rect.left) + 'px';
    pop.style.top = (rect.bottom + 6) + 'px';
    pop.style.minWidth = Math.max(220, rect.width + 80) + 'px';

    const inp = pop.querySelector('input');
    inp.focus();
    inp.oninput = () => {
      const q = inp.value.trim().toLowerCase();
      pop.querySelectorAll('.spp-item').forEach(b => {
        const t = b.textContent.toLowerCase();
        b.style.display = (!q || t.includes(q)) ? '' : 'none';
      });
    };
    pop.querySelectorAll('.spp-item').forEach(b => {
      b.onclick = () => {
        const id = b.getAttribute('data-id');
        const arr = S.compare.slice();
        arr[slot === 'a' ? 0 : 1] = dtype
          ? 'district:' + dtype + ':' + String(id).padStart(2, '0')
          : id;
        S.compare = arr;
        writeState({ compare: arr });
        emit('compare_update', { slot, value: arr[slot === 'a' ? 0 : 1] });
        pop.remove();
        refreshView();
      };
    });

    // Dismiss on outside click
    setTimeout(() => {
      document.addEventListener('click', function once(e) {
        if (!pop.contains(e.target) && e.target !== anchorBtn) {
          pop.remove();
          document.removeEventListener('click', once);
        }
      });
    }, 0);
  }


  // ── Main refresh ───────────────────────────────────────────
  async function refreshView() {
    applyChrome();

    if (!cache.manifest) {
      try { cache.manifest = await fetchJSON('manifest.json'); }
      catch (e) { console.error('manifest load fail', e); }
    }

    // Deep-link recovery: a township/village/ward URL may arrive with no
    // ?county= (shared link, old history entry). Backfill S.county from the
    // place's own index.json before the hierarchy tree or breadcrumb read it.
    if ((S.level === 'township' || S.level === 'village' || S.level === 'ward') && !S.county) {
      const cs = await resolvePlaceCounty(S.level, S.id);
      if (cs) { S.county = cs; writeState({ county: cs }, true); }
    }

    // Hierarchy
    await renderHierarchy();

    // Compare branch
    if (S.compare && S.compare.length) {
      await refreshCompareView();
    } else {
      buildCenterPaneSingle();
      destroyAll();

      let bag;
      if (S.level === 'county') {
        bag = await loadJurisdiction('county', S.id, null);
        bag.displayName = slugToCountyName(S.id);
      } else if (S.level === 'city') {
        // id is the city slug. A city view is always the unified, all-county
        // report — every county the city spans, unioned.
        bag = await loadJurisdiction('city', S.id, null);
        const span = bag.spanCounties || [];
        bag.displayName = bag.cityName || (S.id || '').replace(/_/g, ' ').toUpperCase();
        // Honest multi-county label when the city spans >1 county.
        bag.citySpanLabel = span.length > 1 ? span.map(slugToCountyName).join(' + ') : null;
        bag.county = null;
      } else if (S.level === 'township' || S.level === 'village') {
        bag = await loadJurisdiction(S.level, S.id, S.county);
        bag.displayName = (bag.party && bag.party.jurisdiction_name) || S.id.replace(/_/g, ' ').toUpperCase();
        bag.county = S.county || null;
      } else if (S.level === 'ward') {
        bag = await loadJurisdiction('ward', S.id, S.county);
        bag.displayName = (bag.party && bag.party.jurisdiction_name) || S.id.replace(/_/g, ' ').toUpperCase();
        bag.county = S.county || null;
      } else if (S.level === 'precinct') {
        bag = await loadJurisdiction('precinct', S.id, S.county || DEFAULT_COUNTY);
        bag.displayName = (bag.party && bag.party.precinct) || S.id.replace(/_/g, ' ').toUpperCase();
      } else if (S.level === 'district') {
        bag = await loadJurisdiction('district', S.id, null);
        bag.displayName = S.id;
      }
      renderBreadcrumb(bag);
      renderHero($('hero'), bag);
      renderNarrative(bag);
      renderCharts(bag);
      renderMapSelection(bag);
      wireMenus();
    }

    renderMapForState();

    // GA pageview
    emit('page_view', { page_path: location.search, level: S.level, id: S.id, compare: S.compare ? S.compare.join(',') : null });

    // Compact tag in topbar
    const tag = $('compare-tag');
    if (tag) tag.style.display = S.compare ? '' : 'none';
    const cBtn = $('compare-toggle');
    if (cBtn) cBtn.classList.toggle('is-on', !!S.compare);
  }

  // ── Map dispatch ───────────────────────────────────────────
  // The single "which map does this view get?" resolver. Called by
  // refreshView() and by the background lean preload, so a late-arriving
  // preload repaints the map the CURRENT view wants rather than assuming
  // the county layer (which silently clobbered the precinct layer).
  function renderMapForState() {
    const mapEyebrow = document.querySelector('.right-pane .map-section > .eyebrow');
    // One token per dispatch; every async render below is tagged with it and
    // discards itself if a newer view has since been requested.
    const gen = nextMapGen();
    const inDistrictMode = S.view === 'district' || S.level === 'district';

    if (inDistrictMode) {
      const dtype = S.district_type || 'state_senate_district';
      const layout = DISTRICT_LAYOUTS[dtype] || DISTRICT_LAYOUTS.state_senate_district;
      if (mapEyebrow) mapEyebrow.textContent = 'Ohio · ' + layout.count + ' ' + layout.label + ' · party lean';
      let selectedId = (S.level === 'district') ? S.id : null;
      let compareIds = null;
      if (S.compare) {
        const a = parseSlot(S.compare[0]);
        const b = parseSlot(S.compare[1]);
        compareIds = [
          (a && a.kind === 'district' && a.dtype === dtype) ? a.id : null,
          (b && b.kind === 'district' && b.dtype === dtype) ? b.id : null
        ];
      }
      renderDistrictMap(dtype, selectedId, compareIds, window._districtLeans && window._districtLeans[dtype], gen);
      // Background preload of leans for this district type
      if (!(window._districtLeans && window._districtLeans[dtype])) {
        preloadDistrictLean(dtype).then(leans => {
          window._districtLeans = window._districtLeans || {};
          window._districtLeans[dtype] = leans;
          renderDistrictMap(dtype, selectedId, compareIds, leans, gen);
        });
      }
    } else if (S.compare) {
      if (mapEyebrow) mapEyebrow.textContent = 'Ohio · 88 counties · party lean';
      const a = parseSlot(S.compare[0]);
      const b = parseSlot(S.compare[1]);
      const aName = (a && a.kind === 'county') ? slugToCountyName(a.id) : null;
      const bName = (b && b.kind === 'county') ? slugToCountyName(b.id) : null;
      renderHexMap(window._leans, null, [aName, bName].filter(Boolean), gen);
    } else {
      // Statewide overview: the URL carries no explicit ?id=, so nothing is
      // selected and the map draws all 88 undimmed. S.level/S.id still read
      // the default county for the CENTER pane's benefit (no statewide data file
      // exists), so the map scope is derived from the URL rather than from
      // S.id -- otherwise switching back to Geography arrives with 87 counties
      // greyed around one arbitrary highlight.
      const explicitId = new URLSearchParams(location.search).get('id');

      // For a city, S.id is a CITY slug (not a county) and the view is always
      // the unified all-county report, so there's no single county to highlight.
      // A precinct URL may arrive with no ?county= (shared link, old history
      // entry). loadJurisdiction already falls back to DEFAULT_COUNTY for the
      // center pane, so the map must use the SAME fallback or the two panes
      // disagree -- the hero would read a Montgomery precinct while the map
      // showed the undimmed 88-county layer.
      const precinctCounty = S.county || DEFAULT_COUNTY;

      // For a city, S.id is a CITY slug (not a county) and the view is always
      // the unified all-county report, so there's no single county to highlight.
      const name = !explicitId ? null :
                   S.level === 'county' ? slugToCountyName(S.id) :
                   S.level === 'precinct' ? slugToCountyName(precinctCounty) : null;

      // Which county's precinct geometry could serve this view? A county view
      // uses S.id; every sub-county level carries S.county. A bare city slug
      // spans counties by design (the unified report), so it has no single
      // county and stays on the statewide map.
      const geoCounty = !explicitId ? null :
                        S.level === 'county' ? S.id :
                        S.level === 'precinct' ? precinctCounty :
                        SUBCOUNTY_LEVELS.includes(S.level) ? (S.county || null) : null;

      // A multi-county city (the unified view, S.county === null) has no single
      // county to highlight, so the map used to sit unchanged on the statewide
      // layer -- clicking any of the 21 spanning cities appeared to do nothing.
      // We have no city boundary polygons outside Montgomery, so we cannot draw
      // the city itself; but city_county_map.json already tells us exactly
      // which counties it touches, so light those and grey the rest. Uses the
      // same activeKeys set the precinct layer uses -- no new geodata needed.
      const spanSlugs = (!explicitId || S.level !== 'city') ? null
                        : (window._citySpan && window._citySpan[S.id]) || null;

      const drawCounties = () => {
        if (mapEyebrow) {
          // S.city is only set when the city was picked from the tree; a deep
          // link carries just the slug, so fall back to un-slugging rather
          // than printing a bare "City".
          const cityLabel = S.city ||
            (S.id || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
          mapEyebrow.textContent = spanSlugs && spanSlugs.length
            ? cityLabel + ' · spans ' + spanSlugs.length + ' counties · party lean'
            : 'Ohio · 88 counties · party lean';
        }
        renderHexMap(window._leans, name, null, gen, spanSlugs);
      };

      if (geoCounty && !_precinctGeoMiss[geoCounty]) {
        // Falls back only on a genuine miss; the probe caches the 404 so a
        // county without shapes costs one request per session, not per view.
        renderPrecinctMap(geoCounty, gen).then(ok => {
          if (gen !== _mapGen) return;              // superseded meanwhile
          if (ok) {
            if (mapEyebrow) {
              mapEyebrow.textContent = slugToCountyName(geoCounty) + ' County · precincts · party lean';
            }
          } else {
            drawCounties();
          }
        });
      } else {
        drawCounties();
      }
    }
  }

  // ── Menus + exports ────────────────────────────────────────
  function wireMenus() {
    document.querySelectorAll('[data-menu-trigger]').forEach(t => {
      const id = t.getAttribute('data-menu-trigger');
      t.onclick = (e) => {
        e.stopPropagation();
        const popAll = document.querySelectorAll('.menu-pop');
        popAll.forEach(p => { if (p.getAttribute('data-menu-pop') !== id) p.classList.remove('is-open'); });
        const pop = document.querySelector('[data-menu-pop="' + id + '"]');
        if (pop) pop.classList.toggle('is-open');
      };
    });
    document.addEventListener('click', () => {
      document.querySelectorAll('.menu-pop.is-open').forEach(p => p.classList.remove('is-open'));
    });
    document.querySelectorAll('[data-export]').forEach(btn => {
      btn.onclick = async (e) => {
        e.stopPropagation();
        const fmt = btn.getAttribute('data-export');
        const cid = btn.getAttribute('data-cid');
        await exportChart(cid, fmt);
        document.querySelectorAll('.menu-pop.is-open').forEach(p => p.classList.remove('is-open'));
      };
    });
  }

  async function exportChart(chartId, fmt) {
    if (fmt === 'png') {
      const chart = chartInstances[chartId];
      if (!chart) { toast('Chart not ready'); return; }
      const link = document.createElement('a');
      link.download = chartId + '.png';
      link.href = chart.toBase64Image('image/png', 1);
      link.click();
      emit('export_chart', { chart_id: chartId, format: 'png' });
      toast('PNG exported');
    } else if (fmt === 'json') {
      // Re-derive source URL based on current state + chart id
      const slug = S.level === 'county' ? S.id : (S.county || DEFAULT_COUNTY);
      const map = {
        'chart-party':        `data/${slug}_party_affiliation.json`,
        'chart-decade':       `data/${slug}_decade_distribution.json`,
        'chart-party-decade': `data/${slug}_party_by_decade.json`,
        'chart-gen':          `data/${slug}_generation_distribution.json`,
        'chart-party-gen':    `data/${slug}_party_by_generation.json`,
        'chart-participation-gen': `data/${slug}_participation_by_generation.json`,
        'chart-unc':          `data/${slug}_unc_shadow.json`,
        'city-table':         `data/${slug}_city_summary.json`
      };
      const url = map[chartId];
      if (!url) { toast('JSON source unknown'); return; }
      try {
        const data = await fetchJSON(url);
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const link = document.createElement('a');
        link.download = chartId + '.json';
        link.href = URL.createObjectURL(blob);
        link.click();
        URL.revokeObjectURL(link.href);
        emit('export_chart', { chart_id: chartId, format: 'json' });
        toast('JSON exported');
      } catch (e) { toast('Export failed'); }
    }
  }

  function toast(msg) {
    const existing = document.querySelector('.toast');
    if (existing) existing.remove();
    const el = document.createElement('div');
    el.className = 'toast';
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 1800);
  }

  // ── Tweaks panel ───────────────────────────────────────────
  function buildTweaksPanel() {
    const p = $('tweaks-panel');
    if (!p) return;
    p.innerHTML = '' +
      '<header><h4>Tweaks</h4><button data-close-tweaks>×</button></header>' +
      tweakSeg('style',   'Visual style',  ['current','editorial']) +
      tweakSeg('theme',   'Theme',         ['light','dark']) +
      tweakSeg('layout',  'Layout',        ['3col','2col','single']) +
      tweakSeg('density', 'Density',       ['comfortable','compact']) +
      tweakSeg('view',    'Hierarchy view',['geo','district']);
    p.querySelector('[data-close-tweaks]').onclick = () => p.classList.add('hidden');
    p.querySelectorAll('[data-tweak]').forEach(btn => {
      btn.onclick = () => {
        const key = btn.getAttribute('data-tweak');
        const val = btn.getAttribute('data-val');
        S[key] = val;
        writeState({ [key]: val });
        emit('tweak_change', { key, value: val });
        if (typeof window.gtag === 'function') {
          window.gtag('set', 'user_properties', { [key]: val });
        }
        if (key === 'view') { renderHierarchy(); }
        applyChrome();
        // Some changes need rerender (compact spacings, layout swap)
        if (['layout','density','style'].includes(key)) refreshView();
        buildTweaksPanel(); // refresh active state
      };
    });
  }
  function tweakSeg(key, label, opts) {
    return '<div class="tweak-group"><label class="lbl">' + label + '</label><div class="tweak-seg">' +
      opts.map(o => '<button data-tweak="' + key + '" data-val="' + o + '" class="' + (S[key] === o ? 'active' : '') + '">' + o + '</button>').join('') +
      '</div></div>';
  }

  // ── Search ─────────────────────────────────────────────────
  // Rows the search has force-opened (city/county `.hier-children` wrappers
  // that default to collapsed — v2.css hides them regardless of a matching
  // descendant's own display style). Tracked so a cleared search can close
  // only what it opened, not state the user toggled manually.
  let _searchOpened = [];
  function clearSearchOpens() {
    _searchOpened.forEach(el => el.classList.remove('is-open'));
    _searchOpened = [];
  }
  function wireSearch() {
    const inp = $('global-search');
    if (!inp) return;
    inp.oninput = () => {
      const q = inp.value.trim().toLowerCase();
      clearSearchOpens();
      if (!q) {
        document.querySelectorAll('.hier-row[data-action]').forEach(r => r.style.display = '');
        return;
      }
      // Filter visible rows in hierarchy
      document.querySelectorAll('.hier-row[data-action]').forEach(r => {
        const lbl = (r.textContent || '').toLowerCase();
        const isMatch = lbl.includes(q);
        r.style.display = isMatch ? '' : 'none';
        if (isMatch) {
          // A matching row can be nested inside a collapsed city/county
          // bucket; walk up and open every ancestor so it's actually visible.
          let anc = r.parentElement;
          while (anc) {
            if (anc.classList.contains('hier-children') && !anc.classList.contains('is-open')) {
              anc.classList.add('is-open');
              _searchOpened.push(anc);
            }
            anc = anc.parentElement;
          }
        }
      });
    };
    inp.onblur = () => {
      if (!inp.value) {
        document.querySelectorAll('.hier-row[data-action]').forEach(r => r.style.display = '');
        clearSearchOpens();
      }
    };
    inp.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        emit('search', { search_term: inp.value });
        // Jump to first visible row
        const first = Array.from(document.querySelectorAll('.hier-row[data-action]')).find(r => r.style.display !== 'none');
        if (first) first.click();
      }
    });
  }

  // ── Topbar wiring ──────────────────────────────────────────
  function wireTopbar() {
    $('theme-toggle').onclick = () => {
      S.theme = S.theme === 'dark' ? 'light' : 'dark';
      writeState({ theme: S.theme });
      applyChrome();
      emit('theme_change', { theme: S.theme });
      if (typeof window.gtag === 'function') {
        window.gtag('set', 'user_properties', { theme: S.theme });
      }
      refreshView();
    };
    $('tweaks-toggle').onclick = () => {
      const p = $('tweaks-panel');
      buildTweaksPanel();
      p.classList.toggle('hidden');
    };
    $('compare-toggle').onclick = () => {
      if (S.compare) {
        S.compare = null;
        writeState({ compare: null });
      } else if (S.view === 'district' || S.level === 'district') {
        // District-vs-district default pair within the same type
        const dtype = S.district_type || 'state_senate_district';
        const layout = DISTRICT_LAYOUTS[dtype];
        const baseId = (S.level === 'district' ? S.id : null) || '09';
        const pad = n => String(n).padStart(2, '0');
        const otherId = pad((parseInt(baseId, 10) % layout.count) + 1);
        const arr = ['district:' + dtype + ':' + baseId, 'district:' + dtype + ':' + otherId];
        S.compare = arr;
        writeState({ compare: arr });
      } else {
        // County-vs-county default pair
        const a = S.level === 'county' ? S.id : DEFAULT_COUNTY;
        const b = a === 'franklin' ? 'cuyahoga' : 'franklin';
        S.compare = [a, b];
        writeState({ compare: [a, b] });
      }
      emit('compare_toggle', { on: !!S.compare });
      refreshView();
    };
    // Geography / Districts toggle.
    //
    // These buttons previously carried data-tweak="view" plus an inline onclick
    // firing a `view-set` CustomEvent that NOTHING listened for. The only
    // [data-tweak] handler is bound inside buildTweaksPanel() and reaches just
    // the buttons in that panel, so the toolbar pair was inert: the "click"
    // was really the inline handler reloading the page with ?view=..., which
    // left the map showing an all-dimmed layer (nothing selected yet) and the
    // .active class stuck on Geography.
    //
    // Switching view is a real view change, so it goes through refreshView()
    // -> renderMapForState(), the single map dispatch. Active styling is NOT
    // set here: applyChrome() already publishes S.view to html[data-view], and
    // v2.css styles the buttons off that -- one source of truth, no class to
    // drift.
    document.querySelectorAll('[data-view-btn]').forEach(b => {
      b.onclick = async () => {
        const val = b.getAttribute('data-view-btn');
        if (S.view === val) return;
        S.view = val;
        // A selection made in one view cannot be shown by the other, so reset
        // to that view's own overview rather than stranding S.level/S.id.
        //
        // There is no S.level === 'state': the hierarchy's own "Ohio" row
        // (select-state, :1089) falls back to the default county because no
        // state-wide data file exists for all 88. Follow that same convention
        // here -- inventing a 'state' level would leave refreshView() with no
        // matching loadJurisdiction branch and an empty center pane.
        S.county = null; S.city = null; S.compare = null;
        if (val === 'district') {
          // The hierarchy lists Congressional first; match it rather than
          // inheriting whatever district type was last viewed. Open on
          // District 1 -- there is no all-districts aggregate file, so a null
          // id would send loadJurisdiction('district', null) looking for a
          // file that cannot exist and render an empty, broken-looking pane.
          S.district_type = 'congressional_district';
          S.level = 'district'; S.id = DEFAULT_DISTRICT_ID;
          writeState({ view: val, level: 'district', id: DEFAULT_DISTRICT_ID,
                       county: null, city: null,
                       type: 'congressional_district', compare: null });
        } else {
          // Geography's overview is the whole state. The center pane still
          // needs a jurisdiction to report on (no statewide data file exists
          // for all 88 -- see select-state, :1089), so S.level/S.id fall back
          // to the default county exactly as that row does. The MAP, though, must
          // show all 88 undimmed: that is driven by the absence of ?id= in the
          // URL, which renderMapForState() reads back -- not by a flag here.
          S.district_type = null;
          S.level = 'county'; S.id = DEFAULT_COUNTY;
          writeState({ view: val, level: null, id: null, county: null,
                       city: null, type: null, compare: null });
        }
        emit('view_change', { view: val });
        applyChrome();
        await refreshView();
      };
    });

    // Drawer toggles
    document.querySelectorAll('[data-drawer]').forEach(b => {
      b.onclick = () => {
        const side = b.getAttribute('data-drawer');
        const target = side === 'left' ? document.querySelector('.left-pane') : document.querySelector('.right-pane');
        const bd = document.querySelector('.drawer-backdrop');
        const isOpen = target.classList.toggle('is-open');
        document.querySelectorAll('.left-pane.is-open, .right-pane.is-open').forEach(p => { if (p !== target) p.classList.remove('is-open'); });
        bd.classList.toggle('is-visible', !!document.querySelector('.left-pane.is-open, .right-pane.is-open'));
      };
    });
    document.querySelector('.drawer-backdrop').onclick = closeDrawers;
  }

  // ── Boot ───────────────────────────────────────────────────
  async function boot() {
    applyChrome();
    wireTopbar();
    wireSearch();
    // Initial skeleton
    buildCenterPaneSingle();

    // Map first paint (no leans yet). Tagged with the current generation so
    // refreshView()'s own render supersedes it rather than racing it.
    renderHexMap({}, S.level === 'county' ? slugToCountyName(S.id) : null, null, nextMapGen());

    await refreshView();

    // Preload leans for the map in the background
    // Skip when in district view — the district map fetches its own leans.
    if (S.view !== 'district' && S.level !== 'district') {
      preloadCountyLean().then(leans => {
        window._leans = leans;
        // Re-check state at fire time — user may have switched to district view
        if (S.view === 'district' || S.level === 'district') return;
        // 88 party-JSON fetches land well after the first paint, so this used
        // to repaint the COUNTY map unconditionally -- overwriting a precinct
        // layer that had already rendered for the current view. Re-dispatch
        // through refreshView()'s own map branch instead of deciding here;
        // that keeps one map-choice resolver rather than a second copy that
        // silently drifts (CLAUDE.md section 5).
        renderMapForState();
      }).catch(e => console.warn('lean preload error', e));
    }

    window.addEventListener('popstate', () => {
      Object.assign(S, readState());
      refreshView();
    });
  }
  document.addEventListener('DOMContentLoaded', boot);
})();
