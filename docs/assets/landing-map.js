/* ============================================================
   Precincts.info landing map
   Inline-SVG choropleth of Ohio with pan / zoom / pinch and a
   4-layer switcher (counties / state house / state senate / US
   congress). Zero dependencies: no Leaflet, no tile CDN.

   Data: docs/data/state_map/*.geojson, plain EPSG:4326 lon/lat,
   each carrying a `bounds: [W,S,E,N]` and per-feature
   `properties.lean` precomputed by the map-data build. We only
   COLOR by lean here; we never recompute it.

   Projection, path building, the party-lean table, and shape
   rendering all live in docs/assets/geo-map.js (window.GeoMap),
   shared with the dashboard map in v2.js. This file owns only what
   is specific to the landing page: the layer registry, pan / zoom /
   pinch, the tooltip, and the hero stat. Do NOT re-add local copies
   of the shared helpers -- that duplication is exactly what the
   shared module removed.
   ============================================================ */
(function () {
  'use strict';

  var GeoMap = window.GeoMap;
  if (!GeoMap) { console.error('[landing-map] geo-map.js must load first'); return; }

  var SVG_NS            = GeoMap.SVG_NS;
  var computeProjection = GeoMap.computeProjection;
  var leanLabel         = GeoMap.leanLabel;
  var formatLean        = GeoMap.formatLean;

  // Layer registry: source file, toolbar label, count-eyebrow text,
  // the properties key each feature is identified by (`keyProp`, passed
  // through to GeoMap.render), per-feature display name, and click
  // destination on app.htm.
  var LAYERS = {
    counties: {
      file: 'data/state_map/counties.geojson',
      label: 'Counties',
      eyebrow: '88 counties',
      keyProp: 'slug',
      nameFor: function (p) { return p.name + ' County'; },
      hrefFor: function (p) { return 'app.htm?level=county&id=' + p.slug; }
    },
    house: {
      file: 'data/state_map/house.geojson',
      label: 'State House',
      eyebrow: '99 house districts',
      keyProp: 'id',
      nameFor: function (p) { return 'State House ' + p.name; },
      hrefFor: function (p) {
        return 'app.htm?level=district&type=state_representative_district&id=' + p.id;
      }
    },
    senate: {
      file: 'data/state_map/senate.geojson',
      label: 'State Senate',
      eyebrow: '33 senate districts',
      keyProp: 'id',
      nameFor: function (p) { return 'State Senate ' + p.name; },
      hrefFor: function (p) {
        return 'app.htm?level=district&type=state_senate_district&id=' + p.id;
      }
    },
    congress: {
      file: 'data/state_map/congress.geojson',
      label: 'US Congress',
      eyebrow: '15 congressional districts',
      keyProp: 'id',
      nameFor: function (p) { return 'US Congress ' + p.name; },
      hrefFor: function (p) {
        return 'app.htm?level=district&type=congressional_district&id=' + p.id;
      }
    }
  };
  var LAYER_ORDER = ['counties', 'house', 'senate', 'congress'];

  // ── DOM handles ──────────────────────────────────────────
  var svg      = document.getElementById('ohio-map');
  var viewport = document.getElementById('viewport');
  var figure   = svg ? svg.closest('figure') : null;
  var tooltip  = document.querySelector('.map-tooltip');
  var statusEl = document.getElementById('map-status');
  var eyebrowEl = document.getElementById('layer-eyebrow');
  if (!svg || !viewport) return;

  // ── State ────────────────────────────────────────────────
  var cache = {};              // layer key -> parsed geojson
  var projection = null;       // set once from the first loaded layer
  var activeLayer = 'counties';
  var view = { k: 1, tx: 0, ty: 0 };   // #viewport transform
  var MIN_K = 1, MAX_K = 40;

  function emitGaEvent(name, params) {
    if (typeof window.gtag === 'function') {
      window.gtag('event', name, params || {});
    }
  }

  function setStatus(text, isError) {
    if (!statusEl) return;
    statusEl.textContent = text || '';
    if (isError) statusEl.setAttribute('data-state', 'error');
    else statusEl.removeAttribute('data-state');
  }

  // ── Rendering ────────────────────────────────────────────
  // Projection + path building + shape rendering: GeoMap (geo-map.js).
  function clearViewport() {
    while (viewport.firstChild) viewport.removeChild(viewport.firstChild);
  }

  function renderError(msg) {
    clearViewport();
    var t = document.createElementNS(SVG_NS, 'text');
    t.setAttribute('x', String(projection ? projection.width / 2 : 500));
    t.setAttribute('y', String(projection ? projection.height / 2 : 545));
    t.setAttribute('text-anchor', 'middle');
    t.setAttribute('class', 'map-empty');
    t.textContent = msg;
    viewport.appendChild(t);
  }

  function renderLayer(key) {
    var cfg = LAYERS[key];
    var gj = cache[key];
    if (!gj || !projection) return;
    // setViewBox:false -- the <svg> viewBox is set once in loadLayer from the
    // first layer's bounds and must not be re-set here, or a layer switch
    // would reset the user's pan/zoom. Pass the shared projection for the
    // same reason: all four layers register against one coordinate space.
    GeoMap.render({
      container: viewport,
      geojson: gj,
      proj: projection,
      setViewBox: false,
      mode: 'all',
      keyProp: cfg.keyProp,
      className: 'geo-shape',
      nameFor: cfg.nameFor,
      hrefFor: cfg.hrefFor
    });
  }

  // ── Data loading (counties eager, others lazy + cached) ──
  function loadLayer(key) {
    if (cache[key]) return Promise.resolve(cache[key]);
    setStatus('Loading ' + LAYERS[key].label + '…', false);
    return fetch(LAYERS[key].file)
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (gj) {
        cache[key] = gj;
        if (!projection && Array.isArray(gj.bounds)) {
          projection = computeProjection(gj.bounds);
          svg.setAttribute('viewBox', '0 0 ' + projection.width + ' ' + projection.height.toFixed(1));
        } else if (Array.isArray(gj.bounds) && projection) {
          // All four files are expected to share identical bounds; warn
          // (do not re-fit) if the map-data build ever diverges.
          if (Math.abs(gj.bounds[0] - projection.W) > 1e-6 ||
              Math.abs(gj.bounds[3] - projection.N) > 1e-6) {
            console.warn('[landing-map] bounds mismatch on layer ' + key + '; using counties projection.');
          }
        }
        setStatus('', false);
        return gj;
      });
  }

  function switchLayer(key) {
    if (key === activeLayer && cache[key]) return;
    activeLayer = key;
    emitGaEvent('select_content', { content_type: 'map_layer', item_id: key });
    LAYER_ORDER.forEach(function (k) {
      var btn = document.querySelector('.layer-btn[data-layer="' + k + '"]');
      if (btn) btn.setAttribute('aria-pressed', k === key ? 'true' : 'false');
    });
    if (eyebrowEl) eyebrowEl.textContent = LAYERS[key].eyebrow;
    loadLayer(key)
      .then(function () { renderLayer(key); })    // transform preserved (view unchanged)
      .catch(function (e) {
        console.error('[landing-map] layer load failed:', e);
        setStatus('Could not load ' + LAYERS[key].label, true);
        renderError('Map data failed to load. Please refresh.');
      });
  }

  // ── Pan / zoom transform ─────────────────────────────────
  function clamp(v, lo, hi) { return v < lo ? lo : (v > hi ? hi : v); }

  function clampPan() {
    // Keep the scaled content covering the viewBox (no empty gutters).
    var w = projection ? projection.width : 1000;
    var h = projection ? projection.height : 1090;
    view.tx = clamp(view.tx, w * (1 - view.k), 0);
    view.ty = clamp(view.ty, h * (1 - view.k), 0);
  }
  function applyTransform() {
    viewport.setAttribute('transform',
      'translate(' + view.tx.toFixed(2) + ' ' + view.ty.toFixed(2) + ') scale(' + view.k.toFixed(4) + ')');
    syncResetBtn();
  }

  // The reset control now lives in the topbar, where it is on screen at all
  // times -- so it is hidden until the view is actually dirty, rather than
  // sitting there permanently offering to undo nothing. applyTransform() is
  // the single chokepoint every pan, zoom and reset flows through, so this
  // cannot drift out of sync with the real view state.
  function syncResetBtn() {
    var btn = document.getElementById('reset-view');
    if (!btn) return;
    btn.hidden = (view.k === 1 && view.tx === 0 && view.ty === 0);
  }
  function resetView() {
    view.k = 1; view.tx = 0; view.ty = 0;
    applyTransform();
    emitGaEvent('map_reset');
  }

  // Convert a client point to svg-root user (viewBox) coordinates,
  // correct under preserveAspectRatio letterboxing.
  function toUser(clientX, clientY) {
    var ctm = svg.getScreenCTM();
    if (!ctm) return { x: 0, y: 0, scale: 1 };
    var pt = svg.createSVGPoint();
    pt.x = clientX; pt.y = clientY;
    var u = pt.matrixTransform(ctm.inverse());
    return { x: u.x, y: u.y, scale: ctm.a };  // ctm.a = screen px per user unit
  }

  function zoomAt(userX, userY, k1) {
    k1 = clamp(k1, MIN_K, MAX_K);
    var ratio = k1 / view.k;
    view.tx = userX - (userX - view.tx) * ratio;
    view.ty = userY - (userY - view.ty) * ratio;
    view.k = k1;
    clampPan();
    applyTransform();
  }

  // ── Click-to-fit ─────────────────────────────────────────
  // When zoomed in, a shape can be mostly off-screen; clicking it used to
  // navigate away before you could see it. visibleFraction() measures how much
  // of a shape currently falls inside the viewBox, and fitToShape() frames it.
  // getBBox() is in untransformed viewport units -- the same space view.tx/ty/k
  // operate in -- so no inverse-transform is needed.
  var FIT_THRESHOLD = 0.6;   // below this visible fraction, frame before navigating
  var FIT_PADDING   = 1.25;  // leave ~25% margin around the framed shape

  function shapeBox(el) {
    try { return el.getBBox(); } catch (err) { return null; }
  }

  function visibleFraction(box) {
    // Shape bounds projected into viewBox coordinates under the current view.
    var w = projection ? projection.width : 1000;
    var h = projection ? projection.height : 1090;
    var x0 = box.x * view.k + view.tx, x1 = (box.x + box.width) * view.k + view.tx;
    var y0 = box.y * view.k + view.ty, y1 = (box.y + box.height) * view.k + view.ty;
    var area = (x1 - x0) * (y1 - y0);
    if (area <= 0) return 1;
    var ix = Math.max(0, Math.min(x1, w) - Math.max(x0, 0));
    var iy = Math.max(0, Math.min(y1, h) - Math.max(y0, 0));
    return (ix * iy) / area;
  }

  function fitToShape(box) {
    var w = projection ? projection.width : 1000;
    var h = projection ? projection.height : 1090;
    // Scale so the padded shape fits both axes, never below the current zoom
    // (framing should not zoom the user back out) and never past MAX_K.
    var k1 = Math.min(w / (box.width * FIT_PADDING), h / (box.height * FIT_PADDING));
    k1 = clamp(k1, view.k, MAX_K);
    var cx = box.x + box.width / 2, cy = box.y + box.height / 2;
    view.k = k1;
    view.tx = w / 2 - cx * k1;
    view.ty = h / 2 - cy * k1;
    clampPan();
    applyTransform();
  }

  // Wheel zoom, anchored at the cursor.
  svg.addEventListener('wheel', function (e) {
    e.preventDefault();
    var u = toUser(e.clientX, e.clientY);
    zoomAt(u.x, u.y, view.k * Math.exp(-e.deltaY * 0.0015));
  }, { passive: false });

  // Double-click: 2x zoom at the click point.
  svg.addEventListener('dblclick', function (e) {
    e.preventDefault();
    var u = toUser(e.clientX, e.clientY);
    zoomAt(u.x, u.y, view.k * 2);
  });

  // ── Pointer drag + pinch ─────────────────────────────────
  var pointers = new Map();       // pointerId -> {x, y}
  var dragging = false;
  var moved = 0;                  // total px moved in the current gesture
  var pinchDist = 0;
  // Manhattan (|dx|+|dy|) slop before a gesture counts as a drag rather than a
  // click. Also the point at which we take pointer capture -- see pointerdown.
  var DRAG_SLOP = 10;

  function pointerDist() {
    var pts = Array.from(pointers.values());
    var dx = pts[0].x - pts[1].x, dy = pts[0].y - pts[1].y;
    return Math.sqrt(dx * dx + dy * dy);
  }
  function pointerMid() {
    var pts = Array.from(pointers.values());
    return { x: (pts[0].x + pts[1].x) / 2, y: (pts[0].y + pts[1].y) / 2 };
  }

  svg.addEventListener('pointerdown', function (e) {
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pointers.size === 1) {
      dragging = true;
      moved = 0;
      svg.classList.add('dragging');
      // NOTE: do NOT setPointerCapture here. Capturing on every press retargets
      // the subsequent pointerup/click to the <svg> root, so the click never
      // reaches the <a> under the cursor and nothing navigates (the shape only
      // takes focus -- the yellow outline). Capture is acquired lazily in
      // pointermove, once the gesture is actually a drag. Verified by tracing
      // real mouse input: pointerdown targets <path>, but with capture held
      // pointerup/click both target <svg> and closest('a') is null.
    } else if (pointers.size === 2) {
      dragging = false;           // hand off to pinch
      pinchDist = pointerDist();
    }
    hideTooltip();
  });

  svg.addEventListener('pointermove', function (e) {
    if (!pointers.has(e.pointerId)) {
      // Hover (no button): tooltip only, for fine pointers.
      if (e.pointerType !== 'touch') showTooltip(e);
      return;
    }
    var prev = pointers.get(e.pointerId);
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });

    if (pointers.size === 2) {
      var dist = pointerDist();
      if (pinchDist > 0) {
        var mid = pointerMid();
        var u = toUser(mid.x, mid.y);
        zoomAt(u.x, u.y, view.k * (dist / pinchDist));
      }
      pinchDist = dist;
      return;
    }
    if (dragging) {
      var dx = e.clientX - prev.x, dy = e.clientY - prev.y;
      moved += Math.abs(dx) + Math.abs(dy);
      // Acquire capture only once this is unambiguously a drag, so panning still
      // tracks the cursor outside the SVG while a plain click keeps its target.
      if (moved > DRAG_SLOP && !svg.hasPointerCapture(e.pointerId)) {
        try { svg.setPointerCapture(e.pointerId); } catch (err) {}
      }
      var scale = toUser(e.clientX, e.clientY).scale || 1;
      view.tx += dx / scale;
      view.ty += dy / scale;
      clampPan();
      applyTransform();
    }
  });

  function endPointer(e) {
    if (pointers.has(e.pointerId)) pointers.delete(e.pointerId);
    if (pointers.size < 2) pinchDist = 0;
    // Release capture explicitly so it can never leak into the next gesture.
    if (svg.hasPointerCapture(e.pointerId)) {
      try { svg.releasePointerCapture(e.pointerId); } catch (err) {}
    }
    if (pointers.size === 0) {
      dragging = false;
      svg.classList.remove('dragging');
    }
  }
  svg.addEventListener('pointerup', endPointer);
  svg.addEventListener('pointercancel', endPointer);
  svg.addEventListener('pointerleave', function () { hideTooltip(); });

  // Suppress the click that follows a drag so a pan never navigates.
  // preventDefault() alone suppresses navigation -- do NOT add stopPropagation
  // here; it hides the event from every other listener, which made an earlier
  // bug here invisible to debugging.
  // moved resets on EVERY click, not just suppressed ones, so an interrupted
  // gesture cannot swallow the next legitimate click.
  svg.addEventListener('click', function (e) {
    var wasDrag = moved > DRAG_SLOP;
    moved = 0;
    if (wasDrag) { e.preventDefault(); return; }

    // Click-to-fit: if the clicked shape is mostly outside the viewport, the
    // first click FRAMES it instead of navigating away from a shape the user
    // cannot see; a second click then opens it. Shapes already in view navigate
    // immediately, so the common case is unchanged. Modifier/middle clicks are
    // explicit "open elsewhere" intents and always navigate.
    if (view.k <= 1 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return;
    var a = e.target.closest ? e.target.closest('a') : null;
    if (!a) return;
    var path = a.querySelector('path');
    var box = path ? shapeBox(path) : null;
    if (!box || !box.width || !box.height) return;
    if (visibleFraction(box) >= FIT_THRESHOLD) return;   // visible enough -- navigate
    e.preventDefault();
    emitGaEvent('map_fit_shape', { item_id: a.dataset.name });
    fitToShape(box);
    hideTooltip();
  }, true);

  // ── Tooltip ──────────────────────────────────────────────
  // Build the inner nodes ONCE with the DOM API (no innerHTML) so
  // untrusted feature text only ever reaches the page via textContent
  // -- consistent with the frontend rule that innerHTML is not a path.
  var ttName = null, ttLean = null, ttTotal = null;
  function initTooltip() {
    if (!tooltip || ttName) return;
    ttName = document.createElement('span'); ttName.className = 'tt-name';
    ttLean = document.createElement('span'); ttLean.className = 'tt-lean';
    ttTotal = document.createElement('span'); ttTotal.className = 'tt-total';
    tooltip.appendChild(ttName);
    tooltip.appendChild(document.createElement('br'));
    tooltip.appendChild(ttLean);
    tooltip.appendChild(document.createTextNode(' · '));
    tooltip.appendChild(ttTotal);
  }
  function showTooltip(e) {
    if (!tooltip || !figure) return;
    var a = e.target.closest ? e.target.closest('a') : null;
    if (!a || !a.dataset.name) { hideTooltip(); return; }
    ttName.textContent = a.dataset.name;
    ttLean.textContent = a.dataset.label + ' (' + a.dataset.lean + ')';
    ttTotal.textContent = a.dataset.total + ' voters';
    var rect = figure.getBoundingClientRect();
    var x = e.clientX - rect.left + 14;
    var y = e.clientY - rect.top + 14;
    // Keep inside the figure.
    x = Math.min(x, rect.width - tooltip.offsetWidth - 6);
    tooltip.style.left = Math.max(4, x) + 'px';
    tooltip.style.top = y + 'px';
    tooltip.classList.add('show');
  }
  function hideTooltip() {
    if (tooltip) tooltip.classList.remove('show');
  }

  // ── Toolbar wiring ───────────────────────────────────────
  LAYER_ORDER.forEach(function (k) {
    var btn = document.querySelector('.layer-btn[data-layer="' + k + '"]');
    if (btn) btn.addEventListener('click', function () { switchLayer(k); });
  });
  var resetBtn = document.getElementById('reset-view');
  if (resetBtn) resetBtn.addEventListener('click', resetView);

  // ── Hero stat: total registered voters ───────────────────
  // Statewide file is a doughnut chartConfig; sum whatever cohort
  // array it carries (the 9 cohort slices today) so this stays correct
  // if the cohort count changes.
  function loadHeroStat() {
    var el = document.getElementById('hero-stat');
    if (!el) return;
    fetch('data/ohio_(statewide)_party_affiliation.json')
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (j) {
        var arr = j.chartConfig.datasets[0].data;
        var sum = arr.reduce(function (a, b) { return a + Number(b || 0); }, 0);
        if (sum > 0) el.textContent = sum.toLocaleString();
      })
      .catch(function (e) {
        // Leave the static fallback baked into the HTML in place.
        console.warn('[landing-map] hero stat fetch failed:', e);
      });
  }

  // ── Boot ─────────────────────────────────────────────────
  initTooltip();
  loadHeroStat();
  loadLayer('counties')
    .then(function () {
      renderLayer('counties');
      applyTransform();
    })
    .catch(function (e) {
      console.error('[landing-map] initial load failed:', e);
      setStatus('Could not load the map', true);
      renderError('Map data failed to load. Please refresh.');
    });
})();
