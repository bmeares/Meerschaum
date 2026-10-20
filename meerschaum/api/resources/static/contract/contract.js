// Draws the page contract's elements (design §8.3) inside a plugin page served by the API host:
// `data-mrsm-figure` (Plotly figure JSON, or a path that returns it) and `data-mrsm-map`
// (MapLibre style / GeoJSON), on load and after every htmx swap.
(() => {
  // plotly.js 3 dropped string titles and `titlefont`/`titleside`/`titleposition`, and plotly.js 4
  // dropped the `*mapbox` traces: figures written for plotly.js 2 (hand-written dicts, plotly.py 5)
  // are rewritten to the current names before they are drawn. The figure JSON stays the contract.
  const MAP_TRACES = {scattermapbox: 'scattermap', choroplethmapbox: 'choroplethmap', densitymapbox: 'densitymap'};
  function fixTitle(o) {
    if (!o || typeof o !== 'object' || Array.isArray(o)) return;
    if (typeof o.title === 'string') o.title = {text: o.title};
    for (const [old, key] of [['titlefont', 'font'], ['titleside', 'side'], ['titleposition', 'position']]) {
      if (o[old] === undefined) continue;
      if (!o.title || typeof o.title !== 'object') o.title = {};
      if (o.title[key] === undefined) o.title[key] = o[old];
      delete o[old];
    }
  }
  function normalizeFigure(figure) {
    const layout = figure.layout = figure.layout && typeof figure.layout === 'object' ? figure.layout : {};
    fixTitle(layout);
    for (const key of Object.keys(layout)) {
      if (/^[xyz]?axis\d*$|^coloraxis\d*$/.test(key)) { fixTitle(layout[key]); fixTitle(layout[key] && layout[key].colorbar); }
      else if (key === 'scene' || /^scene\d+$/.test(key)) for (const axis of ['xaxis', 'yaxis', 'zaxis']) fixTitle(layout[key] && layout[key][axis]);
      else if (/^mapbox\d*$/.test(key)) { layout[key.replace('mapbox', 'map')] = layout[key]; delete layout[key]; }
    }
    for (const trace of figure.data || []) {
      if (!trace || typeof trace !== 'object') continue;
      if (MAP_TRACES[trace.type]) trace.type = MAP_TRACES[trace.type];
      if (typeof trace.subplot === 'string') trace.subplot = trace.subplot.replace(/^mapbox/, 'map');
      fixTitle(trace.colorbar); fixTitle(trace.marker && trace.marker.colorbar); fixTitle(trace.line && trace.line.colorbar);
    }
    return figure;
  }
  let maplibrePromise = null;
  // MapLibre GL JS 6 is an ES module (its worker is a sibling module): load it on the first map.
  function loadMaplibre() {
    if (!maplibrePromise) {
      const link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = '/static/contract/maplibre-gl.css';
      document.head.append(link);
      maplibrePromise = import('/static/contract/maplibre-gl.mjs').catch(e => { maplibrePromise = null; throw e; });
    }
    return maplibrePromise;
  }
  function render() {
    document.querySelectorAll('[data-mrsm-figure]:not([data-rendered])').forEach(async el => {
      el.dataset.rendered = 'pending';
      try {
        const raw = el.dataset.mrsmFigure;
        const figure = normalizeFigure(raw.startsWith('/') ? await (await fetch(raw)).json() : JSON.parse(raw));
        await Plotly.newPlot(el, figure.data || [], figure.layout || {}, {showSendToCloud: false, ...figure.config, responsive: true});
        el.dataset.rendered = 'true';
      } catch (_) { delete el.dataset.rendered; el.textContent = 'Chart could not be displayed.'; }
    });
    document.querySelectorAll('[data-mrsm-map]:not([data-rendered])').forEach(async el => {
      el.dataset.rendered = 'pending';
      try {
        const maplibregl = await loadMaplibre();
        const spec = JSON.parse(el.dataset.mrsmMap);
        const geo = spec.type === 'FeatureCollection' || spec.type === 'Feature';
        const map = new maplibregl.Map({container: el, style: geo ? {version: 8, sources: {data: {type: 'geojson', data: spec}}, layers: [{id: 'data', type: 'circle', source: 'data'}]} : spec.style, center: spec.center || [0, 0], zoom: spec.zoom || 1});
        map.addControl(new maplibregl.NavigationControl());
        el.dataset.rendered = 'true';
      } catch (_) { delete el.dataset.rendered; el.textContent = 'Map could not be displayed.'; }
    });
  }
  render();
  document.body.addEventListener('htmx:afterSwap', render);
})();
