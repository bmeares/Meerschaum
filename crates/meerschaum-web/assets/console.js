(() => {
  const theme = localStorage.getItem('mrsm-theme') || (document.documentElement.classList.contains('mrsm-theme-dark') ? 'dark' : 'light');
  function applyTheme(value) { document.documentElement.className = 'mrsm-theme-' + value; localStorage.setItem('mrsm-theme', value); }
  applyTheme(theme);
  document.getElementById('theme-toggle')?.addEventListener('click', () => applyTheme(document.documentElement.classList.contains('mrsm-theme-dark') ? 'light' : 'dark'));
  function describeEvent(r) {
    if (r.event === 'chunk_started' || r.event === 'chunk_finished') {
      let out = ' chunk ' + r.chunk + (r.total != null ? '/' + r.total : '') + (r.source ? ' (' + r.source + ')' : '');
      if (r.begin != null || r.end != null) out += ' [' + (r.begin ?? '') + ' -> ' + (r.end ?? '') + ')';
      if (r.rows != null) out += ' rows=' + r.rows;
      if (r.event === 'chunk_finished') {
        out += r.success === false ? ' FAILED' : ' ok';
        for (const k of ['inserted', 'updated', 'upserted']) if (r[k] != null) out += ' ' + k + '=' + r[k];
        if (r.duration_ms != null) out += ' ' + r.duration_ms + 'ms';
        if (r.message) out += ' ' + r.message;
      }
      return out;
    }
    if (r.event === 'children_triggered') {
      const n = Array.isArray(r.nodes) ? r.nodes.length : r.nodes;
      return ' run=' + r.run_id + ' nodes=' + n + (r.held != null ? ' held=' + (Array.isArray(r.held) ? r.held.length : r.held) : '') + (r.flushes != null ? ' flushes=' + r.flushes : '');
    }
    return '';
  }
  function render() {
    document.querySelectorAll('[data-mrsm-graph]:not([data-rendered])').forEach(el => {
      try { const spec=JSON.parse(el.dataset.mrsmGraph); cytoscape({container:el,elements:spec.elements,style:[{selector:'node',style:{label:'data(label)','background-color':'#3486bd'}},{selector:'node[status="succeeded"],node[status="observed"]',style:{'background-color':'#2e9e5b'}},{selector:'node[status="failed"],node[status="upstream_failed"]',style:{'background-color':'#d1495b'}},{selector:'node[status="pending"],node[status="rebuild_pending"]',style:{'background-color':'#e0a526'}},{selector:'node[status="never_run"]',style:{'background-color':'#9aa5b1'}},{selector:'edge',style:{'target-arrow-shape':'triangle','curve-style':'bezier',label:'data(label)','font-size':8,'text-rotation':'autorotate'}},{selector:'edge[lag>0]',style:{'line-color':'#e0a526','target-arrow-color':'#e0a526',width:3}}],layout:{name:'breadthfirst',directed:true,padding:30}});el.dataset.rendered='true'; } catch (_) { el.textContent='Graph could not be displayed.'; }
    });
    document.querySelectorAll('[data-mrsm-figure]:not([data-rendered])').forEach(async el => {
      try { const raw = el.dataset.mrsmFigure; const figure = raw.startsWith('/') ? await (await fetch(raw)).json() : JSON.parse(raw); await Plotly.newPlot(el, figure.data || [], figure.layout || {}, {...figure.config, responsive:true}); el.dataset.rendered = 'true'; } catch (_) { el.textContent = 'Chart could not be displayed.'; }
    });
    document.querySelectorAll('[data-mrsm-map]:not([data-rendered])').forEach(el => {
      try { const spec = JSON.parse(el.dataset.mrsmMap); const geo = spec.type === 'FeatureCollection' || spec.type === 'Feature'; const map = new maplibregl.Map({container:el, style:geo ? {version:8,sources:{data:{type:'geojson',data:spec}},layers:[{id:'data',type:'circle',source:'data'}]} : spec.style, center:spec.center || [0,0], zoom:spec.zoom || 1}); map.addControl(new maplibregl.NavigationControl()); el.dataset.rendered = 'true'; } catch (_) { el.textContent = 'Map could not be displayed.'; }
    });
    document.querySelectorAll('table[data-sortable] th:not([data-sort-ready])').forEach((th) => { th.dataset.sortReady='true'; const button=document.createElement('button'); button.textContent=th.textContent; th.textContent=''; th.append(button); button.addEventListener('click',()=>{const body=th.closest('table').tBodies[0]; const index=th.cellIndex; const rows=Array.from(body.rows); const reverse=th.getAttribute('aria-sort')==='ascending'; rows.sort((a,b)=>a.cells[index].textContent.localeCompare(b.cells[index].textContent,undefined,{numeric:true})*(reverse?-1:1)); rows.forEach(row=>body.append(row)); th.setAttribute('aria-sort',reverse?'descending':'ascending');}); });
    document.querySelectorAll('[data-mrsm-events]:not([data-rendered])').forEach(el=>{el.dataset.rendered='true';const source=new EventSource(el.dataset.mrsmEvents);let timer=null;source.onmessage=e=>{try{const r=JSON.parse(e.data);el.textContent+=r.ts+' '+r.event+(r.pipe_ref?' '+r.pipe_ref:'')+describeEvent(r)+'\n';}catch(_){el.textContent+=e.data+'\n';}clearTimeout(timer);timer=setTimeout(()=>document.body.dispatchEvent(new Event('mrsm-pipelines')),250);};source.addEventListener('end',()=>{source.close();document.body.dispatchEvent(new Event('mrsm-pipelines'));});});
    document.querySelectorAll('[data-logs]:not([data-rendered])').forEach(el=>{el.dataset.rendered='true';const source=new EventSource(el.dataset.logs);source.onmessage=e=>{el.textContent+=e.data+'\n';};source.addEventListener('stopped',()=>source.close());});
  }
  render(); document.body.addEventListener('htmx:afterSwap', render);
})();
