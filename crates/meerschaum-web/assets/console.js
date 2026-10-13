(() => {
  const theme = localStorage.getItem('mrsm-theme') || (document.documentElement.classList.contains('mrsm-theme-dark') ? 'dark' : 'light');
  function applyTheme(value) { document.documentElement.className = 'mrsm-theme-' + value; localStorage.setItem('mrsm-theme', value); }
  applyTheme(theme);
  document.getElementById('theme-toggle')?.addEventListener('click', () => applyTheme(document.documentElement.classList.contains('mrsm-theme-dark') ? 'light' : 'dark'));
  function render() {
    document.querySelectorAll('[data-mrsm-graph]:not([data-rendered])').forEach(el => {
      try { const spec=JSON.parse(el.dataset.mrsmGraph); cytoscape({container:el,elements:spec.elements,style:[{selector:'node',style:{label:'data(label)','background-color':'#3486bd'}},{selector:'edge',style:{'target-arrow-shape':'triangle','curve-style':'bezier'}}],layout:{name:'breadthfirst',directed:true,padding:30}});el.dataset.rendered='true'; } catch (_) { el.textContent='Graph could not be displayed.'; }
    });
    document.querySelectorAll('[data-mrsm-figure]:not([data-rendered])').forEach(async el => {
      try { const raw = el.dataset.mrsmFigure; const figure = raw.startsWith('/') ? await (await fetch(raw)).json() : JSON.parse(raw); await Plotly.newPlot(el, figure.data || [], figure.layout || {}, {...figure.config, responsive:true}); el.dataset.rendered = 'true'; } catch (_) { el.textContent = 'Chart could not be displayed.'; }
    });
    document.querySelectorAll('[data-mrsm-map]:not([data-rendered])').forEach(el => {
      try { const spec = JSON.parse(el.dataset.mrsmMap); const geo = spec.type === 'FeatureCollection' || spec.type === 'Feature'; const map = new maplibregl.Map({container:el, style:geo ? {version:8,sources:{data:{type:'geojson',data:spec}},layers:[{id:'data',type:'circle',source:'data'}]} : spec.style, center:spec.center || [0,0], zoom:spec.zoom || 1}); map.addControl(new maplibregl.NavigationControl()); el.dataset.rendered = 'true'; } catch (_) { el.textContent = 'Map could not be displayed.'; }
    });
    document.querySelectorAll('table[data-sortable] th:not([data-sort-ready])').forEach((th) => { th.dataset.sortReady='true'; const button=document.createElement('button'); button.textContent=th.textContent; th.textContent=''; th.append(button); button.addEventListener('click',()=>{const body=th.closest('table').tBodies[0]; const index=th.cellIndex; const rows=Array.from(body.rows); const reverse=th.getAttribute('aria-sort')==='ascending'; rows.sort((a,b)=>a.cells[index].textContent.localeCompare(b.cells[index].textContent,undefined,{numeric:true})*(reverse?-1:1)); rows.forEach(row=>body.append(row)); th.setAttribute('aria-sort',reverse?'descending':'ascending');}); });
    document.querySelectorAll('[data-logs]:not([data-rendered])').forEach(el=>{el.dataset.rendered='true';const source=new EventSource(el.dataset.logs);source.onmessage=e=>{el.textContent+=e.data+'\n';};source.addEventListener('stopped',()=>source.close());});
  }
  render(); document.body.addEventListener('htmx:afterSwap', render);
})();
