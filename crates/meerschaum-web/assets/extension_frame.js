/* Trusted renderer for an opaque extension frame. Plugin scripts are never evaluated. */
(() => {
  'use strict';
  const spec = window.__MRSM_EXTENSION__;
  delete window.__MRSM_EXTENSION__;
  const root = document.getElementById('mrsm-extension-root');
  const nodes = new Map(), values = new Map(), pending = new Map();
  let sequence = 0;
  const plain = value => value !== null && typeof value === 'object' && !Array.isArray(value);
  const htmlTags = new Set(['div','span','p','h1','h2','h3','h4','h5','h6','button','label','input','textarea','select','option','ul','ol','li','table','thead','tbody','tfoot','tr','th','td','pre','code','strong','b','em','i','small','hr','br','section','article','header','footer','main','blockquote']);
  const styleProps = new Set(['display','color','backgroundColor','fontSize','fontWeight','fontFamily','textAlign','width','height','minWidth','maxWidth','minHeight','maxHeight','margin','padding','gap','border','borderRadius','overflow','whiteSpace','flexDirection','alignItems','justifyContent']);
  function bounded(value, depth = 0) {
    if (depth > 48) throw Error('The component nesting exceeds its limit.');
    if (typeof value === 'string' && value.length > 1024 * 1024) throw Error('A component value exceeds its limit.');
    if (Array.isArray(value)) {
      if (value.length > 20000) throw Error('Too many component values.');
      value.forEach(item => bounded(item, depth + 1));
    } else if (plain(value)) {
      for (const [key,item] of Object.entries(value)) {
        if (['__proto__','prototype','constructor','dangerouslySetInnerHTML','href','src','srcSet','srcDoc','action','formAction','target','download'].includes(key)) throw Error('An active or URL-bearing component property is unsupported.');
        bounded(item, depth + 1);
      }
    }
  }
  function figure(value) {
    bounded(value);
    const visit = item => {
      if (typeof item === 'string' && /(?:https?:|\/\/|javascript:|data:|url\s*\(|[<>]|&#|&[A-Za-z][A-Za-z0-9]{0,31};|&(?:lt|gt|amp|quot|apos))/i.test(item)) throw Error('Network, HTML or navigation text is unsupported in extension figures.');
      if (Array.isArray(item)) return item.map(visit);
      if (plain(item)) {
        if (Object.prototype.hasOwnProperty.call(item, 'bdata')) {
          // Plotly's own numeric array wire format is binary data, not navigation text.
          // Validate its complete shape before allowing base64 containing '/' through.
          const widths = {i1:1,u1:1,i2:2,u2:2,i4:4,u4:4,f4:4,f8:8};
          if (Object.keys(item).some(key => !['dtype','bdata','shape'].includes(key))
              || !Object.prototype.hasOwnProperty.call(widths, item.dtype)
              || typeof item.bdata !== 'string' || item.bdata.length > 256 * 1024
              || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(item.bdata)) {
            throw Error('Unsupported numeric figure array.');
          }
          const bytes = atob(item.bdata), width = widths[item.dtype];
          if (btoa(bytes) !== item.bdata || bytes.length % width || bytes.length / width > 20000) {
            throw Error('The numeric figure array exceeds its bounds or alignment.');
          }
          const result = {dtype:item.dtype, bdata:item.bdata};
          if (Object.prototype.hasOwnProperty.call(item, 'shape')) {
            if (typeof item.shape !== 'string' || !/^\d{1,6}(?:, ?\d{1,6}){0,3}$/.test(item.shape)) {
              throw Error('Unsupported numeric figure array shape.');
            }
            let count = 1;
            for (const dimension of item.shape.split(',').map(Number)) {
              count *= dimension;
              if (!Number.isSafeInteger(count) || count > 20000) throw Error('The numeric figure array shape exceeds its bound.');
            }
            if (count !== bytes.length / width) throw Error('The numeric figure array shape does not match its bytes.');
            result.shape = item.shape;
          }
          return result;
        }
        const cleaned={};
        for (const [key,value] of Object.entries(item)) {
        // Default plotly.py templates contain dormant geographic/URL-bearing component
        // definitions. Use the trusted runtime's default theme, without importing them.
        if (key === 'template') continue;
        // Plotly commands can introduce new resource-bearing attributes only after a
        // button/slider click. New figures may enter only through the checked Core bridge.
        if (['updatemenus','sliders','frames','transforms'].includes(key.toLowerCase())) throw Error('Dynamic Plotly command controls are unsupported in isolated figures.');
        if (['url','source','images','mapbox','map','geojson','links','link','href','src'].includes(key.toLowerCase())) throw Error('URL-bearing figure fields are unsupported.');
        cleaned[key]=visit(value);
        }
        return cleaned;
      }
      return item;
    };
    value=visit(value);
    if (!plain(value) || !Array.isArray(value.data)) throw Error('Invalid Plotly figure.');
    const traces=new Set(['scatter','bar','box','violin','histogram','histogram2d','histogram2dcontour','heatmap','contour','pie','scattergl','table','indicator','waterfall','funnel','funnelarea','sunburst','treemap','icicle','sankey','ohlc','candlestick','surface','mesh3d','scatter3d','volume','isosurface','cone','streamtube','carpet','scattercarpet','contourcarpet','parcoords','parcats','scatterpolar','barpolar','scatterpolargl','scatterternary']);
    if(value.data.some(trace=>!plain(trace)||!traces.has(trace.type || 'scatter')))throw Error('Geographic, image and external-source trace types are unsupported in isolated figures.');
    return value;
  }
  function save(id, property, value) {
    let props = values.get(id);
    if (!props) values.set(id, props = new Map());
    props.set(property, value);
  }
  function read(binding) {
    const node = nodes.get(binding.id), known = values.get(binding.id);
    if (binding.property === 'value' && node && 'value' in node) return node.type === 'number' || node.type === 'range' ? Number(node.value) : node.value;
    if (binding.property === 'checked' && node) return Boolean(node.checked);
    return known && known.has(binding.property) ? known.get(binding.property) : null;
  }
  function changed(id, property) {
    for (const handler of spec.handlers) {
      if (!handler.dash || !handler.dash.inputs.some(input => input.id === id && input.property === property)) continue;
      const serial = ++sequence;
      if (pending.size >= 2) continue;
      const binding = item => ({id:item.id, property:item.property, value:read(item)});
      const outputs = handler.dash.outputs.map(item => ({id:item.id, property:item.property}));
      const inputs = {output:handler.selector.output, outputs:outputs.length === 1 ? outputs[0] : outputs,
        changedPropIds:[id + '.' + property], inputs:handler.dash.inputs.map(binding), state:handler.dash.state.map(binding)};
      pending.set(serial, handler.id);
      parent.postMessage({kind:'callback', nonce:spec.nonce, serial, handler:handler.id, inputs}, '*');
    }
  }
  function render(value) {
    if (value === null || value === undefined || typeof value === 'boolean') return document.createTextNode('');
    if (typeof value === 'string' || typeof value === 'number') return document.createTextNode(String(value));
    if (Array.isArray(value)) {const fragment=document.createDocumentFragment();value.forEach(item=>fragment.append(render(item)));return fragment;}
    bounded(value);
    if (!plain(value) || typeof value.type !== 'string' || !plain(value.props)) throw Error('Unsupported Dash component declaration.');
    const props=value.props, namespace=value.namespace;
    let tag=value.type.toLowerCase(), mode='html';
    if (namespace === 'dash_core_components') {
      const core={Input:'input',Textarea:'textarea',Dropdown:'select',Slider:'input',Graph:'div',Store:'span',Loading:'div',Markdown:'pre',Checklist:'div',RadioItems:'div',Tabs:'div',Tab:'section'};
      if (!Object.prototype.hasOwnProperty.call(core,value.type)) throw Error('This Dash component requires an unsupported custom runtime.');
      tag=core[value.type];mode=value.type;
    } else if (namespace !== 'dash_html_components') throw Error('Custom component JavaScript is unsupported in this isolated page.');
    if (!htmlTags.has(tag)) throw Error('This active HTML component is unsupported.');
    const node=document.createElement(tag);
    if (props.id !== undefined) {
      if (typeof props.id !== 'string' || props.id.length > 256 || nodes.has(props.id)) throw Error('Pattern, duplicate or invalid component IDs are unsupported.');
      nodes.set(props.id,node);node.id=props.id;
      for (const [key,value] of Object.entries(props)) save(props.id,key,value);
    }
    for (const [key,value] of Object.entries(props)) {
      if (['children','id','figure','options','data','n_clicks','persistence','persisted_props','persistence_type','loading_state'].includes(key)) continue;
      if (key === 'style') {
        if (!plain(value)) throw Error('Invalid component style.');
        for (const [property,setting] of Object.entries(value)) {
          if (!styleProps.has(property) || !['string','number'].includes(typeof setting) || String(setting).length > 256 || (typeof setting === 'number' && (!Number.isFinite(setting) || Math.abs(setting) > 1000000)) || /(?:url\s*\(|[<>\\]|expression\s*\()/i.test(String(setting))) throw Error('Unsupported active component style.');
          node.style[property]=String(setting);
        }
      } else if (['title','className','placeholder','disabled','readOnly','required','min','max','step','type','value','checked','multiple','rows','cols'].includes(key)) {
        if (!['string','number','boolean'].includes(typeof value)) throw Error('Unsupported component property value.');
        if (key === 'type' && !['text','number','range','checkbox','radio','password','email','search','button','submit'].includes(value)) throw Error('Unsupported input type.');
        node[key]=value;
      } else if (!['clearable','searchable','multi','marks','tooltip','vertical','included','updatemode','minHeight','maxHeight','label','selected','value'].includes(key)) {
        throw Error('This component property requires an unsupported runtime: ' + key);
      }
    }
    if (mode === 'Store') node.hidden=true;
    else if (mode === 'Dropdown') {
      if (!Array.isArray(props.options)) throw Error('The Dropdown requires bounded explicit options.');
      for (const option of props.options) {const entry=plain(option)?option:{label:option,value:option};const child=document.createElement('option');child.textContent=String(entry.label);child.value=String(entry.value);node.append(child);}
      if (props.value !== undefined) node.value=String(props.value);
    } else if (mode === 'Checklist' || mode === 'RadioItems') {
      if (!Array.isArray(props.options)) throw Error('The selection requires explicit options.');
      const selected=new Set(Array.isArray(props.value)?props.value:[props.value]);
      for(const option of props.options){const item=plain(option)?option:{label:option,value:option};const label=document.createElement('label'),input=document.createElement('input');input.type=mode==='Checklist'?'checkbox':'radio';input.name=props.id || '';input.value=String(item.value);input.checked=selected.has(item.value);label.append(input,document.createTextNode(String(item.label)));node.append(label);input.addEventListener('change',()=>{const active=Array.from(node.querySelectorAll('input')).filter(input=>input.checked).map(input=>input.value);save(props.id,'value',mode==='Checklist'?active:active[0]);changed(props.id,'value');});}
    } else if (mode === 'Graph') {
      const data=figure(props.figure);
      if (!window.Plotly) throw Error('The trusted Plotly runtime is unavailable.');
      queueMicrotask(()=>window.Plotly.newPlot(node,data.data,data.layout || {},{responsive:true,displaylogo:false,displayModeBar:false}));
    } else node.append(render(props.children));
    if (mode === 'Slider') node.type='range';
    if (props.id) {
      if (tag === 'button') node.addEventListener('click',()=>{save(props.id,'n_clicks',Number(read({id:props.id,property:'n_clicks'}) || 0)+1);changed(props.id,'n_clicks');});
      if (['input','textarea','select'].includes(tag)) node.addEventListener('change',()=>changed(props.id,'value'));
    }
    return node;
  }
  function update(id, property, value) {
    bounded(value);
    const node=nodes.get(id);
    if (!node || !['children','value','checked','disabled','figure','data','n_clicks'].includes(property)) throw Error('An undeclared callback output is unsupported.');
    if (property === 'children') node.replaceChildren(render(value));
    else if (property === 'figure') {const data=figure(value);if(!window.Plotly)throw Error('The trusted Plotly runtime is unavailable.');window.Plotly.react(node,data.data,data.layout || {},{responsive:true,displaylogo:false,displayModeBar:false});}
    else if (property !== 'data' && property !== 'n_clicks') node[property]=value;
    save(id,property,value);
  }
  document.addEventListener('click',event=>{if(event.target.closest && event.target.closest('a,form'))event.preventDefault();},true);
  document.addEventListener('submit',event=>event.preventDefault(),true);
  window.addEventListener('message',event=>{
    if(event.source!==parent || !plain(event.data) || event.data.nonce!==spec.nonce || event.data.kind!=='callback_result')return;
    const handler=pending.get(event.data.serial);
    if(!handler)return;
    pending.delete(event.data.serial);
    const declared=spec.handlers.find(entry=>entry.id===handler);
    try{
      if(event.data.error)throw Error(String(event.data.error));
      const updates=event.data.updates && event.data.updates.response || {};
      for(const [id,props]of Object.entries(updates))for(const [property,value]of Object.entries(props)){
        if(!declared.dash.outputs.some(output=>output.id===id && output.property===property))throw Error('The callback returned another output.');
        update(id,property,value);
      }
    }catch(error){root.dataset.error=String(error.message || error);}
  });
  try {
    if(spec.renderer==='dash')root.append(render(spec.layout));
    // HTML was already parsed and sanitized by Main with a strict inert allowlist.
    else {root.innerHTML=spec.html;for(const node of root.querySelectorAll('[data-mrsm-figure]')){const data=figure(JSON.parse(node.dataset.mrsmFigure));if(!window.Plotly)throw Error('The trusted Plotly runtime is unavailable.');window.Plotly.newPlot(node,data.data,data.layout || {},{responsive:true,displaylogo:false,displayModeBar:false});}}
    parent.postMessage({kind:'ready',nonce:spec.nonce},'*');
  } catch(error) {root.replaceChildren(document.createTextNode(String(error.message || error)));root.dataset.error=String(error.message || error);parent.postMessage({kind:'error',nonce:spec.nonce,message:String(error.message || error)},'*');}
})();
