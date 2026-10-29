#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`@web_page(theme=..., head=[...])` and `add_head(...)` in the web console.

The console is one single-page Dash app, so its `<head>` is written once per page load. The host
adds to it (through `dash_app.interpolate_index`, so a plugin's own `index_string` still applies):

- the tags of `add_head()`, on every page;
- a small script holding each plugin page's theme and head tags. When a page with a custom theme
  opens (on load or by in-app navigation), it points the console's two theme stylesheets
  (`#mrsm-theme-dark`, `#mrsm-theme-light`, one of which is enabled at a time) at the page's
  stylesheet, so exactly one theme applies; the console's theme returns on other pages. A page's
  stylesheets and meta tags are added while it is open, its scripts once.

A page-contract page's document (`/_pages/...?frame=1`) gets its theme and tags in its own head
(`meerschaum.api.host.dash.page_document`).
"""

from __future__ import annotations

import json

from meerschaum.utils.typing import Any, Dict, List

_INSTALLED_ATTR: str = '_mrsm_page_heads'
_CONSOLE_THEMES = ('dark', 'light')

_SCRIPT: str = """(function(){
var pages=%s;
var keys=Object.keys(pages).sort(function(a,b){return b.length-a.length;});
var current,loaded={},original={};
function pageFor(path){
  path=(path||'/').replace(/\\/+$/,'')||'/';
  for(var i=0;i<keys.length;i++){var key=keys[i];if(path===key||path.indexOf(key+'/')===0){return key;}}
  return null;
}
function themeLinks(){
  return [document.getElementById('mrsm-theme-dark'),document.getElementById('mrsm-theme-light')]
    .filter(function(link){return !!link;});
}
function apply(){
  var key=pageFor(window.location.pathname);
  if(key===current){return;}
  current=key;
  var page=key?pages[key]:null;
  var nodes=document.querySelectorAll('[data-mrsm-page-head]');
  for(var i=0;i<nodes.length;i++){if(nodes[i].tagName!=='SCRIPT'){nodes[i].parentNode.removeChild(nodes[i]);}}
  themeLinks().forEach(function(link){
    if(!(link.id in original)){original[link.id]=link.getAttribute('href');}
    var href=(page&&page.theme)||original[link.id];
    if(link.getAttribute('href')!==href){link.setAttribute('href',href);}
  });
  if(!page||!page.head){return;}
  page.head.forEach(function(markup,index){
    var template=document.createElement('template');
    template.innerHTML=markup;
    Array.prototype.forEach.call(template.content.childNodes,function(node){
      if(node.nodeType!==1){return;}
      var element;
      if(node.tagName==='SCRIPT'){
        var id=key+'#'+index;
        if(loaded[id]){return;}
        loaded[id]=true;
        element=document.createElement('script');
        Array.prototype.forEach.call(node.attributes,function(attr){element.setAttribute(attr.name,attr.value);});
        element.text=node.text;
        element.async=false;
      }else{
        element=document.importNode(node,true);
      }
      element.setAttribute('data-mrsm-page-head',key);
      document.head.appendChild(element);
    });
  });
}
['pushState','replaceState'].forEach(function(name){
  var method=window.history[name];
  window.history[name]=function(){var result=method.apply(this,arguments);try{apply();}catch(e){}return result;};
});
window.addEventListener('popstate',apply);
window.addEventListener('_dashprivate_pushstate',apply);
apply();
})();"""


def console_path(page_str: str) -> str:
    """
    Return the console path of a `@web_page` (`/dash/<path>`).
    """
    rest = str(page_str).strip().strip('/')
    if rest == 'dash' or rest.startswith('dash/'):
        return '/' + rest
    return '/dash/' + rest


def page_heads() -> Dict[str, Dict[str, Any]]:
    """
    Return `{console path: {'theme': stylesheet URL or None, 'head': [html, ...]}}` for the
    plugin pages with a custom theme or head tags. A page-contract page's tags belong to its own
    document, so only its theme is listed (the console around its frame follows it).
    """
    from meerschaum.plugins import _plugin_endpoints_to_pages
    try:
        from meerschaum.api.host.dash import get_page_contract_pages
        contract = set(get_page_contract_pages())
    except Exception:
        contract = set()
    out: Dict[str, Dict[str, Any]] = {}
    for _page_group, pages in list(_plugin_endpoints_to_pages.items()):
        for page_str, page in list(pages.items()):
            theme = page.get('theme', None)
            theme = theme if theme not in (None, *_CONSOLE_THEMES) else None
            head = [] if page_str in contract else list(page.get('head', None) or [])
            if theme is None and not head:
                continue
            out[console_path(page_str)] = {'theme': theme, 'head': head}
    return out


def _script_json(value: Any) -> str:
    """JSON safe inside a `<script>` element."""
    return (
        json.dumps(value, separators=(',', ':'))
        .replace('</', '<\\/')
        .replace(' ', '\\u2028')
        .replace(' ', '\\u2029')
    )


def head_html() -> str:
    """
    Return what the host adds to the console's `<head>`: the `add_head()` tags and the page
    themes script (when a page has a theme or tags).
    """
    from meerschaum.plugins import get_console_head
    parts: List[str] = list(get_console_head())
    pages = page_heads()
    if pages:
        parts.append(f'<script id="mrsm-page-heads">{_SCRIPT % _script_json(pages)}</script>')
    return '\n'.join(parts)


def insert_into_head(document: str, extra: str) -> str:
    """
    Insert `extra` at the end of an HTML document's `<head>` (or at its start without one).
    """
    if not extra:
        return document
    index = document.lower().rfind('</head>')
    if index == -1:
        return extra + document
    return document[:index] + extra + '\n' + document[index:]


def install_page_heads(dash_app: Any) -> None:
    """
    Add `head_html()` to every console page the Dash app renders (once per app).
    """
    if getattr(dash_app, _INSTALLED_ATTR, False):
        return
    original = dash_app.interpolate_index
    warned = {'value': False}

    def interpolate_index(**kwargs):
        document = original(**kwargs)
        try:
            extra = head_html()
        except Exception as e:
            ### A plugin's tags never take the console down.
            if not warned['value']:
                warned['value'] = True
                from meerschaum.utils.warnings import warn
                warn(f"Could not add the plugins' page themes and head tags: {e}", stack=False)
            return document
        return insert_into_head(document, extra)

    dash_app.interpolate_index = interpolate_index
    setattr(dash_app, _INSTALLED_ATTR, True)
