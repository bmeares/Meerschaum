/* Local, accessible stage cards remain Dash controls; SVG only draws their connections. */
(function () {
    function setView(view) {
        const page = document.getElementById('pipelines-page');
        if (!page) return;
        page.dataset.view = view;
        for (const value of ['workspace', 'runs']) document.getElementById('pipeline-view-' + value)?.setAttribute('aria-selected', String(value === view));
        if (view === 'runs') requestAnimationFrame(() => {
            document.querySelectorAll('#pipeline-runs-panel .js-plotly-plot').forEach(plot => {
                if (window.Plotly) window.Plotly.Plots.resize(plot);
            });
        });
    }
    function setPane(pane) {
        const workspace = document.getElementById('pipeline-workspace');
        if (!workspace) return;
        workspace.dataset.pane = pane;
        document.querySelectorAll('.pipeline-mobile-tabs [data-pane]').forEach(button => button.setAttribute('aria-selected', String(button.dataset.pane === pane)));
        if (pane === 'graph') requestAnimationFrame(() => document.getElementById('pipeline-fit')?.click());
    }
    function exitFullscreen() {
        if (document.fullscreenElement?.id === 'pipeline-workspace') document.exitFullscreen().catch(() => {});
        document.getElementById('pipeline-workspace')?.classList.remove('pipeline-fullscreen');
    }
    window.mrsmClosePipelineOutput = function () {
        window._mrsmOutputDismissed = true;
        document.getElementById('pipeline-output-dock')?.classList.add('pipeline-output-empty');
        setPane('graph');
    };
    window.mrsmShowPipelineOutput = function (store) {
        if (!store) return;
        window._mrsmOutputDismissed = false;
        const dock = document.getElementById('pipeline-output-dock');
        dock?.classList.remove('pipeline-output-empty');
        if (dock) dock.dataset.outputMode = 'pipeline';
        setView('workspace');
        setPane('output');
        requestAnimationFrame(() => dock?.scrollIntoView({block:'nearest'}));
    };
    function showOutput(ref) {
        const dock = document.getElementById('pipeline-output-dock');
        if (dock) dock.dataset.outputMode = 'pipe';
        setView('workspace');
        window._mrsmOutputDismissed = false;
        document.getElementById('pipeline-output-dock')?.classList.remove('pipeline-output-empty');
        setPane('output');
        const rows = Array.from((window._mrsmNodeOutput || new Map()).values()).filter(item => item.ref === ref);
        if (rows.length) window.dash_clientside.set_props('pipeline-output-tab', {value: rows.at(-1).job});
        else {
            document.getElementById('pipeline-output-dock')?.classList.remove('pipeline-output-empty');
            window.dash_clientside.set_props('pipeline-node-alert', {children:'No open execution output for this Pipe. Sync it or inspect its recent syncs.'});
        }
        document.getElementById('pipeline-output-dock')?.scrollIntoView({block:'nearest'});
    }
    window.mrsmPaintPipelineStatuses = function (statuses) {
        const colors = {running:'#3486bd', succeeded:'#2e9e5b', failed:'#d1495b',
                        upstream_failed:'#d1495b', cancelled:'#9aa5b1', skipped:'#9aa5b1', pending:'#e0a526'};
        document.querySelectorAll('.pipeline-stage').forEach(card => {
            if (!card.dataset.plannedColor) card.dataset.plannedColor = card.style.getPropertyValue('--stage-color');
            const status = (statuses || {})[card.dataset.ref];
            card.dataset.status = status || '';
            let badge = card.querySelector('.pipeline-live-status');
            if (!status) {
                card.style.setProperty('--stage-color', card.dataset.plannedColor);
                if (badge) badge.remove();
                return;
            }
            card.style.setProperty('--stage-color', colors[status] || '#9aa5b1');
            card.title = card.querySelector('code').textContent + ' — ' + status;
            if (!badge) { badge = document.createElement('small'); badge.className = 'pipeline-live-status'; card.appendChild(badge); }
            badge.textContent = status.replaceAll('_', ' ');
        });
        const filter = document.getElementById('pipeline-canvas-status');
        if (filter && filter.onchange) filter.dispatchEvent(new Event('change'));
    };
    function mount() {
        const page = document.getElementById('pipelines-page');
        if (page && page._mrsmViewSearch !== location.search) {
            const previous = page._mrsmViewSearch === undefined ? null : new URLSearchParams(page._mrsmViewSearch);
            page._mrsmViewSearch = location.search;
            const query = new URLSearchParams(location.search);
            const changedPipeline = previous && ['pipeline', 'instance'].some(key => previous.get(key) !== query.get(key));
            if (query.has('run_id') || ['status', 'pipe'].some(key => query.has(key))) setView('runs');
            else if (!previous || changedPipeline) setView('workspace');
        }
        const workspace = document.getElementById('pipeline-workspace');
        const inspector = document.getElementById('pipeline-inspector');
        if (workspace && inspector) workspace.classList.toggle('pipeline-has-selection', !inspector.hidden);
        const canvas = document.getElementById('pipeline-canvas');
        const world = document.getElementById('pipeline-canvas-world');
        if (!canvas || !world) return;
        const currentCards = Array.from(world.querySelectorAll('.pipeline-stage'));
        if (world._mrsmMountedCards && currentCards.length === world._mrsmMountedCards.length
            && currentCards.every((card, index) => card === world._mrsmMountedCards[index])) return;
        world._mrsmMountedCards = currentCards;
        world.querySelectorAll(':scope > svg').forEach(element => element.remove());
        const ns = 'http://www.w3.org/2000/svg';
        const svg = document.createElementNS(ns, 'svg');
        svg.setAttribute('width', world.style.width);
        svg.setAttribute('height', world.style.height);
        svg.style.cssText = 'position:absolute;pointer-events:none;overflow:visible';
        svg.setAttribute('aria-hidden', 'true');
        const cards = new Map(Array.from(world.querySelectorAll('.pipeline-stage')).map(el=>[el.dataset.node,el]));
        const layout = JSON.parse(world.dataset.layout || '{}');
        cards.forEach((card, key) => {
            const position = layout[card.dataset.ref] || layout[key];
            if (position) { card.style.left = position.x + 'px'; card.style.top = position.y + 'px'; }
        });
        function drawEdges() {
        svg.replaceChildren();
        for (const edge of JSON.parse(world.dataset.edges || '[]')) {
            const a = cards.get(edge.parent), b = cards.get(edge.child);
            if (!a || !b) continue;
            const x = a.offsetLeft + a.offsetWidth, y = a.offsetTop + a.offsetHeight/2;
            const u = b.offsetLeft - 8, v = b.offsetTop + b.offsetHeight/2;
            const path = document.createElementNS(ns, 'path');
            // End the curve before a straight landing, so its tangent never crosses the arrow.
            const landing = u - 16, middle = (x + landing) / 2;
            path.setAttribute('d', `M${x},${y} C${middle},${y} ${middle},${v} ${landing},${v} L${u-7},${v}`);
            path.setAttribute('fill', 'none'); path.setAttribute('stroke', '#809aad');
            path.setAttribute('stroke-width', '2');
            path.setAttribute('stroke-linecap', 'round');
            svg.appendChild(path);
            const arrow = document.createElementNS(ns, 'path');
            arrow.setAttribute('d', `M${u-8},${v-5} L${u},${v} L${u-8},${v+5}`);
            arrow.setAttribute('fill', 'none');
            arrow.setAttribute('stroke', '#a7c8df');
            arrow.setAttribute('stroke-width', '2');
            arrow.setAttribute('stroke-linecap', 'round');
            arrow.setAttribute('stroke-linejoin', 'round');
            svg.appendChild(arrow);
        }
        }
        drawEdges();
        world.prepend(svg);
        const viewportKey = 'mrsm-pipeline-viewport:' + location.pathname + ':' + (new URLSearchParams(location.search).get('instance') || '') + ':' + world.dataset.pipeline;
        let saved = {};
        try { saved = JSON.parse(sessionStorage.getItem(viewportKey) || '{}'); } catch (_) {}
        let scale = saved.scale || 1, x = saved.x || 0, y = saved.y || 0, drag, moved = false, pressedGroup;
        const draw = () => {
            world.style.transform = `translate(${x}px,${y}px) scale(${scale})`;
            try { sessionStorage.setItem(viewportKey, JSON.stringify({scale, x, y})); } catch (_) {}
        };
        const zoom = factor => {
            const next = Math.max(.15, Math.min(2, scale * factor));
            x = canvas.clientWidth/2 - (canvas.clientWidth/2-x)*next/scale;
            y = canvas.clientHeight/2 - (canvas.clientHeight/2-y)*next/scale;
            scale = next; draw();
        };
        const fit = () => {
            const visibleCards = Array.from(cards.values()).filter(card => getComputedStyle(card).visibility !== 'hidden');
            const minX = Math.min(0, ...visibleCards.map(card => card.offsetLeft));
            const minY = Math.min(0, ...visibleCards.map(card => card.offsetTop));
            const maxX = Math.max(world.offsetWidth, ...visibleCards.map(card => card.offsetLeft + card.offsetWidth));
            const maxY = Math.max(world.offsetHeight, ...visibleCards.map(card => card.offsetTop + card.offsetHeight));
            scale = Math.min(1, canvas.clientWidth / Math.max(1, maxX-minX+32), canvas.clientHeight / Math.max(1, maxY-minY+32));
            x = (canvas.clientWidth-(maxX-minX)*scale)/2-minX*scale;
            y = (canvas.clientHeight-(maxY-minY)*scale)/2-minY*scale; draw();
        };
        document.getElementById('pipeline-fit').onclick = fit;
        canvas._mrsmResizeObserver?.disconnect();
        let lastWidth = 0, lastHeight = 0;
        canvas._mrsmResizeObserver = new ResizeObserver(entries => {
            const {width, height} = entries[0].contentRect;
            if (!width || !height || (width === lastWidth && height === lastHeight)) return;
            lastWidth = width; lastHeight = height;
            requestAnimationFrame(fit);
        });
        canvas._mrsmResizeObserver.observe(canvas);
        document.getElementById('pipeline-fullscreen').onclick = async () => {
            const workspace = document.getElementById('pipeline-workspace');
            try {
                if (document.fullscreenElement) await document.exitFullscreen();
                else if (workspace.requestFullscreen) await workspace.requestFullscreen();
                else workspace.classList.toggle('pipeline-fullscreen');
            } catch (_) { workspace.classList.toggle('pipeline-fullscreen'); }
        };
        document.getElementById('pipeline-center').onclick = () => {
            const selected = Array.from(cards.values()).filter(card => card.getAttribute('aria-pressed') === 'true');
            if (selected.length) {
                const left = Math.min(...selected.map(card => card.offsetLeft));
                const right = Math.max(...selected.map(card => card.offsetLeft + card.offsetWidth));
                const top = Math.min(...selected.map(card => card.offsetTop));
                const bottom = Math.max(...selected.map(card => card.offsetTop + card.offsetHeight));
                scale = Math.min(1, canvas.clientWidth / (right-left+80), canvas.clientHeight / (bottom-top+80));
                x = canvas.clientWidth/2 - (left+right)/2*scale;
                y = canvas.clientHeight/2 - (top+bottom)/2*scale; draw();
            }
        };
        const add = () => {
            const editor = document.getElementById('pipeline-draft-editor');
            if (editor) { editor.open = true; const picker = editor.querySelector('details'); if (picker) picker.open = true; editor.scrollIntoView({block: 'nearest'}); }
        };
        document.getElementById('pipeline-add-node').onclick = add;
        const search = document.getElementById('pipeline-canvas-search');
        const statusFilter = document.getElementById('pipeline-canvas-status');
        const filter = () => cards.forEach(card => {
            const text = search.value.toLowerCase();
            const wanted = statusFilter.value;
            const status = (window._mrsmPipelineStatuses || {})[card.dataset.ref];
            card.classList.toggle('pipeline-search-hidden', !card.textContent.toLowerCase().includes(text) || !!(wanted && wanted !== status));
        });
        search.oninput = filter; statusFilter.onchange = filter;
        document.getElementById('pipeline-collapse-groups').onclick = () => {
            world.classList.toggle('pipeline-groups-collapsed');
        };
        canvas.oncontextmenu = e => {
            e.preventDefault();
            document.querySelectorAll('.pipeline-context-menu').forEach(el => el.remove());
            const node = e.target.closest('.pipeline-stage');
            const menu = document.createElement('div'); menu.className = 'pipeline-context-menu';
            menu.setAttribute('role', 'menu');
            menu.style.left = Math.min(e.clientX, innerWidth - 230) + 'px';
            menu.style.top = Math.min(e.clientY, innerHeight - 220) + 'px';
            const button = (label, fn) => {
                const b = document.createElement('button'); b.textContent = ({'Inspect Pipe':'ⓘ ', 'View output':'▤ ', 'Remove membership in draft':'− ', 'Add existing / register new Pipe':'+ '}[label] || '▶ ') + label; b.setAttribute('role', 'menuitem');
                b.onclick = () => { menu.remove(); fn(); }; menu.appendChild(b);
            };
            if (node) {
                button('Inspect Pipe', () => node.click());
                for (const [scope, label] of [['this', 'Sync this Pipe'], ['parents', 'Sync with all parents'], ['children', 'Sync with all children']])
                    button(label, () => { setPane('output'); window.dash_clientside.set_props('pipeline-context', {data:{action:'sync', ref:node.dataset.ref, scope, token:Date.now()}}); });
                button('View output', () => { node.click(); showOutput(node.dataset.ref); });
                button('Remove membership in draft', () => {
                    window.dash_clientside.set_props('pipeline-context', {data:{action:'remove', ref:node.dataset.ref, token:Date.now()}}); add();
                });
            } else button('Add existing / register new Pipe', add);
            document.getElementById('pipeline-workspace').appendChild(menu);
            menu.querySelector('button').focus();
        };
        document.getElementById('pipeline-zoom-in').onclick = () => zoom(1.25);
        document.getElementById('pipeline-zoom-out').onclick = () => zoom(.8);
        canvas.onpointerdown = e => {
            const node = e.target.closest('.pipeline-stage');
            if (e.target.closest('button') && !(node && e.altKey)) return;
            e.preventDefault();
            window.getSelection()?.removeAllRanges();
            moved = false;
            pressedGroup = e.target.closest('.pipeline-group');
            drag = {node, x:e.clientX-x, y:e.clientY-y, startX:e.clientX, startY:e.clientY, left:node?.offsetLeft, top:node?.offsetTop}; canvas.setPointerCapture(e.pointerId);
        };
        canvas.onpointermove = e => {if (drag) {moved = moved || Math.hypot(e.clientX-drag.startX, e.clientY-drag.startY) > 5;if (drag.node) {
            drag.node.style.left = (drag.left + (e.clientX-drag.startX)/scale) + 'px';
            drag.node.style.top = (drag.top + (e.clientY-drag.startY)/scale) + 'px';
            drawEdges();
        } else { x=e.clientX-drag.x;y=e.clientY-drag.y;draw(); }}};
        canvas.onpointerup = canvas.onpointercancel = () => {
            if (drag?.node && moved) {
                cards.forEach((card, key) => { layout[card.dataset.ref] = {x:card.offsetLeft, y:card.offsetTop}; });
                window.dash_clientside.set_props('pipeline-layout-draft', {data:layout});
            }
            drag=null;
        };
        canvas.onwheel = e => {if (e.ctrlKey || e.metaKey) {e.preventDefault();zoom(e.deltaY<0?1.1:1/1.1);}};
        canvas.onkeydown = e => {
            if (e.target !== canvas) return;
            if (e.key === '+' || e.key === '=') zoom(1.25);
            else if (e.key === '-') zoom(.8);
            else if (e.key === '0') fit();
            else if (e.key.startsWith('Arrow')) {e.preventDefault();x += e.key==='ArrowLeft'?40:e.key==='ArrowRight'?-40:0;y += e.key==='ArrowUp'?40:e.key==='ArrowDown'?-40:0;draw();}
        };
        canvas.onclick = e => {
            if (moved) { moved = false; e.preventDefault(); e.stopPropagation(); return; }
            const group = pressedGroup || e.target.closest('.pipeline-group');
            pressedGroup = null;
            if (group && !e.target.closest('button')) group.querySelector('button').click();
            const selected = e.target.closest('.pipeline-stage');
            if (selected) {
                setPane('details');
                window._mrsmPipelineSelectedRef = selected.dataset.ref;
                if (e.ctrlKey || e.metaKey || e.shiftKey) selected.classList.toggle('pipeline-multi-selected');
                else cards.forEach(card => card.classList.remove('pipeline-multi-selected'));
                cards.forEach(card=>card.setAttribute('aria-pressed', String(card===selected || card.classList.contains('pipeline-multi-selected'))));
                const inspector = document.getElementById('pipeline-inspector');
                inspector.hidden = false;
                inspector.setAttribute('aria-busy', 'true');
                const body = document.getElementById('pipeline-inspector-body');
                if (body) body.style.opacity = '.4';
                document.getElementById('pipeline-workspace').classList.add('pipeline-has-selection');
            }
        };
        const focused = Array.from(cards.values()).find(card => card.dataset.ref === (world.dataset.focus || window._mrsmPipelineSelectedRef));
        if (focused) {
            x = canvas.clientWidth/2 - (focused.offsetLeft + focused.offsetWidth/2)*scale;
            y = canvas.clientHeight/2 - (focused.offsetTop + focused.offsetHeight/2)*scale;
            focused.setAttribute('aria-pressed', 'true');
        }
        window.mrsmPaintPipelineStatuses(window._mrsmPipelineStatuses);
        // Keep labels readable; Fit provides a complete overview on demand.
        draw();
    }
    document.addEventListener('keydown', e => {
        if (e.target.matches('.pipeline-node-output-icon') && ['Enter', ' '].includes(e.key)) { e.preventDefault(); e.target.click(); }
        if (e.key === 'Escape') {
            exitFullscreen();
            document.querySelectorAll('.pipeline-context-menu').forEach(el => el.remove());
            document.getElementById('pipeline-workspace')?.classList.remove('pipeline-fullscreen');
        }
    });
    document.addEventListener('click', e => {
        const runLink = e.target.closest('.pipeline-run-link[data-run-id]');
        if (runLink) { e.preventDefault(); window.dash_clientside.set_props('pipeline-selected-run', {data:runLink.dataset.runId}); return; }
        const outputIcon = e.target.closest('.pipeline-node-output-icon');
        if (outputIcon) { e.preventDefault(); e.stopPropagation(); showOutput(outputIcon.closest('.pipeline-stage').dataset.ref); return; }
        if (e.target.closest('#pipeline-recent-runs')) {
            window.dash_clientside.set_props('pipeline-selected-run', {data:null});
            const query = new URLSearchParams(location.search); query.delete('run_id');
            window.dash_clientside.set_props('pipelines-location', {search:query.size ? '?' + query : ''});
        }
        if (e.target.closest('#pipeline-view-output')) window.mrsmShowPipelineOutput(window._mrsmPipelineRun);
        if (e.target.closest('#pipeline-node-sync')) { const dock = document.getElementById('pipeline-output-dock'); if (dock) dock.dataset.outputMode = 'pipe'; }
        if (e.target.closest('#pipeline-output-close')) window.mrsmClosePipelineOutput();
        const viewButton = e.target.closest('#pipeline-view-workspace, #pipeline-view-runs');
        if (viewButton) setView(viewButton.id === 'pipeline-view-runs' ? 'runs' : 'workspace');
        const paneButton = e.target.closest('.pipeline-mobile-tabs [data-pane]');
        if (paneButton) setPane(paneButton.dataset.pane);
        if (e.target.closest('#pipeline-exit-fullscreen')) exitFullscreen();
        if (e.target.closest('#pipeline-node-sync')) { window._mrsmOutputDismissed = false; setPane('output'); }
        if (e.target.closest('.pipeline-group-title')) { setView('workspace'); setPane('graph'); }

        if (!e.target.closest('.pipeline-context-menu')) document.querySelectorAll('.pipeline-context-menu').forEach(el => el.remove());
        if (e.target.closest('#pipeline-node-view-output')) showOutput(window._mrsmPipelineSelectedRef);
        if (e.target.closest('#pipeline-inspector-close')) {
            setPane('graph');
            window._mrsmPipelineSelectedRef = null;
            document.querySelectorAll('.pipeline-stage').forEach(card => card.setAttribute('aria-pressed', 'false'));
        }
    });
    document.addEventListener('click', e => {
        const icon = e.target.closest('.pipeline-node-output-icon');
        if (icon) { e.preventDefault(); e.stopPropagation(); showOutput(icon.closest('.pipeline-stage').dataset.ref); }
    }, true);
    new MutationObserver(mount).observe(document.documentElement, {childList:true, subtree:true, attributes:true, attributeFilter:['hidden']});
    document.addEventListener('DOMContentLoaded', mount);
})();
