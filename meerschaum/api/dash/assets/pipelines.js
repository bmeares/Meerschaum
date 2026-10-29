/* The Pipeline canvas: stage cards remain Dash controls; SVG only draws their connections.
 *
 * A Pipeline is read-only until Edit (`#pipelines-page[data-edit=true]`): then dragging a card
 * moves it (the shared layout, saved with Save), dragging from a card's handle to another card
 * proposes a parent -> child edge, and the menus offer Add Pipe and Remove from Pipeline. Zoom
 * and pan are remembered per Pipeline in this browser. */
(function () {
    function page() { return document.getElementById('pipelines-page'); }
    function editing() { return page()?.dataset.edit === 'true'; }
    function setView(view) {
        const element = page();
        if (!element) return;
        element.dataset.view = view;
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
        if (pane === 'graph') requestAnimationFrame(() => document.getElementById('pipeline-fit')?.click());
    }
    function exitFullscreen() {
        if (document.fullscreenElement?.id === 'pipeline-workspace') document.exitFullscreen().catch(() => {});
        document.getElementById('pipeline-workspace')?.classList.remove('pipeline-fullscreen');
    }
    function closeInspector() {
        const inspector = document.getElementById('pipeline-inspector');
        if (inspector) inspector.hidden = true;
        document.getElementById('pipeline-workspace')?.classList.remove('pipeline-has-selection');
        window._mrsmPipelineSelectedRef = null;
        document.querySelectorAll('.pipeline-stage').forEach(card => card.setAttribute('aria-pressed', 'false'));
        setPane('graph');
    }
    function openEditor() {
        const editor = document.getElementById('pipeline-draft-editor');
        if (editor && editing()) editor.open = true;
    }
    window.mrsmClosePipelineOutput = function () {
        window._mrsmOutputDismissed = true;
        document.getElementById('pipeline-output-dock')?.classList.add('pipeline-output-empty');
        setPane('graph');
    };
    window.mrsmShowPipelineOutput = function (store) {
        const dock = document.getElementById('pipeline-output-dock');
        if (!dock) return;
        window._mrsmOutputDismissed = false;
        dock.classList.remove('pipeline-output-empty');
        dock.dataset.outputMode = store ? 'pipeline' : 'pipe';
        setView('workspace');
        setPane('output');
        requestAnimationFrame(() => dock.scrollIntoView({block: 'nearest'}));
    };
    // The Pipeline run a member's output belongs to, resolved when it is clicked: the selected run
    // once the server has listed its members (`pipeline-current-run`), else this page's own live
    // run (`pipeline-live-run`), whose explicit members are on the canvas as soon as it starts.
    // Reading only the server's answer lost a click made before that answer arrived.
    function memberRun(ref) {
        const current = window._mrsmCurrentPipelineRun;
        if (current && current.labels && current.labels[ref]) return {...current, label: current.labels[ref]};
        const live = window._mrsmPipelineRun;
        if (!live || !live.run_id || !live.pipeline) return null;
        const world = document.getElementById('pipeline-canvas-world');
        if (world && world.dataset.pipeline && world.dataset.pipeline !== live.pipeline) return null;
        const card = Array.from(document.querySelectorAll('.pipeline-stage[data-ref]'))
            .find(node => node.dataset.ref === ref);
        if (!card || card.dataset.role !== 'explicit') return null;
        const label = (card.querySelector('code') || {}).textContent || ref;
        return {run_id: live.run_id, instance: live.registry, job: live.job, pipeline: live.pipeline, label};
    }
    function showOutput(ref) {
        const dock = document.getElementById('pipeline-output-dock');
        if (!dock) return;
        dock.dataset.outputMode = 'pipe';
        setView('workspace');
        window._mrsmOutputDismissed = false;
        dock.classList.remove('pipeline-output-empty');
        setPane('output');
        // A member of this Pipeline's selected (or running) run shows its part of that run.
        const run = memberRun(ref);
        const rows = Array.from((window._mrsmNodeOutput || new Map()).values()).filter(item => item.ref === ref);
        if (run) window.dash_clientside.set_props('pipeline-member-output', {data: {
            key: 'run:' + run.run_id + '|' + ref, ref, label: run.label, run_id: run.run_id,
            instance: run.instance, job: run.job, pipeline: run.pipeline, scope: 'pipeline', token: Date.now()}});
        else if (rows.length) window.dash_clientside.set_props('pipeline-output-tab', {value: rows.at(-1).key || rows.at(-1).job});
        else window.dash_clientside.set_props('pipeline-node-alert', {children: 'No output for this Pipe yet.'});
        dock.scrollIntoView({block: 'nearest'});
    }
    window.mrsmPaintPipelineStatuses = function (statuses) {
        const colors = {running:'#3486bd', succeeded:'#2e9e5b', failed:'#d1495b',
                        upstream_failed:'#d1495b', cancelled:'#9aa5b1', skipped:'#9aa5b1', pending:'#e0a526'};
        document.querySelectorAll('.pipeline-stage:not(.pipeline-placeholder)').forEach(card => {
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
            const label = card.querySelector('code');
            if (label) card.title = label.textContent + ' — ' + status;
            if (!badge) { badge = document.createElement('small'); badge.className = 'pipeline-live-status'; card.appendChild(badge); }
            badge.textContent = status.replaceAll('_', ' ');
        });
        document.getElementById('pipeline-canvas')?._mrsmFilter?.();
    };
    // Canvas tools are delegated (see the document listeners below), so a re-rendered button
    // works at once and always acts on the canvas mounted now.
    function on(id, handler) {
        const canvas = document.getElementById('pipeline-canvas');
        if (canvas) (canvas._mrsmActions = canvas._mrsmActions || {})[id] = handler;
    }
    function mount() {
        const element = page();
        if (element && element._mrsmViewSearch !== location.search) {
            element._mrsmViewSearch = location.search;
            const query = new URLSearchParams(location.search);
            if (query.has('run_id') || ['status', 'pipe'].some(key => query.has(key))) setView('runs');
        }
        openRequested();
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
        world.querySelectorAll(':scope > svg').forEach(svgElement => svgElement.remove());
        const ns = 'http://www.w3.org/2000/svg';
        const svg = document.createElementNS(ns, 'svg');
        svg.setAttribute('width', world.style.width);
        svg.setAttribute('height', world.style.height);
        svg.style.cssText = 'position:absolute;pointer-events:none;overflow:visible';
        svg.setAttribute('aria-hidden', 'true');
        const cards = new Map(currentCards.map(card => [card.dataset.node, card]));
        // The saved shared layout, under Edit mode's unsaved moves (kept across re-renders).
        const draftLayout = window._mrsmLayoutDraft;
        const layout = Object.assign(JSON.parse(world.dataset.layout || '{}'),
            draftLayout && draftLayout.key === (world.dataset.registry + '|' + world.dataset.pipeline) ? draftLayout.layout : {});
        cards.forEach((card, key) => {
            card.dataset.homeLeft = card.style.left; card.dataset.homeTop = card.style.top;
            const position = layout[card.dataset.ref] || layout[key];
            if (position) { card.style.left = position.x + 'px'; card.style.top = position.y + 'px'; }
        });
        let proposal = null;
        function drawEdges() {
            svg.replaceChildren();
            const curve = (x, y, u, v, color, dashed) => {
                const path = document.createElementNS(ns, 'path');
                // End the curve before a straight landing, so its tangent never crosses the arrow.
                const landing = u - 16, middle = (x + landing) / 2;
                path.setAttribute('d', `M${x},${y} C${middle},${y} ${middle},${v} ${landing},${v} L${u-7},${v}`);
                path.setAttribute('fill', 'none'); path.setAttribute('stroke', color);
                path.setAttribute('stroke-width', '2');
                path.setAttribute('stroke-linecap', 'round');
                if (dashed) path.setAttribute('stroke-dasharray', '6 5');
                svg.appendChild(path);
                const arrow = document.createElementNS(ns, 'path');
                arrow.setAttribute('d', `M${u-8},${v-5} L${u},${v} L${u-8},${v+5}`);
                arrow.setAttribute('fill', 'none');
                arrow.setAttribute('stroke', dashed ? color : '#a7c8df');
                arrow.setAttribute('stroke-width', '2');
                arrow.setAttribute('stroke-linecap', 'round');
                arrow.setAttribute('stroke-linejoin', 'round');
                svg.appendChild(arrow);
            };
            for (const edge of JSON.parse(world.dataset.edges || '[]')) {
                const a = cards.get(edge.parent), b = cards.get(edge.child);
                if (!a || !b) continue;
                curve(a.offsetLeft + a.offsetWidth, a.offsetTop + a.offsetHeight/2,
                      b.offsetLeft - 8, b.offsetTop + b.offsetHeight/2, '#809aad', false);
            }
            if (proposal) curve(proposal.x, proposal.y, proposal.u, proposal.v, '#e0a526', true);
        }
        drawEdges();
        world.prepend(svg);
        // Discarding unsaved moves puts every card back where the saved layout has it.
        canvas._mrsmResetLayout = () => {
            const shared = JSON.parse(world.dataset.layout || '{}');
            for (const key of Object.keys(layout)) delete layout[key];
            Object.assign(layout, shared);
            cards.forEach((card, key) => {
                const position = shared[card.dataset.ref] || shared[key];
                card.style.left = position ? position.x + 'px' : card.dataset.homeLeft;
                card.style.top = position ? position.y + 'px' : card.dataset.homeTop;
            });
            drawEdges();
        };
        const viewportKey = 'mrsm-pipeline-viewport:' + (world.dataset.registry || '') + '|' + (world.dataset.pipeline || world.dataset.focus || location.pathname);
        let saved = {};
        try { saved = JSON.parse(localStorage.getItem(viewportKey) || '{}'); } catch (_) {}
        let scale = saved.scale || 1, x = saved.x || 0, y = saved.y || 0;
        // A gesture outlives a re-render (an edit's draft re-renders the canvas, sometimes the
        // canvas element itself): it lives on the page, and a replaced card is found by its Pipe.
        const g = window._mrsmCanvasGesture = window._mrsmCanvasGesture || {drag: null, moved: false};
        if (g.viewportKey !== viewportKey) {
            g.drag = null; g.pinch = null; g.touches = new Map(); g.moved = false;
            g.viewportKey = viewportKey;
        }
        g.touches = g.touches || new Map();
        if (g.drag) {
            if (g.drag.node && !g.drag.node.isConnected) g.drag.node = currentCards.find(card => card.dataset.ref === g.drag.ref) || null;
            if (g.drag.handle && !g.drag.handle.isConnected) g.drag.handle = currentCards.find(card => card.dataset.ref === g.drag.ref) || null;
        }
        // A card being dragged keeps its live position through a re-render.
        if (g.drag && g.drag.ref && g.drag.lastLeft !== undefined) {
            const dragged = currentCards.find(card => card.dataset.ref === g.drag.ref);
            if (dragged) { dragged.style.left = g.drag.lastLeft + 'px'; dragged.style.top = g.drag.lastTop + 'px'; }
        }
        const draw = () => {
            world.style.transform = `translate(${x}px,${y}px) scale(${scale})`;
            try { localStorage.setItem(viewportKey, JSON.stringify({scale, x, y})); } catch (_) {}
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
        on('pipeline-fit', fit);
        canvas._mrsmResizeObserver?.disconnect();
        let lastWidth = 0, lastHeight = 0, fitted = !!saved.scale;
        canvas._mrsmResizeObserver = new ResizeObserver(entries => {
            const {width, height} = entries[0].contentRect;
            if (!width || !height || (width === lastWidth && height === lastHeight)) return;
            lastWidth = width; lastHeight = height;
            if (!fitted) { fitted = true; requestAnimationFrame(fit); }
        });
        canvas._mrsmResizeObserver.observe(canvas);
        on('pipeline-fullscreen', async () => {
            const target = document.getElementById('pipeline-workspace');
            if (!target) return;
            try {
                if (document.fullscreenElement) await document.exitFullscreen();
                else if (target.requestFullscreen) await target.requestFullscreen();
                else target.classList.toggle('pipeline-fullscreen');
            } catch (_) { target.classList.toggle('pipeline-fullscreen'); }
        });
        on('pipeline-center', () => {
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
        });
        on('pipeline-add-node', openEditor);
        const filter = () => cards.forEach(card => {
            const text = (document.getElementById('pipeline-canvas-search')?.value || '').toLowerCase();
            const wanted = document.getElementById('pipeline-canvas-status')?.value;
            const status = (window._mrsmPipelineStatuses || {})[card.dataset.ref];
            card.classList.toggle('pipeline-search-hidden', !card.textContent.toLowerCase().includes(text) || !!(wanted && wanted !== status));
        });
        canvas._mrsmFilter = filter;
        on('pipeline-collapse-groups', () => world.classList.toggle('pipeline-groups-collapsed'));
        const readOnlyCanvas = !document.getElementById('pipeline-inspector');
        canvas.oncontextmenu = e => {
            if (readOnlyCanvas) return;
            e.preventDefault();
            openMenu(e.target.closest('.pipeline-stage:not(.pipeline-placeholder)'), e.clientX, e.clientY);
        };
        function openMenu(node, left, top) {
            document.querySelectorAll('.pipeline-context-menu').forEach(menuElement => menuElement.remove());
            const menu = document.createElement('div'); menu.className = 'pipeline-context-menu';
            menu.setAttribute('role', 'menu');
            menu.style.left = Math.min(left, innerWidth - 230) + 'px';
            menu.style.top = Math.min(top, innerHeight - 220) + 'px';
            const button = (label, fn) => {
                const b = document.createElement('button'); b.textContent = ({'Inspect Pipe':'ⓘ ', 'View output':'▤ ', 'Remove from Pipeline':'− ', 'Add Pipe':'+ '}[label] || '▶ ') + label; b.setAttribute('role', 'menuitem');
                b.onclick = () => { menu.remove(); fn(); }; menu.appendChild(b);
            };
            if (node) {
                button('Inspect Pipe', () => node.click());
                for (const [scope, label] of [['this', 'Sync this Pipe'], ['parents', 'Sync with all parents'], ['children', 'Sync with all children']])
                    button(label, () => { setPane('output'); window.dash_clientside.set_props('pipeline-context', {data:{action:'sync', ref:node.dataset.ref, scope, token:Date.now()}}); });
                button('View output', () => { node.click(); showOutput(node.dataset.ref); });
                if (editing()) button('Remove from Pipeline', () => {
                    window.dash_clientside.set_props('pipeline-context', {data:{action:'remove', ref:node.dataset.ref, token:Date.now()}});
                });
            } else if (editing()) button('Add Pipe', openEditor);
            if (!menu.children.length) return;
            document.getElementById('pipeline-workspace').appendChild(menu);
            menu.querySelector('button').focus();
        }
        canvas._mrsmOpenMenu = openMenu;
        on('pipeline-zoom-in', () => zoom(1.25));
        on('pipeline-zoom-out', () => zoom(.8));
        const cancelDrag = () => {
            if (g.drag?.node) {
                g.drag.node.style.left = g.drag.originalLeft;
                g.drag.node.style.top = g.drag.originalTop;
            }
            proposal = null; g.drag = null; drawEdges();
        };
        const touchPair = () => Array.from(g.touches.values()).slice(0, 2);
        const beginPinch = () => {
            const [a, b] = touchPair();
            if (!a || !b) return;
            cancelDrag();
            const box = canvas.getBoundingClientRect();
            const mx = (a.x + b.x)/2 - box.left, my = (a.y + b.y)/2 - box.top;
            g.pinch = {distance: Math.max(1, Math.hypot(b.x-a.x, b.y-a.y)), scale,
                       worldX: (mx-x)/scale, worldY: (my-y)/scale};
            g.moved = true; g.suppressTouchClick = true;
            for (const pointer of g.touches.keys()) { try { canvas.setPointerCapture(pointer); } catch (_) {} }
        };
        canvas.onpointerdown = e => {
            if (e.pointerType !== 'touch') { g.suppressTouchClick = false; g.moved = false; }
            if (e.pointerType === 'touch') {
                if (!g.touches.size) { g.suppressTouchClick = false; g.moved = false; }
                g.touches.set(e.pointerId, {x:e.clientX, y:e.clientY});
                if (g.touches.size >= 2) { e.preventDefault(); beginPinch(); return; }
            }
            const node = e.target.closest('.pipeline-stage');
            const handle = e.target.closest('.pipeline-edge-handle');
            const edit = editing();
            if (e.target.closest('a')) return;
            if (!handle && e.target.closest('button') && !(node && edit)) return;
            if (node?.classList.contains('pipeline-placeholder')) return;
            e.preventDefault();
            window.getSelection()?.removeAllRanges();
            g.moved = false;
            g.drag = {node: edit ? node : null, handle: edit && handle ? node : null, x:e.clientX-x, y:e.clientY-y,
                    startX:e.clientX, startY:e.clientY, left:node?.offsetLeft, top:node?.offsetTop,
                    originalLeft:node?.style.left, originalTop:node?.style.top, pointer:e.pointerId, ref:node?.dataset.ref};
            // Capture only once it is a drag, so a plain click still reaches its card.
            if (!g.drag.node && !g.drag.handle) canvas.setPointerCapture(e.pointerId);
        };
        const worldPoint = e => {
            const box = canvas.getBoundingClientRect();
            return {px: (e.clientX - box.left - x) / scale, py: (e.clientY - box.top - y) / scale};
        };
        canvas.onpointermove = e => {
            if (e.pointerType === 'touch' && g.touches.has(e.pointerId)) {
                g.touches.set(e.pointerId, {x:e.clientX, y:e.clientY});
                if (g.pinch && g.touches.size >= 2) {
                    e.preventDefault();
                    const [a, b] = touchPair(), box = canvas.getBoundingClientRect();
                    scale = Math.max(.15, Math.min(2, g.pinch.scale * Math.hypot(b.x-a.x, b.y-a.y) / g.pinch.distance));
                    x = (a.x+b.x)/2 - box.left - g.pinch.worldX*scale;
                    y = (a.y+b.y)/2 - box.top - g.pinch.worldY*scale;
                    draw(); return;
                }
            }
            // A press that landed on a card while it was being replaced never reached the canvas:
            // the gesture starts from here instead of being lost.
            if (g.drag && !(e.buttons & 1)) { g.drag = null; return; }
            if (!g.drag && (e.buttons & 1) && !e.target.closest?.('a')) canvas.onpointerdown(e);
            if (!g.drag || g.drag.pointer !== e.pointerId) return;
            const wasMoved = g.moved;
            g.moved = g.moved || Math.hypot(e.clientX-g.drag.startX, e.clientY-g.drag.startY) > 5;
            if (g.moved && !wasMoved && (g.drag.node || g.drag.handle)) { try { canvas.setPointerCapture(g.drag.pointer); } catch (_) {} }
            if (!g.moved && (g.drag.node || g.drag.handle)) return;
            if (g.drag.handle) {
                const {px, py} = worldPoint(e);
                proposal = {x: g.drag.handle.offsetLeft + g.drag.handle.offsetWidth, y: g.drag.handle.offsetTop + g.drag.handle.offsetHeight/2, u: px, v: py};
                drawEdges();
            } else if (g.drag.node) {
                g.drag.lastLeft = g.drag.left + (e.clientX-g.drag.startX)/scale;
                g.drag.lastTop = g.drag.top + (e.clientY-g.drag.startY)/scale;
                g.drag.node.style.left = g.drag.lastLeft + 'px';
                g.drag.node.style.top = g.drag.lastTop + 'px';
                drawEdges();
            } else { x=e.clientX-g.drag.x; y=e.clientY-g.drag.y; draw(); }
        };
        canvas.onpointerup = e => {
            g.touches.delete(e.pointerId);
            if (g.pinch) {
                if (g.touches.size >= 2) beginPinch();
                else {
                    g.pinch = null;
                    const [remaining] = g.touches.entries();
                    g.drag = remaining ? {pointer: remaining[0], x:remaining[1].x-x, y:remaining[1].y-y,
                        startX:remaining[1].x, startY:remaining[1].y} : null;
                }
                return;
            }
            if (g.drag?.pointer !== e.pointerId) return;
            if (g.drag?.handle && g.moved) {
                const target = document.elementsFromPoint(e.clientX, e.clientY)
                    .map(element => element.closest('.pipeline-stage:not(.pipeline-placeholder)')).find(Boolean);
                if (target && target !== g.drag.handle)
                    window.dash_clientside.set_props('pipeline-edge-proposal', {data: {parent: g.drag.handle.dataset.ref, child: target.dataset.ref, token: Date.now()}});
                proposal = null; drawEdges();
            } else if ((g.drag?.node || g.drag?.lastLeft !== undefined) && g.moved) {
                cards.forEach((card, key) => { layout[card.dataset.ref] = {x:card.offsetLeft, y:card.offsetTop}; });
                if (g.drag.lastLeft !== undefined) layout[g.drag.ref] = {x: g.drag.lastLeft, y: g.drag.lastTop};
                // Known at once, so a re-render arriving before the store updates keeps the move.
                window._mrsmLayoutDraft = {key: world.dataset.registry + '|' + world.dataset.pipeline, layout: Object.assign({}, layout)};
                window.dash_clientside.set_props('pipeline-layout-draft', {data: Object.assign({}, layout)});
            }
            g.drag = null;
        };
        canvas.onpointercancel = e => {
            cancelDrag();
            g.touches.clear(); g.pinch = null; g.moved = true; g.suppressTouchClick = true;
        };
        canvas.onlostpointercapture = e => {
            if (g.touches.has(e.pointerId)) canvas.onpointercancel(e);
        };
        canvas.onwheel = e => {if (e.ctrlKey || e.metaKey) {e.preventDefault();zoom(e.deltaY<0?1.1:1/1.1);}};
        canvas.onkeydown = e => {
            const node = e.target.closest?.('.pipeline-stage:not(.pipeline-placeholder)');
            if (node && (e.key === 'ContextMenu' || (e.shiftKey && e.key === 'F10'))) {
                e.preventDefault();
                const box = node.getBoundingClientRect();
                openMenu(node, box.left + 24, box.top + 24);
                return;
            }
            if (e.target !== canvas) return;
            if (e.key === '+' || e.key === '=') zoom(1.25);
            else if (e.key === '-') zoom(.8);
            else if (e.key === '0') fit();
            else if (e.key.startsWith('Arrow')) {e.preventDefault();x += e.key==='ArrowLeft'?40:e.key==='ArrowRight'?-40:0;y += e.key==='ArrowUp'?40:e.key==='ArrowDown'?-40:0;draw();}
        };
        canvas.onclick = e => {
            if (e.detail !== 0 && (g.suppressTouchClick || g.moved)) {
                g.moved = false; e.preventDefault(); e.stopPropagation(); return;
            }
            if (readOnlyCanvas) return;
            const selected = e.target.closest('.pipeline-stage:not(.pipeline-placeholder)');
            if (selected) {
                setPane('details');
                window._mrsmPipelineSelectedRef = selected.dataset.ref;
                if (e.ctrlKey || e.metaKey || e.shiftKey) selected.classList.toggle('pipeline-multi-selected');
                else cards.forEach(card => card.classList.remove('pipeline-multi-selected'));
                cards.forEach(card => card.setAttribute('aria-pressed', String(card === selected || card.classList.contains('pipeline-multi-selected'))));
                const inspectorElement = document.getElementById('pipeline-inspector');
                if (inspectorElement) {
                    inspectorElement.hidden = false;
                    inspectorElement.setAttribute('aria-busy', 'true');
                }
                const body = document.getElementById('pipeline-inspector-body');
                if (body) body.style.opacity = '.4';
                document.getElementById('pipeline-workspace')?.classList.add('pipeline-has-selection');
            }
        };
        const focused = Array.from(cards.values()).find(card => card.dataset.ref === (world.dataset.focus || window._mrsmPipelineSelectedRef));
        if (focused && !saved.scale) {
            x = canvas.clientWidth/2 - (focused.offsetLeft + focused.offsetWidth/2)*scale;
            y = canvas.clientHeight/2 - (focused.offsetTop + focused.offsetHeight/2)*scale;
            focused.setAttribute('aria-pressed', 'true');
        }
        window.mrsmPaintPipelineStatuses(window._mrsmPipelineStatuses);
        draw();
        // These cards are wired up (tests wait for it after a re-render).
        world.dataset.mounted = 'true';
    }
    // `?open=plan|analyze|schedule|copy|rename|clear|drop|delete` (from the list's cards) opens
    // that dialog once the Pipeline is loaded.
    function openRequested() {
        const element = page();
        if (!element || element.dataset.loaded !== 'true') return;
        const query = new URLSearchParams(location.search);
        const requested = query.get('open');
        if (!requested) return;
        const ids = {plan: 'pipeline-plan-open', analyze: 'pipeline-analyze', schedule: 'pipeline-manage-schedule',
                     copy: 'pipeline-manage-copy', rename: 'pipeline-manage-rename', clear: 'pipeline-manage-clear',
                     drop: 'pipeline-manage-drop', delete: 'pipeline-delete'};
        query.delete('open');
        window.history.replaceState({}, '', location.pathname + (query.size ? '?' + query : ''));
        element._mrsmViewSearch = location.search;
        document.getElementById(ids[requested])?.click();
    }
    // Leaving a dirty Edit-mode draft asks first (it also stays in this browser to resume).
    function dirty() { return page()?.dataset.dirty === 'true'; }
    window.addEventListener('beforeunload', e => { if (dirty()) { e.preventDefault(); e.returnValue = ''; } });
    document.addEventListener('click', e => {
        const link = e.target.closest('a[href]');
        if (!link || !dirty() || link.target === '_blank' || link.closest('#pipelines-page .pipeline-workspace')) return;
        if (!window.confirm('Leave this Pipeline with unsaved changes?')) { e.preventDefault(); e.stopImmediatePropagation(); }
    }, true);
    document.addEventListener('keydown', e => {
        if (e.target.matches?.('.pipeline-node-output-icon') && ['Enter', ' '].includes(e.key)) { e.preventDefault(); e.target.click(); }
        if (e.key === 'Escape') {
            const viewOptions = document.querySelector('.pipeline-view-options[open]');
            if (viewOptions) { viewOptions.open = false; viewOptions.querySelector('summary')?.focus(); return; }
            const editor = document.getElementById('pipeline-draft-editor');
            if (editor?.open) { editor.open = false; return; }
            if (document.querySelector('.pipeline-context-menu')) {
                document.querySelectorAll('.pipeline-context-menu').forEach(menu => menu.remove());
                return;
            }
            const inspector = document.getElementById('pipeline-inspector');
            if (inspector && !inspector.hidden && !document.fullscreenElement) { document.getElementById('pipeline-inspector-close')?.click(); return; }
            exitFullscreen();
        }
    });
    document.addEventListener('click', e => {
        document.querySelectorAll('.pipeline-view-options[open]').forEach(options => {
            if (!options.contains(e.target)) options.open = false;
        });
        const tool = e.target.closest('button[id]');
        const action = tool && document.getElementById('pipeline-canvas')?._mrsmActions?.[tool.id];
        if (action) { action(); return; }
    });
    for (const type of ['input', 'change']) document.addEventListener(type, e => {
        if (['pipeline-canvas-search', 'pipeline-canvas-status'].includes(e.target.id))
            document.getElementById('pipeline-canvas')?._mrsmFilter?.();
    });
    document.addEventListener('click', e => {
        const runLink = e.target.closest('.pipeline-run-link[data-run-id]');
        if (runLink) { e.preventDefault(); window.dash_clientside.set_props('pipeline-selected-run', {data:runLink.dataset.runId}); return; }
        if (e.target.closest('#pipeline-recent-runs')) {
            window.dash_clientside.set_props('pipeline-selected-run', {data:null});
            const query = new URLSearchParams(location.search); query.delete('run_id');
            window.dash_clientside.set_props('pipelines-location', {search:query.size ? '?' + query : ''});
        }
        if (e.target.closest('#pipeline-view-output')) {
            if (window._mrsmPipelineRun) window.mrsmShowPipelineOutput(window._mrsmPipelineRun);
            else window.mrsmShowPipelineOutput(null);
        }
        if (e.target.closest('#pipeline-node-sync')) {
            const dock = document.getElementById('pipeline-output-dock'); if (dock) dock.dataset.outputMode = 'pipe';
            window._mrsmOutputDismissed = false; setPane('output');
        }
        if (e.target.closest('#pipeline-run-now')) setView('workspace');
        if (e.target.closest('#pipeline-output-close')) window.mrsmClosePipelineOutput();
        const viewButton = e.target.closest('#pipeline-view-workspace, #pipeline-view-runs');
        if (viewButton) setView(viewButton.id === 'pipeline-view-runs' ? 'runs' : 'workspace');
        if (!e.target.closest('.pipeline-context-menu')) document.querySelectorAll('.pipeline-context-menu').forEach(menu => menu.remove());
        if (e.target.closest('#pipeline-node-view-output')) showOutput(window._mrsmPipelineSelectedRef);
        if (e.target.closest('#pipeline-inspector-close')) closeInspector();
    });
    document.addEventListener('click', e => {
        const icon = e.target.closest('.pipeline-node-output-icon');
        if (icon) { e.preventDefault(); e.stopPropagation(); showOutput(icon.closest('.pipeline-stage').dataset.ref); }
    }, true);
    document.addEventListener('toggle', e => {
        if (e.target.id === 'pipeline-draft-editor')
            document.getElementById('pipeline-add-node')?.setAttribute('aria-expanded', String(e.target.open));
    }, true);
    new MutationObserver(mount).observe(document.documentElement, {childList:true, subtree:true, attributes:true, attributeFilter:['hidden', 'data-loaded']});
    document.addEventListener('DOMContentLoaded', mount);
})();
