/* Local, accessible stage cards remain Dash controls; SVG only draws their connections. */
(function () {
    window.mrsmPaintPipelineStatuses = function (statuses) {
        const colors = {running:'#3486bd', succeeded:'#2e9e5b', failed:'#d1495b',
                        upstream_failed:'#d1495b', skipped:'#9aa5b1', pending:'#e0a526'};
        document.querySelectorAll('.pipeline-stage').forEach(card => {
            if (!card.dataset.plannedColor) card.dataset.plannedColor = card.style.getPropertyValue('--stage-color');
            const status = (statuses || {})[card.dataset.ref];
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
    };
    function mount() {
        const canvas = document.getElementById('pipeline-canvas');
        const world = document.getElementById('pipeline-canvas-world');
        if (!canvas || !world || world.dataset.mounted) return;
        world.dataset.mounted = 'true';
        const ns = 'http://www.w3.org/2000/svg';
        const svg = document.createElementNS(ns, 'svg');
        svg.setAttribute('width', world.style.width);
        svg.setAttribute('height', world.style.height);
        svg.style.cssText = 'position:absolute;pointer-events:none;overflow:visible';
        svg.setAttribute('aria-hidden', 'true');
        const cards = new Map(Array.from(world.querySelectorAll('.pipeline-stage')).map(el=>[el.dataset.node,el]));
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
        world.prepend(svg);
        let scale = 1, x = 0, y = 0, drag, moved = false, pressedGroup;
        const draw = () => {world.style.transform = `translate(${x}px,${y}px) scale(${scale})`;};
        const zoom = factor => {
            const next = Math.max(.15, Math.min(2, scale * factor));
            x = canvas.clientWidth/2 - (canvas.clientWidth/2-x)*next/scale;
            y = canvas.clientHeight/2 - (canvas.clientHeight/2-y)*next/scale;
            scale = next; draw();
        };
        const fit = () => {
            scale = Math.min(1, canvas.clientWidth/world.offsetWidth, canvas.clientHeight/world.offsetHeight);
            x = (canvas.clientWidth-world.offsetWidth*scale)/2;
            y = (canvas.clientHeight-world.offsetHeight*scale)/2; draw();
        };
        document.getElementById('pipeline-fit').onclick = fit;
        document.getElementById('pipeline-zoom-in').onclick = () => zoom(1.25);
        document.getElementById('pipeline-zoom-out').onclick = () => zoom(.8);
        canvas.onpointerdown = e => {
            if (e.target.closest('button')) return;
            moved = false;
            pressedGroup = e.target.closest('.pipeline-group');
            drag = {x:e.clientX-x, y:e.clientY-y, startX:e.clientX, startY:e.clientY}; canvas.setPointerCapture(e.pointerId);
        };
        canvas.onpointermove = e => {if (drag) {moved = moved || Math.hypot(e.clientX-drag.startX, e.clientY-drag.startY) > 5;x=e.clientX-drag.x;y=e.clientY-drag.y;draw();}};
        canvas.onpointerup = canvas.onpointercancel = () => {drag=null;};
        canvas.onwheel = e => {if (e.ctrlKey || e.metaKey) {e.preventDefault();zoom(e.deltaY<0?1.1:1/1.1);}};
        canvas.onkeydown = e => {
            if (e.target !== canvas) return;
            if (e.key === '+' || e.key === '=') zoom(1.25);
            else if (e.key === '-') zoom(.8);
            else if (e.key === '0') fit();
            else if (e.key.startsWith('Arrow')) {e.preventDefault();x += e.key==='ArrowLeft'?40:e.key==='ArrowRight'?-40:0;y += e.key==='ArrowUp'?40:e.key==='ArrowDown'?-40:0;draw();}
        };
        canvas.onclick = e => {
            if (moved) { moved = false; return; }
            const group = pressedGroup || e.target.closest('.pipeline-group');
            pressedGroup = null;
            if (group && !e.target.closest('button')) group.querySelector('button').click();
            const selected = e.target.closest('.pipeline-stage');
            if (selected) cards.forEach(card=>card.setAttribute('aria-pressed', String(card===selected)));
        };
        const focused = Array.from(cards.values()).find(card => card.dataset.ref === world.dataset.focus);
        if (focused) {
            x = canvas.clientWidth/2 - focused.offsetLeft - focused.offsetWidth/2;
            y = canvas.clientHeight/2 - focused.offsetTop - focused.offsetHeight/2;
            focused.setAttribute('aria-pressed', 'true');
        }
        window.mrsmPaintPipelineStatuses(window._mrsmPipelineStatuses);
        // Keep labels readable; Fit provides a complete overview on demand.
        draw();
    }
    document.addEventListener('click', e => {
        if (e.target.closest('#pipeline-inspector-close')) {
            document.querySelectorAll('.pipeline-stage').forEach(card => card.setAttribute('aria-pressed', 'false'));
        }
    });
    new MutationObserver(mount).observe(document.documentElement, {childList:true, subtree:true});
    document.addEventListener('DOMContentLoaded', mount);
})();
