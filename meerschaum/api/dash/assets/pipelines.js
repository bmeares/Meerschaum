/* Local, accessible stage cards remain Dash controls; SVG only draws their connections. */
(function () {
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
        const defs = document.createElementNS(ns, 'defs');
        const marker = document.createElementNS(ns, 'marker');
        Object.entries({id:'pipeline-arrow',viewBox:'0 0 10 10',refX:'9',refY:'5',markerWidth:'6',markerHeight:'6',orient:'auto-start-reverse'}).forEach(([k,v])=>marker.setAttribute(k,v));
        const arrow = document.createElementNS(ns, 'path');
        arrow.setAttribute('d', 'M 0 0 L 10 5 L 0 10 z'); arrow.setAttribute('fill', '#809aad');
        marker.appendChild(arrow); defs.appendChild(marker); svg.appendChild(defs);
        const cards = new Map(Array.from(world.querySelectorAll('.pipeline-stage')).map(el=>[el.dataset.ref,el]));
        for (const edge of JSON.parse(world.dataset.edges || '[]')) {
            const a = cards.get(edge.parent), b = cards.get(edge.child);
            if (!a || !b) continue;
            const x = a.offsetLeft + a.offsetWidth, y = a.offsetTop + a.offsetHeight/2;
            const u = b.offsetLeft - 6, v = b.offsetTop + b.offsetHeight/2;
            const path = document.createElementNS(ns, 'path');
            path.setAttribute('d', `M${x},${y} C${(x+u)/2},${y} ${(x+u)/2},${v} ${u},${v}`);
            path.setAttribute('fill', 'none'); path.setAttribute('stroke', '#809aad');
            path.setAttribute('stroke-width', '2'); path.setAttribute('marker-end', 'url(#pipeline-arrow)');
            svg.appendChild(path);
        }
        world.prepend(svg);
        let scale = 1, x = 0, y = 0, drag;
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
            drag = {x:e.clientX-x, y:e.clientY-y}; canvas.setPointerCapture(e.pointerId);
        };
        canvas.onpointermove = e => {if (drag) {x=e.clientX-drag.x;y=e.clientY-drag.y;draw();}};
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
            const selected = e.target.closest('.pipeline-stage');
            if (selected) cards.forEach(card=>card.setAttribute('aria-pressed', String(card===selected)));
        };
        const focused = cards.get(world.dataset.focus);
        if (focused) {
            x = canvas.clientWidth/2 - focused.offsetLeft - focused.offsetWidth/2;
            y = canvas.clientHeight/2 - focused.offsetTop - focused.offsetHeight/2;
            focused.setAttribute('aria-pressed', 'true');
        }
        // Keep labels readable; Fit provides a complete overview on demand.
        draw();
    }
    new MutationObserver(mount).observe(document.documentElement, {childList:true, subtree:true});
    document.addEventListener('DOMContentLoaded', mount);
})();
