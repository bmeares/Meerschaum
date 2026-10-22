/* ANSI-safe output shared by the Pipe drawer and Pipeline command stream. */
(function () {
    window.mrsmRenderPipelineOutput = function (view, text) {
        if (!view) return;
        const follow = view._mrsmFollow !== false;
        if (!view._mrsmScrollBound) {
            view._mrsmScrollBound = true;
            view.addEventListener('scroll', () => {
                view._mrsmFollow = view.scrollHeight - view.scrollTop - view.clientHeight < 40;
            }, {passive:true});
        }
        if (view._mrsmText === text) return;
        view._mrsmText = text;
        if (window.AnsiUp) {
            const ansi = new window.AnsiUp();
            view.innerHTML = ansi.ansi_to_html(text);
        } else view.textContent = text;
        if (follow) {
            view.scrollTop = view.scrollHeight;
            // Opening a previously hidden disclosure gives it its scrollable height next frame.
            requestAnimationFrame(() => { if (view._mrsmFollow !== false) view.scrollTop = view.scrollHeight; });
        }
    };
    window.mrsmUpdateOutputCancel = function () {
        const item = window._mrsmNodeOutput?.get(window._mrsmNodeOutputSelected);
        const button = document.getElementById('pipeline-cancel-node-run');
        if (button) button.style.display = item && ['running', 'paused'].includes(item.status) ? '' : 'none';
    };
    window.mrsmFinishNodeOutput = function (item) {
        window.mrsmUpdateOutputCancel();
        const items = Array.from((window._mrsmNodeOutput || new Map()).values()).filter(row => row.ref === item.ref);
        if (items.at(-1)?.job === item.job) {
            const statuses = {...(window._mrsmPipelineStatuses || {})};
            if (['running', 'pending'].includes(statuses[item.ref])) statuses[item.ref] = 'stopped';
            window._mrsmPipelineStatuses = statuses;
            window.mrsmPaintPipelineStatuses?.(statuses);
        }
        window.dash_clientside?.set_props('pipelines-interval', {n_intervals: Date.now()});
    };
    // Output may be mounted while its stream is already open.
    new MutationObserver(() => window.mrsmUpdateOutputCancel()).observe(document.documentElement, {childList:true, subtree:true});
})();
