/* Search and sort only the entries already authorized and rendered by the host. */
(() => {
    let mounted = null;
    function apply() {
        const table = document.querySelector('.pipelines-runbook');
        const count = document.getElementById('pipelines-visible-count');
        if (!table) { if (count) count.textContent = ''; return; }
        const entries = Array.from(table.querySelectorAll(':scope > .pipeline-list-entry'));
        const query = (document.getElementById('pipelines-search')?.value || '').trim().toLocaleLowerCase();
        const sort = document.getElementById('pipelines-sort')?.value || 'name';
        const byName = (a, b) => a.dataset.pipeline.localeCompare(b.dataset.pipeline, undefined, {numeric: true});
        const statuses = {failed: 0, running: 1, pending: 2, cancelled: 3, succeeded: 4};
        const ordered = entries.slice().sort((a, b) => {
            if (sort === 'name-desc') return byName(b, a);
            if (sort === 'recent') return (Date.parse(b.dataset.started) || 0) - (Date.parse(a.dataset.started) || 0) || byName(a, b);
            if (sort === 'status') return (statuses[a.dataset.status] ?? 5) - (statuses[b.dataset.status] ?? 5) || byName(a, b);
            if (sort === 'members') return Number(b.dataset.members) - Number(a.dataset.members) || byName(a, b);
            return byName(a, b);
        });
        if (ordered.some((entry, index) => entry !== entries[index])) ordered.forEach(entry => table.appendChild(entry));
        let visible = 0;
        for (const entry of ordered) {
            entry.hidden = !!query && !entry.dataset.search.toLocaleLowerCase().includes(query);
            if (!entry.hidden) visible += 1;
        }
        if (count) {
            const noun = entries.length === 1 ? 'Pipeline' : 'Pipelines';
            const label = query ? `${visible} of ${entries.length} ${noun}` : `${visible} ${noun}`;
            if (count.textContent !== label) count.textContent = label;
        }
        const empty = document.getElementById('pipelines-no-matches');
        if (empty) empty.hidden = visible > 0;
    }
    function mount() {
        const table = document.querySelector('.pipelines-runbook');
        if (mounted === table) return;
        mounted = table;
        apply();
    }
    for (const type of ['input', 'change']) document.addEventListener(type, event => {
        if (['pipelines-search', 'pipelines-sort'].includes(event.target.id)) apply();
    });
    new MutationObserver(mount).observe(document.documentElement, {subtree: true, childList: true});
    document.addEventListener('DOMContentLoaded', mount);
    mount();
})();
