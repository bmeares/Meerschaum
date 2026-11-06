/* Sorting and paging use only the already-authorized breakdown in this browser. */
(function () {
    'use strict';
    window.dash_clientside = window.dash_clientside || {};
    const state = {};
    window.dash_clientside.mrsmPerformance = {
        /* A refresh's update (`performance-live-data`): each mounted part, graph and breakdown
         * store takes its value by key; one the update does not name stays as it is. */
        live: function (data) {
            const noUpdate = window.dash_clientside.no_update;
            const outputs = window.dash_clientside.callback_context.outputs_list || [];
            const kinds = [['parts', 'part'], ['charts', 'chart'], ['groups', 'group']];
            return kinds.map(([kind, key], index) => {
                const values = (data && data[kind]) || {};
                return (outputs[index] || []).map(output => {
                    const name = output.id[key];
                    return Object.prototype.hasOwnProperty.call(values, name) ? values[name] : noUpdate;
                });
            });
        },
        groups: function (data, search, sort, requestedPage) {
            if (!data) return [[], 1, 1, ''];
            const component = (type, props, namespace) => ({
                namespace: namespace || 'dash_html_components', type: type, props: props
            });
            const query = String(search || '').trim().toLocaleLowerCase();
            const rows = (data.rows || []).filter(row =>
                String(row.label).toLocaleLowerCase().includes(query));
            rows.sort((a, b) => {
                if (sort === 'name') return a.label.localeCompare(b.label, undefined, {numeric: true});
                const value = row => {
                    const n = row.sort[sort || 'duration_seconds'];
                    return n == null || !Number.isFinite(Number(n)) ? -Infinity : Number(n);
                };
                return value(b) - value(a) || a.label.localeCompare(b.label);
            });
            const size = window.matchMedia('(max-width: 767.98px)').matches ? 5 : 15;
            const pages = Math.max(1, Math.ceil(rows.length / size));
            let page = Math.max(1, Math.min(pages, Number(requestedPage) || 1));
            const changed = (window.dash_clientside.callback_context.triggered || []).map(item => item.prop_id);
            const previous = state[data.kind];
            if (!previous || previous.scope !== data.scope || previous.query !== query || previous.sort !== sort) {
                page = 1;
            } else if (changed.some(id => id.endsWith('.data')) || !changed.some(id => id.endsWith('.active_page'))) {
                page = Math.max(1, Math.min(pages, previous.page));
            }
            state[data.kind] = {scope: data.scope, query: query, sort: sort, page: page};
            const start = (page - 1) * size;
            const visible = rows.slice(start, start + size);
            const body = visible.map(row => {
                // A Pipe reads `Pipe('ck', 'mk', 'lk')`, colored like the Pipes page.
                const label = row.repr ? component('Span', {className: 'mrsm-pipe-repr',
                    children: row.repr.map(part => component('Span', {children: part[0],
                        className: 'mrsm-repr-' + part[1]}))}) : row.label;
                const name = [component('Link', {children: label, href: row.href,
                    className: 'console-name-link'}, 'dash_core_components')];
                if (row.running) name.push(component('Small', {children: row.running + ' running',
                    className: 'performance-running'}));
                return component('Tr', {key: row.key, children: [
                    component('Td', {children: name, 'data-label': data.headers[0],
                        className: 'performance-row-name'}),
                    ...row.cells.map((value, index) => component('Td', {
                        children: value, 'data-label': data.headers[index + 1],
                        className: index === 1 && row.sort.failed > 0 ? 'text-danger' : ''
                    }))
                ]});
            });
            const table = component('Table', {className:
                'table table-sm table-hover console-table performance-table performance-group-table', children: [
                    component('Thead', {children: component('Tr', {children: data.headers.map(header =>
                        component('Th', {scope: 'col', children: header}))})}),
                    component('Tbody', {children: body})
                ]});
            const content = rows.length ? table : component('P', {children: 'Nothing matches this search.',
                className: 'text-muted performance-no-matches'});
            const range = rows.length ? (start + 1) + '–' + (start + visible.length) + ' of ' +
                rows.length.toLocaleString() + ' ' + data.plural : '0 ' + data.plural;
            return [content, pages, page, range];
        }
    };
})();
