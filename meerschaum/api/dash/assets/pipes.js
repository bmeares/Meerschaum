/* The Pipes pages (`/dash/pipes`, a pipe's page and New pipe).
 *
 * - A row of the Pipes table opens its pipe's page (the Pipe link in its first cell does the
 *   same for the keyboard; Ctrl/Cmd-click opens a new tab).
 * - The table is HTML (one `dcc.Markdown`, see `pipes_table.py`): a header's sort button sets
 *   `pipes-sort-click`, which the sort callback reads.
 * - Choosing another global instance on a Pipes page shows that instance's table (the reload of
 *   `global_instance.js` would otherwise keep a deep link's `?instance=`).
 * - `window.mrsmPipes.follow(store)` streams an action's output (`/logs/<job>/stream`, the
 *   server's event stream of the job's log) into the page's output panel, rendering its ANSI
 *   colors; when the job ends the page re-reads the active tab (`pipe-detail-refresh`).
 */
(function () {
    'use strict';

    document.addEventListener('click', function (event) {
        var target = event.target;
        if (!target || !target.closest) return;
        var sortButton = target.closest('#pipes-table button[data-sort]');
        if (sortButton) {
            setProps('pipes-sort-click', {data: {column: sortButton.getAttribute('data-sort'), at: Date.now()}});
            return;
        }
        var row = target.closest('tr.pipes-table-row[data-href]');
        if (!row) return;
        if (target.closest('a, button, input, select, textarea, summary, label')) return;
        var selection = window.getSelection ? String(window.getSelection()) : '';
        if (selection) return;
        var href = row.getAttribute('data-href');
        if (event.metaKey || event.ctrlKey) {
            window.open(href, '_blank', 'noopener');
            return;
        }
        if (window.mrsmNavigate) window.mrsmNavigate(href);
        else window.location.assign(href);
    });

    // Capture on `window` runs before `global_instance.js` (on `document`), which reloads the page.
    window.addEventListener('change', function (event) {
        var target = event.target;
        if (!target || target.id !== 'pages-instance-select') return;
        if (window.location.pathname.indexOf('/dash/pipes') !== 0) return;
        window.history.replaceState(window.history.state, '', '/dash/pipes');
    }, true);

    var LOG_PREFIX = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2} \| ?/;
    var MAX_LINES = 5000;
    var current = {source: null, key: null, lines: []};

    function setProps(id, props) {
        if (window.dash_clientside && window.dash_clientside.set_props) {
            window.dash_clientside.set_props(id, props);
        }
    }

    function render() {
        var view = document.getElementById('pipe-action-output');
        if (!view) {
            // The page changed: stop following.
            close();
            return;
        }
        var follow = (view.scrollHeight - view.scrollTop - view.clientHeight) < 40;
        var text = current.lines.join('\n');
        if (window.AnsiUp) {
            view.innerHTML = new window.AnsiUp().ansi_to_html(text);
        } else {
            view.textContent = text;
        }
        if (follow) view.scrollTop = view.scrollHeight;
    }

    function status(text, kind) {
        setProps('pipe-action-status', {children: text, className: 'pipes-output-status pipes-output-' + kind});
    }

    function close() {
        if (current.source) {
            current.source.close();
        }
        current.source = null;
    }

    function finished(kind, text) {
        close();
        status(text, kind);
        setProps('pipe-detail-refresh', {data: Date.now()});
    }

    function follow(store) {
        if (!store || !store.job) return;
        var key = store.job + ':' + store.started;
        if (current.key === key) return;
        close();
        current.key = key;
        current.lines = [];
        setProps('pipe-action-panel', {hidden: false});
        setProps('pipe-action-command', {children: store.command || ''});
        render();
        if (!store.stream) {
            current.lines.push('Started job ' + store.job + '.');
            render();
            status('started', 'running');
            return;
        }
        status('running', 'running');
        var source = new EventSource(store.stream);
        current.source = source;
        source.onmessage = function (event) {
            if (current.source !== source) return;
            current.lines.push(String(event.data).replace(/\n$/, '').replace(LOG_PREFIX, ''));
            if (current.lines.length > MAX_LINES) current.lines.splice(0, current.lines.length - MAX_LINES);
            render();
        };
        source.addEventListener('stopped', function () {
            if (current.source !== source) return;
            finished('finished', 'finished');
        });
        source.onerror = function () {
            if (current.source !== source) return;
            if (source.readyState === 2) finished('disconnected', 'disconnected');
        };
    }

    window.mrsmPipes = {
        follow: follow,
        close: function () {
            close();
            current.key = null;
        },
    };
})();
