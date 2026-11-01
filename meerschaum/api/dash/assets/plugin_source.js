/* A plugin's source on its page: the file from `plugin-source-data` ({source, styles, runs}, see
 * `meerschaum.api.dash.plugin_sources.source_tokens`) drawn as text nodes with line numbers
 * (never as HTML), Copy, and fullscreen for the tree and the file (Escape leaves it). */
(function () {
    'use strict';
    window.dash_clientside = window.dash_clientside || {};

    function render(data) {
        const code = document.getElementById('plugin-source-code');
        if (!code || !data || typeof data.source !== 'string') return window.dash_clientside.no_update;
        const source = data.source, styles = data.styles || [], runs = data.runs || [];
        const fragment = document.createDocumentFragment();
        let line = document.createElement('span');
        line.className = 'source-line';
        const endLine = () => {
            fragment.appendChild(line);
            line = document.createElement('span');
            line.className = 'source-line';
        };
        const append = (text, style) => {
            if (!text) return;
            const node = style >= 0 && styles[style] ? document.createElement('span') : null;
            if (node) {
                node.className = 'source-token-' + styles[style];
                node.textContent = text;
                line.appendChild(node);
            } else {
                line.appendChild(document.createTextNode(text));
            }
        };
        let offset = 0;
        const runList = runs.length ? runs : [[source.length, -1]];
        for (const [length, style] of runList) {
            const text = source.slice(offset, offset + length);
            offset += length;
            const pieces = text.split('\n');
            pieces.forEach((piece, index) => {
                append(piece, style);
                // Each line keeps its newline, so selecting and copying lines keeps them apart.
                if (index < pieces.length - 1) { line.appendChild(document.createTextNode('\n')); endLine(); }
            });
        }
        if (offset < source.length) append(source.slice(offset), -1);
        if (line.childNodes.length || !fragment.childNodes.length) fragment.appendChild(line);
        code.replaceChildren(fragment);
        code.closest('.plugin-source-preview')?.setAttribute('data-rendered', 'true');
        return window.dash_clientside.no_update;
    }
    window.dash_clientside.mrsmPluginSource = { render: render };

    function setFullscreen(browser, on) {
        browser.classList.toggle('plugin-source-fullscreen', on);
        document.body.classList.toggle('plugin-source-noscroll', on);
        const button = browser.querySelector('[data-source-action="fullscreen"]');
        if (button) {
            const label = on ? 'Exit fullscreen' : 'Fullscreen';
            button.setAttribute('aria-pressed', on ? 'true' : 'false');
            button.setAttribute('aria-label', label);
            button.title = label;
        }
    }

    document.addEventListener('click', function (event) {
        const button = event.target.closest && event.target.closest('[data-source-action]');
        if (!button) return;
        const browser = button.closest('.plugin-source-browser');
        if (!browser) return;
        const action = button.getAttribute('data-source-action');
        if (action === 'fullscreen') {
            setFullscreen(browser, !browser.classList.contains('plugin-source-fullscreen'));
        } else if (action === 'copy') {
            const store = document.getElementById('plugin-source-code');
            const text = store ? store.textContent : '';
            if (!navigator.clipboard) return;
            navigator.clipboard.writeText(text).then(function () {
                button.classList.add('copied');
                setTimeout(function () { button.classList.remove('copied'); }, 1500);
            }).catch(function () { /* the page may not be allowed to write the clipboard */ });
        }
    });

    document.addEventListener('keydown', function (event) {
        if (event.key !== 'Escape') return;
        const browser = document.querySelector('.plugin-source-browser.plugin-source-fullscreen');
        if (!browser) return;
        setFullscreen(browser, false);
        browser.querySelector('[data-source-action="fullscreen"]')?.focus();
    });
})();
