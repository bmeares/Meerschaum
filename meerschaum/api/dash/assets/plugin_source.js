/* A plugin's source on its page: the file from `plugin-source-data` ({source, styles, runs}, see
 * `meerschaum.api.dash.plugin_sources.source_tokens`) drawn as text nodes with line numbers
 * (never as HTML), Copy, and fullscreen for the tree and the file (Escape leaves it).
 *
 * While a file is edited (`data.editing`), the code is a textarea with a line-number gutter: Tab
 * indents (Escape, then Tab, moves the focus on), Ctrl+S / Cmd+S saves, the tree keeps the file
 * open until Save or Cancel, and leaving the page with unsaved changes asks first. */
(function () {
    'use strict';
    window.dash_clientside = window.dash_clientside || {};

    const INDENT = '    ';
    let loaded = null;
    let tabReleased = false;

    function editor() { return document.getElementById('plugin-source-editor'); }

    function lineCount(text) { return (String(text || '').match(/\n/g) || []).length + 1; }

    // The editor is as tall as its text, up to 65% of the screen (fullscreen: the rest of the
    // screen); the gutter is exactly as tall, so its numbers never stretch the row.
    function fitEditor(area) {
        const browser = area.closest('.plugin-source-browser');
        const gutter = area.parentElement && area.parentElement.querySelector('.plugin-source-gutter');
        if (browser && browser.classList.contains('plugin-source-fullscreen')) {
            area.style.height = '';
            if (gutter) gutter.style.height = '';
            return;
        }
        const ratio = window.matchMedia('(max-width: 767.98px)').matches ? 0.55 : 0.65;
        const max = Math.max(240, Math.round(window.innerHeight * ratio));
        if (gutter) gutter.style.height = '0px';
        area.style.height = 'auto';
        area.style.height = Math.min(Math.max(area.scrollHeight + 2, 192), max) + 'px';
        if (gutter) gutter.style.height = area.offsetHeight + 'px';
    }

    function updateGutter(area) {
        const gutter = area && area.parentElement && area.parentElement.querySelector('.plugin-source-gutter');
        if (!gutter) return;
        const count = lineCount(area.value);
        if (Number(gutter.dataset.lines) !== count) {
            const numbers = [];
            for (let n = 1; n <= count; n++) numbers.push(n);
            gutter.textContent = numbers.join('\n');
            gutter.dataset.lines = String(count);
        }
        gutter.scrollTop = area.scrollTop;
    }

    function renderCode(code, data) {
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
    }

    function render(data) {
        if (!data || typeof data.source !== 'string') return window.dash_clientside.no_update;
        const editing = !!data.editing;
        const target = editing ? editor() : document.getElementById('plugin-source-code');
        if (!target) return window.dash_clientside.no_update;
        const browser = target.closest('.plugin-source-browser');
        if (browser) browser.classList.toggle('plugin-source-editing', editing);
        loaded = editing ? {path: data.path, source: data.source} : null;
        if (editing) {
            updateGutter(target);
            fitEditor(target);
            target.focus({preventScroll: true});
            target.setSelectionRange(0, 0);
        } else {
            renderCode(target, data);
        }
        target.closest('.plugin-source-preview')?.setAttribute('data-rendered', 'true');
        return window.dash_clientside.no_update;
    }
    window.dash_clientside.mrsmPluginSource = { render: render };

    function dirty() {
        const area = editor();
        return !!(loaded && area && area.value !== loaded.source);
    }

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
        const area = browser.querySelector('#plugin-source-editor');
        if (area) fitEditor(area);
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

    document.addEventListener('input', function (event) {
        if (event.target && event.target.id === 'plugin-source-editor') {
            updateGutter(event.target);
            fitEditor(event.target);
        }
    });

    // `scroll` does not bubble: listen while capturing.
    document.addEventListener('scroll', function (event) {
        if (event.target && event.target.id === 'plugin-source-editor') updateGutter(event.target);
    }, true);

    function insertText(area, text) {
        // `insertText` keeps the browser's undo history and tells React the value changed.
        if (document.execCommand && document.execCommand('insertText', false, text)) return;
        area.setRangeText(text, area.selectionStart, area.selectionEnd, 'end');
        area.dispatchEvent(new Event('input', {bubbles: true}));
    }

    document.addEventListener('keydown', function (event) {
        const area = event.target && event.target.id === 'plugin-source-editor' ? event.target : null;
        if (area) {
            if ((event.ctrlKey || event.metaKey) && !event.altKey && event.key.toLowerCase() === 's') {
                event.preventDefault();
                document.querySelector('.plugin-source-save')?.click();
                return;
            }
            if (event.key === 'Escape') { tabReleased = true; }
            else if (event.key === 'Tab' && !tabReleased && !event.shiftKey && !event.ctrlKey
                     && !event.altKey && !event.metaKey) {
                event.preventDefault();
                insertText(area, INDENT);
                return;
            } else if (event.key !== 'Tab' && event.key !== 'Shift') { tabReleased = false; }
        }
        if (event.key !== 'Escape') return;
        const browser = document.querySelector('.plugin-source-browser.plugin-source-fullscreen');
        if (!browser) return;
        setFullscreen(browser, false);
        browser.querySelector('[data-source-action="fullscreen"]')?.focus();
    });

    document.addEventListener('focusin', function (event) {
        if (event.target && event.target.id === 'plugin-source-editor') tabReleased = false;
    });

    window.addEventListener('beforeunload', function (event) {
        if (!dirty()) return;
        event.preventDefault();
        event.returnValue = '';
    });

    window.addEventListener('resize', function () {
        const area = editor();
        if (area) fitEditor(area);
    });
})();
