/* A plugin's source on its page: the file from `plugin-source-data` ({source, styles, runs}, see
 * `meerschaum.api.dash.plugin_sources.source_tokens`) drawn as text nodes with line numbers
 * (never as HTML), Copy, and fullscreen for the tree and the file (Escape leaves it).
 *
 * While a file is edited (`data.editing`), a code editor (CodeMirror, `codemirror.bundle.js`,
 * loaded the first time a file is edited) fills `#plugin-source-editor`: it highlights as it is
 * typed, Tab indents (Escape, then Tab, moves the focus on), Ctrl+S / Cmd+S saves, and its text
 * follows every edit into `plugin-source-draft`, which Save reads. The Vim button switches Vim
 * keybindings (`:w` saves), remembered in this browser. Dash never re-renders the
 * editor while it is typed in. The tree keeps the file open until Save or Cancel, and leaving the
 * page with unsaved changes asks first. */
(function () {
    'use strict';
    window.dash_clientside = window.dash_clientside || {};

    // The bundle sits next to this script (same fingerprint query, so a new release reloads it).
    const OWN_SRC = document.currentScript ? document.currentScript.src : '';
    const BUNDLE_SRC = OWN_SRC
        ? OWN_SRC.replace(/plugin_source\.js(\?.*)?$/, 'codemirror.bundle.js$1')
        : '/dash/assets/codemirror.bundle.js';
    let bundle = null;
    let current = null;  // {path, source, editor, mount}

    function loadBundle() {
        if (window.MrsmCodeEditor) return Promise.resolve(window.MrsmCodeEditor);
        if (bundle) return bundle;
        bundle = new Promise(function (resolve, reject) {
            const script = document.createElement('script');
            script.src = BUNDLE_SRC;
            script.async = true;
            script.onload = function () {
                window.MrsmCodeEditor ? resolve(window.MrsmCodeEditor) : reject(new Error('no editor'));
            };
            script.onerror = function () { bundle = null; reject(new Error('editor not loaded')); };
            document.head.appendChild(script);
        });
        return bundle;
    }

    const VIM_KEY = 'mrsm-plugin-source-vim';
    function vimPreferred() {
        try { return window.localStorage.getItem(VIM_KEY) === '1'; } catch (e) { return false; }
    }
    function rememberVim(on) {
        try { window.localStorage.setItem(VIM_KEY, on ? '1' : '0'); } catch (e) { /* private mode */ }
    }
    function showVim(on) {
        const button = document.querySelector('[data-source-action="vim"]');
        if (!button) return;
        button.setAttribute('aria-pressed', on ? 'true' : 'false');
        button.classList.toggle('active', on);
    }

    function setText(text) {
        window.dash_clientside.set_props('plugin-source-draft', {data: text});
    }

    function save() { document.querySelector('.plugin-source-save')?.click(); }

    /* Without the bundle (blocked or offline), a plain text area still edits the file. */
    function plainEditor(mount, data) {
        const area = document.createElement('textarea');
        area.className = 'plugin-source-plain-editor';
        area.value = data.source;
        area.spellcheck = false;
        area.setAttribute('wrap', 'off');
        area.setAttribute('aria-label', 'Edit ' + data.path);
        area.addEventListener('input', function () { setText(area.value); });
        area.addEventListener('keydown', function (event) {
            if ((event.ctrlKey || event.metaKey) && !event.altKey && event.key.toLowerCase() === 's') {
                event.preventDefault();
                save();
            }
        });
        mount.replaceChildren(area);
        return {getText: () => area.value, focus: () => area.focus(), destroy: () => area.remove()};
    }

    function closeEditor() {
        if (current && current.editor) current.editor.destroy();
        current = null;
    }

    function openEditor(mount, data) {
        closeEditor();
        const state = {path: data.path, source: data.source, editor: null, mount};
        current = state;
        mount.setAttribute('aria-busy', 'true');
        const ready = function (editor) {
            mount.removeAttribute('aria-busy');
            // Another file (or Cancel) replaced this one while the bundle loaded.
            if (current !== state || !mount.isConnected) { editor.destroy(); return; }
            state.editor = editor;
            mount.closest('.plugin-source-preview')?.setAttribute('data-editor-ready', 'true');
            editor.focus();
        };
        loadBundle().then(function (api) {
            if (current !== state || !mount.isConnected) return;
            mount.replaceChildren();
            const vim = vimPreferred();
            ready(api.create(mount, {doc: data.source, path: data.path, onChange: setText, onSave: save, vim}));
            showVim(vim);
        }).catch(function () {
            if (current !== state || !mount.isConnected) return;
            document.querySelector('[data-source-action="vim"]')?.remove();
            ready(plainEditor(mount, data));
        });
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
        const target = document.getElementById(editing ? 'plugin-source-editor' : 'plugin-source-code');
        if (!target) return window.dash_clientside.no_update;
        const browser = target.closest('.plugin-source-browser');
        if (browser) {
            browser.classList.toggle('plugin-source-editing', editing);
            setFullscreen(browser, browser.classList.contains('plugin-source-fullscreen'));
        }
        if (editing) {
            openEditor(target, data);
        } else {
            closeEditor();
            renderCode(target, data);
        }
        target.closest('.plugin-source-preview')?.setAttribute('data-rendered', 'true');
        return window.dash_clientside.no_update;
    }
    /** The edited text (`null` while no editor is open). */
    function text() {
        return current && current.editor && current.mount.isConnected ? current.editor.getText() : null;
    }
    window.dash_clientside.mrsmPluginSource = {render, text};

    function dirty() {
        return !!(current && current.editor && current.mount.isConnected
                  && current.editor.getText() !== current.source);
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
    }

    document.addEventListener('click', function (event) {
        const button = event.target.closest && event.target.closest('[data-source-action]');
        if (!button) return;
        const browser = button.closest('.plugin-source-browser');
        if (!browser) return;
        const action = button.getAttribute('data-source-action');
        if (action === 'fullscreen') {
            setFullscreen(browser, !browser.classList.contains('plugin-source-fullscreen'));
        } else if (action === 'vim') {
            const editor = current && current.editor;
            if (!editor || !editor.setVim) return;
            const on = !editor.isVim();
            editor.setVim(on);
            rememberVim(on);
            showVim(on);
            editor.focus();
        } else if (action === 'copy') {
            const code = document.getElementById('plugin-source-code');
            const text = current && current.editor && current.mount.isConnected
                ? current.editor.getText() : (code ? code.textContent : '');
            if (!navigator.clipboard) return;
            navigator.clipboard.writeText(text).then(function () {
                button.classList.add('copied');
                setTimeout(function () { button.classList.remove('copied'); }, 1500);
            }).catch(function () { /* the page may not be allowed to write the clipboard */ });
        }
    });

    document.addEventListener('keydown', function (event) {
        // An Escape the editor used (closing its search panel) does not also leave fullscreen, and
        // in Vim mode Escape inside the editor is always Vim's.
        if (event.key !== 'Escape' || event.defaultPrevented) return;
        if (current && current.editor && current.editor.isVim && current.editor.isVim()
            && event.target && event.target.closest && event.target.closest('#plugin-source-editor')) return;
        const browser = document.querySelector('.plugin-source-browser.plugin-source-fullscreen');
        if (!browser) return;
        setFullscreen(browser, false);
        browser.querySelector('[data-source-action="fullscreen"]')?.focus();
    });

    window.addEventListener('beforeunload', function (event) {
        if (!dirty()) return;
        event.preventDefault();
        event.returnValue = '';
    });
})();
