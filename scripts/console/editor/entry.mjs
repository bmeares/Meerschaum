// The console's plugin source editor (`window.MrsmCodeEditor`), bundled by `build.sh` into
// `meerschaum/api/dash/assets/codemirror.bundle.js`. The page loads it only when a file is
// opened for editing (`assets/plugin_source.js`); reading a file never loads it.
//
//   const editor = window.MrsmCodeEditor.create(parent, {
//       doc, path, onChange(text), onSave(), vim: false,
//   });
//   editor.getText(); editor.setVim(true); editor.focus(); editor.destroy();

import {EditorState, Compartment} from '@codemirror/state';
import {
    EditorView, keymap, lineNumbers, highlightActiveLineGutter, highlightSpecialChars,
    drawSelection, dropCursor, rectangularSelection, crosshairCursor, highlightActiveLine,
} from '@codemirror/view';
import {defaultKeymap, history, historyKeymap, indentWithTab} from '@codemirror/commands';
import {
    indentOnInput, indentUnit, bracketMatching, foldGutter, foldKeymap, syntaxHighlighting,
    HighlightStyle, StreamLanguage,
} from '@codemirror/language';
import {searchKeymap, highlightSelectionMatches} from '@codemirror/search';
import {closeBrackets, closeBracketsKeymap} from '@codemirror/autocomplete';
import {tags} from '@lezer/highlight';
import {vim, Vim} from '@replit/codemirror-vim';
import {python} from '@codemirror/lang-python';
import {rust} from '@codemirror/lang-rust';
import {javascript} from '@codemirror/lang-javascript';
import {json} from '@codemirror/lang-json';
import {yaml} from '@codemirror/lang-yaml';
import {markdown} from '@codemirror/lang-markdown';
import {html} from '@codemirror/lang-html';
import {css} from '@codemirror/lang-css';
import {sql} from '@codemirror/lang-sql';
import {toml} from '@codemirror/legacy-modes/mode/toml';
import {shell} from '@codemirror/legacy-modes/mode/shell';
import {properties} from '@codemirror/legacy-modes/mode/properties';
import {dockerFile} from '@codemirror/legacy-modes/mode/dockerfile';

const VERSION = '6.43.15';

/** The language of a file, by its name (none: plain text). */
function languageFor(path) {
    const name = String(path || '').split('/').pop().toLowerCase();
    const suffix = name.includes('.') ? name.slice(name.lastIndexOf('.')) : '';
    if (name === 'dockerfile' || name.startsWith('dockerfile.')) return StreamLanguage.define(dockerFile);
    switch (suffix) {
        case '.py': case '.pyi': case '.pyw': return python();
        case '.rs': return rust();
        case '.js': case '.mjs': case '.cjs': return javascript();
        case '.jsx': return javascript({jsx: true});
        case '.ts': case '.mts': case '.cts': return javascript({typescript: true});
        case '.tsx': return javascript({typescript: true, jsx: true});
        case '.json': case '.ipynb': return json();
        case '.yaml': case '.yml': return yaml();
        case '.md': case '.markdown': return markdown();
        case '.html': case '.htm': return html();
        case '.css': return css();
        case '.sql': return sql();
        case '.toml': return StreamLanguage.define(toml);
        case '.sh': case '.bash': case '.zsh': return StreamLanguage.define(shell);
        case '.ini': case '.cfg': case '.conf': case '.env': return StreamLanguage.define(properties);
        default: return [];
    }
}

// The same colors as the reader's tokens (`.source-token-*` in `assets/console_pages.css`).
const COLORS = {
    keyword: '#d7a8ff', string: '#b4d99b', comment: '#9ba8b4', number: '#efbe86',
    name: '#8cd3ef', text: '#dceaf6', muted: '#7d8b98', invalid: '#ff8a8a',
};

const highlight = HighlightStyle.define([
    {tag: [tags.keyword, tags.controlKeyword, tags.moduleKeyword, tags.operatorKeyword,
        tags.definitionKeyword, tags.modifier, tags.self, tags.operator], color: COLORS.keyword},
    {tag: [tags.string, tags.special(tags.string), tags.regexp, tags.character, tags.docString],
        color: COLORS.string},
    {tag: [tags.comment, tags.lineComment, tags.blockComment, tags.docComment], color: COLORS.comment,
        fontStyle: 'italic'},
    {tag: [tags.number, tags.bool, tags.null, tags.atom, tags.standard(tags.variableName),
        tags.constant(tags.variableName)], color: COLORS.number},
    {tag: [tags.function(tags.variableName), tags.function(tags.propertyName),
        tags.definition(tags.function(tags.variableName)), tags.className, tags.typeName,
        tags.namespace, tags.macroName, tags.meta, tags.annotation, tags.attributeName,
        tags.tagName], color: COLORS.name},
    {tag: [tags.heading], color: COLORS.name, fontWeight: 'bold'},
    {tag: [tags.link, tags.url], color: COLORS.name, textDecoration: 'underline'},
    {tag: tags.emphasis, fontStyle: 'italic'},
    {tag: tags.strong, fontWeight: 'bold'},
    {tag: tags.invalid, color: COLORS.invalid},
]);

const theme = EditorView.theme({
    '&': {color: COLORS.text, backgroundColor: '#17212b', fontSize: '0.8rem', height: '100%'},
    '.cm-scroller': {
        fontFamily: 'SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
        lineHeight: '1.5', overflow: 'auto',
    },
    '.cm-content': {caretColor: '#85caff', padding: '12px 0'},
    '.cm-cursor, .cm-dropCursor': {borderLeftColor: '#85caff', borderLeftWidth: '2px'},
    '&.cm-focused': {outline: 'none'},
    '&.cm-focused .cm-selectionBackground, .cm-selectionBackground, .cm-content ::selection':
        {backgroundColor: '#24425f !important'},
    '.cm-gutters': {backgroundColor: '#17212b', color: COLORS.muted, border: 'none',
        borderRight: '1px solid #2b3946'},
    '.cm-lineNumbers .cm-gutterElement': {padding: '0 12px 0 16px', minWidth: '3.5em'},
    '.cm-activeLine': {backgroundColor: '#1d2a37'},
    '.cm-activeLineGutter': {backgroundColor: '#1d2a37', color: COLORS.text},
    '.cm-foldPlaceholder': {backgroundColor: '#24425f', border: 'none', color: COLORS.text},
    '.cm-matchingBracket, &.cm-focused .cm-matchingBracket':
        {backgroundColor: '#2f4a63', outline: '1px solid #4f7192'},
    '.cm-selectionMatch': {backgroundColor: '#2a3b4c'},
    '.cm-searchMatch': {backgroundColor: '#5a4a1f', outline: '1px solid #8a6f2a'},
    '.cm-searchMatch.cm-searchMatch-selected': {backgroundColor: '#24425f'},
    '.cm-panels': {backgroundColor: '#131c25', color: COLORS.text},
    '.cm-panels.cm-panels-bottom': {borderTop: '1px solid #47515c'},
    '.cm-panel input, .cm-panel button': {fontSize: '0.85rem'},
    '.cm-textfield': {backgroundColor: '#0f171f', color: COLORS.text, border: '1px solid #47515c'},
    '.cm-button': {backgroundImage: 'none', backgroundColor: '#213449', color: COLORS.text,
        border: '1px solid #47515c'},
    '.cm-tooltip': {backgroundColor: '#131c25', color: COLORS.text, border: '1px solid #47515c'},
}, {dark: true});

// Vim's `:w` (and `:write`) saves through the editor that runs it.
const saveHandlers = new WeakMap();
Vim.defineEx('write', 'w', (cm) => {
    // `cm` is Vim's adapter; `cm.cm6` is the view.
    const save = saveHandlers.get(cm.cm6);
    if (save) save();
});

/**
 * An editor in `parent` holding `doc`. `onChange(text)` follows every edit; `onSave()` runs on
 * Ctrl+S / Cmd+S (and `:w` with `vim`). Tab indents; Escape then Tab moves the focus on (in Vim
 * mode, Escape is Vim's). `setVim(on)` switches Vim keybindings without reloading the text.
 */
function create(parent, options = {}) {
    const {doc = '', path = '', onChange = null, onSave = null} = options;
    const keys = new Compartment();
    // Vim's keymap must come before every other one.
    const mode = (on) => (on ? vim({status: true}) : []);
    const extensions = [
        keys.of(mode(!!options.vim)),
        lineNumbers(), highlightActiveLineGutter(), highlightSpecialChars(), history(), foldGutter(),
        drawSelection(), dropCursor(), EditorState.allowMultipleSelections.of(true), indentOnInput(),
        indentUnit.of('    '), EditorState.tabSize.of(4), syntaxHighlighting(highlight),
        bracketMatching(), closeBrackets(), rectangularSelection(), crosshairCursor(),
        highlightActiveLine(), highlightSelectionMatches(), theme, languageFor(path),
        EditorView.contentAttributes.of({'aria-label': `Edit ${path}`, spellcheck: 'false',
            autocapitalize: 'off', autocorrect: 'off'}),
        keymap.of([
            {key: 'Mod-s', preventDefault: true, run: () => { if (onSave) onSave(); return true; }},
            ...closeBracketsKeymap, ...defaultKeymap, ...searchKeymap, ...historyKeymap,
            ...foldKeymap, indentWithTab,
        ]),
        EditorView.updateListener.of((update) => {
            if (update.docChanged && onChange) onChange(update.state.doc.toString());
        }),
    ];
    const view = new EditorView({state: EditorState.create({doc, extensions}), parent});
    if (onSave) saveHandlers.set(view, onSave);
    let vimOn = !!options.vim;
    return {
        view,
        getText: () => view.state.doc.toString(),
        isVim: () => vimOn,
        setVim: (on) => {
            vimOn = !!on;
            view.dispatch({effects: keys.reconfigure(mode(vimOn))});
        },
        focus: () => view.focus(),
        destroy: () => view.destroy(),
    };
}

window.MrsmCodeEditor = {version: VERSION, create, languageFor};
