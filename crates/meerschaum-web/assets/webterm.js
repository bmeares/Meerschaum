(() => {
  'use strict';
  // The web terminal front end: xterm.js on the terminado websocket protocol served by
  // `meerschaum-server` (`["stdin", text]` / `["set_size", rows, cols, h, w]` to the server,
  // `["setup", {}]` / `["stdout", text]` / `["disconnect", 1]` back). It mounts on every
  // `[data-mrsm-webterm]` element: `data-ws` is the websocket path, `data-mode` is `standalone`
  // (`/webterm/<session>`: dark as in v4, `?theme=light` for a light page; the Dash console embeds
  // it in an iframe) or `embedded` (inside a page that has the extra key row and a
  // `mrsm-theme-dark` class on `<html>`).
  const SEQUENCES = { esc: '\x1b', tab: '\t', up: '\x1b[A', down: '\x1b[B', right: '\x1b[C', left: '\x1b[D' };
  const DARK = { background: '#000000', foreground: '#e6e6e6', cursor: '#e6e6e6', selectionBackground: '#3b4a5a' };
  const LIGHT = { background: '#ffffff', foreground: '#1f2328', cursor: '#1f2328', selectionBackground: '#b6d4f5' };
  const FONT = '"DejaVu Sans Mono", "Liberation Mono", monospace';
  const sessions = new Set();

  const LIGHT_PAGE = new URLSearchParams(location.search).get('theme') === 'light';
  if (LIGHT_PAGE) document.documentElement.classList.add('mrsm-theme-light');

  function dark(mode) {
    if (mode === 'standalone') return !LIGHT_PAGE;
    return document.documentElement.classList.contains('mrsm-theme-dark');
  }

  function mount(el) {
    if (el.dataset.rendered || typeof Terminal === 'undefined') return null;
    el.dataset.rendered = 'true';
    const mode = el.dataset.mode || 'embedded';
    const root = el.closest('.mrsm-webterm') || document;
    const term = new Terminal({
      fontFamily: FONT,
      fontSize: 16,
      lineHeight: 1.2,
      allowProposedApi: true,
      scrollback: 100000,
      cursorBlink: true,
      theme: dark(mode) ? DARK : LIGHT,
    });
    const fit = new FitAddon.FitAddon();
    term.loadAddon(fit);
    term.loadAddon(new Unicode11Addon.Unicode11Addon());
    term.unicode.activeVersion = '11';
    term.open(el);

    const session = { term, fit, el, root, mode, socket: null, mods: { ctrl: false, shift: false }, disposed: false, timer: null, fitting: false, reconnects: 0 };
    sessions.add(session);

    const send = (frame, userInput = true) => {
      if (userInput && frame[0] === 'stdin') session.lineDirty = true;
      const socket = session.socket;
      if (socket && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(frame));
    };
    session.send = send;
    const sendSize = () => send(['set_size', term.rows, term.cols, el.clientHeight, el.clientWidth]);

    // Fit after layout has settled (two frames), once per burst of resizes.
    const scheduleFit = () => {
      if (session.fitting || session.disposed) return;
      session.fitting = true;
      requestAnimationFrame(() => requestAnimationFrame(() => {
        session.fitting = false;
        if (session.disposed || !el.clientWidth || !el.clientHeight) return;
        try { fit.fit(); } catch (_) { /* hidden */ }
      }));
    };
    session.scheduleFit = scheduleFit;
    term.onResize(sendSize);
    session.idle = false;
    session.lineDirty = false;
    session.requests = new Map();
    term.parser.registerOscHandler(133, (data) => {
      if (data === 'A') { session.idle = true; session.lineDirty = false; }
      if (data === 'C') session.idle = false;
      return true;
    });
    // This pinned xterm bundle distinguishes real input from automatic terminal replies.
    // onData alone includes reedline's cursor-position/device queries at an idle prompt.
    const core = term._core && term._core.coreService;
    const userInput = core && typeof core.onUserInput === 'function'
      ? core.onUserInput(() => { session.lineDirty = true; }) : null;
    term.onData((data) => {
      const terminalReply = /^(?:\x1b\[[?>]?[\d;]+[Rcn])+$/.test(data);
      if (!userInput && !terminalReply) session.lineDirty = true;
      send(['stdin', data], false);
    });

    const connect = () => {
      if (session.disposed) return;
      const protocol = location.protocol === 'https:' ? 'wss' : 'ws';
      const socket = new WebSocket(protocol + '://' + location.host + el.dataset.ws);
      session.socket = socket;
      socket.onopen = () => {
        session.idle = false;
        if (session.reconnects) term.reset();
        try { fit.fit(); } catch (_) { /* hidden */ }
        sendSize();
      };
      socket.onmessage = (event) => {
        let message;
        try { message = JSON.parse(event.data); } catch (_) { return; }
        if (message[0] === 'stdout') term.write(message[1]);
        else if (message[0] === 'disconnect') term.write('\r\n\r\n[CLOSED]\r\n');
      };
      socket.onclose = (event) => {
        if (session.disposed || session.socket !== socket) return;
        // An unclean close (the server went away) reconnects; the shell ending or the
        // server refusing the session (clean closes) does not.
        if (!event.wasClean && event.code !== 1005) {
          session.reconnects += 1;
          session.timer = setTimeout(connect, 1000);
        }
      };
    };
    session.connect = connect;

    session.dispose = () => {
      session.disposed = true;
      clearTimeout(session.timer);
      const socket = session.socket;
      if (socket) {
        socket.onopen = socket.onmessage = socket.onclose = socket.onerror = null;
        if (socket.readyState <= WebSocket.OPEN) socket.close(1000, 'Closing');
      }
      resizer.disconnect();
      themes.disconnect();
      if (userInput) userInput.dispose();
      term.dispose();
      el.textContent = '';
      delete el.dataset.rendered;
      sessions.delete(session);
      if (window.terminal && window.terminal.session === session) delete window.terminal;
    };

    const resizer = new ResizeObserver(scheduleFit);
    resizer.observe(el);
    const themes = new MutationObserver(() => { term.options.theme = dark(mode) ? DARK : LIGHT; });
    if (mode !== 'standalone') themes.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });

    // The v4 page's globals, which tests and plugin scripts use.
    window.terminal = {
      term, fit, session,
      get socket() { return session.socket; },
    };
    try { fit.fit(); } catch (_) { /* hidden */ }
    connect();
    return session;
  }

  function mountAll() {
    document.querySelectorAll('[data-mrsm-webterm]:not([data-rendered])').forEach(mount);
  }

  // Sticky CTRL / SHIFT from the key row: applied to the next single key typed in the terminal.
  function showMods(session) {
    session.root.querySelectorAll('[data-key="ctrl"],[data-key="shift"]').forEach((button) => {
      const on = session.mods[button.dataset.key];
      button.classList.toggle('active', on);
      button.setAttribute('aria-pressed', on ? 'true' : 'false');
    });
  }
  // ponytail: relies on keydown.key being a single char; soft keyboards that report
  // 'Unidentified' / keyCode 229 do not combine, physical and Bluetooth keyboards do.
  window.addEventListener('keydown', (event) => {
    for (const session of sessions) {
      const mods = session.mods;
      if (!mods.ctrl && !mods.shift) continue;
      if (!session.el.contains(event.target)) continue;
      const key = event.key;
      if (!key || key.length !== 1) return;
      let ch = mods.shift ? key.toUpperCase() : key;
      if (mods.ctrl) {
        const code = ch.toUpperCase().charCodeAt(0);
        if (code >= 64 && code <= 95) ch = String.fromCharCode(code - 64);
        else if (code >= 97 && code <= 122) ch = String.fromCharCode(code - 96);
      }
      session.send(['stdin', ch]);
      session.mods = { ctrl: false, shift: false };
      showMods(session);
      event.preventDefault();
      event.stopImmediatePropagation();
      return;
    }
  }, true);

  // Tapping a key button must not take focus from the terminal's hidden textarea, or the soft
  // keyboard closes. The click still fires.
  document.addEventListener('pointerdown', (event) => {
    if (event.target.closest && event.target.closest('.mrsm-webterm-key, .mrsm-webterm-control')) event.preventDefault();
  }, true);

  function sessionOf(button) {
    const root = button.closest('.mrsm-webterm');
    for (const session of sessions) if (session.root === root) return session;
    return null;
  }
  document.addEventListener('click', (event) => {
    const button = event.target.closest && event.target.closest('[data-key], [data-webterm-action]');
    if (!button) return;
    const session = sessionOf(button);
    if (!session) return;
    if (button.dataset.key) {
      const key = button.dataset.key;
      if (key === 'ctrl' || key === 'shift') {
        session.mods[key] = !session.mods[key];
        showMods(session);
      } else if (SEQUENCES[key]) {
        session.send(['stdin', SEQUENCES[key]]);
      }
      session.term.focus();
      return;
    }
    const action = button.dataset.webtermAction;
    if (action === 'refresh') {
      const el = session.el;
      session.dispose();
      mount(el);
    } else if (action === 'fullscreen') {
      const on = session.root.classList.toggle('mrsm-webterm-fullscreen');
      button.textContent = on ? '\u2715' : '⛶';
      session.scheduleFit();
    } else if (action === 'new-window') {
      session.send(['new_window']);
      session.term.focus();
    }
  });

  // The v4 terminal page took keys from a parent dashboard over postMessage.
  window.addEventListener('message', (event) => {
    if (!event.isTrusted || event.origin !== location.origin || !event.data) return;
    const session = [...sessions][0];
    if (!session) return;
    if (event.data.action === '__EXEC') {
      const id = event.data.request_id;
      let result = session.requests.get(id);
      if (!result) {
        const connected = session.socket && session.socket.readyState === WebSocket.OPEN;
        const valid = typeof event.data.command === 'string' && !/[\x00-\x1f]/.test(event.data.command);
        const accepted = connected && session.idle && !session.lineDirty && valid;
        result = {action: '__EXEC_ACK', request_id: id, accepted,
          message: !connected ? 'Terminal is disconnected. Reconnect and submit again.' :
            !accepted ? 'Terminal is busy or has unfinished input. Finish it before submitting.' : ''};
        session.requests.set(id, result);
        if (accepted) {
          session.idle = false;
          session.send(['stdin', event.data.command + '\r']);
          session.term.focus();
        }
      }
      event.source.postMessage(result, event.origin);
    }
    else if (event.data.action === '__SEND_KEY') session.send(['stdin', event.data.key || '']);
    // The console's "+" button: a new tmux window running `mrsm` (v4's `__TMUX_NEW_WINDOW`).
    else if (event.data.action === '__TMUX_NEW_WINDOW') session.send(['new_window']);
    // The console's instance selector: `instance <keys>` typed at the shell after a Ctrl-C that
    // clears the line (what v4's page typed; `subaction_text` is v4's name for the keys).
    else if (event.data.action === 'instance') {
      const keys = event.data.instance || event.data.subaction_text;
      if (typeof keys === 'string' && keys && !/[\x00-\x1f]/.test(keys)) {
        if (session.idle && !session.lineDirty) {
          session.idle = false;
          session.send(['stdin', 'instance ' + keys + '\r']);
        }
      }
    }
    else if (event.data.action === '__TOGGLE_MOD' && event.data.mod in session.mods) {
      session.mods[event.data.mod] = !!event.data.on;
      showMods(session);
    }
  });

  window.mrsmWebterm = { mount, mountAll, sessions };
  const start = () => (document.fonts && document.fonts.ready ? document.fonts.ready : Promise.resolve()).then(mountAll);
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
  document.body && document.body.addEventListener('htmx:afterSwap', mountAll);
})();
