/* Local asciinema 3 players and copy buttons (works across Zensical instant navigation). */
(() => {
  // Delegate clipboard handling so it works across instant navigation.
  document.addEventListener('click', async (event) => {
    const button = event.target.closest('[data-copy]');
    if (!button) return;
    const status = button.parentElement?.querySelector('[role="status"]');
    const original = button.innerHTML;
    try {
      await navigator.clipboard.writeText(button.dataset.copy);
      if (status) {
        status.textContent = 'Copied!';
      } else {
        button.textContent = 'Copied!';
      }
      window.setTimeout(() => {
        if (status) status.textContent = '';
        else button.innerHTML = original;
      }, 1800);
    } catch (_) {
      if (status) status.textContent = 'Copy: ' + button.dataset.copy;
    }
  });

  const reducedMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)')?.matches ?? false;
  const active = new Map(); // element -> { player, autoPlay, userPaused }

  const parseBool = (el, name) => {
    if (!el.hasAttribute(name)) return false;
    const v = (el.getAttribute(name) || '').trim().toLowerCase();
    return v === '' || v === 'true' || v === '1' || v === name;
  };

  const parseIntAttr = (el, name) => {
    const raw = el.getAttribute(name);
    if (!raw) return undefined;
    const n = Number.parseInt(raw, 10);
    return Number.isFinite(n) && n > 0 ? n : undefined;
  };

  const parseFloatAttr = (el, name, fallback) => {
    const raw = el.getAttribute(name);
    if (!raw) return fallback;
    const n = Number.parseFloat(raw);
    return Number.isFinite(n) && n > 0 ? n : fallback;
  };

  const mountPlayer = (element) => {
    if (active.has(element) || !window.AsciinemaPlayer) return;
    if (element.offsetParent === null || element.getBoundingClientRect().width < 40) return;

    const source = element.getAttribute('src');
    if (!source) return;

    const wantAutoPlay = parseBool(element, 'autoplay') && !reducedMotion;
    const wantLoop = parseBool(element, 'loop');
    const wantPreload = parseBool(element, 'preload') || wantAutoPlay;
    const rows = parseIntAttr(element, 'rows');
    const cols = parseIntAttr(element, 'cols');
    const speed = parseFloatAttr(element, 'speed', 1);
    const idleTimeLimit = parseFloatAttr(element, 'idle-time-limit', 2);
    const poster = element.getAttribute('poster') || (wantAutoPlay ? undefined : 'npt:0:04');

    const opts = {
      fit: element.getAttribute('fit') || 'width',
      autoPlay: wantAutoPlay,
      loop: wantLoop,
      preload: wantPreload,
      speed,
      idleTimeLimit,
      theme: element.getAttribute('theme') || 'asciinema',
    };
    if (rows) opts.rows = rows;
    if (cols) opts.cols = cols;
    if (poster) opts.poster = poster;

    element.setAttribute('role', 'group');
    element.setAttribute(
      'aria-label',
      element.getAttribute('title') || 'Terminal recording. Use the playback controls to watch.'
    );
    element.innerHTML = '';

    const player = AsciinemaPlayer.create(source, element, opts);
    const state = { player, autoPlay: wantAutoPlay, userPaused: false };

    if (typeof player.addEventListener === 'function') {
      player.addEventListener('pause', () => {
        if (element.dataset.mrsmAutoPaused === '1') return;
        state.userPaused = true;
      });
      player.addEventListener('play', () => {
        state.userPaused = false;
      });
    }
    active.set(element, state);
  };

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        const element = entry.target;
        const state = active.get(element);
        if (!entry.isIntersecting) {
          if (state) {
            element.dataset.mrsmAutoPaused = '1';
            state.player.pause();
            delete element.dataset.mrsmAutoPaused;
          }
          continue;
        }
        if (!state) {
          mountPlayer(element);
        } else if (state.autoPlay && !state.userPaused) {
          state.player.play();
        }
      }
    },
    { threshold: 0 }
  );

  const cleanupDetached = () => {
    for (const [element, state] of active.entries()) {
      if (!document.contains(element)) {
        try {
          observer.unobserve(element);
          if (typeof state.player.dispose === 'function') state.player.dispose();
        } catch (_) {
          // Ignore disposal errors on already-torn-down nodes.
        }
        active.delete(element);
      }
    }
  };

  const observeAll = () => {
    cleanupDetached();
    document.querySelectorAll('asciinema-player').forEach((element) => {
      if (element.dataset.observed === 'true') return;
      element.dataset.observed = 'true';
      observer.observe(element);

      // Mount visible autoplay players immediately on first paint.
      if (parseBool(element, 'autoplay') && !element.closest('details:not([open])')) {
        window.requestAnimationFrame(() => mountPlayer(element));
      }

      // Mount cleanly when an enclosing collapsible <details> opens.
      const details = element.closest('details');
      if (details) {
        details.addEventListener('toggle', () => {
          if (details.open) {
            window.requestAnimationFrame(() => mountPlayer(element));
          } else {
            const state = active.get(element);
            if (state) {
              element.dataset.mrsmAutoPaused = '1';
              state.player.pause();
              delete element.dataset.mrsmAutoPaused;
            }
          }
        });
      }
    });
  };

  if (typeof document$ !== 'undefined') document$.subscribe(observeAll);
  else if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', observeAll);
  else observeAll();
})();
