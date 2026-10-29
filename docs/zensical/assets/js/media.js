/* Local asciinema 3 players, including after Zensical instant navigation. */
(() => {
  // Delegate clipboard handling so it also works after instant navigation.
  document.addEventListener('click', async (event) => {
    const button = event.target.closest('[data-copy]');
    if (!button) return;
    const status = button.parentElement.querySelector('[role="status"]');
    try {
      await navigator.clipboard.writeText(button.dataset.copy);
      if (status) status.textContent = 'Copied!';
    } catch (_) {
      if (status) status.textContent = 'Copy: ' + button.dataset.copy;
    }
  });
  const players = new WeakMap();
  const observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      const element = entry.target;
      const current = players.get(element);
      if (!entry.isIntersecting) {
        if (current) current.pause();
        continue;
      }
      if (current) continue;
      const source = element.getAttribute('src');
      if (!source || !window.AsciinemaPlayer) continue;
      element.setAttribute('role', 'group');
      element.setAttribute('aria-label', element.getAttribute('title') || 'Terminal recording. Use the playback controls to watch.');
      const player = AsciinemaPlayer.create(source, element, {
        fit: 'width', autoPlay: false, loop: false, idleTimeLimit: 10,
        theme: 'asciinema',
      });
      players.set(element, player);
    }
  });
  const mount = () => {
    document.querySelectorAll('asciinema-player').forEach((element) => {
      if (!element.dataset.observed) {
        element.dataset.observed = 'true';
        observer.observe(element);
      }
    });
  };
  if (typeof document$ !== 'undefined') document$.subscribe(mount);
  else if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
  else mount();
})();
