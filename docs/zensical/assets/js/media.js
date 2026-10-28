/* Local asciinema 3 players, including after Zensical instant navigation. */
(() => {
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
        fit: 'width', autoPlay: false, loop: false, idleTimeLimit: 2,
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
