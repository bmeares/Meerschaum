/*
 * The API host answers a console callback without a valid session with `401` and the header
 * `X-Mrsm-Login` (the login page's path). Send the browser there (with `?next=` the current page)
 * instead of leaving the page waiting for an update that never comes. Nothing happens while the
 * login or registration form is shown, and a burst of redirects stops after a few in a row.
 * A `409` with `X-Mrsm-Reload` (a page loaded from an older console) reloads the page.
 * A `503` with `Retry-After` (the host could not read the session store, so the callback did not
 * run) is sent again after that delay, a few times, instead of leaving the callback's outputs
 * unrendered: the renderer never repeats a failed callback by itself.
 */
(function () {
  if (window.__mrsmSessionGuard || !window.fetch) { return; }
  window.__mrsmSessionGuard = true;
  var originalFetch = window.fetch.bind(window);
  var redirecting = false;
  var HISTORY_KEY = 'mrsm-login-redirects';

  function onLoginForm() {
    return !!(document.getElementById('login-button') || document.getElementById('register-button'));
  }

  function recentRedirects() {
    try {
      var now = Date.now();
      var stamps = JSON.parse(window.sessionStorage.getItem(HISTORY_KEY) || '[]')
        .filter(function (stamp) { return now - stamp < 30000; });
      stamps.push(now);
      window.sessionStorage.setItem(HISTORY_KEY, JSON.stringify(stamps));
      return stamps.length;
    } catch (e) {
      return 1;
    }
  }

  function requestUrl(input) {
    if (typeof input === 'string') { return input; }
    return (input && input.url) || '';
  }

  var RELOAD_KEY = 'mrsm-stale-reloads';
  var reloading = false;

  /* A page loaded from an older console asks for callbacks the server no longer has: the server
   * answers `409` with `X-Mrsm-Reload`, and the page reloads (at most twice a minute). */
  function reloadStalePage(response, input) {
    if (
      reloading
      || response.status !== 409
      || !response.headers.get('X-Mrsm-Reload')
      || requestUrl(input).indexOf('_dash-update-component') === -1
    ) {
      return;
    }
    var stamps = [];
    var now = Date.now();
    try {
      stamps = JSON.parse(window.sessionStorage.getItem(RELOAD_KEY) || '[]')
        .filter(function (stamp) { return now - stamp < 60000; });
    } catch (e) { stamps = []; }
    if (stamps.length >= 2) {
      console.warn('Meerschaum: this page is out of date, and reloading it did not help.');
      return;
    }
    stamps.push(now);
    try { window.sessionStorage.setItem(RELOAD_KEY, JSON.stringify(stamps)); } catch (e) { /* storage may be unavailable */ }
    reloading = true;
    window.location.reload();
  }

  /* The host answers a callback it could not authorize because the session store was busy (the
   * instance locked or timing out) with `503` and `Retry-After`, before running it
   * (`meerschaum/api/host/_callbacks.py`): sending the same request again is safe. */
  var RETRY_LIMIT = 6;
  var RETRY_MAX_MS = 2000;

  function aborted(init) {
    return !!(init && init.signal && init.signal.aborted);
  }

  function retryable(response, input, init) {
    return response.status === 503
      && response.headers.get('Retry-After') !== null
      && typeof input === 'string'
      && input.indexOf('_dash-update-component') !== -1
      && !aborted(init);
  }

  function retryDelay(response) {
    var seconds = parseFloat(response.headers.get('Retry-After'));
    var ms = Math.min((seconds >= 0 ? seconds : 1) * 1000, RETRY_MAX_MS);
    return ms * (0.75 + Math.random() * 0.5);
  }

  function send(input, init, attempt) {
    return originalFetch(input, init).then(function (response) {
      var retry = false;
      try { retry = attempt < RETRY_LIMIT && retryable(response, input, init); } catch (e) { retry = false; }
      if (!retry) { return response; }
      return new Promise(function (resolve) { setTimeout(resolve, retryDelay(response)); })
        .then(function () { return aborted(init) ? response : send(input, init, attempt + 1); });
    });
  }

  window.fetch = function (input, init) {
    return send(input, init, 0).then(function (response) {
      try { reloadStalePage(response, input); } catch (e) { /* never break the response */ }
      try {
        var loginPath = response.headers.get('X-Mrsm-Login');
        if (
          response.status === 401
          && loginPath
          && requestUrl(input).indexOf('_dash-update-component') !== -1
          && !redirecting
          && !onLoginForm()
          && window.location.pathname.replace(/\/+$/, '') !== loginPath.replace(/\/+$/, '')
        ) {
          if (recentRedirects() > 3) {
            console.warn('Meerschaum: this page needs a session, but signing in did not give one.');
            return response;
          }
          redirecting = true;
          try {
            window.localStorage.removeItem('session-store');
            window.localStorage.removeItem('session-store_timestamp');
          } catch (e) { /* storage may be unavailable */ }
          var next = window.location.pathname + window.location.search + window.location.hash;
          window.location.assign(loginPath + '?next=' + encodeURIComponent(next));
        }
      } catch (e) { /* never break the response */ }
      return response;
    });
  };
})();
