/*
 * The API host answers a console callback without a valid session with `401` and the header
 * `X-Mrsm-Login` (the login page's path). Send the browser there (with `?next=` the current page)
 * instead of leaving the page waiting for an update that never comes. Nothing happens while the
 * login or registration form is shown, and a burst of redirects stops after a few in a row.
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

  window.fetch = function (input, init) {
    return originalFetch(input, init).then(function (response) {
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
