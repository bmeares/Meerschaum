/* The console's global instance and in-page navigation.
 *
 * Choosing an instance (the web console's `instance-select` or a page's `pages-instance-select`)
 * stores it in the `mrsm-instance` cookie, which every page reads as its instance (a `?instance=`
 * deep link still overrides it). Choosing one on a Pipelines page returns to the Pipelines list,
 * the registry's own page.
 *
 * `window.mrsmNavigate(url, replace)` changes the URL the way `dcc.Link` does, so the console's
 * `dcc.Location`s route to it. A rendered `[data-mrsm-redirect]` (legacy links) is followed once.
 */
(function () {
    'use strict';
    var COOKIE = 'mrsm-instance';

    function setInstance(keys) {
        if (!keys) return;
        document.cookie = COOKIE + '=' + encodeURIComponent(keys) + '; path=/; max-age=31536000; SameSite=Lax';
    }

    window.mrsmNavigate = function (url, replace) {
        if (replace) window.history.replaceState({}, '', url);
        else window.history.pushState({}, '', url);
        window.dispatchEvent(new CustomEvent('_dashprivate_pushstate'));
        window.scrollTo(0, 0);
    };

    document.addEventListener('change', function (e) {
        var target = e.target;
        if (!target || !target.id) return;
        if (target.id === 'instance-select') setInstance(target.value);
        else if (target.id === 'pages-instance-select') {
            setInstance(target.value);
            if (location.pathname.indexOf('/dash/pipelines') === 0) window.location.assign('/dash/pipelines');
            else window.location.reload();
        }
    }, true);

    function followRedirect() {
        var marker = document.querySelector('[data-mrsm-redirect]');
        if (!marker || marker._mrsmFollowed) return;
        marker._mrsmFollowed = true;
        window.mrsmNavigate(marker.getAttribute('data-mrsm-redirect'), true);
    }
    new MutationObserver(followRedirect).observe(document.documentElement, {childList: true, subtree: true});
    document.addEventListener('DOMContentLoaded', followRedirect);
})();
