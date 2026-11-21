/* The console's global instance, acting team switcher, nested Pages menu, and in-page navigation.
 *
 * Choosing an instance (the web console's `instance-select` or a page's `pages-instance-select`)
 * stores it in the `mrsm-instance` cookie, which every page reads as its instance (a `?instance=`
 * deep link still overrides it). Choosing one on a Pipelines page returns to the Pipelines list,
 * the registry's own page.
 *
 * Choosing a team (`pages-team-select`) stores it in the `mrsm-team` cookie (or clears it for
 * All teams), clears `mrsm-instance` so the newly selected team's default instance takes effect
 * cleanly, and reloads the current section without stale `?instance=` deep links.
 *
 * `window.mrsmNavigate(url, replace)` changes the URL the way `dcc.Link` does, so the console's
 * `dcc.Location`s route to it. A rendered `[data-mrsm-redirect]` (legacy links) is followed once.
 */
(function () {
    'use strict';
    var COOKIE = 'mrsm-instance';
    var TEAM_COOKIE = 'mrsm-team';

    function setInstance(keys) {
        if (!keys) return;
        document.cookie = COOKIE + '=' + encodeURIComponent(keys) + '; path=/; max-age=31536000; SameSite=Lax';
    }

    function clearInstance() {
        document.cookie = COOKIE + '=; path=/; max-age=0; SameSite=Lax';
    }

    function setTeam(team) {
        if (!team) {
            document.cookie = TEAM_COOKIE + '=; path=/; max-age=0; SameSite=Lax';
            return;
        }
        document.cookie = TEAM_COOKIE + '=' + encodeURIComponent(team) + '; path=/; max-age=31536000; SameSite=Lax';
    }

    window.mrsmNavigate = function (url, replace) {
        if (replace) window.history.replaceState({}, '', url);
        else window.history.pushState({}, '', url);
        window.dispatchEvent(new CustomEvent('_dashprivate_pushstate'));
        window.scrollTo(0, 0);
    };

    // Keep the Pages dropdown open when toggling nested collapsible groups (`<summary>`).
    document.addEventListener('click', function (e) {
        var summary = e.target && e.target.closest ? e.target.closest('.console-nav-pages-group-toggle') : null;
        if (summary) {
            e.stopPropagation();
        }
    });

    document.addEventListener('change', function (e) {
        var target = e.target;
        if (!target || !target.id) return;
        if (target.id === 'instance-select') setInstance(target.value);
        else if (target.id === 'pages-instance-select') {
            setInstance(target.value);
            if (location.pathname.indexOf('/dash/pipelines') === 0) window.location.assign('/dash/pipelines');
            else if (location.pathname.replace(/\/$/, '') === '/dash/performance') {
                const url = new URL(window.location.href);
                for (const key of ['instance', 'pipe', 'pipeline']) url.searchParams.delete(key);
                window.location.assign(url.pathname + url.search);
            }
            else window.location.reload();
        }
        else if (target.id === 'pages-team-select') {
            setTeam(target.value);
            clearInstance();
            var path = location.pathname.replace(/\/+$/, '');
            if (path.indexOf('/dash/pipelines/') === 0) {
                window.location.assign('/dash/pipelines');
            } else if (path.indexOf('/dash/pipes/') === 0 && path !== '/dash/pipes/new') {
                window.location.assign('/dash/pipes');
            } else {
                const url = new URL(window.location.href);
                for (const key of ['instance', 'pipe', 'pipeline']) url.searchParams.delete(key);
                window.location.assign(url.pathname + url.search);
            }
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
