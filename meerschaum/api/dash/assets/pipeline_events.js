/*
 * Live Syncing Engine runs on the Pipelines page.
 *
 * Each followed run is an `EventSource` on the Rust server's
 * `GET /engine/runs/{run_id}/events/stream` (same origin, authorized by the console session
 * cookie). Its typed events paint the stages' live statuses and chunk progress at once, and
 * ask the page's callbacks for a fresh Gantt, stage table and activity log (`pipeline-run-tick`,
 * at most every TICK_MS) instead of a fixed poll. `pipeline-run-streams` tells the callbacks
 * which runs stream (`open`), so a run without a stream (`failed`: the route is not allowed, the
 * user lacks `pipes:write`, the instance is not served) keeps the slow fallback poll.
 *
 * Every stream ends with a terminal event: `run_finished`, or the server's `run_closed` for a run
 * that will not finish (`reason`: `interrupted`, `holder_gone`, or `idle`, which falls back to
 * polling). A follow is closed on either, and after MAX_ERRORS failed reconnects in a row, so the
 * browser never reconnects in a loop. Only open follows paint live statuses.
 */
(function () {
    'use strict';

    var TICK_MS = 1500;
    var STREAM_POLL_MS = 15000;
    var FALLBACK_POLL_MS = 5000;
    var RETRY_MS = 30000;
    var MAX_ERRORS = 5;
    var EVENT_NAMES = [
        'run_started', 'node_planned', 'node_started', 'chunk_started', 'chunk_finished',
        'node_finished', 'node_failed', 'node_skipped', 'node_rebuild_pending',
        'watermark_advanced', 'stream_flushed', 'children_triggered', 'stream_restarted',
        'run_finished', 'run_closed'
    ];
    var NODE_STATUS = {
        node_planned: 'pending', node_started: 'running', node_skipped: 'skipped',
        node_failed: 'failed', node_rebuild_pending: 'upstream_failed'
    };
    // Events after which the server-rendered views (Gantt, stage table, log) are refreshed.
    var TICK_EVENTS = {
        run_started: 1, node_started: 1, chunk_finished: 1, node_finished: 1, node_failed: 1,
        node_skipped: 1, node_rebuild_pending: 1, stream_flushed: 1, run_finished: 1
    };

    var follows = window._mrsmRunFollows = window._mrsmRunFollows || new Map();
    var lastTick = 0;
    var tickTimer = null;

    function setProps(id, props) {
        if (window.dash_clientside && window.dash_clientside.set_props) {
            window.dash_clientside.set_props(id, props);
        }
    }

    /** The status of a stage from one event (`null`: no change). */
    function statusOf(event) {
        if (event.event === 'node_finished') {
            return (event.result || {}).success === false ? 'failed' : 'succeeded';
        }
        if (event.event === 'node_failed' && event.will_retry) return 'running';
        return NODE_STATUS[event.event] || null;
    }

    /** Fold one event into a followed run's live state. */
    function apply(follow, event) {
        follow.last = event.seq;
        var ref = event.pipe_ref;
        if (!ref) return;
        var status = statusOf(event);
        if (status) follow.statuses[ref] = status;
        var progress = follow.progress[ref];
        if (event.event === 'node_started' || !progress) {
            progress = follow.progress[ref] = {done: 0, failed: 0, total: null, rows: 0};
        }
        if (event.event === 'chunk_started' && event.total != null) progress.total = event.total;
        if (event.event === 'chunk_finished') {
            progress.done += 1;
            if (event.success === false) progress.failed += 1;
            progress.rows += event.rows || event.inserted || 0;
        }
        if (event.event === 'stream_flushed') progress.rows += event.rows || 0;
    }

    /** Live statuses of the runs still streaming (they override the persisted ones). */
    window.mrsmLiveRunStatuses = function () {
        var merged = {};
        follows.forEach(function (follow) {
            if (!follow.finished && follow.state === 'open') Object.assign(merged, follow.statuses);
        });
        return merged;
    };

    /** The chunk progress of a stage (`''` when there is none). */
    window.mrsmChunkProgress = function (ref) {
        var text = '';
        follows.forEach(function (follow) {
            var p = follow.progress[ref];
            if (follow.finished || follow.state !== 'open' || !p || (!p.done && p.total == null)) return;
            text = p.total != null ? ('chunk ' + p.done + '/' + p.total) : (p.done + (p.done === 1 ? ' chunk' : ' chunks'));
            if (p.rows) text += ' · ' + p.rows + ' rows';
            if (p.failed) text += ' · ' + p.failed + ' failed';
        });
        return text;
    };

    /** Add the chunk progress of running stages to their status badges. */
    window.mrsmDecorateChunkProgress = function () {
        document.querySelectorAll('.pipeline-stage').forEach(function (card) {
            var badge = card.querySelector('.pipeline-live-status');
            if (!badge || card.dataset.status !== 'running') return;
            var progress = window.mrsmChunkProgress(card.dataset.ref);
            badge.textContent = 'running' + (progress ? ' · ' + progress : '');
        });
    };

    function paint() {
        var merged = Object.assign({}, window._mrsmPipelineStatuses || {}, window.mrsmLiveRunStatuses());
        if (window.mrsmPaintPipelineStatuses) window.mrsmPaintPipelineStatuses(merged);
        window.mrsmDecorateChunkProgress();
    }

    /** Ask the callbacks for fresh server-rendered views, at most every TICK_MS. */
    function tick(now) {
        var wait = Math.max(0, lastTick + TICK_MS - Date.now());
        if (now) wait = 0;
        if (tickTimer) {
            if (!now) return;
            clearTimeout(tickTimer);
        }
        tickTimer = setTimeout(function () {
            tickTimer = null;
            lastTick = Date.now();
            setProps('pipeline-run-tick', {data: lastTick});
        }, wait);
    }

    function publishStates() {
        var states = {};
        var fallback = false;
        follows.forEach(function (follow, runId) {
            states[runId] = follow.state;
            if (follow.state === 'failed') fallback = true;
        });
        setProps('pipeline-run-streams', {data: states});
        setProps('pipelines-interval', {interval: fallback ? FALLBACK_POLL_MS : STREAM_POLL_MS});
    }

    /**
     * Follow a run's event stream (`path`: the stream's same-origin path). `kind` groups
     * follows (`selected`: the run the Runs view shows, `node`: a canvas sync).
     */
    window.mrsmFollowEngineRun = function (runId, path, kind) {
        if (!runId || !path || typeof EventSource === 'undefined') return null;
        var existing = follows.get(runId);
        if (existing && !(existing.state === 'failed' && Date.now() - existing.failedAt > RETRY_MS)) {
            existing.kinds[kind || 'selected'] = true;
            return existing;
        }
        if (existing) {
            // A refused stream is asked for again now and then (the run may have started since).
            kind = kind || Object.keys(existing.kinds)[0];
            follows.delete(runId);
        }
        var follow = {
            run_id: runId, path: path, state: 'connecting', last: null, finished: false,
            statuses: {}, progress: {}, kinds: {}
        };
        follow.kinds[kind || 'selected'] = true;
        var source = new EventSource(path, {withCredentials: true});
        follow.source = source;
        follows.set(runId, follow);
        source.onopen = function () {
            if (follow.state !== 'open') { follow.state = 'open'; publishStates(); }
        };
        // `errors` counts failed (re)connects in a row; an event resets it.
        follow.errors = 0;
        function fail() {
            source.close();
            follow.state = 'failed';
            follow.failedAt = Date.now();
            publishStates();
            paint();
            tick(true);
        }
        function onEvent(message) {
            var event;
            try { event = JSON.parse(message.data); } catch (e) { return; }
            if (event.run_id !== runId) return;
            follow.errors = 0;
            if (event.event === 'run_closed') {
                // The run will not send `run_finished` (its process died, or it went quiet):
                // stop following; the persisted state (polled) shows what happened.
                if (event.reason === 'idle') { fail(); return; }
                source.close();
                follow.finished = true;
                follow.state = 'finished';
                publishStates();
                paint();
                tick(true);
                return;
            }
            if (follow.state !== 'open') { follow.state = 'open'; publishStates(); }
            apply(follow, event);
            paint();
            if (event.event === 'run_finished') {
                // The server ends the stream; closing keeps the browser from reconnecting.
                source.close();
                follow.finished = true;
                follow.state = 'finished';
                publishStates();
                tick(true);
                return;
            }
            if (TICK_EVENTS[event.event]) tick(false);
        }
        EVENT_NAMES.forEach(function (name) { source.addEventListener(name, onEvent); });
        source.onerror = function () {
            // The browser reconnects by itself (sending `Last-Event-ID`) unless the server
            // refused the stream; then the page falls back to polling. A stream that keeps
            // failing to resume is given up the same way.
            if (follow.finished) return;
            follow.errors = (follow.errors || 0) + 1;
            if (source.readyState === 2 || follow.errors >= MAX_ERRORS) fail();
        };
        publishStates();
        return follow;
    };

    /** Stop following a run (or, with `kind`, drop only that reason to follow it). */
    window.mrsmUnfollowEngineRun = function (runId, kind) {
        var follow = follows.get(runId);
        if (!follow) return;
        if (kind) {
            delete follow.kinds[kind];
            if (Object.keys(follow.kinds).length) return;
        }
        if (follow.source) follow.source.close();
        follows.delete(runId);
        publishStates();
        paint();
    };

    /** Stop following every run (a new session). */
    window.mrsmUnfollowAllEngineRuns = function () {
        follows.forEach(function (follow) { if (follow.source) follow.source.close(); });
        follows.clear();
        publishStates();
    };

    /** Follow exactly one run of `kind` (or none). */
    window.mrsmFollowOnly = function (kind, runId, path) {
        follows.forEach(function (follow, id) {
            if (id !== runId && follow.kinds[kind]) window.mrsmUnfollowEngineRun(id, kind);
        });
        if (runId && path) window.mrsmFollowEngineRun(runId, path, kind);
    };
})();
