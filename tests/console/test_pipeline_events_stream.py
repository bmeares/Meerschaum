"""
The Pipelines page follows a running sync over the Rust server's
`GET /engine/runs/{run_id}/events/stream` (same origin, authorized by the console session cookie)
instead of polling the run's persisted detail.
"""
import pytest
from tests.console.test_console import SETUP_PIPES, _wait_text, _errors

pytestmark = pytest.mark.console


def test_canvas_sync_is_followed_over_the_event_stream(server, tab):
    setup = server.python(SETUP_PIPES + "\nfrom meerschaum.core.Pipeline._saved import save_pipeline\n"
                          "save_pipeline('stream-test', ['sql:w7console|sql:w7console|w7parent|None'], 'sql:w7console')\n")
    assert setup.returncode == 0, setup.stderr[-3000:]
    tab.login()
    tab.goto('/dash/pipelines/stream-test', '#pipelines-graph-div')
    page = tab.page
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    page.locator('.pipeline-stage[data-ref="sql:w7console|sql:w7console|w7parent|None"]').first.click()
    page.wait_for_selector('#pipeline-node-sync')
    page.locator('#pipeline-node-sync').click()
    _wait_text(page, '#pipeline-node-run-label', 'canvas-', timeout=30000)
    # The page opens the run's stream (waiting for the job to start the run) and receives its
    # events up to `run_finished`, then closes it instead of reconnecting.
    snapshot = page.wait_for_function(
        """() => {
            const f = Array.from((window._mrsmRunFollows || new Map()).values()).find(
                f => f.run_id.includes('-canvas-') && f.state === 'finished' && f.last !== null);
            return f ? {path: f.path, closed: f.source.readyState, statuses: {...f.statuses}} : null;
        }""",
        timeout=90000,
    )
    # Job completion may legitimately remove the follow before the next browser call. Keep
    # the values observed with the finished event, rather than looking the entry up again.
    try:
        follow = snapshot.json_value()
    finally:
        snapshot.dispose()
    assert follow['path'].startswith('/engine/runs/') and '-canvas-' in follow['path'], follow
    assert follow['closed'] == 2, follow
    assert follow['statuses'].get('sql:w7console|sql:w7console|w7parent|None') == 'succeeded', follow
    assert _errors(tab) == [], _errors(tab)
