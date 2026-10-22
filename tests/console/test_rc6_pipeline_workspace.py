"""Exercise the Pipeline workspace in the actual Dash renderer."""
import pytest
from tests.console.test_console import SETUP_PIPES, _wait_text, _errors

pytestmark = pytest.mark.console


def test_pipeline_workspace_fullscreen_inspector_draft_and_pipe_sync(server, tab):
    try:
        setup = server.python(SETUP_PIPES + "\nfrom meerschaum.core.Pipeline._saved import save_pipeline\nsave_pipeline('workspace-test', ['sql:w7console|sql:w7console|w7parent|None'], 'sql:w7console')\n")
        assert setup.returncode == 0, setup.stderr[-3000:]
        tab.login()
        tab.goto('/dash/pipelines', '#pipelines-graph-div')
        page = tab.page
        page.wait_for_selector('.pipeline-stage', timeout=60000)
        page.locator('#pipeline-fullscreen').click()
        page.wait_for_function("document.fullscreenElement?.id === 'pipeline-workspace' || document.querySelector('#pipeline-workspace.pipeline-fullscreen')")
        assert not page.locator('#pipeline-output-dock').is_visible()
        assert page.locator('#pipeline-canvas').bounding_box()['height'] >= 300
        page.keyboard.press('Escape')
        page.wait_for_function('!document.fullscreenElement')
        canvas = page.locator('#pipeline-canvas')
        box = canvas.bounding_box()
        page.mouse.move(box['x'] + 20, box['y'] + 300)
        page.mouse.down()
        page.mouse.move(box['x'] + 80, box['y'] + 330, steps=8)
        page.mouse.up()
        assert page.evaluate('String(window.getSelection())') == ''
        page.locator('.pipeline-stage').first.click()
        page.wait_for_selector('#pipeline-resolve-symlinks')
        _wait_text(page, '#pipeline-inspector', 'Query')
        page.wait_for_function("!document.getElementById('pipeline-inspector').hasAttribute('aria-busy')")
        assert page.locator('#pipeline-resolve-symlinks').is_checked()
        page.locator('#pipeline-resolve-symlinks').uncheck()
        assert not page.locator('#pipeline-resolve-symlinks').is_checked()
        # Context menu and keyboard-accessible inspector controls offer the same sync scopes.
        page.locator('.pipeline-stage').first.click(button='right')
        assert page.locator('.pipeline-context-menu button', has_text='Sync with all parents').count() == 1
        page.keyboard.press('Escape')
        page.locator('#pipeline-node-sync').click()
        _wait_text(page, '#pipeline-node-run-label', 'execute-', timeout=30000)
        page.wait_for_function("document.getElementById('pipeline-node-output').textContent.length > 0", timeout=60000)
        assert page.locator('#pipeline-output-tab').is_visible()
        page.locator('#pipeline-output-close').click()
        # Select a registration, then edit membership as a reversible draft before saving.
        page.locator('.pipeline-group-title', has_text='workspace-test').click()
        _wait_text(page, '#pipeline-summary', 'workspace-test')
        # Adding a node opens the same instance-aware builder without registering a definition.
        page.locator('#pipeline-add-node').click()
        assert page.locator('#pipeline-draft-editor').get_attribute('open') is not None
        assert page.locator('#pipeline-add-instance').is_visible()
        assert page.locator('#pipeline-draft-members').is_visible()
        def add_child():
            page.locator('#pipeline-draft-members').click()
            option = page.locator('[role=option], .Select-option, .dash-dropdown-option').filter(has_text='w7child').last
            option.wait_for(state='visible')
            option.click()
        add_child()
        _wait_text(page, '#pipeline-draft-notice', 'Unsaved')
        page.locator('#pipeline-draft-undo').click()
        _wait_text(page, '#pipeline-draft-notice', 'matches saved')
        page.locator('#pipeline-draft-redo').click()
        _wait_text(page, '#pipeline-draft-notice', 'Unsaved')
        page.locator('#pipeline-draft-discard').click()
        _wait_text(page, '#pipeline-draft-notice', 'matches saved')
        add_child()
        page.locator('#pipeline-draft-save').click()
        _wait_text(page, '#pipeline-manage-alert', 'Saved pipeline')
        saved = server.python("from meerschaum.core.Pipeline._saved import list_pipelines; d=list_pipelines('sql:w7console')['workspace-test']; assert len(d['targets']) == 2, d")
        assert saved.returncode == 0, saved.stderr[-2000:]
        # Saving updates definition, draft and registration list through separate callbacks.
        page.evaluate('''() => {
            window._graphChanged = Date.now();
            window._graphSettling?.disconnect();
            window._graphSettling = new MutationObserver(() => { window._graphChanged = Date.now(); });
            window._graphSettling.observe(document.getElementById('pipelines-graph-div'), {childList:true, subtree:true});
        }''')
        page.wait_for_function("Date.now() - window._graphChanged > 1000 && document.getElementById('pipelines-graph-div').getAttribute('data-dash-is-loading') !== 'true'")
        page.locator('#pipeline-fit').click()
        node = page.locator('.pipeline-stage[data-ref="sql:w7console|sql:w7console|w7parent|None"]').first
        node.scroll_into_view_if_needed()
        node.wait_for(state='visible')
        box = node.bounding_box()
        original_left = node.evaluate('(node) => node.style.left')
        page.evaluate('''() => {
            window._rc6PointerEvents = [];
            for (const type of ['pointerdown', 'pointerup', 'pointercancel']) document.addEventListener(type,
                event => window._rc6PointerEvents.push({type, alt:event.altKey, target:event.target.className,
                    ref:event.target.closest('.pipeline-stage')?.dataset.ref}), true);
        }''')
        page.keyboard.down('Alt')
        page.mouse.move(box['x'] + box['width']/2, box['y'] + box['height']/2)
        page.mouse.down()
        page.mouse.move(box['x'] + box['width']/2 + 55, box['y'] + box['height']/2 + 25, steps=8)
        page.mouse.up()
        page.keyboard.up('Alt')
        assert node.evaluate('(node) => node.style.left') != original_left, page.evaluate('window._rc6PointerEvents')
        page.locator('#pipeline-save-layout').click()
        _wait_text(page, '#pipeline-layout-alert', 'Shared layout saved')
        saved_layout = server.python("from meerschaum.core.Pipeline._saved import list_pipelines; assert list_pipelines('sql:w7console')['workspace-test']['layout']")
        assert saved_layout.returncode == 0, saved_layout.stderr[-2000:]
        page.locator('.pipeline-stage').first.click()
        page.wait_for_selector('#pipeline-resolve-symlinks')
        page.wait_for_function("!document.getElementById('pipeline-inspector').hasAttribute('aria-busy')")
        from pathlib import Path
        artifacts = Path.home() / '.cache' / 'mrsm-rc6-review'
        artifacts.mkdir(parents=True, exist_ok=True)
        page.locator('#pipeline-draft-editor').evaluate('(node) => { node.open = false; }')
        page.locator('#pipeline-fit').click()
        page.screenshot(path=str(artifacts / 'pipeline-workspace.png'), full_page=True)
        page.locator('#pipeline-fullscreen').click()
        page.wait_for_function("document.fullscreenElement?.id === 'pipeline-workspace'")
        page.locator('#pipeline-fit').click()
        page.screenshot(path=str(artifacts / 'pipeline-fullscreen.png'))
        page.keyboard.press('Escape')
        assert _errors(tab) == [], _errors(tab)
        page.locator('#sign-out-button').click()
        page.wait_for_selector('#username-input')
        page.wait_for_function('!window._mrsmNodeOutput || window._mrsmNodeOutput.size === 0')
        assert page.evaluate('window._mrsmPipelineSource == null')
    except Exception:
        from pathlib import Path
        artifacts = Path.home() / '.cache' / 'mrsm-rc6-review'
        artifacts.mkdir(parents=True, exist_ok=True)
        (artifacts / 'pipeline-failure.log').write_text(server.log() + '\n' + '\n'.join(tab.errors))
        try:
            tab.page.screenshot(path=str(artifacts / 'pipeline-failure.png'), full_page=True, timeout=5000)
        except Exception:
            pass
        raise


@pytest.mark.parametrize('width', [375, 1280])
def test_pipeline_page_views_and_stage_table_fit_the_viewport(server, tab, width):
    """Keep wide execution data local to its table while every page action remains reachable."""
    import re
    from pathlib import Path
    setup = server.python(SETUP_PIPES)
    assert setup.returncode == 0, setup.stderr[-2000:]
    sync = server.mrsm('sync', 'pipes', '-i', 'sql:w7console', '--nopretty')
    assert sync.returncode == 0, sync.stdout[-2000:] + sync.stderr[-2000:]
    page = tab.page
    page.set_viewport_size({'width': width, 'height': 850})
    tab.login()
    tab.goto('/dash/pipelines', '#pipelines-graph-div')
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    artifacts = Path.home() / '.cache' / 'mrsm-pipeline-mobile-review'
    artifacts.mkdir(parents=True, exist_ok=True)
    assert page.locator('#pipeline-workspace-panel').is_visible()
    assert not page.locator('#pipeline-runs-panel').is_visible()
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    def wait_for_nodes_to_fit():
        page.wait_for_function('''() => {
            const canvas = document.getElementById('pipeline-canvas').getBoundingClientRect();
            return Array.from(document.querySelectorAll('.pipeline-stage')).every(node => {
                const box = node.getBoundingClientRect();
                return box.left >= canvas.left - 1 && box.right <= canvas.right + 1
                    && box.top >= canvas.top - 1 && box.bottom <= canvas.bottom + 1;
            });
        }''')
    wait_for_nodes_to_fit()
    page.screenshot(path=str(artifacts / f'workspace-{width}.png'), full_page=True)
    page.locator('.pipeline-stage').first.click()
    page.wait_for_selector('#pipeline-resolve-symlinks')
    page.wait_for_function("!document.getElementById('pipeline-inspector').hasAttribute('aria-busy')")
    assert page.locator('#pipeline-resolve-symlinks').is_checked()
    if width < 1000:
        assert page.locator('#pipeline-workspace').get_attribute('data-pane') == 'details'
        assert not page.locator('#pipelines-graph-div').is_visible()
        assert page.locator('#pipeline-inspector').is_visible()
    else:
        wait_for_nodes_to_fit()
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    page.screenshot(path=str(artifacts / f'pipe-details-{width}.png'), full_page=True)
    page.locator('#pipeline-inspector-close').click()
    page.locator('#pipeline-fullscreen').click()
    page.wait_for_function("document.fullscreenElement?.id === 'pipeline-workspace' || document.querySelector('#pipeline-workspace.pipeline-fullscreen')")
    assert page.locator('#pipeline-canvas').bounding_box()['height'] >= 300
    if width < 1000:
        page.locator('#pipeline-mobile-output').click()
        assert page.locator('#pipeline-output-dock').is_visible()
        page.locator('#pipeline-exit-fullscreen').click()
        page.locator('#pipeline-mobile-graph').click()
    else:
        page.keyboard.press('Escape')
    page.wait_for_function('!document.fullscreenElement')
    page.locator('#pipeline-add-node').click()
    assert page.locator('#pipeline-draft-editor').get_attribute('open') is not None
    page.locator('#pipeline-draft-editor > summary').click()
    page.locator('#pipeline-view-runs').click()
    page.wait_for_selector('#pipelines-gantt .barlayer .point', timeout=60000)
    stages = page.locator('.pipeline-stages-scroll')
    assert stages.is_visible()
    assert stages.get_attribute('tabindex') == '0'
    assert page.locator('#pipelines-nodes-table tbody tr').count() == 2
    assert page.locator('#pipelines-nodes-table tbody td').first.bounding_box()['width'] >= 280
    if width < 1000:
        assert stages.evaluate('(node) => node.scrollWidth > node.clientWidth')
        stages.evaluate('(node) => { node.scrollLeft = 180; }')
        assert stages.evaluate('(node) => node.scrollLeft') > 0
        stages.evaluate('(node) => { node.scrollLeft = 0; }')
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    # Test the real populated renderer, including any wrapper controls beneath Plotly.
    assert page.locator('#pipeline-runs-panel').get_by_role('button', name=re.compile('download', re.I)).count() == 0
    assert page.locator('#pipeline-runs-panel').get_by_role('link', name=re.compile('download', re.I)).count() == 0
    assert page.locator('#pipeline-runs-panel summary').filter(has_text=re.compile('download', re.I)).count() == 0
    assert page.locator('#pipelines-gantt .modebar').count() == 0
    page.screenshot(path=str(artifacts / f'runs-{width}.png'), full_page=True)
    assert _errors(tab) == [], _errors(tab)


def test_pipeline_toolbar_output_and_run_selection_are_local(server, tab):
    setup = server.python(SETUP_PIPES)
    assert setup.returncode == 0, setup.stderr[-2000:]
    for _ in range(2):
        result = server.mrsm('sync', 'pipes', '-i', 'sql:w7console', '--nopretty')
        assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    page = tab.page
    tab.goto('/dash/pipelines', '.pipeline-stage')
    page.set_viewport_size({'width': 1440, 'height': 1000})
    selector = page.locator('.pipeline-selector').bounding_box()
    tools = page.locator('.pipeline-tools > summary')
    before = tools.bounding_box()
    tools.click()
    after = tools.bounding_box()
    assert abs(before['y'] - after['y']) <= 2
    assert abs(page.locator('.pipeline-selector').bounding_box()['width'] - selector['width']) <= 2
    tools.click()
    options = page.locator('.pipeline-view-options > summary')
    before = options.bounding_box()
    options.click()
    assert abs(options.bounding_box()['y'] - before['y']) <= 2
    options.click()
    page.locator('.pipeline-node-output-icon').first.click()
    assert page.locator('#pipeline-output-dock').is_visible()
    assert not page.locator('#pipeline-inspector').is_visible()
    dock = page.locator('#pipeline-output-dock').bounding_box()
    canvas = page.locator('#pipeline-canvas').bounding_box()
    assert dock['y'] < canvas['y'] + canvas['height']
    page.locator('#pipeline-output-close').click()
    assert not page.locator('#pipeline-output-dock').is_visible()
    page.evaluate("window.mrsmPaintPipelineStatuses(Object.fromEntries(Array.from(document.querySelectorAll('.pipeline-stage')).map(node => [node.dataset.ref, 'running'])))")
    assert page.locator('.pipeline-stage[data-status=running]').count() >= 2
    page.locator('#pipeline-view-runs').click()
    page.wait_for_selector('#pipeline-runs-overview .barlayer .point')
    page.wait_for_function("document.querySelectorAll('#pipelines-runs-table tbody tr').length >= 2")
    original_url = page.url
    selected = page.locator('#pipelines-runs-table .pipeline-run-link').last
    run_id = selected.get_attribute('data-run-id')
    selected.click()
    _wait_text(page, '#pipelines-gantt .gtitle', run_id)
    assert page.url == original_url
    assert page.locator('#pipelines-gantt .modebar').count() == 0
    assert page.locator('#pipeline-runs-overview .modebar').count() == 0
    assert page.evaluate("document.querySelector('#pipelines-gantt .js-plotly-plot').layout.dragmode") is False
    assert _errors(tab) == [], _errors(tab)


def test_pipeline_output_ansi_follow_scroll_and_controls(server, tab):
    setup = server.python(SETUP_PIPES)
    assert setup.returncode == 0, setup.stderr[-2000:]
    tab.login()
    page = tab.page
    tab.goto('/dash/pipelines', '.pipeline-stage')
    page.locator('.pipeline-node-output-icon').first.click()
    assert page.locator('#pipeline-output-dock').is_visible()
    assert page.locator('#pipeline-output-expand').count() == 0
    assert page.locator('#pipeline-output-dock h5').count() == 0
    assert not page.locator('#pipeline-cancel-node-run').is_visible()
    page.evaluate("window.mrsmRenderPipelineOutput(document.getElementById('pipeline-node-output'), '\\x1b[31mred\\x1b[0m <img src=x onerror=alert(1)>')")
    assert page.locator('#pipeline-node-output img').count() == 0
    assert page.locator('#pipeline-node-output span').first.evaluate('(node) => !!node.style.color')
    for target in ['pipeline-node-output', 'pipeline-command-output']:
        if target == 'pipeline-command-output':
            page.evaluate("document.getElementById('pipeline-output-dock').dataset.outputMode = 'pipeline'")
        page.evaluate("id => window.mrsmRenderPipelineOutput(document.getElementById(id), Array.from({length:200}, (_,i) => 'line '+i).join('\\n'))", target)
        page.wait_for_function("id => { const v=document.getElementById(id); return v.scrollHeight-v.scrollTop-v.clientHeight<40; }", arg=target)
        page.locator('#' + target).evaluate('(node) => { node.scrollTop = 0; node.dispatchEvent(new Event("scroll")); }')
        page.evaluate("id => window.mrsmRenderPipelineOutput(document.getElementById(id), Array.from({length:220}, (_,i) => 'line '+i).join('\\n'))", target)
        assert page.locator('#' + target).evaluate('(node) => node.scrollTop') == 0
    assert _errors(tab) == [], _errors(tab)


def test_cancelled_sync_stops_node_spinner_and_hides_cancel(context):
    from tests.console.conftest import Server, Tab
    server = Server('cancel-regression', native=False)
    (server.plugins / 'w7canceltest.py').write_text("import time\ndef fetch(pipe, **kwargs):\n    print('Waiting for cancellation', flush=True)\n    time.sleep(60)\n    return [{'id': 1}]\n")
    try:
        server.start()
        setup = server.python("import meerschaum as mrsm\nfrom meerschaum.core.Pipeline._saved import save_pipeline\np=mrsm.Pipe('plugin:w7canceltest','w7cancel',instance='sql:w7console',parameters={'columns':{'id':'id'}})\nassert p.register()[0]\nsave_pipeline('cancel-test',['sql:w7console|plugin:w7canceltest|w7cancel|None'],'sql:w7console')\n")
        assert setup.returncode == 0, setup.stderr[-2000:]
        tab = Tab(context.new_page(), server.base_url)
        tab.login()
        page = tab.page
        tab.goto('/dash/pipelines?pipeline=cancel-test', '#pipeline-saved-name')
        try:
            page.wait_for_selector('.pipeline-stage', timeout=15000)
        except Exception:
            raise AssertionError(page.url + '\n' + page.inner_text('#pipelines-page') + '\n' + server.log()[-6000:] + '\n' + str(_errors(tab)))
        page.locator('.pipeline-stage').first.click()
        page.wait_for_selector('#pipeline-node-sync')
        page.locator('#pipeline-node-sync').click()
        page.wait_for_selector('#pipeline-cancel-node-run', state='visible')
        try:
            page.wait_for_function("document.querySelector('.pipeline-stage[data-status=running]') !== null", timeout=30000)
        except Exception:
            diagnostics = page.evaluate("({statuses:window._mrsmPipelineStatuses, cache:Array.from((window._mrsmNodeOutput || new Map()).values()).map(r=>({status:r.status,lines:r.lines,run:r.run_id}))})")
            raise AssertionError(str(diagnostics) + '\n' + server.log()[-6000:])
        page.locator('#pipeline-cancel-node-run').click()
        page.wait_for_selector('#pipeline-cancel-node-run', state='hidden', timeout=30000)
        page.wait_for_function("document.querySelector('.pipeline-stage[data-status=running]') === null", timeout=30000)
        assert 'Select a Pipe execution' not in page.inner_text('#pipeline-cancel-alert')
        tab.goto('/dash/pipelines', '.pipeline-stage')
        page.locator('#pipeline-view-runs').click()
        _wait_text(page, '#pipelines-runs-table', 'stopped')
        assert page.locator('.pipeline-stage[data-status=running]').count() == 0
        assert _errors(tab) == [], _errors(tab)
    finally:
        server.stop()


@pytest.mark.parametrize('width', [375, 1440])
def test_pipeline_history_deep_link_run_output_and_confirmed_delete(server, tab, width):
    from urllib.parse import parse_qs, urlsplit
    name = f'rc7-flow-{width}'
    setup = server.python(SETUP_PIPES + f"\nfrom meerschaum.core.Pipeline._saved import save_pipeline\nsave_pipeline('{name}', ['sql:w7console|sql:w7console|w7parent|None'], 'sql:w7console')\n")
    assert setup.returncode == 0, setup.stderr[-2000:]
    tab.login()
    page = tab.page
    page.set_viewport_size({'width': width, 'height': 1000})
    tab.goto(f'/dash/pipelines?pipeline={name}&instance=sql%3Aw7console', '#pipeline-saved-name')
    try:
        page.wait_for_selector('#pipeline-run-now', timeout=15000)
    except Exception:
        raise AssertionError(page.url + '\n' + page.inner_text('#pipelines-page') + '\n' + server.log()[-6000:] + '\n' + str(_errors(tab)))
    page.locator('#pipeline-run-now').click()
    page.wait_for_function("document.getElementById('pipeline-command-output').textContent.length > 0", timeout=60000)
    page.locator('#pipeline-command-output').wait_for(state='visible')
    assert not page.locator('#pipeline-output-tab').is_visible()
    run_id = page.evaluate('window._mrsmPipelineRun.run_id')
    _wait_text(page, '#pipeline-command-label', run_id)
    from pathlib import Path
    artifacts = Path.home() / '.cache' / 'mrsm-rc7-review'
    artifacts.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(artifacts / f'pipeline-output-{width}.png'), full_page=True)
    page.locator('#pipeline-output-close').click()
    assert not page.locator('#pipeline-output-dock').is_visible()
    page.locator('#pipeline-view-output').click()
    page.locator('#pipeline-command-output').wait_for(state='visible')
    page.locator('#pipeline-output-close').click()
    # The same URL used by lazy console history links survives dropdown hydration and reload.
    from urllib.parse import urlencode
    url = '/dash/pipelines?' + urlencode({'pipeline':name, 'instance':'sql:w7console', 'run_id':run_id})
    tab.goto(url, '#pipelines-gantt .gtitle')
    _wait_text(page, '#pipelines-gantt .gtitle', run_id)
    page.wait_for_function("document.getElementById('pipeline-saved-name').textContent.includes('rc7-flow')")
    page.wait_for_timeout(2200)  # Include asynchronous dropdown hydration and the first live poll.
    assert parse_qs(urlsplit(page.url).query)['run_id'] == [run_id]
    page.reload()
    _wait_text(page, '#pipelines-gantt .gtitle', run_id)
    assert parse_qs(urlsplit(page.url).query)['run_id'] == [run_id]
    # An older selected run must not keep focus when Run now starts a new one.
    page.locator('#pipeline-run-now').click()
    page.wait_for_function('(previous) => window._mrsmPipelineRun?.run_id && window._mrsmPipelineRun.run_id !== previous', arg=run_id)
    page.locator('#pipeline-command-output').wait_for(state='visible')
    # Every launch path switches output attribution, including node context-menu actions.
    page.locator('#pipeline-output-close').click()
    if width < 1000:
        page.locator('#pipeline-mobile-graph').click()
    try:
        page.locator('.pipeline-stage').first.click(button='right')
    except Exception:
        raise AssertionError(page.inner_text('#pipelines-graph-div') + '\n' + server.log()[-6000:] + '\n' + str(_errors(tab)))
    page.locator('.pipeline-context-menu button', has_text='Sync this Pipe').click()
    page.wait_for_function("document.getElementById('pipeline-output-dock').dataset.outputMode === 'pipe'")
    assert page.locator('#pipeline-output-tab').is_visible()
    assert not page.locator('#pipeline-command-output').is_visible()
    page.locator('#pipeline-output-close').click()
    page.locator('#pipeline-view-output').click()
    page.locator('#pipeline-command-output').wait_for(state='visible')
    page.locator('#pipeline-manage-toggle').click()
    page.once('dialog', lambda dialog: dialog.dismiss())
    with page.expect_event('dialog') as pending:
        page.locator('#pipeline-delete').click()
    assert name in pending.value.message and 'sql:w7console' in pending.value.message
    check = server.python(f"from meerschaum.core.Pipeline._saved import list_pipelines; assert '{name}' in list_pipelines('sql:w7console')")
    assert check.returncode == 0, check.stderr[-2000:]
    page.once('dialog', lambda dialog: dialog.accept())
    with page.expect_event('dialog') as pending:
        page.locator('#pipeline-delete').click()
    _wait_text(page, '#pipeline-manage-alert', 'Deleted')
    check = server.python(f"from meerschaum.core.Pipeline._saved import list_pipelines; import meerschaum as mrsm; assert '{name}' not in list_pipelines('sql:w7console'); assert mrsm.Pipe('sql:w7console','w7parent',instance='sql:w7console').get_id() is not None")
    assert check.returncode == 0, check.stderr[-2000:]
    assert not page.locator('#pipeline-output-dock').is_visible()
    assert 'run_id' not in parse_qs(urlsplit(page.url).query)
    assert _errors(tab) == [], _errors(tab)


def test_unavailable_run_returns_to_recent_runs_without_hiding_timeline(server, tab):
    tab.login()
    page = tab.page
    tab.goto('/dash/pipelines?run_id=rc7-missing-run&instance=sql%3Aw7console', '#pipeline-recent-runs')
    _wait_text(page, '#pipeline-runs-panel', "Run 'rc7-missing-run' is unavailable")
    page.locator('#pipeline-recent-runs').click()
    page.wait_for_function("!new URLSearchParams(location.search).has('run_id')")
    assert page.locator('#pipeline-runs-panel').is_visible()
    assert _errors(tab) == [], _errors(tab)


def test_console_pipeline_card_history_link_loads_requested_run(server, tab):
    from urllib.parse import parse_qs, urlsplit
    name = 'rc7-card-history'
    setup = server.python(SETUP_PIPES + f"\nfrom meerschaum.core.Pipeline._saved import save_pipeline\nsave_pipeline('{name}', ['sql:w7console|sql:w7console|w7parent|None'], 'sql:w7console')\n")
    assert setup.returncode == 0, setup.stderr[-2000:]
    result = server.mrsm('sync', 'pipelines', name, '-i', 'sql:w7console', '--nopretty')
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    page = tab.page
    page.locator('#get-pipelines-button').click()
    card = page.locator('.card').filter(has=page.locator('.card-header code', has_text=name)).first
    card.locator('.accordion-button').click()
    link = card.locator('.pipeline-run-link').first
    link.wait_for()
    run_id = parse_qs(urlsplit(link.get_attribute('href')).query)['run_id'][0]
    link.click()
    _wait_text(page, '#pipelines-gantt .gtitle', run_id)
    page.wait_for_timeout(2200)
    assert parse_qs(urlsplit(page.url).query)['run_id'] == [run_id]
    assert parse_qs(urlsplit(page.url).query)['pipeline'] == [name]
    assert _errors(tab) == [], _errors(tab)
