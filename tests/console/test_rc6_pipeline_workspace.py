"""Exercise the Pipelines pages (list, detail, New pipeline, explorer) in the actual Dash renderer."""
import pytest
from tests.console.test_console import SETUP_PIPES, _wait_text, _errors

pytestmark = pytest.mark.console

PARENT = 'sql:w7console|sql:w7console|w7parent|None'
CHILD = 'sql:w7console|sql:w7console|w7child|None'


def _save(name, targets, **kwargs):
    extra = ''.join(f', {key}={value!r}' for key, value in kwargs.items())
    return (SETUP_PIPES + "\nfrom meerschaum.core.Pipeline._saved import save_pipeline\n"
            f"save_pipeline({name!r}, {targets!r}, 'sql:w7console'{extra})\n")


def _open(tab, name, query=''):
    tab.goto(f'/dash/pipelines/{name}' + query, '#pipelines-page')
    tab.page.wait_for_function("document.getElementById('pipelines-page')?.dataset.loaded === 'true'", timeout=60000)


def _artifacts(name):
    from pathlib import Path
    path = Path.home() / '.cache' / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_list_opens_a_read_only_pipeline_then_edit_mode_saves(server, tab):
    setup = server.python(_save('workspace-test', [CHILD], parents=True))
    assert setup.returncode == 0, setup.stderr[-3000:]
    tab.login()
    page = tab.page
    try:
        tab.goto('/dash/pipelines', '#pipelines-list')
        card = page.locator('.pipeline-list-card').filter(has_text='workspace-test')
        card.wait_for(timeout=60000)
        ### One member that follows its parents, before any run counted them.
        assert '1 Pipe + parents · 1 instance' in card.inner_text()
        card.locator('.pipeline-title-link').click()
        page.wait_for_function("location.pathname === '/dash/pipelines/workspace-test'")
        page.wait_for_selector('.pipeline-stage', timeout=60000)
        ### Read-only: only explicit members, traversal collapsed; no edit controls.
        assert page.locator('.pipeline-placeholder', has_text='+1 upstream').count() == 1
        assert not page.locator('#pipeline-add-node').is_visible()
        assert not page.locator('#pipeline-draft-save').is_visible()
        page.locator('.pipeline-placeholder').click()
        page.wait_for_selector(f'.pipeline-stage[data-ref="{PARENT}"]')
        ### Fullscreen keeps the canvas tools; Escape restores the view.
        page.locator('#pipeline-fullscreen').click()
        page.wait_for_function("document.fullscreenElement?.id === 'pipeline-workspace' || document.querySelector('#pipeline-workspace.pipeline-fullscreen')")
        assert page.locator('#pipeline-canvas').bounding_box()['height'] >= 300
        page.keyboard.press('Escape')
        page.wait_for_function('!document.fullscreenElement')
        ### Dragging the canvas never selects text; a node opens the inspector drawer.
        canvas = page.locator('#pipeline-canvas')
        box = canvas.bounding_box()
        page.mouse.move(box['x'] + 20, box['y'] + 300)
        page.mouse.down()
        page.mouse.move(box['x'] + 80, box['y'] + 330, steps=8)
        page.mouse.up()
        assert page.evaluate('String(window.getSelection())') == ''
        page.locator(f'.pipeline-stage[data-ref="{CHILD}"]').click()
        page.wait_for_selector('#pipeline-resolve-symlinks')
        page.wait_for_function("!document.getElementById('pipeline-inspector').hasAttribute('aria-busy')")
        assert page.locator('.pipeline-open-pipe', has_text='Open Pipe').count() == 1
        drawer = page.locator('#pipeline-inspector').bounding_box()
        assert drawer['x'] + drawer['width'] >= box['x'] + box['width'] - 2
        page.locator('#pipeline-inspector-close').click()
        ### The node menu has a keyboard alternative and no membership edits outside Edit mode.
        page.locator(f'.pipeline-stage[data-ref="{CHILD}"]').click(button='right')
        assert page.locator('.pipeline-context-menu button', has_text='Sync with all parents').count() == 1
        assert page.locator('.pipeline-context-menu button', has_text='Remove from Pipeline').count() == 0
        page.keyboard.press('Escape')
        ### Edit: Draft badge, Add Pipe (the stepper's member panel), unsaved count, Save.
        page.locator('#pipeline-edit-toggle').click()
        page.wait_for_function("document.getElementById('pipelines-page').dataset.edit === 'true'")
        assert page.locator('#pipeline-draft-badge').is_visible()
        page.locator('#pipeline-add-node').click()
        page.wait_for_function("document.getElementById('pipeline-draft-editor').open")
        _wait_text(page, '#pipeline-draft-editor', 'w7child')
        page.locator('#pipeline-draft-members').click()
        option = page.locator('[role=option], .Select-option, .dash-dropdown-option').filter(has_text='w7parent').last
        option.wait_for(state='visible')
        option.click()
        _wait_text(page, '#pipeline-draft-notice', '1 unsaved change')
        page.keyboard.press('Escape')
        page.keyboard.press('Escape')
        canvas_shows = ("(args) => document.querySelector(`.pipeline-stage[data-ref=\"${args[0]}\"]`)?.dataset.role === args[1]"
                        " && document.getElementById('pipeline-canvas-world')?.dataset.mounted === 'true'")
        page.locator('#pipeline-draft-undo').click()
        page.wait_for_function("document.getElementById('pipelines-page').dataset.dirty === 'false'")
        page.wait_for_function(canvas_shows, arg=[PARENT, 'upstream'], timeout=30000)
        page.locator('#pipeline-draft-redo').click()
        _wait_text(page, '#pipeline-draft-notice', '1 unsaved change')
        ### The canvas has re-rendered the redone draft (the parent is a member again) and wired it up.
        page.wait_for_function(canvas_shows, arg=[PARENT, 'explicit'], timeout=30000)
        ### Dragging a node moves it (the shared layout, saved with Save).
        node = page.locator(f'.pipeline-stage[data-ref="{CHILD}"]').first
        original_left = node.evaluate('(node) => node.style.left')
        nbox = node.bounding_box()
        page.mouse.move(nbox['x'] + nbox['width'] / 2, nbox['y'] + nbox['height'] / 2)
        page.mouse.down()
        page.mouse.move(nbox['x'] + nbox['width'] / 2 + 55, nbox['y'] + nbox['height'] / 2 + 25, steps=8)
        page.mouse.up()
        assert node.evaluate('(node) => node.style.left') != original_left
        page.locator('#pipeline-draft-save').click()
        _wait_text(page, '#pipeline-manage-alert', "Saved Pipeline('workspace-test')")
        page.wait_for_function("document.getElementById('pipelines-page').dataset.edit === 'false'")
        saved = server.python("from meerschaum.core.Pipeline._saved import list_pipelines; d=list_pipelines('sql:w7console')['workspace-test']; assert len(d['targets']) == 2 and d['layout'], d")
        assert saved.returncode == 0, saved.stderr[-2000:]
        assert _errors(tab) == [], _errors(tab)
    except Exception:
        artifacts = _artifacts('mrsm-rc6-review')
        (artifacts / 'pipeline-failure.log').write_text(server.log() + '\n' + '\n'.join(tab.errors))
        try:
            page.screenshot(path=str(artifacts / 'pipeline-failure.png'), full_page=True, timeout=5000)
        except Exception:
            pass
        raise


def test_edit_mode_edges_drafts_and_leaving(server, tab):
    """Dragging between nodes proposes an edge; drafts survive a reload and Discard leaves Edit mode."""
    setup = server.python(_save('edges-test', [PARENT, CHILD]))
    assert setup.returncode == 0, setup.stderr[-3000:]
    tab.login()
    page = tab.page
    page.set_viewport_size({'width': 1440, 'height': 950})
    _open(tab, 'edges-test')
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    page.locator('#pipeline-edit-toggle').click()
    page.wait_for_function("document.getElementById('pipelines-page').dataset.edit === 'true'")
    ### The child already has the parent (an inferred edge), so drag the child onto the parent.
    handle = page.locator(f'.pipeline-stage[data-ref="{CHILD}"] .pipeline-edge-handle')
    target = page.locator(f'.pipeline-stage[data-ref="{PARENT}"]').bounding_box()
    hbox = handle.bounding_box()
    page.mouse.move(hbox['x'] + 6, hbox['y'] + 6)
    page.mouse.down()
    page.mouse.move(target['x'] + target['width'] / 2, target['y'] + target['height'] / 2, steps=12)
    page.mouse.up()
    _wait_text(page, '#pipeline-edit-alert', 'cycle', timeout=30000)
    ### A Pipeline's draft is kept in the browser: Resume after reloading.
    page.locator(f'.pipeline-stage[data-ref="{CHILD}"]').click(button='right')
    page.locator('.pipeline-context-menu button', has_text='Remove from Pipeline').click()
    _wait_text(page, '#pipeline-draft-notice', '1 unsaved change')
    page.once('dialog', lambda dialog: dialog.accept())
    page.reload()
    page.wait_for_function("document.getElementById('pipelines-page')?.dataset.loaded === 'true'", timeout=60000)
    page.locator('#pipeline-edit-toggle').click()
    page.locator('#pipeline-draft-resume').wait_for(state='visible')
    page.locator('#pipeline-draft-resume').click()
    _wait_text(page, '#pipeline-draft-notice', '1 unsaved change')
    page.locator('#pipeline-draft-discard').click()
    page.wait_for_function("document.getElementById('pipelines-page').dataset.edit === 'false'")
    page.wait_for_selector(f'.pipeline-stage[data-ref="{CHILD}"]', timeout=30000)
    assert _errors(tab) == [], _errors(tab)


@pytest.mark.parametrize('width', [375, 1280])
def test_pipeline_page_views_and_stage_table_fit_the_viewport(server, tab, width):
    """Keep wide execution data local to its table while every page action remains reachable."""
    import re
    setup = server.python(_save('fit-test', [PARENT, CHILD]))
    assert setup.returncode == 0, setup.stderr[-2000:]
    sync = server.mrsm('sync', 'pipelines', 'fit-test', '-i', 'sql:w7console', '--nopretty')
    assert sync.returncode == 0, sync.stdout[-2000:] + sync.stderr[-2000:]
    page = tab.page
    page.set_viewport_size({'width': width, 'height': 850})
    tab.login()
    _open(tab, 'fit-test')
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    artifacts = _artifacts('mrsm-pipeline-mobile-review')
    assert page.locator('#pipeline-workspace-panel').is_visible()
    assert not page.locator('#pipeline-runs-panel').is_visible()
    for control in ('#pipeline-run-now', '#pipeline-plan-open', '#pipeline-edit-toggle', '#pipeline-manage-menu'):
        assert page.locator(control).is_visible(), control
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    page.locator('#pipeline-fit').click()
    page.wait_for_function('''() => {
        const canvas = document.getElementById('pipeline-canvas').getBoundingClientRect();
        return Array.from(document.querySelectorAll('.pipeline-stage')).every(node => {
            const box = node.getBoundingClientRect();
            return box.left >= canvas.left - 1 && box.right <= canvas.right + 1
                && box.top >= canvas.top - 1 && box.bottom <= canvas.bottom + 1;
        });
    }''')
    page.screenshot(path=str(artifacts / f'workspace-{width}.png'), full_page=True)
    page.locator('.pipeline-stage').first.click()
    page.wait_for_selector('#pipeline-resolve-symlinks')
    page.wait_for_function("!document.getElementById('pipeline-inspector').hasAttribute('aria-busy')")
    assert page.locator('#pipeline-resolve-symlinks').is_checked()
    inspector = page.locator('#pipeline-inspector').bounding_box()
    if width < 1000:
        ### A full-screen sheet on mobile.
        assert inspector['width'] >= width - 2 and inspector['x'] <= 1
    else:
        assert inspector['width'] <= 440
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    page.screenshot(path=str(artifacts / f'pipe-details-{width}.png'), full_page=True)
    page.locator('#pipeline-inspector-close').click()
    page.locator('#pipeline-plan-open').click()
    page.wait_for_selector('#pipeline-plan-body table', timeout=60000)
    page.keyboard.press('Escape')
    page.locator('#pipeline-view-runs').click()
    page.wait_for_selector('#pipelines-gantt .barlayer .point', timeout=60000)
    stages = page.locator('.pipeline-stages-scroll')
    assert stages.is_visible()
    assert stages.get_attribute('tabindex') == '0'
    assert page.locator('#pipelines-nodes-table tbody tr').count() == 2
    if width < 1000:
        assert stages.evaluate('(node) => node.scrollWidth > node.clientWidth')
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    assert page.locator('#pipeline-runs-panel').get_by_role('button', name=re.compile('download', re.I)).count() == 0
    assert page.locator('#pipelines-gantt .modebar').count() == 0
    page.screenshot(path=str(artifacts / f'runs-{width}.png'), full_page=True)
    assert _errors(tab) == [], _errors(tab)


def test_pipeline_toolbar_output_and_run_selection_are_local(server, tab):
    setup = server.python(_save('toolbar-test', [PARENT, CHILD]))
    assert setup.returncode == 0, setup.stderr[-2000:]
    for _ in range(2):
        result = server.mrsm('sync', 'pipelines', 'toolbar-test', '-i', 'sql:w7console', '--nopretty')
        assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    page = tab.page
    page.set_viewport_size({'width': 1440, 'height': 1000})
    _open(tab, 'toolbar-test')
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    options = page.locator('.pipeline-view-options > summary')
    before = options.bounding_box()
    options.click()
    assert abs(options.bounding_box()['y'] - before['y']) <= 2
    zoom_in, zoom_out = page.locator('#pipeline-zoom-in').bounding_box(), page.locator('#pipeline-zoom-out').bounding_box()
    assert abs(zoom_in['y'] - zoom_out['y']) < 2 and abs(zoom_out['x'] + zoom_out['width'] - zoom_in['x']) < 2
    options.click()
    controls = page.evaluate("Array.from(document.querySelector('.pipeline-canvas-controls').children).map(e => e.id || e.className)")
    assert controls[-1] == 'pipeline-fullscreen' and controls[-2] == 'pipeline-view-options', controls
    page.locator('.pipeline-node-output-icon').first.click()
    assert page.locator('#pipeline-output-dock').is_visible()
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
    assert page.evaluate("document.querySelector('#pipelines-gantt .js-plotly-plot').layout.dragmode") is False
    assert _errors(tab) == [], _errors(tab)


def test_pipeline_output_ansi_follow_scroll_and_controls(server, tab):
    setup = server.python(_save('output-test', [PARENT]))
    assert setup.returncode == 0, setup.stderr[-2000:]
    tab.login()
    page = tab.page
    _open(tab, 'output-test')
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    page.locator('.pipeline-node-output-icon').first.click()
    assert page.locator('#pipeline-output-dock').is_visible()
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
        _open(tab, 'cancel-test')
        page.wait_for_selector('.pipeline-stage', timeout=15000)
        page.locator('.pipeline-stage').first.click()
        page.wait_for_selector('#pipeline-node-sync')
        page.locator('#pipeline-node-sync').click()
        page.wait_for_selector('#pipeline-cancel-node-run', state='visible')
        page.wait_for_function("document.querySelector('.pipeline-stage[data-status=running]') !== null", timeout=30000)
        page.locator('#pipeline-cancel-node-run').click()
        page.wait_for_selector('#pipeline-cancel-node-run', state='hidden', timeout=30000)
        page.wait_for_function("document.querySelector('.pipeline-stage[data-status=running]') === null", timeout=30000)
        assert 'Select a Pipe execution' not in page.inner_text('#pipeline-cancel-alert')
        assert _errors(tab) == [], _errors(tab)
    finally:
        server.stop()


@pytest.mark.parametrize('width', [375, 1440])
def test_pipeline_history_deep_link_run_output_and_confirmed_delete(server, tab, width):
    from urllib.parse import parse_qs, urlsplit, urlencode
    name = f'rc7-flow-{width}'
    setup = server.python(_save(name, [PARENT]))
    assert setup.returncode == 0, setup.stderr[-2000:]
    tab.login()
    page = tab.page
    page.set_viewport_size({'width': width, 'height': 1000})
    _open(tab, name, '?instance=sql%3Aw7console')
    page.locator('#pipeline-run-now').click()
    page.wait_for_function("document.getElementById('pipeline-command-output').textContent.length > 0", timeout=60000)
    page.locator('#pipeline-command-output').wait_for(state='visible')
    assert not page.locator('#pipeline-output-tab').is_visible()
    run_id = page.evaluate('window._mrsmPipelineRun.run_id')
    _wait_text(page, '#pipeline-command-label', run_id)
    page.locator('#pipeline-output-close').click()
    assert not page.locator('#pipeline-output-dock').is_visible()
    page.locator('#pipeline-view-output').click()
    page.locator('#pipeline-command-output').wait_for(state='visible')
    page.locator('#pipeline-output-close').click()
    ### A member's own part of the run.
    page.locator(f'.pipeline-stage[data-ref="{PARENT}"] .pipeline-node-output-icon').click()
    _wait_text(page, '#pipeline-node-run-label', f"Pipeline('{name}')")
    ### The member's finished line, in plain words (`Synced Pipe(...): 1 inserted`).
    page.wait_for_function("document.getElementById('pipeline-node-output').textContent.includes('Synced ')", timeout=60000)
    page.locator('#pipeline-output-close').click()
    ### The history link survives hydration and reload.
    tab.goto(f'/dash/pipelines/{name}?' + urlencode({'instance': 'sql:w7console', 'run_id': run_id}), '#pipelines-gantt .gtitle')
    _wait_text(page, '#pipelines-gantt .gtitle', run_id)
    page.wait_for_timeout(2200)
    assert parse_qs(urlsplit(page.url).query)['run_id'] == [run_id]
    page.reload()
    _wait_text(page, '#pipelines-gantt .gtitle', run_id)
    ### Delete confirms the identity, keeps member Pipes and returns to the list.
    page.locator('#pipeline-manage-menu').click()
    page.once('dialog', lambda dialog: dialog.dismiss())
    with page.expect_event('dialog') as pending:
        page.locator('#pipeline-delete').click()
    assert name in pending.value.message and 'sql:w7console' in pending.value.message
    check = server.python(f"from meerschaum.core.Pipeline._saved import list_pipelines; assert '{name}' in list_pipelines('sql:w7console')")
    assert check.returncode == 0, check.stderr[-2000:]
    page.locator('#pipeline-manage-menu').click()
    page.once('dialog', lambda dialog: dialog.accept())
    with page.expect_event('dialog'):
        page.locator('#pipeline-delete').click()
    page.wait_for_function("location.pathname === '/dash/pipelines'", timeout=30000)
    check = server.python(f"from meerschaum.core.Pipeline._saved import list_pipelines; import meerschaum as mrsm; assert '{name}' not in list_pipelines('sql:w7console'); assert mrsm.Pipe('sql:w7console','w7parent',instance='sql:w7console').get_id() is not None")
    assert check.returncode == 0, check.stderr[-2000:]
    assert _errors(tab) == [], _errors(tab)


def test_unavailable_run_returns_to_recent_runs_without_hiding_timeline(server, tab):
    setup = server.python(_save('missing-run-test', [PARENT]))
    assert setup.returncode == 0, setup.stderr[-2000:]
    tab.login()
    page = tab.page
    tab.goto('/dash/pipelines/missing-run-test?run_id=rc7-missing-run&instance=sql%3Aw7console', '#pipeline-recent-runs')
    _wait_text(page, '#pipeline-runs-panel', "Run 'rc7-missing-run' is unavailable")
    page.locator('#pipeline-recent-runs').click()
    page.wait_for_function("!new URLSearchParams(location.search).has('run_id')")
    assert page.locator('#pipeline-runs-panel').is_visible()
    assert _errors(tab) == [], _errors(tab)


def test_console_pipeline_card_history_link_loads_requested_run(server, tab):
    from urllib.parse import parse_qs, urlsplit
    name = 'rc7-card-history'
    setup = server.python(_save(name, [PARENT]))
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
    href = link.get_attribute('href')
    assert urlsplit(href).path == f'/dash/pipelines/{name}'
    run_id = parse_qs(urlsplit(href).query)['run_id'][0]
    link.click()
    _wait_text(page, '#pipelines-gantt .gtitle', run_id)
    page.wait_for_timeout(2200)
    assert parse_qs(urlsplit(page.url).query)['run_id'] == [run_id]
    assert _errors(tab) == [], _errors(tab)


def test_stepper_explorer_copy_and_legacy_links(server, tab):
    """Explore a Pipe, Save as Pipeline through the stepper (nothing saved before Create), copy it."""
    setup = server.python(_save('legacy-test', [CHILD]))
    assert setup.returncode == 0, setup.stderr[-2000:]
    tab.login()
    page = tab.page
    page.set_viewport_size({'width': 1280, 'height': 900})
    tab.goto('/dash/pipelines?pipeline=legacy-test', None)
    page.wait_for_function("location.pathname === '/dash/pipelines/legacy-test'", timeout=30000)
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    tab.goto('/dash/pipelines', '#pipelines-list')
    page.locator('.pipeline-list-card').first.wait_for(timeout=60000)
    page.locator('#pipelines-find-pipe').click()
    page.locator('[role=option], .Select-option, .dash-dropdown-option').filter(has_text='w7child').last.click()
    page.wait_for_function("location.pathname === '/dash/pipelines/explore'", timeout=30000)
    page.wait_for_selector('#pipeline-explore-graph .pipeline-stage', timeout=60000)
    assert page.locator('#pipeline-explore-graph .pipeline-stage').count() == 2
    page.locator('#pipeline-explore-save').click()
    page.wait_for_function("location.pathname === '/dash/pipelines/new'", timeout=30000)
    page.wait_for_selector('#pipeline-new-stepper li[data-state=current]')
    page.wait_for_timeout(1000)
    page.locator('#pipeline-new-next').click()
    _wait_text(page, '#pipeline-new-alert', 'name')
    page.locator('#pipeline-new-name').fill('stepped')
    page.locator('#pipeline-new-next').click()
    page.wait_for_function("document.getElementById('pipeline-new-page').dataset.step === 'members'")
    _wait_text(page, '.pipeline-new-step[data-step=members]', 'w7child')
    page.locator('#pipeline-new-next').click()
    page.wait_for_function("document.getElementById('pipeline-new-page').dataset.step === 'traversal'")
    page.locator('#pipeline-new-next').click()
    page.wait_for_function("document.getElementById('pipeline-new-page').dataset.step === 'review'")
    _wait_text(page, '#pipeline-new-review', 'pipeline-stepped', timeout=60000)
    assert page.locator('#pipeline-new-apply-tags').is_checked()
    unsaved = server.python("from meerschaum.core.Pipeline._saved import list_pipelines; assert 'stepped' not in list_pipelines('sql:w7console')")
    assert unsaved.returncode == 0, unsaved.stderr[-2000:]
    page.screenshot(path=str(_artifacts('mrsm-rc14-review') / 'new-review.png'), full_page=True)
    page.locator('#pipeline-new-create').click()
    page.wait_for_function("location.pathname === '/dash/pipelines/stepped'", timeout=60000)
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    tagged = server.python("import meerschaum as mrsm; p = mrsm.Pipe('sql:w7console', 'w7child', instance='sql:w7console'); assert 'pipeline-stepped' in p.tags, p.tags")
    assert tagged.returncode == 0, tagged.stderr[-2000:]
    page.wait_for_function("document.getElementById('pipelines-page')?.dataset.loaded === 'true'")
    page.locator('#pipeline-manage-menu').click()
    page.locator('#pipeline-manage-copy').click()
    page.locator('#pipeline-copy-name').fill('stepped-copy')
    page.locator('#pipeline-copy-confirm').click()
    page.wait_for_function("location.pathname === '/dash/pipelines/stepped-copy'", timeout=30000)
    _wait_text(page, '#pipeline-title', 'stepped-copy')
    ### The list links the cards' Plan to the Pipeline's plan dialog.
    tab.goto('/dash/pipelines/stepped?open=plan', '#pipelines-page')
    page.wait_for_selector('#pipeline-plan-body table', timeout=60000)
    assert 'open=' not in page.url
    assert _errors(tab) == [], _errors(tab)


def test_the_global_instance_is_shared_by_the_console_and_pipelines(server, tab):
    """The registry is the console's one instance selector, on every Pipelines page."""
    setup = server.python(_save('instance-test', [PARENT]))
    assert setup.returncode == 0, setup.stderr[-2000:]
    tab.login()
    page = tab.page
    page.set_viewport_size({'width': 1280, 'height': 900})
    tab.goto('/dash/pipelines', '#pages-instance-select')
    page.locator('.pipeline-list-card', has_text='instance-test').wait_for(timeout=60000)
    select = page.locator('#pages-instance-select')
    values = select.evaluate("(node) => Array.from(node.options).map(option => option.value)")
    assert 'sql:local' in values and select.input_value() == 'sql:w7console', values
    other = 'sql:local'
    with page.expect_navigation():
        select.select_option(value=other)
    page.wait_for_selector('#pages-instance-select')
    assert page.locator('#pages-instance-select').input_value() == other
    cookie = [c for c in page.context.cookies() if c['name'] == 'mrsm-instance'][0]['value']
    from urllib.parse import unquote
    assert unquote(cookie) == other
    tab.goto('/dash/terminal', '#instance-select')
    page.wait_for_function("(value) => document.getElementById('instance-select').value === value", arg=other)
    ### Every page's selector shows the global instance (the Overview's included).
    tab.goto('/dash', '#pages-instance-select')
    assert page.locator('#pages-instance-select').input_value() == other
    page.evaluate("document.cookie = 'mrsm-instance=' + encodeURIComponent('sql:w7console') + '; path=/'")
    tab.goto('/dash/pipelines', '#pipelines-list')
    page.locator('.pipeline-list-card', has_text='instance-test').wait_for(timeout=60000)
    assert _errors(tab) == [], _errors(tab)
