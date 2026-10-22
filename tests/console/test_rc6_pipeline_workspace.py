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
        assert page.locator('#pipeline-output-dock').is_visible()
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
        node = page.locator('.pipeline-stage').first
        node.scroll_into_view_if_needed()
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
