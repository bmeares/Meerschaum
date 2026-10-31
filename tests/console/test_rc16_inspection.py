"""Real rc16 inspection / Lineage / mobile navigation / metrics with synthetic local data."""

from __future__ import annotations

import json
import pathlib
from urllib.parse import urlencode

import pytest

from tests.console.conftest import Server, Tab, USERNAME, PASSWORD

pytestmark = pytest.mark.console
INSPECTION_PASSWORD = 'correct-horse-inspection-16'
SECRET_VALUES = ('rc16-password-never-show', 'rc16-nested-token-never-show')


def artifacts():
    path = pathlib.Path.home() / '.cache' / 'mrsm-rc16-ui' / 'browser'
    path.mkdir(parents=True, exist_ok=True)
    return path


SETUP = '''
import io, pathlib, tarfile, time
import meerschaum as mrsm
from meerschaum.config import paths
from meerschaum.core import User, Plugin
from meerschaum.core.Pipeline._saved import save_pipeline
c = mrsm.get_connector('sql:w7console')
for name, role in [('rc16-ops','viewer'),('rc16-other','viewer'),('rc16-none','custom')]:
    attrs = {'role':role}
    if role == 'custom': attrs['scopes'] = []
    assert c.register_user(User(name, %(password)r, instance=c, attributes=attrs))[0]
for name, username in [('Ops','rc16-ops'),('Sales','rc16-other')]:
    team = mrsm.Team(name, instance=c)
    assert team.register()[0]
    assert team.add_members(username)[0]
c.exec('CREATE TABLE rc16_readings (id INTEGER PRIMARY KEY, dt TEXT, value REAL)')
c.exec("INSERT INTO rc16_readings VALUES (1,'2026-10-28 01:00:00',21.5)")
parent = mrsm.Pipe('sql:w7console','rc16_parent',instance=c,
    columns={'primary':'id','datetime':'dt'}, dtypes={'dt':'datetime'},
    parameters={'fetch':{'definition':'SELECT * FROM rc16_readings'}})
child = mrsm.Pipe('sql:w7console','rc16_child',instance=c,
    columns={'primary':'id','datetime':'dt'}, dtypes={'dt':'datetime'},
    parameters={'sql':"SELECT * FROM {{ Pipe('sql:w7console', 'rc16_parent') }}"})
assert parent.register()[0] and child.register()[0]
assert parent.sync()[0]
refs = ['sql:w7console|sql:w7console|' + name + '|None' for name in ('rc16_parent','rc16_child')]
save_pipeline('rc16_inspect',refs,c,access={'visibility':'public'})
assert mrsm.Pipeline('rc16_inspect',instance=c).sync().success
# A real failure in this scratch database supplies the failure count without a remote source.
failure = mrsm.Pipe('sql:w7console','rc16_failure',instance=c,
    parameters={'fetch':{'definition':'SELECT * FROM missing_rc16_source'}})
assert failure.register()[0]
assert not failure.sync()[0]
hidden = mrsm.Pipe('rc16','HIDDEN_RC16_METRIC',instance=c,
    columns={'primary':'id'}, parameters={'access':{'visibility':'private','owner':%(admin)r}})
assert hidden.register()[0]
assert hidden.sync([{'id':1,'value':99}])[0]
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    metrics = mrsm.get_engine_metrics(instance=c)
    if (metrics['summary']['failed'] >= 1
        and any(row['metric_key'] == 'HIDDEN_RC16_METRIC' for row in metrics['pipes'])):
        break
    time.sleep(0.1)
else:
    raise AssertionError('Successful, failing, and private sync metrics were not recorded.')
plugin = Plugin('rc16_repository',user_id=c.get_user_id(User(%(admin)r,instance=c)))
assert c.register_plugin(plugin)[0]
archive_root = pathlib.Path(paths.PLUGINS_ARCHIVES_RESOURCES_PATH)
archive_root.mkdir(parents=True,exist_ok=True)
with tarfile.open(archive_root/'rc16_repository.tar.gz','w:gz') as archive:
    content=b'def fetch(pipe, **kwargs):\\n    return [{"value": 42}]\\n'
    member=tarfile.TarInfo('rc16_repository/__init__.py'); member.size=len(content)
    archive.addfile(member,io.BytesIO(content))
''' % {'password': INSPECTION_PASSWORD, 'admin': USERNAME}


@pytest.fixture(scope='module')
def inspection_server():
    config = {'meerschaum': {'connectors': {'sql': {
        'rc16_ops': {'flavor':'postgresql','host':'db.invalid','database':'readings','schema':'weather',
                     'username':'demo','password':SECRET_VALUES[0],'teams':['Ops'],
                     'options':{'sslmode':'require','api_key':SECRET_VALUES[1]}},
        'rc16_sales': {'flavor':'sqlite','database':'DO_NOT_OPEN.db','teams':['Sales']},
    }}}}
    server = Server('rc16-inspection', config=config, native=False, args=['--workers','2'])
    (server.plugins/'rc16_installed.py').write_text('"""A local source for inspection."""\n\ndef fetch(pipe, **kwargs):\n    return [{"value": 16}]\n')
    package = server.plugins/'rc18_source'
    (package/'helpers').mkdir(parents=True)
    (package/'__init__.py').write_text('"""Explore synthetic source files."""\n\ndef fetch(pipe, **kwargs):\n    return [{"value": 18}]\n')
    (package/'helpers'/'sample.rs').write_text('pub fn reading() -> u32 {\n    18\n}\n')
    (package/'helpers'/'literal.md').write_text('```\n<script>window.sourceExecuted = true</script>\n````\n')
    try:
        server.start()
        result = server.python(SETUP)
        assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
        yield server
        assert not (server.root/'DO_NOT_OPEN.db').exists()
    finally:
        if server.log_path.exists():
            (artifacts()/'server.log').write_text(server.log_path.read_text(errors='replace'))
        server.stop()


def test_lineage_opens_a_standalone_graph_after_a_saved_pipeline(inspection_server, browser):
    context = browser.new_context(viewport={'width':1440,'height':900})
    try:
        tab = Tab(context.new_page(), inspection_server.base_url)
        tab.login(path='/dash')
        tab.goto('/dash/pipelines/rc16_inspect','#pipeline-canvas')
        tab.page.locator('.pipeline-stage').first.wait_for(timeout=30000)
        # Navigate within the same document to exercise callbacks left by the saved Pipeline.
        tab.page.evaluate("window.mrsmNavigate('/dash/pipes/sql:w7console/rc16_child')")
        tab.page.locator('#pipe-detail-tabs').wait_for()
        tab.page.locator('#pipe-detail-tabs .nav-link:text-is("Lineage")').click()
        tab.page.locator('a.pipes-explore-link').wait_for()
        tab.page.locator('a.pipes-explore-link').click()
        tab.page.wait_for_url('**/dash/pipelines/explore?*')
        tab.page.locator('#pipeline-explore-graph .pipeline-stage').first.wait_for(timeout=30000)
        assert tab.page.locator('#pipeline-saved-name').count() == 0
        assert "Pipeline('None')" not in tab.page.inner_text('body')
        assert "Pipeline 'None'" not in tab.page.inner_text('body')
        assert 'rc16_parent' in tab.page.inner_text('#pipeline-explore-graph')
        ref = 'sql:w7console|sql:w7console|rc16_child|None'
        tab.goto('/dash/pipelines?' + urlencode({'pipe':ref,'pipeline':'None'}),'#pipeline-explore-graph')
        tab.page.wait_for_url('**/dash/pipelines/explore?*')
        tab.page.locator('#pipeline-explore-graph .pipeline-stage').first.wait_for(timeout=30000)
        assert '/pipelines/None' not in tab.page.url
        assert not tab.errors, tab.errors
        tab.page.screenshot(path=str(artifacts()/'lineage-1440.png'))
    finally:
        context.close()


@pytest.mark.parametrize('width',[1440,390])
@pytest.mark.parametrize('role',['admin','ops','other','none'])
def test_connector_and_plugin_inspection_obey_roles(inspection_server,browser,width,role):
    context = browser.new_context(viewport={'width':width,'height':900})
    try:
        username = USERNAME if role == 'admin' else 'rc16-'+role
        password = PASSWORD if role == 'admin' else INSPECTION_PASSWORD
        tab = Tab(context.new_page(),inspection_server.base_url)
        tab.login(username,password,path='/dash')
        tab.goto('/dash/connectors','#pages-navbar-div')
        if role == 'none':
            assert tab.page.locator('#connectors-table').count() == 0
            assert tab.page.locator('#console-nav-connectors').count() == 0
        else:
            tab.page.locator('#connectors-table').wait_for()
            rows = tab.page.locator('#connectors-table tbody')
            if role == 'admin':
                own = rows.locator('tr[data-keys="sql:rc16_ops"]')
                assert rows.locator('button:text-is("Inspect")').count() == 0
                assert tab.page.locator('#connectors-table thead th').all_text_contents() == ['Connector','Source']
                own.get_by_role('button',name='sql:rc16_ops',exact=True).click()
                modal = tab.page.locator('.connector-info-modal.show')
                modal.wait_for()
                text = modal.inner_text()
                assert 'weather' in text and 'db.invalid' in text and '5432' in text
                assert modal.get_by_role('button',name='Test connection',exact=True).is_visible()
                modal.get_by_role('button',name='Inspect',exact=True).click()
                modal.get_by_text('Supported keys',exact=True).wait_for()
                text = modal.inner_text()
                assert 'Supported keys' in text and 'Required keys' in text
                bounds = modal.locator('.modal-dialog').bounding_box()
                assert bounds['x'] >= 0 and bounds['x']+bounds['width'] <= width+1, bounds
                assert modal.evaluate('element => element.scrollWidth <= element.clientWidth + 1')
                tab.page.screenshot(path=str(artifacts()/f'connector-modal-{width}.png'))
                modal.get_by_role('button',name='Close',exact=True).last.click()
                modal.wait_for(state='hidden')
                rows.locator('tr[data-keys="sql:w7console"]').get_by_role('button',name='sql:w7console',exact=True).click()
                modal.wait_for()
                modal.get_by_role('button',name='Test connection',exact=True).click()
                modal.get_by_text('Connected in',exact=False).wait_for(timeout=30000)
                modal.get_by_role('button',name='Close',exact=True).last.click()
                modal.wait_for(state='hidden')
            else:
                expected, hidden = ('ops','sales') if role == 'ops' else ('sales','ops')
                assert rows.locator(f'tr[data-keys="sql:rc16_{expected}"]').count() == 1
                assert rows.locator(f'tr[data-keys="sql:rc16_{hidden}"]').count() == 0
                assert 'db.invalid' not in rows.inner_text() and 'weather' not in rows.inner_text()
                assert rows.locator('summary').count() == 0
            for secret in SECRET_VALUES:
                assert secret not in tab.page.content()
            assert tab.page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
            tab.page.screenshot(path=str(artifacts()/f'connectors-{role}-{width}.png'))

        tab.goto('/dash/plugins','#pages-navbar-div')
        if role == 'none':
            assert tab.page.locator('#plugins-tabs').count() == 0
            assert tab.page.locator('#console-nav-plugins').count() == 0
        else:
            tab.page.locator('#installed-plugins-table').wait_for(timeout=30000)
            link = tab.page.locator('a[href="/dash/plugins/installed/rc16_installed"]')
            assert link.count() == (1 if role == 'admin' else 0)
            tab.goto('/dash/plugins/installed/rc16_installed','#plugins-content-div')
            if role == 'admin':
                tab.page.wait_for_function("document.getElementById('plugin-source-content')?.textContent.includes('return')")
                assert 'value' in tab.page.inner_text('#plugin-source-content')
                tab.page.locator('#plugin-source-content').scroll_into_view_if_needed()
                tab.page.screenshot(path=str(artifacts()/f'installed-source-{width}.png'))
            else:
                tab.page.get_by_text('Installed source is available to administrators.').wait_for()
                assert tab.page.locator('#plugin-source-content').count() == 0
            tab.goto('/dash/plugins/rc16_repository','#plugins-content-div')
            tab.page.wait_for_function("document.getElementById('plugin-source-content')?.textContent.includes('42')")
            assert tab.page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
            tab.page.locator('#plugin-source-content').scroll_into_view_if_needed()
            tab.page.screenshot(path=str(artifacts()/f'repository-source-{role}-{width}.png'))
        assert not tab.errors, tab.errors
    finally:
        context.close()


@pytest.mark.parametrize('width',[1440,390])
def test_source_tree_highlights_rust_and_keeps_markdown_literal(inspection_server,browser,width):
    context = browser.new_context(viewport={'width':width,'height':900})
    try:
        tab = Tab(context.new_page(),inspection_server.base_url)
        tab.login(path='/dash')
        tab.goto('/dash/plugins/installed/rc18_source','#plugin-source-content')
        tab.page.locator('.plugin-source-code .source-token-keyword').first.wait_for()
        assert 'Explore synthetic source files.' in tab.page.inner_text('.plugin-detail-description')
        assert 'Package' in tab.page.inner_text('.plugin-detail-properties')
        tab.page.locator('.plugin-source-tree summary:text-is("helpers")').click()
        tab.page.get_by_role('button',name='helpers/sample.rs',exact=True).click()
        tab.page.wait_for_function("document.querySelector('.plugin-source-toolbar code')?.textContent === 'helpers/sample.rs'")
        tab.page.locator('.plugin-source-code .source-token-keyword').first.wait_for()
        assert tab.page.locator('.plugin-source-code .source-token-keyword').count() >= 2
        assert 'reading' in tab.page.inner_text('.plugin-source-code')
        assert tab.page.locator('.plugin-source-file.active').count() == 1
        tab.page.screenshot(path=str(artifacts()/f'source-tree-{width}.png'))
        tab.page.get_by_role('button',name='helpers/literal.md',exact=True).click()
        tab.page.wait_for_function("document.querySelector('.plugin-source-toolbar code')?.textContent === 'helpers/literal.md'")
        assert '<script>window.sourceExecuted = true</script>' in tab.page.inner_text('.plugin-source-code')
        assert tab.page.evaluate('window.sourceExecuted === undefined')
        assert tab.page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        assert not tab.errors,tab.errors
    finally:
        context.close()


def test_mobile_hamburger_stays_in_place_during_open_and_close(inspection_server,browser):
    context = browser.new_context(viewport={'width':390,'height':844})
    geometry = []
    try:
        tab = Tab(context.new_page(),inspection_server.base_url)
        tab.login(path='/dash')
        for width in (375,390,768,1024):
            tab.page.set_viewport_size({'width':width,'height':844})
            tab.goto('/dash/pipes','#navbar-toggler')
            button = tab.page.locator('#navbar-toggler')
            button.wait_for(state='visible')
            before = button.bounding_box()
            tab.page.evaluate('''() => {
                window._navFrames = [];
                window._navWatching = true;
                const watch = () => {
                    if (!window._navWatching) return;
                    const b=document.getElementById('navbar-toggler').getBoundingClientRect();
                    window._navFrames.push({x:b.x,y:b.y,width:b.width,height:b.height});
                    requestAnimationFrame(watch);
                }; watch();
            }''')
            button.click()
            tab.page.locator('#navbar-collapse.show').wait_for()
            tab.page.wait_for_timeout(400)
            opened = button.bounding_box()
            button.click()
            tab.page.wait_for_function("!document.getElementById('navbar-collapse').classList.contains('show')")
            tab.page.wait_for_timeout(400)
            closed = button.bounding_box()
            frames = tab.page.evaluate('window._navWatching=false;window._navFrames')
            assert len(frames) > 10
            for frame in [opened,closed,*frames]:
                assert abs(frame['x']-before['x']) <= 1 and abs(frame['y']-before['y']) <= 1, (width,before,frame)
                assert frame['width'] >= 44 and frame['height'] >= 44
            overflow = tab.page.evaluate('document.documentElement.scrollWidth - innerWidth')
            assert overflow <= 1, (width, overflow)
            geometry.append({'width':width,'before':before,'open':opened,'closed':closed,
                             'sampled_frames':len(frames), 'overflow_px':overflow,
                             'max_delta_x':max(abs(frame['x']-before['x']) for frame in frames),
                             'max_delta_y':max(abs(frame['y']-before['y']) for frame in frames)})
        (artifacts()/'hamburger-geometry.json').write_text(json.dumps(geometry,indent=2)+'\n')
        assert not tab.errors,tab.errors
    finally:
        context.close()


@pytest.mark.parametrize('width', [390, 1440])
def test_performance_plots_real_measurements_without_hidden_pipes(inspection_server,browser,width):
    def luminance(color):
        channels = [float(value) / 255 for value in color.removeprefix('rgb(').removesuffix(')').split(',')]
        linear = [value / 12.92 if value <= 0.04045 else ((value + 0.055) / 1.055) ** 2.4
                  for value in channels]
        return sum(value * weight for value, weight in zip(linear, (0.2126, 0.7152, 0.0722)))

    context = browser.new_context(viewport={'width':width,'height':844})
    try:
        tab = Tab(context.new_page(),inspection_server.base_url)
        tab.login('rc16-ops',INSPECTION_PASSWORD,path='/dash')
        tab.goto('/dash/performance','#performance-page')
        tab.page.locator('#performance-avg_duration_seconds .js-plotly-plot').wait_for(timeout=30000)
        assert tab.page.locator('#performance-results .js-plotly-plot').count() == 1
        assert tab.page.locator('#performance-rows_per_second .js-plotly-plot').count() == 1
        for chart in ('avg_duration_seconds','rows_per_second','results'):
            plot = tab.page.locator(f'#performance-{chart} .js-plotly-plot')
            bounds = plot.bounding_box()
            assert bounds and bounds['height'] >= 250 and bounds['width'] >= 150, bounds
            plot.scroll_into_view_if_needed()
            tooltip = plot.evaluate('''graph => {
                const point = graph.data[0].y.findIndex(value => value > 0);
                Plotly.Fx.hover(graph, {xval: graph._fullLayout.xaxis.d2l(graph.data[0].x[point])}, 'xy');
                const layer = graph.querySelector('.hoverlayer');
                const background = layer.querySelector('.legend .bg');
                return {
                    background: background && getComputedStyle(background).fill,
                    text: [...layer.querySelectorAll('.legend text')].map(element => ({
                        value: element.textContent, color: getComputedStyle(element).fill,
                    })),
                };
            }''')
            assert tooltip['background'] and tooltip['text'], tooltip

            background = luminance(tooltip['background'])
            assert background < 0.1, tooltip
            for text in tooltip['text']:
                assert (luminance(text['color']) + 0.05) / (background + 0.05) >= 4.5, tooltip
            if chart == 'results':
                assert any('Succeeded' in item['value'] for item in tooltip['text']), tooltip
                assert any('Failed' in item['value'] for item in tooltip['text']), tooltip
            plot.screenshot(path=str(artifacts()/f'performance-{chart}-hover-{width}.png'))
            plot.evaluate('graph => Plotly.Fx.unhover(graph)')
        body = tab.page.inner_text('#performance-body')
        assert 'rc16_parent' in body and 'rc16_failure' in body
        assert 'HIDDEN_RC16_METRIC' not in tab.page.content()
        tab.page.select_option('#performance-window','7d')
        tab.page.locator('#performance-results .js-plotly-plot').wait_for()
        assert tab.page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        tab.page.screenshot(path=str(artifacts()/f'performance-{width}.png'),full_page=True)
        tab.page.reload(wait_until='domcontentloaded')
        tab.page.locator('#performance-results .js-plotly-plot').wait_for()
        bundle = tab.page.evaluate('''() => performance.getEntriesByType('resource')
            .filter(item => new URL(item.name).pathname.endsWith('/plotly.min.js'))
            .map(item => ({bytes:item.transferSize, decoded:item.decodedBodySize}))''')
        assert len(bundle) == 1 and bundle[0]['bytes'] <= 512, bundle
        assert tab.page.evaluate("typeof Plotly.version === 'string'")
        assert not tab.errors,tab.errors
    finally:
        context.close()


@pytest.mark.parametrize('width', [390, 1440])
def test_performance_refresh_keeps_visuals_visible(inspection_server,browser,width):
    context = browser.new_context(viewport={'width':width,'height':844})
    try:
        tab = Tab(context.new_page(),inspection_server.base_url)
        tab.login('rc16-ops',INSPECTION_PASSWORD,path='/dash')
        tab.goto('/dash/performance','#performance-page')
        tab.page.locator('#performance-results .js-plotly-plot').wait_for(timeout=30000)
        refresh = tab.page.locator('#performance-refresh')
        tab.page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        for trigger in ('performance-refresh.n_clicks', 'performance-interval.n_intervals'):
            refresh.scroll_into_view_if_needed()
            tab.page.evaluate("window._performancePlots = [...document.querySelectorAll('#performance-body .js-plotly-plot')]")
            before = [plot.bounding_box() for plot in tab.page.locator('#performance-body .js-plotly-plot').all()]
            held = []
            observed = []

            def hold_refresh(route):
                payload = route.request.post_data_json or {}
                observed.append((payload.get('output'), payload.get('changedPropIds')))
                if ('performance-body.children' in str(payload.get('output'))
                    and trigger in payload.get('changedPropIds', [])):
                    held.append(route)
                else:
                    route.continue_()

            tab.page.route('**/*_dash-update-component*', hold_refresh)
            try:
                if trigger == 'performance-refresh.n_clicks':
                    refresh.click()
                else:
                    tab.page.evaluate("window.dash_clientside.set_props('performance-interval', {n_intervals: 200})")
                tab.page.wait_for_function("document.getElementById('performance-refresh').disabled")
                tab.page.wait_for_timeout(150)
                assert held, (trigger, observed)
                plots = tab.page.locator('#performance-body .js-plotly-plot').all()
                assert len(plots) == 3 and all(plot.is_visible() for plot in plots)
                after = [plot.bounding_box() for plot in plots]
                for original, current in zip(before, after):
                    assert all(abs(original[key] - current[key]) <= 1 for key in ('x','y','width','height'))
                assert tab.page.locator('#performance-refresh-status').is_visible()
                tab.page.screenshot(path=str(artifacts()/f'performance-refresh-{width}-{trigger.split(".")[0]}.png'))
            finally:
                for route in held:
                    route.continue_()
                tab.page.unroute('**/*_dash-update-component*', hold_refresh)
            tab.page.wait_for_function("!document.getElementById('performance-refresh').disabled")
            tab.page.locator('#performance-refresh-status').wait_for(state='hidden')
            assert tab.page.evaluate("window._performancePlots.every(plot => plot.isConnected)")
        assert not tab.errors,tab.errors
    finally:
        context.close()
