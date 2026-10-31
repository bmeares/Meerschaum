"""A recovered network interruption must not mask genuine component errors."""
from types import SimpleNamespace

from tests.console.conftest import Tab


def test_missing_component_requires_its_own_interrupted_bundle():
    page = SimpleNamespace(on=lambda *args: None)
    message = 'Error: dash_html_components was not found.'
    bundle = 'http://127.0.0.1/dash/_dash-component-suites/dash/html/dash_html_components.js'
    tab = Tab(page, 'http://127.0.0.1')
    tab._on_pageerror(message)
    assert len(tab.errors) == 1
    tab._on_requestfailed(SimpleNamespace(failure='net::ERR_NETWORK_CHANGED', url=bundle))
    tab._on_pageerror(message)
    assert len(tab.errors) == 1
    tab._on_response(SimpleNamespace(status=200, url=bundle))
    tab._on_pageerror(message)
    assert len(tab.errors) == 2
    other = Tab(page, 'http://127.0.0.1')
    other._on_requestfailed(SimpleNamespace(failure='net::ERR_NETWORK_CHANGED', url='/other.js'))
    other._on_pageerror(message)
    assert len(other.errors) == 1
