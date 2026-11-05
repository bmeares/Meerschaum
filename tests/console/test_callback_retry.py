#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A callback the API host could not authorize because its session store was busy (`503` with
`Retry-After`, `meerschaum/api/host/_callbacks.py`: the callback did not run) is sent again by the
console (`assets/session_guard.js`), so the page still renders; the renderer never repeats a
failed callback by itself, which left a page without its content (opt-in, see `conftest.py`).
"""

from __future__ import annotations

import json

import pytest

from tests.console.conftest import Tab

pytestmark = pytest.mark.console

BUSY = json.dumps({
    'success': False,
    'message': "The console's sessions cannot be read right now. Try again shortly.",
})


def test_a_callback_the_busy_session_store_refused_is_sent_again(server, tab):
    tab.login(path='/dash')
    page = tab.page
    refused = []

    def busy_store(route):
        outputs = Tab._callback_outputs(route.request)
        if 'page-layout-div.children' in outputs and len(refused) < 2:
            refused.append(outputs)
            route.fulfill(
                status=503, body=BUSY, content_type='application/json',
                headers={'Retry-After': '1', 'Cache-Control': 'no-store'},
            )
            return
        route.continue_()

    page.route('**/_dash-update-component*', busy_store)
    tab.goto('/dash/pipelines', '#pipelines-list-page')
    assert len(refused) == 2, refused
    ### The refusals above are the only failed responses the page saw.
    assert [error for error in tab.errors if not error.startswith('503 ')] == []

