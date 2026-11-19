"""Native lifecycle markers cannot invent a raw connector beside sealed API aliases."""

from meerschaum.config.environment import get_connector_env_vars, _NATIVE_EXECUTION_ENV_VARS


def test_native_execution_markers_are_reserved_without_hiding_explicit_api_connectors():
    env = dict.fromkeys(_NATIVE_EXECUTION_ENV_VARS, '1')
    env.update({
        'MRSM_API_DATA': 'https://fixture.invalid:8443',
        'MRSM_API_EXECUTION_DATA': 'https://other.invalid:8443',
        'MRSM_SQL_DATA': 'sqlite:///:memory:',
        'MRSM_PLUGIN_SOURCE': 'plugin://fixture',
    })
    assert get_connector_env_vars(env) == [
        'MRSM_API_DATA', 'MRSM_API_EXECUTION_DATA', 'MRSM_SQL_DATA', 'MRSM_PLUGIN_SOURCE',
    ]
