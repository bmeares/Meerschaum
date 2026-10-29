"""Test service ports. Fixed local defaults; isolated CI never falls back to shared ports."""

import json
import os


LEGACY_PORTS = {
    'timescaledb': 5439, 'timescaledb_ha': 5459, 'postgis': 5449, 'postgresql': 5559,
    'mariadb': 3309, 'mysql': 3310, 'mssql': 1439, 'oracle': 1529, 'valkey': 6399, 'api': 8989,
}


def service_port(flavor):
    if flavor == 'timescaledb-ha':
        flavor = 'timescaledb_ha'
    if os.environ.get('MRSM_TEST_ISOLATED') == '1':
        ports = json.loads(os.environ['MRSM_TEST_PORTS'])
        if flavor in ports:
            port = ports[flavor]
            if (type(port) is not int or not 1024 < port <= 65535
                    or port in {8000, 8998, 8989, 3000, 5432}):
                raise ValueError(f'Invalid isolated {flavor} test port: {port!r}')
            return port
        if flavor in os.environ.get('MRSM_TEST_FLAVORS', '').split(','):
            raise ValueError(f'No owned port for selected test service {flavor!r}')
        # Modules construct their complete connector catalog before deselection. Unselected
        # services must remain inert, even when the developer's legacy stack is running.
        return 1
    if flavor == 'api':
        return int(os.environ.get('MRSM_TEST_API_PORT') or LEGACY_PORTS[flavor])
    return LEGACY_PORTS[flavor]
