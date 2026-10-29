"""Instance connectors per flavor (the repository's disposable test containers)."""

def flavor_attrs(flavor: str, db_dir: str) -> dict:
    """Attributes of the `sql:trn_<flavor>` connector. `trn` schemas where the flavor has them."""
    if flavor == 'sqlite':
        return {'flavor': 'sqlite', 'database': f'{db_dir}/inst_sqlite.db'}
    pg = {'username': 'test', 'password': 'test1234', 'database': 'testdb', 'host': 'localhost', 'schema': 'trn'}
    if flavor == 'postgis':
        return {**pg, 'flavor': 'postgis', 'port': 5449}
    if flavor == 'timescaledb':
        return {**pg, 'flavor': 'timescaledb', 'port': 5439}
    if flavor == 'postgresql':
        return {**pg, 'flavor': 'postgresql', 'port': 5559}
    if flavor in ('mariadb', 'mysql'):
        # a private database (created by v4_build.py, dropped by v4_cleanup.py): the test containers' default
        # databases are shared with other suites
        return {'flavor': flavor, 'username': 'root', 'password': 'my-secret-pw', 'database': 'trndb',
                'host': 'localhost', 'port': 3309 if flavor == 'mariadb' else 3310}
    if flavor == 'mssql':
        return {'flavor': 'mssql', 'username': 'sa', 'password': 'supersecureSECRETPASSWORD123!',
                'database': 'trndb', 'host': 'localhost', 'port': 1439, 'schema': 'trn'}
    if flavor == 'oracle':
        # a private common user (the `xe` service is the container root, where local users cannot be created), created by
        # v4_admin.py and dropped with CASCADE: the objects live in its schema
        return {'flavor': 'oracle', 'host': 'localhost', 'port': 1529, 'database': 'xe', 'username': 'C##TRN', 'password': 'trnpass1234'}
    raise ValueError(flavor)


def admin_attrs(flavor: str) -> dict:
    """Attributes of a connector to an existing database of the same server (to create and drop `trndb`)."""
    a = dict(flavor_attrs(flavor, ''))
    if flavor in ('mariadb', 'mysql'):
        a['database'] = 'mysql'
    elif flavor == 'oracle':
        a['username'], a['password'] = 'system', 'oracle'
    elif flavor == 'mssql':
        a['database'] = 'master'
        a.pop('schema', None)
    return a


PRIVATE_DATABASE = ('mariadb', 'mysql', 'mssql')
PRIVATE_USER = ('oracle',)
SCHEMA_FLAVORS = ('postgis', 'timescaledb', 'postgresql')
