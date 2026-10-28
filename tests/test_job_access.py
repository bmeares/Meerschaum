#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Who a job belongs to, and who may see it (rc15 decision AC9), through `meerschaum.jobs`:
`job.owner`, `job.team`, `filter_visible_jobs`, `ownership_properties`, `get_team_jobs`,
`move_team_jobs`, a team rename or deletion moving its jobs (`mrsm.Team`), and a job started by
another job belonging to that job's owner.

The jobs here are written, never started (except the deletion race and the job started by a
job), in this process's root.
"""

import time
import uuid

import pytest

import meerschaum as mrsm
from meerschaum.jobs import (
    Job,
    can_see_job,
    filter_visible_jobs,
    get_team_jobs,
    job_access,
    move_team_jobs,
    ownership_properties,
)


def _name(prefix: str = 'p5') -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def make_job():
    """Write jobs (not started) with unique names; delete them at the end."""
    jobs = []

    def _make(properties=None, name=None):
        job = Job(name or _name(), ['show', 'version'], executor_keys='local', _properties=properties)
        assert job.daemon.write_properties()[0]
        jobs.append(job)
        return job

    yield _make
    for job in jobs:
        try:
            job.delete()
        except Exception:
            pass


def _run_properties(owner, team):
    """The properties of a console run (`POST /execute`) started by `owner`."""
    return {
        **ownership_properties(owner, team),
        'mrsm_execution': {'kind': 'temporary_execution', 'origin': 'api_execute'},
    }


def test_local_jobs_belong_to_everyone(make_job):
    job = make_job()
    assert job.owner is None
    assert job.team is None
    assert job_access(job) == {'owner': None, 'team': None, 'private': False}
    assert can_see_job(None, job)


def test_ownership_properties_record_who_started_a_job(make_job):
    assert ownership_properties('alice', 'Data') == {
        'owner': 'alice', 'team': 'Data', 'mrsm_owner': 'alice',
    }
    assert ownership_properties('alice', 'Everyone')['team'] is None
    assert ownership_properties() == {'owner': None, 'team': None}
    job = make_job(ownership_properties('alice', 'Data'))
    assert (job.owner, job.team) == ('alice', 'Data')
    ### A fresh `Job` reads them back from the job's directory.
    again = Job(job.name, executor_keys='local')
    assert (again.owner, again.team) == ('alice', 'Data')


def test_filter_visible_jobs_applies_the_team_rule(make_job):
    from meerschaum.core.access import Principal
    local = make_job()
    data = make_job(ownership_properties('alice', 'Data'))
    ops = make_job(ownership_properties('dave', 'Ops'))
    run = make_job(_run_properties('alice', 'Data'), name=_name('execute'))
    jobs = {job.name: job for job in (local, data, ops, run)}

    def visible(principal):
        return sorted(filter_visible_jobs(principal, jobs))

    bob = Principal(username='bob', teams=[('Data', 'member')])
    assert visible(bob) == sorted([local.name, data.name])
    ### The owner sees their own console run; the team's manager sees it too.
    assert visible(Principal(username='alice', teams=[('Data', 'member')])) == sorted(
        [local.name, data.name, run.name]
    )
    assert visible(Principal(username='carol', teams=[('data', 'manager')])) == sorted(
        [local.name, data.name, run.name]
    )
    ### Dictionaries and the console's viewers (`teams` as a dict) work as well.
    assert visible({'username': 'dave', 'teams': [['Ops', 'member']]}) == sorted([local.name, ops.name])
    assert visible(Principal(username='root', is_admin=True)) == sorted(jobs)
    assert visible(None) == [local.name]

    class Viewer:
        username = 'erin'
        is_admin = False
        no_auth = False
        teams = {'Everyone': 'member', 'Ops': 'manager'}

    assert visible(Viewer()) == sorted([local.name, ops.name])

    class SignedOut(Viewer):
        username = 'alice'
        signed_in = False
        teams = {'Data': 'member'}

    ### A console viewer who is not signed in is anonymous.
    assert visible(SignedOut()) == [local.name]
    ### A list keeps its order and shape.
    assert filter_visible_jobs(bob, [ops, data, local]) == [data, local]


@pytest.fixture
def sqlite_instance(tmp_path):
    """A SQLite instance with users (alice, bob in Data; dave in Ops; an admin)."""
    from meerschaum.core import User
    label = 'p5_' + uuid.uuid4().hex[:8]
    conn = mrsm.get_connector('sql', label, flavor='sqlite', database=str(tmp_path / 'p5.db'))
    for username, user_type in [('alice', None), ('bob', None), ('dave', None), ('root', 'admin')]:
        user = User(username, 'pw', type=user_type, instance=conn)
        assert conn.register_user(user)[0]
    assert mrsm.Team('Data', instance=conn, members=['alice', 'bob']).register()[0]
    assert mrsm.Team('Ops', instance=conn, members=['dave']).register()[0]
    return conn


def test_filter_visible_jobs_resolves_users_on_their_instance(make_job, sqlite_instance):
    from meerschaum.core import User
    conn = sqlite_instance
    local = make_job()
    data = make_job(ownership_properties('alice', 'Data'))
    ops = make_job(ownership_properties('dave', 'Ops'))
    jobs = {job.name: job for job in (local, data, ops)}
    assert sorted(filter_visible_jobs(User('bob', instance=conn), jobs)) == sorted([local.name, data.name])
    assert sorted(filter_visible_jobs('dave', jobs, instance=conn)) == sorted([local.name, ops.name])
    assert sorted(filter_visible_jobs(User('root', instance=conn), jobs)) == sorted(jobs)
    ### Someone who cannot be read is in no team (their own jobs and Everyone's).
    assert sorted(filter_visible_jobs('alice', jobs, instance='sql:p5_missing_instance')) == sorted(
        [local.name, data.name]
    )


def test_renaming_or_deleting_a_team_moves_its_jobs(make_job, sqlite_instance):
    conn = sqlite_instance
    data = make_job(ownership_properties('alice', 'Data'))
    ops = make_job(ownership_properties('dave', 'Ops'))
    assert data.name in get_team_jobs('Data')
    assert data.name in get_team_jobs('data')
    assert ops.name not in get_team_jobs('Data')

    ### `mrsm.Team.rename()` renames it in the jobs that name it.
    assert mrsm.Team('Data', instance=conn).rename('Analytics')[0]
    assert Job(data.name, executor_keys='local').team == 'Analytics'
    assert Job(data.name, executor_keys='local').owner == 'alice'
    ### ... and `delete()` moves them to Everyone.
    assert mrsm.Team('Analytics', instance=conn).delete()[0]
    assert Job(data.name, executor_keys='local').team is None
    assert Job(ops.name, executor_keys='local').team == 'Ops'

    ### The helper itself.
    success, message = move_team_jobs('Ops', 'Operations')
    assert success, message
    assert 'Moved' in message and "team 'Operations'" in message
    assert Job(ops.name, executor_keys='local').team == 'Operations'
    success, message = move_team_jobs('Operations')
    assert success and message.endswith('to Everyone.'), message
    assert Job(ops.name, executor_keys='local').team is None


def test_a_job_deleted_right_after_it_was_created_stays_deleted():
    """The detached supervisor never recreates a deleted job's directory (wave-1 gate)."""
    names = []
    for _ in range(3):
        job = Job(_name('quick'), ['show', 'version'], executor_keys='local')
        names.append(job.name)
        assert job.start()[0]
        assert Job(job.name, executor_keys='local').delete()[0]
    time.sleep(3)
    for name in names:
        assert not Job(name, executor_keys='local').exists(), f"{name} came back"


def test_a_job_started_by_a_job_belongs_to_that_job_s_owner(tmp_path):
    """
    A job started from inside another job (`start pipeline NAME` or a plugin action run from the
    console's Execute box, here a Python file the parent job runs) belongs to that job's owner,
    in its team (not to Everyone).
    """
    parent_name, child_name = _name('parent'), _name('child')
    script = tmp_path / 'start_child.py'
    script.write_text(
        "from meerschaum.jobs import Job\n"
        f"job = Job({child_name!r}, ['show', 'version'], executor_keys='local')\n"
        "assert job.daemon.write_properties()[0]\n"
    )
    parent = Job(
        parent_name,
        ['python', str(script)],
        executor_keys='local',
        _properties={
            **ownership_properties('alice', 'Data'),
            'mrsm_execution': {'kind': 'temporary_execution', 'origin': 'api_execute'},
        },
    )
    try:
        assert parent.start()[0]
        deadline = time.time() + 90
        child = Job(child_name, executor_keys='local')
        while time.time() < deadline and not child.exists():
            time.sleep(0.25)
            child = Job(child_name, executor_keys='local')
        assert child.exists(), Job(parent_name, executor_keys='local').get_logs()
        assert (child.owner, child.team) == ('alice', 'Data')
        ### A job of the team (not a private console run like its parent).
        assert job_access(child) == {'owner': 'alice', 'team': 'Data', 'private': False}
        assert can_see_job({'username': 'bob', 'teams': [('Data', 'member')]}, child)
        assert not can_see_job({'username': 'dave', 'teams': [('Ops', 'member')]}, child)
        ### Outside a job, a new job is Everyone's.
        local = Job(_name('local'), ['show', 'version'], executor_keys='local')
        assert (local.owner, local.team) == (None, None)
    finally:
        for name in (child_name, parent_name):
            try:
                Job(name, executor_keys='local').delete()
            except Exception:
                pass
