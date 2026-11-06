#! /usr/bin/env python3
# vim:fenc=utf-8

"""
A job written by Meerschaum 4 keeps its command. v4 stored a job's sysargs only in its
`pickle.pkl` and left the label empty for jobs created through the API, so 5.0.0 showed such jobs
with no command. The pickles come from Meerschaum 4.1.4
(`crates/meerschaum-jobs/tests/fixtures/gen_v4_pickles.py`).
"""

import json
import pathlib
import shutil
import uuid

FIXTURE = (
    pathlib.Path(__file__).parent.parent
    / 'crates' / 'meerschaum-jobs' / 'tests' / 'fixtures' / 'v4_pickles.json'
)


def test_v4_jobs_keep_the_command_from_their_pickle():
    import meerschaum as mrsm
    from meerschaum.config.paths import DAEMON_RESOURCES_PATH

    cases = json.loads(FIXTURE.read_text(encoding='utf-8'))['cases']
    made = []
    try:
        for case in cases:
            name = f"test-v4-{case['name']}-{uuid.uuid4().hex[:8]}"
            path = pathlib.Path(DAEMON_RESOURCES_PATH) / name
            path.mkdir(parents=True)
            made.append(path)
            (path / 'properties.json').write_text(
                json.dumps({'restart': True, 'label': '', 'result': [True, 'Exited.']}),
                encoding='utf-8',
            )
            (path / 'pickle.pkl').write_bytes(bytes.fromhex(case['pickle_hex']))
            job = mrsm.Job(name, executor_keys='local')
            assert job.sysargs == (case['sysargs'] or []), case['name']
    finally:
        for path in made:
            shutil.rmtree(path, ignore_errors=True)
