#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Report what the Python `meerschaum.jobs.Job` class sees for jobs created by Rust.

Used by `tests/python_interop.rs` (not a fixture generator): it runs this script with the
reference interpreter (`.venv-ref/bin/python -s`) in a minimal environment whose
`MRSM_ROOT_DIR` holds the Rust-created jobs, and compares the JSON printed on stdout.

    read_jobs.py [--stop NAME ...] NAME [NAME ...]

`--stop NAME` stops that job with Python's `Job.stop()` before the states are read.
"""

import json
import sys


def iso(dt):
    return dt.isoformat() if dt is not None else None


def job_state(job):
    return {
        'status': job.status,
        'pid': job.pid,
        'sysargs': job.sysargs,
        'executor_keys': job.executor_keys,
        'result': list(job.result),
        'logs': job.get_logs(),
        'label': job.label,
        'daemon_label': job.daemon.label,
        'restart': job.restart,
        'began': iso(job.began),
        'paused': iso(job.paused),
        'ended': iso(job.ended),
        'stop_time': iso(job.stop_time),
        'hidden': job.hidden,
        'exists': job.exists(),
        'env': job.env,
        'delete_after_completion': job.delete_after_completion,
        'is_blocking_on_stdin': job.is_blocking_on_stdin(),
        'str': str(job),
        'properties': job.daemon.properties,
    }


def main():
    from meerschaum.jobs import Job

    args = sys.argv[1:]
    to_stop, names = [], []
    while args:
        arg = args.pop(0)
        if arg == '--stop':
            to_stop.append(args.pop(0))
        else:
            names.append(arg)

    out = {'stopped': {}, 'jobs': {}}
    for name in to_stop:
        out['stopped'][name] = list(Job(name, executor_keys='local').stop())
    for name in names:
        out['jobs'][name] = job_state(Job(name, executor_keys='local'))
    sys.stdout.write(json.dumps(out))


if __name__ == '__main__':
    main()
