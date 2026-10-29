"""Wheel artifact isolation and validation gates, without building a wheel or running CI."""

import ast
from pathlib import Path
import re
import runpy

import pytest


REPO = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO / '.github' / 'workflows'
release_matrices = runpy.run_path(str(REPO / 'scripts' / 'ci' / 'release_matrix.py'))[
    'release_matrices'
]


def load(name):
    from meerschaum.utils.yaml import yaml
    return yaml.safe_load((WORKFLOWS / name).read_text())


def artifact(entry):
    return entry.get('artifact', 'wheels-' + entry['os'])


def steps_with(job, action):
    return [step for step in job['steps'] if step.get('uses', '').startswith(action)]


def stage_allowed(*, experimental=False, musl=False, build='success', candidate='true',
                  smoke='skipped', alpine='skipped', freethreaded='false', cancelled=False):
    """Evaluate the staging gate for real Actions outcomes, including optional job failures."""
    expression = load('wheel-platform.yaml')['jobs']['stage']['if'].removeprefix('${{').removesuffix('}}')
    values = {
        'inputs.experimental': experimental,
        'inputs.musl': musl,
        'needs.build.result': build,
        'needs.build.outputs.candidate': candidate,
        'needs.smoke.result': smoke,
        'needs.smoke-musl.result': alpine,
        'needs.smoke-freethreaded.outputs.verified': freethreaded,
    }
    expression = expression.replace('!cancelled()', repr(not cancelled))
    for key, value in values.items():
        expression = expression.replace(key, repr(value))
    expression = expression.replace('&&', ' and ').replace('||', ' or ').replace('!', ' not ')
    tree = ast.parse(expression.strip(), mode='eval')
    assert all(isinstance(node, (
        ast.Expression, ast.BoolOp, ast.UnaryOp, ast.And, ast.Or, ast.Not,
        ast.Compare, ast.Eq, ast.Constant,
    )) for node in ast.walk(tree)), expression
    return bool(eval(compile(tree, '<wheel staging gate>', 'eval'), {'__builtins__': {}}))


@pytest.mark.parametrize('version,count,binaries', [('5.0.0', 7, 5), ('5.0.0-rc.15', 2, 1)])
def test_required_matrix_is_complete_and_experimental_is_separate(version, count, binaries):
    matrices = release_matrices(version)
    standard = matrices['wheels']['include']
    optional = matrices['experimental_wheels']['include']
    assert len(standard) == count
    assert len(matrices['binaries']['include']) == binaries
    assert not any(entry.get('freethreaded') for entry in standard)
    assert len(optional) == 1 and optional[0]['freethreaded'] is True
    assert optional[0]['target'] == 'x86_64-unknown-linux-gnu'
    assert artifact(optional[0]) == 'wheels-ubuntu-latest-cp314t'
    names = [artifact(entry) for entry in [*standard, *optional]]
    assert len(names) == len(set(names))
    if count == 7:
        assert {entry['target'] for entry in standard} == {
            'x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu',
            'x86_64-unknown-linux-musl', 'aarch64-unknown-linux-musl',
            'aarch64-apple-darwin', 'x86_64-apple-darwin', 'x86_64-pc-windows-msvc',
        }
    for entry in standard:
        if entry.get('musl'):
            assert entry['os'] == 'ubuntu-latest'
            assert entry['smoke_os'] == (
                'ubuntu-24.04-arm' if entry['wheel_arch'] == 'aarch64' else 'ubuntu-latest'
            )


def test_platform_smokes_depend_only_on_their_own_build():
    caller, platform = load('wheels.yaml'), load('wheel-platform.yaml')
    assert caller['jobs']['build']['uses'] == './.github/workflows/wheel-platform.yaml'
    assert caller['jobs']['build']['with']['experimental'] is False
    assert caller['jobs']['experimental']['if'] == '${{ inputs.experimental }}'
    assert caller['jobs']['experimental']['with']['experimental'] is True
    assert platform['jobs']['smoke']['strategy']['matrix']['python'] == ['3.11', '3.14']
    for name in ['smoke', 'smoke-musl', 'smoke-freethreaded']:
        assert platform['jobs'][name]['needs'] == 'build'
        assert platform['jobs'][name]['runs-on'] == '${{ inputs.smoke-os }}'
    assert 'alpine:3.22 sh /smoke.sh' in platform['jobs']['smoke-musl']['steps'][-1]['run']


@pytest.mark.parametrize('outcomes,expected', [
    ({'smoke': 'success'}, True),
    ({'smoke': 'failure'}, False),
    ({'smoke': 'cancelled'}, False),
    ({'musl': True, 'alpine': 'success'}, True),
    ({'musl': True, 'alpine': 'failure', 'smoke': 'success'}, False),
    ({'experimental': True, 'freethreaded': 'true'}, True),
    ({'experimental': True, 'freethreaded': 'false', 'smoke': 'success'}, False),
    ({'experimental': True, 'freethreaded': 'true', 'build': 'failure'}, False),
    ({'experimental': True, 'freethreaded': 'true', 'candidate': 'false'}, False),
    ({'smoke': 'success', 'cancelled': True}, False),
])
def test_failed_or_skipped_smoke_never_produces_a_validated_artifact(outcomes, expected):
    assert stage_allowed(**outcomes) is expected


def test_provenance_is_complete_and_checked_before_installation():
    platform = load('wheel-platform.yaml')
    build = platform['jobs']['build']
    producer = next(step for step in build['steps'] if step.get('name') == 'Record exact-commit wheel provenance')
    assert '--kind wheel --profile release' in producer['run']
    assert '--toolchain-file wheel-toolchain.txt' in producer['run']
    maturin = steps_with(build, 'PyO3/maturin-action')[0]
    assert maturin['with']['before-script-linux'] == 'rustc -Vv > wheel-toolchain.txt'
    for name in ['smoke', 'smoke-musl', 'smoke-freethreaded']:
        job = platform['jobs'][name]
        source = steps_with(job, 'actions/checkout')[0]['with']
        assert source['path'] == 'smoke-src'
        assert {'scripts/ci/artifacts.py', 'scripts/check_packaging.py', 'Cargo.toml',
                'Cargo.lock', 'rust-toolchain.toml'} <= set(source['sparse-checkout'].splitlines())
        verify_index = next(i for i, step in enumerate(job['steps']) if step.get('name') == 'Verify exact-commit wheel provenance')
        install_index = next(i for i, step in enumerate(job['steps']) if step.get('name', '').startswith('Install the'))
        assert verify_index < install_index
        verify = job['steps'][verify_index]
        assert verify['working-directory'] == 'smoke-src'
        assert '--profile release --kind wheel' in verify['run']
        assert '--target "${{ inputs.target }}"' in verify['run']
        assert steps_with(job, 'actions/download-artifact')[0]['with']['name'] == 'wheel-candidate-${{ inputs.artifact }}'
    assert steps_with(build, 'actions/upload-artifact')[0]['with']['path'] == 'dist/*'
    validated = steps_with(platform['jobs']['stage'], 'actions/upload-artifact')[0]['with']
    assert validated['name'] == '${{ inputs.artifact }}'
    assert validated['path'] == 'dist/*'


def test_only_explicit_builds_run_and_all_checkouts_preserve_the_actual_sha():
    caller = load('wheels.yaml')
    triggers = caller.get('on', caller.get(True))
    assert set(triggers) == {'workflow_call', 'workflow_dispatch'}
    for event in triggers.values():
        assert event['inputs']['experimental']['default'] is False
    for filename in ['wheels.yaml', 'wheel-platform.yaml']:
        workflow = load(filename)
        for job in workflow['jobs'].values():
            for step in job.get('steps', []):
                if step.get('uses', '').startswith('actions/checkout'):
                    assert step['with']['ref'] == '${{ github.sha }}'
                assert not re.search(r'\b(?:twine upload|cargo publish|gh release create|docker push)\b', step.get('run', ''))
