#!/usr/bin/env python3
"""Build from sealed SDK bytes and publish the exact Docker images that passed checks."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import zipfile

spec = importlib.util.spec_from_file_location('docker_candidates', Path(__file__).with_name('candidates.py'))
candidates = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidates)
gate = candidates.gate
GROUPS = ('minimal', 'full', 'slim', 'rust', 'rust-full')
IMAGE = 'bmeares/meerschaum'
TARGETS = {'linux/amd64': 'x86_64-unknown-linux-gnu', 'linux/arm64': 'aarch64-unknown-linux-gnu'}
EXECUTOR_CASES = (
    'generated_container_starts_api_job_and_discovers_a_python_source',
    'generated_container_starts_credentialed_team_job',
    'generated_container_api_job_delivers_plugin_rows',
    'generated_container_team_job_delivers_plugin_rows',
    'generated_container_discovers_a_granted_native_resource',
    'public_host_api_terminal_shows_version_after_reconnect',
    'public_remote_api_terminal_shows_version_after_reconnect',
    'generated_executors_resume_jobs_after_outage_and_preserve_team_state_on_recreation',
)


class MissingImages(ValueError):
    """A valid candidate's local images have been pruned or are on another daemon."""


def command(*args: str):
    return json.loads(subprocess.check_output(list(args), text=True))


def version() -> str:
    return subprocess.check_output(['python3', 'scripts/check_packaging.py', '--version'], text=True).strip()


def prepare_sdk(run_id: str, platform: str, output: Path) -> dict:
    source = gate.artifacts.checkout()
    candidates.trusted_run(run_id, 'release.yaml', commit=source['commit'])
    names = candidates.available(run_id)
    required, optional = gate.expected_catalog()
    if not (required.keys() | {'release-candidate-manifest'}) <= names:
        raise ValueError('The complete sealed SDK artifacts must still be available.')
    output.mkdir(parents=True, exist_ok=False)
    sealed = output / 'sealed'
    candidates.download(run_id, sealed, *(sorted(required.keys() | (optional.keys() & names))))
    manifests = output / 'candidate'
    candidates.download(run_id, manifests, 'release-candidate-manifest')
    record = gate.verify(sealed, manifests / 'release-candidate.json')
    if record['run_id'] != run_id or record['repository'] != candidates.repository():
        raise ValueError('The sealed SDK came from another Actions run or repository.')
    target = TARGETS[platform]
    wheel_name = next(name for name, entry in required.items()
                      if entry['kind'] == 'wheel' and entry['target'] == target)
    binary_name = next(name for name, entry in required.items()
                       if entry['kind'] == 'binary' and entry['target'] == target)
    wheel = next((sealed / wheel_name).glob('*.whl'))
    binary = sealed / binary_name / required[binary_name]['program']
    (output / 'wheels').mkdir()
    shutil.copyfile(wheel, output / 'wheels' / wheel.name)
    shutil.copyfile(binary, output / 'mrsm')
    (output / 'mrsm').chmod(0o755)
    info = {'source': source, 'candidate_run_id': run_id, 'platform': platform,
            'binary_sha256': gate.artifacts.sha256(binary),
            'wheel_sha256': gate.artifacts.sha256(wheel)}
    (output / 'sdk-info.json').write_text(json.dumps(info, indent=2) + '\n')
    # Keep the verified receipts outside the build context's SDK payload.
    shutil.rmtree(sealed)
    shutil.rmtree(manifests)
    return info


def inspect_image(reference: str) -> dict:
    return command('docker', 'image', 'inspect', reference)[0]


def check_executor_image(image_id: str, sdk: Path, info: dict) -> None:
    """Exercise the sealed Linux image with generated Teams on the native Docker runner."""
    wheels = list((sdk / 'wheels').glob('*.whl'))
    if len(wheels) != 1 or gate.artifacts.sha256(wheels[0]) != info['wheel_sha256']:
        raise ValueError('The executor check requires the sealed Linux wheel.')
    if gate.artifacts.sha256(sdk / 'mrsm') != info['binary_sha256']:
        raise ValueError('The executor check requires the sealed Linux CLI.')
    with zipfile.ZipFile(wheels[0]) as wheel:
        extensions = [entry for entry in wheel.namelist()
                      if re.fullmatch(r'meerschaum/_core/_native[^/]*\.so', entry)]
        if len(extensions) != 1:
            raise ValueError('Expected one native SDK extension in the sealed Linux wheel.')
        with wheel.open(extensions[0]) as extension:
            sdk_sha256 = hashlib.file_digest(extension, 'sha256').hexdigest()
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('MRSM_', 'TPR_'))
           and key not in ('TMUX', 'TMUX_PANE', 'GH_TOKEN', 'GITHUB_TOKEN', 'CARGO_TARGET_DIR')}
    with tempfile.TemporaryDirectory(prefix='mrsm-release-executors-', dir=env.get('RUNNER_TEMP')) as root:
        plugins = Path(root) / 'plugins'
        plugins.mkdir()
        env.update(MRSM_ROOT_DIR=root, MRSM_PLUGINS_DIR=str(plugins), MRSM_NO_AUTO_INSTALL='1',
                   MRSM_TEST_EXECUTOR_IMAGE=image_id, MRSM_TEST_EXECUTOR_PROVISION_FACTORY='1',
                   MRSM_TEST_EXECUTOR_NATIVE_SHA256=info['binary_sha256'],
                   MRSM_TEST_EXECUTOR_SDK_SHA256=sdk_sha256)
        args = ['cargo', 'test', '--locked', '-p', 'meerschaum-cli', '--test', 'executor_container',
                '--', '--ignored', '--exact', '--test-threads=1', *EXECUTOR_CASES]
        # libtest accepts a missing exact filter and can exit successfully with zero tests.
        result = subprocess.run(args, env=env, text=True, stdout=subprocess.PIPE)
        print(result.stdout, flush=True)
        result.check_returncode()
        if f'test result: ok. {len(EXECUTOR_CASES)} passed;' not in result.stdout:
            raise ValueError('The installed executor suite did not exercise every selected case.')


def required_checks(platform: str) -> dict:
    checks = {'runtime_smokes': 'success', 'size_gate': 'success', 'facade_smoke': 'success'}
    if platform == 'linux/amd64':
        checks['installed_team_executors'] = 'success'
    return checks


def record_images(output: Path, platform: str, sdk: Path) -> dict:
    info = json.loads((sdk / 'sdk-info.json').read_text())
    source = gate.artifacts.checkout()
    if info['source'] != source or info['platform'] != platform:
        raise ValueError('The images must use the sealed SDK for this source and platform.')
    arch = platform.split('/')[1]
    images = {}
    tag_prefix = f'{IMAGE}:candidate-{os.environ["GITHUB_RUN_ID"]}-{os.environ.get("GITHUB_RUN_ATTEMPT", "1")}-{arch}'
    for group in GROUPS:
        image = inspect_image(f'{IMAGE}:{version()}-{group}')
        if (f'{image["Os"]}/{image["Architecture"]}' != platform
                or image['Config']['Labels'].get('org.opencontainers.image.revision') != source['commit']):
            raise ValueError(f'Image {group} has the wrong source or platform.')
        if not re.fullmatch('sha256:[0-9a-f]{64}', image['Id']):
            raise ValueError('Invalid Docker image ID.')
        local = f'{tag_prefix}-{group}'
        subprocess.run(['docker', 'tag', image['Id'], local], check=True)
        images[group] = {'id': image['Id'], 'local_tag': local}
    if platform == 'linux/amd64':
        check_executor_image(images['full']['id'], sdk, info)
    record = {'schema': 1, 'source': source, 'version': version(), 'platform': platform,
              'sdk': info, 'run_id': os.environ['GITHUB_RUN_ID'],
              'checks': required_checks(platform),
              'images': images}
    output.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
    return record


def validate_record(path: Path, platform: str, sdk_run: str, *, local: bool = True,
                    sdk: Path | None = None) -> dict:
    record = json.loads(path.read_text())
    source = gate.artifacts.checkout()
    if (record.get('schema') != 1 or record.get('source') != source
            or record.get('version') != version() or record.get('platform') != platform
            or record.get('sdk', {}).get('source') != source
            or record['sdk'].get('candidate_run_id') != sdk_run
            or record['sdk'].get('platform') != platform
            or set(record.get('images', {})) != set(GROUPS)
            or record.get('checks') != required_checks(platform)):
        raise ValueError('Docker candidate source, SDK, platform, image catalog or checks differ.')
    if sdk is not None and record['sdk'] != json.loads((sdk / 'sdk-info.json').read_text()):
        raise ValueError('Docker candidate SDK checksums differ from the sealed SDK.')
    missing = []
    for group, entry in record['images'].items():
        if not re.fullmatch('sha256:[0-9a-f]{64}', entry['id']):
            raise ValueError('Invalid candidate image ID.')
        if not local:
            continue
        try:
            image = inspect_image(entry['id'])
        except subprocess.CalledProcessError:
            missing.append(group)
            continue
        if (image['Id'] != entry['id'] or f'{image["Os"]}/{image["Architecture"]}' != platform
                or image['Config']['Labels'].get('org.opencontainers.image.revision') != source['commit']):
            raise ValueError('The local image differs from the verified candidate.')
    if missing:
        raise MissingImages(f'Tested candidate images are missing: {missing}. Rebuild explicitly and rerun every gate.')
    return record


def fetch_images(run_id: str, platform: str, sdk_run: str, output: Path) -> dict:
    candidates.trusted_run(run_id, 'docker-build.yaml', commit=gate.artifacts.checkout()['commit'])
    name = 'docker-candidate-' + platform.split('/')[1]
    if name not in candidates.available(run_id):
        raise ValueError('Docker candidate receipt is missing or expired.')
    candidates.download(run_id, output, name)
    path = output / 'docker-candidate.json'
    record = validate_record(path, platform, sdk_run, local=False)
    if record['run_id'] != run_id:
        raise ValueError('Docker receipt belongs to another Actions run.')
    return record


def promote(path: Path, platform: str, sdk_run: str, *, dry_run: bool = False,
            sdk: Path | None = None) -> None:
    record = validate_record(path, platform, sdk_run, sdk=sdk)
    arch = platform.split('/')[1]
    # Validate the whole catalog before the first registry mutation.
    for group, image in record['images'].items():
        tag = f'{IMAGE}:{record["version"]}-{group}-{arch}'
        commands = [['docker', 'tag', image['id'], tag], ['docker', 'push', tag]]
        for args in commands:
            if dry_run:
                print(' '.join(args))
            else:
                subprocess.run(args, check=True)
        if dry_run:
            continue
        manifest = command('docker', 'buildx', 'imagetools', 'inspect', '--raw', tag)
        if manifest.get('config', {}).get('digest') != image['id']:
            raise ValueError(f'The pushed {group} config digest differs from the tested image.')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('sdk', 'record', 'fetch', 'check', 'promote'))
    parser.add_argument('--platform', choices=tuple(TARGETS), required=True)
    parser.add_argument('--sdk-run-id', required=True)
    parser.add_argument('--image-run-id')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--sdk', type=Path)
    parser.add_argument('--record', type=Path)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    try:
        if args.command == 'sdk':
            prepare_sdk(args.sdk_run_id, args.platform, args.output)
        elif args.command == 'record':
            record_images(args.output, args.platform, args.sdk)
        elif args.command == 'fetch':
            fetch_images(args.image_run_id, args.platform, args.sdk_run_id, args.output)
        elif args.command == 'check':
            validate_record(args.record, args.platform, args.sdk_run_id, sdk=args.sdk)
        else:
            promote(args.record, args.platform, args.sdk_run_id, dry_run=args.dry_run, sdk=args.sdk)
    except MissingImages as error:
        parser.exit(2, str(error) + '\n')
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f'Docker candidate: {error}\n')


if __name__ == '__main__':
    main()
