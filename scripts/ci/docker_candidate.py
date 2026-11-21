#!/usr/bin/env python3
"""Build from sealed SDK bytes and publish the exact Docker images that passed checks.

A candidate run pushes each architecture's images to the GHCR staging repository and records their
digests (`record`); the amd64 `full` image is pulled by digest on the native Docker runner for the
installed Team executor suite (`executors`). Publication fetches both receipts from that successful
run (`fetch`), checks them against a freshly verified sealed SDK and the staging registry (`check`),
and copies exactly those digests to Docker Hub registry to registry (`promote`): nothing is rebuilt.
"""
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


# The user every published image runs as (the uid of 5.0 and 5.1 images): a runner's own uid never leaks in.
IMAGE_USER = '1000:1000'
CANDIDATES = 'candidate'
# BuildKit target of each published group: `minimal` and `slim` are one image.
TARGET_OF = {'minimal': 'slim', 'slim': 'slim', 'full': 'full', 'rust': 'rust', 'rust-full': 'rust-full'}
DIGEST = re.compile('sha256:[0-9a-f]{64}')
SCHEMA = 2


class MissingImages(ValueError):
    """A valid receipt's candidate digests are no longer in the staging registry."""


def command(*args: str):
    return json.loads(subprocess.check_output(list(args), text=True))


def version() -> str:
    return subprocess.check_output(['python3', 'scripts/check_packaging.py', '--version'], text=True).strip()


def candidate_repository() -> str:
    """The staging repository on GHCR: `ghcr.io/<owner>/<repository>/candidate`, in lower case."""
    return f'ghcr.io/{candidates.repository().lower()}/{CANDIDATES}'


def registry_image(reference: str) -> tuple[dict, dict]:
    """The raw manifest and the image configuration of one registry reference."""
    manifest = command('docker', 'buildx', 'imagetools', 'inspect', '--raw', reference)
    config = command('docker', 'buildx', 'imagetools', 'inspect', '--format', '{{json .Image}}', reference)
    return manifest, config


def registry_digest(reference: str) -> str:
    """The digest a registry serves for a tag or digest reference (the `Digest:` line of the summary)."""
    summary = subprocess.check_output(['docker', 'buildx', 'imagetools', 'inspect', reference], text=True)
    for line in summary.splitlines():
        key, _, value = line.partition(':')
        if key.strip() == 'Digest' and DIGEST.fullmatch(value.strip()):
            return value.strip()
    raise ValueError(f'The registry reported no digest for {reference}.')


def check_registry_image(reference: str, entry: dict, platform: str, commit: str) -> None:
    """The reference is one image manifest with the recorded config, platform, source and user."""
    try:
        manifest, config = registry_image(reference)
    except subprocess.CalledProcessError as error:
        raise MissingImages(f'{reference} is not in the registry.') from error
    if 'manifests' in manifest or manifest.get('config', {}).get('digest') != entry['config']:
        raise ValueError(f'{reference} is not the single image manifest that was tested.')
    settings = config.get('config') or {}
    if (f'{config.get("os")}/{config.get("architecture")}' != platform
            or (settings.get('Labels') or {}).get('org.opencontainers.image.revision') != commit
            or settings.get('User') != IMAGE_USER):
        raise ValueError(f'{reference} has the wrong platform, source revision or user.')


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


def required_checks() -> dict:
    return {'runtime_smokes': 'success', 'size_gate': 'success', 'facade_smoke': 'success'}


def needs_executors(platform: str) -> bool:
    """The installed Team executor suite runs on the native amd64 Docker runner."""
    return platform == 'linux/amd64'


def record_images(output: Path, platform: str, sdk: Path, metadata: Path, repository: str) -> dict:
    """Record the digests the build pushed to the staging repository and the smokes tested."""
    info = json.loads((sdk / 'sdk-info.json').read_text())
    source = gate.artifacts.checkout()
    if info['source'] != source or info['platform'] != platform:
        raise ValueError('The images must use the sealed SDK for this source and platform.')
    if repository != candidate_repository():
        raise ValueError('Candidates must be staged in this repository\'s GHCR namespace.')
    images = {}
    for group in GROUPS:
        built = json.loads((metadata / f'{TARGET_OF[group]}.json').read_text())
        entry = {'digest': built.get('containerimage.digest'),
                 'config': built.get('containerimage.config.digest')}
        if not all(isinstance(value, str) and DIGEST.fullmatch(value) for value in entry.values()):
            raise ValueError(f'The {group} build recorded no image digest.')
        check_registry_image(f'{repository}@{entry["digest"]}', entry, platform, source['commit'])
        images[group] = entry
    record = {'schema': SCHEMA, 'source': source, 'version': version(), 'platform': platform,
              'sdk': info, 'run_id': os.environ['GITHUB_RUN_ID'], 'repository': repository,
              'checks': required_checks(), 'images': images}
    output.write_text(json.dumps(record, indent=2, sort_keys=True) + '\n')
    return record


def validate_record(path: Path, platform: str, sdk_run: str, *, registry: bool = True,
                    sdk: Path | None = None) -> dict:
    record = json.loads(path.read_text())
    source = gate.artifacts.checkout()
    if (record.get('schema') != SCHEMA or record.get('source') != source
            or record.get('version') != version() or record.get('platform') != platform
            or record.get('repository') != candidate_repository()
            or record.get('sdk', {}).get('source') != source
            or record['sdk'].get('candidate_run_id') != sdk_run
            or record['sdk'].get('platform') != platform
            or set(record.get('images', {})) != set(GROUPS)
            or record.get('checks') != required_checks()):
        raise ValueError('Docker candidate source, SDK, platform, repository, image catalog or checks differ.')
    if sdk is not None and record['sdk'] != json.loads((sdk / 'sdk-info.json').read_text()):
        raise ValueError('Docker candidate SDK checksums differ from the sealed SDK.')
    for group, entry in record['images'].items():
        if set(entry) != {'digest', 'config'} or not all(DIGEST.fullmatch(str(v)) for v in entry.values()):
            raise ValueError('Invalid candidate image digest.')
        if record['images'][TARGET_OF[group]] != entry:
            raise ValueError('`minimal` and `slim` must be the same tested image.')
    if registry:
        missing = []
        for group, entry in record['images'].items():
            try:
                check_registry_image(f'{record["repository"]}@{entry["digest"]}', entry, platform,
                                     source['commit'])
            except MissingImages:
                missing.append(group)
        if missing:
            raise MissingImages(f'Tested candidate digests are missing from the staging registry: {missing}. '
                                'Dispatch a new candidate run; every gate runs again.')
    return record


def executor_record(path: Path, record: dict) -> dict:
    """The installed Team executor receipt for exactly this candidate's `full` image."""
    executors = json.loads(path.read_text())
    if (executors.get('schema') != SCHEMA or executors.get('source') != record['source']
            or executors.get('run_id') != record['run_id'] or executors.get('platform') != record['platform']
            or executors.get('image') != record['images']['full']
            or executors.get('installed_team_executors') != 'success'):
        raise ValueError('The installed Team executor receipt does not cover this candidate.')
    return executors


def check_executors(record_path: Path, output: Path, platform: str, sdk_run: str, sdk: Path) -> dict:
    """Pull the tested `full` image by digest and run the installed Team executor suite on it."""
    if not needs_executors(platform):
        raise ValueError(f'The installed Team executor suite runs on linux/amd64, not {platform}.')
    record = validate_record(record_path, platform, sdk_run, sdk=sdk)
    if record['run_id'] != os.environ['GITHUB_RUN_ID']:
        raise ValueError('The executor suite tests the candidate of its own run.')
    full = record['images']['full']
    reference = f'{record["repository"]}@{full["digest"]}'
    subprocess.run(['docker', 'pull', '--quiet', '--platform', platform, reference], check=True)
    local = command('docker', 'image', 'inspect', reference)[0]['Id']
    # The classic image store names an image by its config digest, the containerd store by its manifest.
    if local not in (full['config'], full['digest']):
        raise ValueError('The pulled image differs from the tested candidate.')
    check_executor_image(local, sdk, record['sdk'])
    receipt = {'schema': SCHEMA, 'source': record['source'], 'run_id': record['run_id'],
               'platform': platform, 'image': full, 'installed_team_executors': 'success'}
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    return receipt


def fetch_images(run_id: str, platform: str, sdk_run: str, output: Path) -> dict:
    candidates.trusted_run(run_id, 'docker-build.yaml', commit=gate.artifacts.checkout()['commit'])
    arch = platform.split('/')[1]
    names = {'docker-candidate-' + arch: output / 'candidate'}
    if needs_executors(platform):
        names['docker-executors-' + arch] = output / 'executors'
    available = candidates.available(run_id)
    if not names.keys() <= available:
        raise ValueError('Docker candidate receipts are missing or expired.')
    for name, directory in names.items():
        candidates.download(run_id, directory, name)
    record = validate_record(output / 'candidate' / 'docker-candidate.json', platform, sdk_run,
                             registry=False)
    if record['run_id'] != run_id:
        raise ValueError('Docker receipt belongs to another Actions run.')
    if needs_executors(platform):
        executor_record(output / 'executors' / 'docker-executors.json', record)
    return record


def check_publication(receipts: Path, platform: str, sdk_run: str, sdk: Path | None = None) -> dict:
    """Everything publication requires: the receipts, the sealed SDK and the staged digests."""
    record = validate_record(receipts / 'candidate' / 'docker-candidate.json', platform, sdk_run, sdk=sdk)
    if needs_executors(platform):
        executor_record(receipts / 'executors' / 'docker-executors.json', record)
    return record


def promote(receipts: Path, platform: str, sdk_run: str, *, dry_run: bool = False,
            sdk: Path | None = None) -> None:
    """Copy the tested digests to the architecture tags of the public repository, registry to registry."""
    # Validate the whole catalog before the first registry mutation.
    record = check_publication(receipts, platform, sdk_run, sdk=sdk)
    arch = platform.split('/')[1]
    for group in GROUPS:
        entry = record['images'][group]
        tag = f'{IMAGE}:{record["version"]}-{group}-{arch}'
        # One source manifest is copied as it is (no index around it), so the tag serves the tested digest.
        args = ['docker', 'buildx', 'imagetools', 'create', '--prefer-index=false', '-t', tag,
                f'{record["repository"]}@{entry["digest"]}']
        if dry_run:
            print(' '.join(args))
            continue
        subprocess.run(args, check=True)
        if registry_digest(tag) != entry['digest']:
            raise ValueError(f'The published {group} digest differs from the tested image.')
        check_registry_image(tag, entry, platform, record['source']['commit'])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('sdk', 'record', 'executors', 'fetch', 'check', 'promote'))
    parser.add_argument('--platform', choices=tuple(TARGETS), required=True)
    parser.add_argument('--sdk-run-id', required=True)
    parser.add_argument('--image-run-id')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--sdk', type=Path)
    parser.add_argument('--record', type=Path)
    parser.add_argument('--receipts', type=Path, help='directory of a fetched candidate (`fetch --output`)')
    parser.add_argument('--metadata-dir', type=Path)
    parser.add_argument('--repository')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    try:
        if args.command == 'sdk':
            prepare_sdk(args.sdk_run_id, args.platform, args.output)
        elif args.command == 'record':
            record_images(args.output, args.platform, args.sdk, args.metadata_dir, args.repository)
        elif args.command == 'executors':
            check_executors(args.record, args.output, args.platform, args.sdk_run_id, args.sdk)
        elif args.command == 'fetch':
            fetch_images(args.image_run_id, args.platform, args.sdk_run_id, args.output)
        elif args.command == 'check':
            check_publication(args.receipts, args.platform, args.sdk_run_id, sdk=args.sdk)
        else:
            promote(args.receipts, args.platform, args.sdk_run_id, dry_run=args.dry_run, sdk=args.sdk)
    except MissingImages as error:
        parser.exit(2, str(error) + '\n')
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f'Docker candidate: {error}\n')


if __name__ == '__main__':
    main()
