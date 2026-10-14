"""Discover registered Cargo targets and Criterion workload IDs, without a UI list."""
import subprocess
import tomllib
from pathlib import Path


def registered(metadata, root, configured):
    root = Path(root).resolve()
    known = {(suite['package'], suite['bench']): suite for suite in configured}
    suites, targets = [], []
    for package in metadata['packages']:
        if package['id'] not in metadata['workspace_members']:
            continue
        manifest = tomllib.loads(Path(package['manifest_path']).read_text())
        definitions = {bench['name']: bench for bench in manifest.get('bench', [])}
        criterion = any(dep['name'] == 'criterion' for dep in package['dependencies'])
        for target in package['targets']:
            if 'bench' not in target['kind']:
                continue
            identifier = f"{package['name']}/{target['name']}"
            entry = {'id': identifier, 'package': package['name'], 'target': target['name'], 'status': 'registered', 'workloads': []}
            targets.append(entry)
            definition = definitions.get(target['name'], {})
            if not criterion or definition.get('harness', True):
                entry.update(status='unsupported', reason='Automatic timing supports explicitly registered Criterion targets with harness=false; this target needs an audited runner adapter')
                continue
            previous = known.get((package['name'], target['name']), {})
            suite = {'id': identifier, 'package': package['name'], 'bench': target['name'],
                     'features': target.get('required-features', previous.get('features', [])),
                     'file': str(Path(target['src_path']).resolve().relative_to(root)),
                     'filter': '', 'workloads': [], 'auto_discover': True,
                     'legacy_workloads': previous.get('workloads', [])}
            suites.append(suite)
    return sorted(suites, key=lambda s: s['id']), sorted(targets, key=lambda t: t['id'])


def workload_id(suite, original):
    return original if original in suite.get('legacy_workloads', []) else f"{suite['id']}/{original}"


def criterion_workloads(output, suite):
    # Criterion --list emits '<full_id>: benchmark'. Never guess from code or logs.
    originals = [line[:-11] for line in output.splitlines() if line.endswith(': benchmark')]
    if not originals or len(set(originals)) != len(originals):
        raise ValueError('Target returned no unique Criterion workload IDs from --list')
    if any(not value or len(value) > 500 or any(ord(char) < 32 for char in value) for value in originals):
        raise ValueError('Malformed Criterion workload ID')
    names = {workload_id(suite, name): name for name in originals}
    return names


def enumerate_criterion(executable, suite, cwd, env):
    try:
        result = subprocess.run([executable, "--list"], cwd=cwd, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
        if result.returncode:
            raise ValueError("Criterion --list failed: " + result.stdout[-1000:])
        return {"status": "discovered", "workload_names": criterion_workloads(result.stdout, suite)}
    except (ValueError, subprocess.TimeoutExpired) as error:
        return {"status": "failed", "reason": str(error)}
