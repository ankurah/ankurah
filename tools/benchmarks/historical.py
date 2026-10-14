"""Audited benchmark surface adapter: no production algorithm changes."""
import hashlib
import subprocess
import tomllib
from pathlib import Path

ORIGINAL_HARNESS = '893721a97551e379b2db6c45de472cfa0b99b04f'
LEGACY_HARNESS = 'ebdb1727254c675a4d37236ecf23c16c06c4df12'
VERSION = 'historical-dag-surface-v1'


class AdapterUnavailable(ValueError):
    """The selected workload primitive is absent, rather than a build outage."""



def file_at(root, revision, path):
    result = subprocess.run(['git', 'show', f'{revision}:{path}'], cwd=root, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return result.stdout if result.returncode == 0 else None


def select_harness(root, revision):
    bench = file_at(root, revision, 'core/benches/event_dag.rs')
    main_history = subprocess.run(['git', 'merge-base', '--is-ancestor', revision, 'HEAD'], cwd=root).returncode == 0
    if main_history and bench and b'fn linear_deep' in bench:
        return revision
    data = file_at(root, revision, 'proto/src/data.rs') or b''
    return ORIGINAL_HARNESS if b'pub struct Event {' in data and b'EventBody' not in data else LEGACY_HARNESS


def apply_surface(source, support):
    source = Path(source)
    manifest = source / 'core/Cargo.toml'
    parsed = tomllib.loads(manifest.read_text())
    if 'bench-internals' in parsed.get('features', {}) and (source / 'core/src/bench_support.rs').exists():
        return None
    if not (source / 'core/src/event_dag/comparison.rs').exists():
        raise AdapterUnavailable('No audited adapter: the event_dag comparison primitive does not exist in this source; earlier algorithms require a separate workload contract')
    original_lock = (source / 'Cargo.lock').read_bytes()
    before = {path: (source / path).read_text() for path in ('core/Cargo.toml', 'core/src/lib.rs', 'core/src/event_dag/relation.rs')}
    support = support.replace(b'pub use crate::event_dag::DEFAULT_BUDGET;', b'pub const DEFAULT_BUDGET: usize = crate::event_dag::DEFAULT_BUDGET;')
    if b'pub(crate) struct EventLayers' in (source / 'core/src/event_dag/layers.rs').read_bytes():
        support = support.replace(b'use crate::event_dag::accumulator::{ComparisonResult, EventLayers};',
                                  b'use crate::event_dag::accumulator::ComparisonResult;\nuse crate::event_dag::layers::EventLayers;')
    (source / 'core/src/bench_support.rs').write_bytes(support)
    lib = source / 'core/src/lib.rs'
    if 'pub mod bench_support;' not in lib.read_text():
        lib.write_text(lib.read_text() + '\n#[cfg(feature = "bench-internals")]\npub mod bench_support;\n')
    relation = source / 'core/src/event_dag/relation.rs'
    relation.write_text(relation.read_text().replace('pub(crate) enum AbstractCausalRelation', 'pub enum AbstractCausalRelation'))
    text = manifest.read_text()
    if 'bench-internals' not in parsed.get('features', {}):
        text = text.replace('[features]', '[features]\nbench-internals = []', 1)
    if 'criterion' not in parsed.get('dev-dependencies', {}):
        text = text.replace('[dev-dependencies]', '[dev-dependencies]\ncriterion = { version = "=0.5.1", features = ["async_tokio"] }', 1)
    if not any(bench.get('name') == 'event_dag' for bench in parsed.get('bench', [])):
        text += '\n[[bench]]\nname = "event_dag"\nharness = false\n'
    manifest.write_text(text)
    return {'version': VERSION, 'before': before,
            'after': {path: (source / path).read_text() for path in before},
            'overlays': {'core/src/bench_support.rs': support.decode()},
            'original_lockfile_sha256': hashlib.sha256(original_lock).hexdigest()}


def assert_lock_preserved(original, effective):
    def packages(value):
        return {(p['name'], p['version'], p.get('source')) for p in tomllib.loads(value.decode())['package']}
    removed = packages(original) - packages(effective)
    if removed:
        raise ValueError(f'Instrument dependency resolution changed original locked packages: {sorted(removed)}')
