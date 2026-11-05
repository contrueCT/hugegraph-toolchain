# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prepare official SF1 (default), SF10 or SF100 and its five-query reference.

Reuse the archive inspector/converter and SQLite oracle. Only Person/knows CSV
is copied during the existing inventory pass. No database is started or scanned.
"""
import argparse
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools' / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


assets = module('graph_dataset_assets', 'ldbc-assets.py')
converter = module('graph_dataset_converter', 'ldbc-prepare-archive.py')
reference = module('graph_dataset_reference', 'graph-reference.py')


def read(path):
    return json.loads(Path(path).read_text())


def prepare(args):
    catalog = read(ROOT / 'benchmarks/graph-query-v0.1/datasets.json')
    selected = next(d for d in catalog['datasets'] if d['scaleFactor'] == args.scale)
    limits = selected['limits']
    seconds = args.max_seconds or limits['seconds']
    workspace = args.max_bytes or limits['workspaceBytes']
    reserve = limits['reserveBytes'] if args.reserve_free_bytes is None else args.reserve_free_bytes
    download = args.max_download_bytes or limits['downloadBytes']
    if seconds < 1 or workspace < 4096 or reserve < 0 or download < 1:
        raise ValueError('Invalid preparation budget')
    out, cache = Path(args.out).resolve(), Path(args.cache or '.hgbench/graph-cache/sf' + str(args.scale)).resolve()
    if out == cache or out in cache.parents or cache in out.parents:
        raise ValueError('Keep the download cache and prepared output in separate directories')
    identity = {'scaleFactor': args.scale, 'descriptor': selected, 'seconds': seconds,
                'workspaceBytes': workspace, 'reserveBytes': reserve, 'downloadBytes': download,
                'seed': args.seed, 'poolSize': args.pool_size,
                'implementation': {name: reference.digest(ROOT / 'tools' / name) for name in
                    ['graph-dataset.py', 'graph-reference.py', 'ldbc-assets.py',
                     'ldbc-archive.py', 'ldbc-prepare-archive.py', 'ldbc-prepare.py', 'ldbc-shards.py']}}
    if args.dry_run:
        return {'executed': False, 'out': str(out), 'cache': str(cache), **identity,
                'stages': ['download-initial', 'inventory-and-reference-projection', 'convert', 'reference'],
                'targetRequests': 0, 'verification': 'source digest, row counts and endpoints; no target scan',
                'budgetMeaning': 'Ceilings, not estimated use. No expanded full CSV copy or update stream download.'}
    existed = out.exists()
    if existed and not args.resume:
        raise ValueError('Output exists; use --resume for the same preparation or select a new directory')
    out.mkdir(parents=True, exist_ok=True)
    with (out / '.prepare.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state_path = out / 'prepare-state.json'
        if existed:
            previous = read(state_path)
            same = previous['identity'] == identity
            if (out / 'dataset.json').exists():
                same = ({k: v for k, v in previous['identity'].items() if k != 'implementation'} ==
                        {k: v for k, v in identity.items() if k != 'implementation'})
            if not same:
                raise ValueError('Preparation options/source changed; use a new output directory')
        end = time.monotonic() + seconds
        def remaining():
            value = int(end - time.monotonic())
            if value < 1:
                raise TimeoutError('Whole preparation deadline reached')
            return value
        next_disk = 0
        def guard():
            nonlocal next_disk
            remaining()
            if time.monotonic() >= next_disk:
                if shutil.disk_usage(out).free < reserve:
                    raise ValueError('Filesystem free-space reserve reached')
                next_disk = time.monotonic() + .2
        def available():
            size = 0
            for path in out.rglob('*'):
                if path.is_symlink():
                    raise ValueError('Symlink in prepared workspace')
                if path.is_file():
                    size += path.stat().st_size
            if workspace - size < 65536:
                raise ValueError('Prepared workspace byte ceiling reached')
            return workspace - size - 65536
        def stage(name, **fields):
            converter.shards.atomic_json(state_path, {'schemaVersion': 1, 'stage': name,
                                                     'identity': identity, **fields})
            print(json.dumps({'stage': name, 'scaleFactor': args.scale, **fields}), file=sys.stderr, flush=True)
        complete = out / 'dataset.json'
        if complete.exists():
            result = read(complete)
            for name, fact in result['artifacts'].items():
                if reference.digest(out / name) != fact:
                    raise ValueError('Prepared metadata changed: ' + name)
            return {**result, 'reused': True}
        stage('download')
        frozen_path = out / 'catalog.json'
        if not frozen_path.exists():
            observed = cache / 'asset.json'
            asset = read(observed) if observed.exists() else selected['asset']
            if {k: asset[k] for k in ('id', 'role', 'scale', 'url')} != {
                    k: selected['asset'][k] for k in ('id', 'role', 'scale', 'url')}:
                raise ValueError('Cached asset descriptor differs')
            if selected['asset']['sha256'] and asset['sha256'] != selected['asset']['sha256']:
                raise ValueError('Cached asset differs from the published lock')
            transfer = assets.TransferLimits(cache, max(download + 1024**2, download * 2),
                                            max(1, reserve), remaining(), 20 * 1024**2)
            asset = assets.acquire_resumable(asset, cache, download, capture=not bool(asset['sha256']),
                                            limits=transfer, stream_hash=True)
            converter.shards.atomic_json(observed, asset)
            frozen = {k: v for k, v in catalog.items() if k not in ('datasets', 'defaultScale')}
            frozen.update(id='ldbc-snb-interactive-v1-official', assets=[asset])
            converter.shards.atomic_json(frozen_path, frozen)
        frozen = assets.catalog(frozen_path)
        asset = frozen['assets'][0]
        stage('inventory')
        inventory_path = out / 'inventory.json'
        projection = out / 'reference-csv'
        if not inventory_path.exists():
            if projection.is_symlink():
                raise ValueError('Projection directory must not be a symlink')
            if projection.exists():
                shutil.rmtree(projection)
            projection.mkdir()
            assets.inspect_asset(asset, cache, inventory_path, limits['expandedBytes'], projection=projection,
                                 projection_limit=workspace // 4, check=guard)
        inventory = read(inventory_path)
        files = [{**r, 'path': 'dynamic/' + Path(r['path']).name} for r in inventory['files']
                 if Path(r['path']).parent.name == 'dynamic' and
                 reference.re.fullmatch(r'(person|person_knows_person)_\d+_\d+\.csv', Path(r['path']).name)]
        projection_manifest = out / 'reference-csv.json'
        converter.shards.atomic_json(projection_manifest, {'inputAsset': asset, 'files': files})
        stage('convert', vertices=inventory['vertices'], edges=inventory['edges'])
        converted = out / 'prepared'
        prepared_path = converted / 'prepare-manifest.json'
        if not prepared_path.exists():
            converter.convert(frozen_path, inventory_path, cache, converted,
                shard_rows=500000, shard_bytes=64 * 1024**2,
                max_data_bytes=workspace * 4, max_stored_bytes=workspace,
                max_index_bytes=workspace // 2, max_total_bytes=min(available(), workspace * 3 // 4),
                reserve_free_bytes=reserve, max_seconds=remaining(), compression='gzip', resume=converted.exists())
        stage('reference')
        index = out / 'reference-index'
        common = {'max_seconds': remaining(), 'max_bytes': available(), 'min_free_bytes': reserve}
        if not (index / 'index.json').exists():
            if index.is_symlink():
                raise ValueError('Reference index directory must not be a symlink')
            if index.exists():
                shutil.rmtree(index)
            reference.index(argparse.Namespace(out=str(index), manifest=str(projection_manifest),
                hg_manifest=str(prepared_path), data=str(projection), **common))
        reference_file = out / 'reference.json'
        if not reference_file.exists():
            reference.export(argparse.Namespace(index=str(index), out=str(reference_file), seed=args.seed,
                pool_size=args.pool_size, max_visited=1000000,
                **{**common, 'max_seconds': remaining(), 'max_bytes': available()}))
        stage('complete')
        result = {'schemaVersion': 1, 'kind': 'graph-dataset', 'scaleFactor': args.scale,
                  'prepared': str(prepared_path.relative_to(out)), 'reference': 'reference.json',
                  'source': asset, 'vertices': inventory['vertices'], 'edges': inventory['edges'],
                  'artifacts': {name: reference.digest(out / name) for name in
                                ['catalog.json', 'inventory.json', 'prepared/prepare-manifest.json', 'reference.json']},
                  'targetImported': False, 'performanceAccepted': False}
        converter.shards.atomic_json(complete, result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scale', type=int, choices=(1, 10, 100), default=1)
    parser.add_argument('--out', required=True)
    parser.add_argument('--cache')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--max-seconds', type=int)
    parser.add_argument('--max-bytes', type=int)
    parser.add_argument('--max-download-bytes', type=int)
    parser.add_argument('--reserve-free-bytes', type=int)
    parser.add_argument('--seed', type=int, default=3346)
    parser.add_argument('--pool-size', type=int, default=128)
    args = parser.parse_args()
    if not (2 <= args.pool_size <= 100000 and 0 <= args.seed <= 0xffffffff):
        parser.error('Invalid seed or pool size')
    def cancelled(signum, frame):
        raise TimeoutError('Preparation cancelled or deadline reached; completed stages retained')
    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGALRM, cancelled)
    signal.alarm(args.max_seconds or {1: 7200, 10: 21600, 100: 172800}[args.scale])
    try:
        print(json.dumps(prepare(args)))
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
