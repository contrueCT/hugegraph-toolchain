#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#/

"""Shared, bounded transport and identity checks for frozen LDBC archives.

Graph mappings and reference query semantics stay in their respective components.
"""
from contextlib import contextmanager
import hashlib
from pathlib import Path, PurePosixPath
import re
import subprocess
import threading

def member_path(value):
    path = PurePosixPath(value)
    if path.is_absolute() or '..' in path.parts or not path.parts or '\\' in value or '\x00' in value:
        raise ValueError('Unsafe archive member path')
    return path


def table_of(path):
    if path.parent.name not in ('static', 'dynamic') or path.suffix != '.csv':
        return None
    match = re.fullmatch(r'(.+)_\d+_\d+\.csv', path.name)
    if not match:
        raise ValueError('Unknown graph table filename: ' + str(path))
    return match[1]


def embedded_updates(members):
    """Identify Datagen update CSVs co-packaged beside static/dynamic initial tables.

    These remain fully hashed input members, but are not initial graph rows or the
    selected catalog updates asset. In particular, equal row counts do not prove equality.
    """
    roots = {member_path(name).parent.parent for name in members
             if member_path(name).parent.name in ('static', 'dynamic') and member_path(name).suffix == '.csv'}
    if len(roots) != 1:
        raise ValueError('Expected one initial graph archive root')
    root = next(iter(roots))
    return {name: entry for name, entry in members.items() if member_path(name).parent == root and
            re.fullmatch(r'updateStream_\d+_\d+_(person|forum)\.csv', member_path(name).name)}


@contextmanager
def archive_stream(path, asset, budget):
    """The exact bytes fed to zstd are also hashed; no full pre-read or expanded copies."""
    if path.is_symlink() or not path.is_file() or path.stat().st_size != asset['bytes']:
        raise ValueError('Cached archive missing or size drift')
    digest, count, errors = hashlib.sha256(), 0, []
    with path.open('rb') as source:
        decoder = subprocess.Popen(['zstd', '-dc'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL)
        def feed():
            nonlocal count
            try:
                while True:
                    raw = source.read(1024 * 1024)
                    if not raw:
                        break
                    budget.check()
                    count += len(raw)
                    if count > asset['bytes']:
                        raise ValueError('Compressed archive grew')
                    digest.update(raw)
                    decoder.stdin.write(raw)
            except Exception as error:
                errors.append(error)
                decoder.kill()
            finally:
                try:
                    decoder.stdin.close()
                except BrokenPipeError:
                    pass
        worker = threading.Thread(target=feed, daemon=True)
        worker.start()
        try:
            yield decoder.stdout
            # Exhaust the compressed stream and force the decoder checksum check.
            extra = 0
            while True:
                raw = decoder.stdout.read(65536)
                if not raw:
                    break
                extra += len(raw)
                if extra > 1024 * 1024 or raw.strip(b'\0'):
                    raise ValueError('Unexpected trailing archive content')
            code = decoder.wait(timeout=30)
            worker.join(timeout=5)
            if worker.is_alive() or errors or code or count != asset['bytes'] or digest.hexdigest() != asset['sha256']:
                raise ValueError('Compressed archive identity or decoder verification failed')
        finally:
            if decoder.poll() is None:
                decoder.kill()
                decoder.wait(timeout=5)
            decoder.stdout.close()
            worker.join(timeout=5)


def validated_inputs(catalog, inventory):
    if (catalog.get('serializer'), catalog.get('dateFormatter')) != ('CsvBasic', 'LongDateFormatter'):
        raise ValueError('Archive input requires CsvBasic/LongDateFormatter')
    initial = [a for a in catalog['assets'] if a['role'] == 'initial']
    if len(initial) != 1 or initial[0] != inventory.get('asset'):
        raise ValueError('Inventory does not match the frozen initial asset')
    asset = initial[0]
    if (not re.fullmatch('[0-9a-f]{64}', asset.get('sha256') or '') or
            type(asset.get('bytes')) is not int or asset['bytes'] <= 0):
        raise ValueError('Asset identity is not frozen')
    if inventory.get('archiveVerification') != 'same-stream-sha256-and-length-before-publication':
        raise ValueError('Verified archive inventory required')
    members = {}
    for entry in inventory['files']:
        name = str(member_path(entry['path']))
        if (name in members or type(entry.get('bytes')) is not int or entry['bytes'] < 0 or
                not re.fullmatch('[0-9a-f]{64}', entry['sha256'])):
            raise ValueError('Invalid or duplicate inventory member')
        members[name] = entry
    if not members or len(members) > 100000:
        raise ValueError('Inventory member count outside limit')
    return asset, members
