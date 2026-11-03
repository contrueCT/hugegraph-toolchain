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

"""Bounded immutable JSONL shards. Recovery deterministically replays input and verifies existing shards."""

import gzip
import hashlib
import json
import os
from pathlib import Path


def digest(path):
    result, size = hashlib.sha256(), 0
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
            size += len(chunk)
    return result.hexdigest(), size


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".partial")
    if temporary.is_symlink() or path.is_symlink():
        raise ValueError("Symlink in conversion metadata")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ShardSet:
    def __init__(self, output, rows, shard_bytes, data_bytes, compression="none", stored_bytes=None):
        stored_bytes = data_bytes if stored_bytes is None else stored_bytes
        if min(rows, shard_bytes, data_bytes, stored_bytes) <= 0 or compression not in ("none", "gzip"):
            raise ValueError("Shard limits and data budget must be positive")
        self.output, self.rows, self.shard_bytes = Path(output), rows, shard_bytes
        self.data_bytes, self.total = data_bytes, 0
        self.stored_bytes, self.stored, self.compression = stored_bytes, 0, compression
        self.files = []
        self.reused = 0

    def writer(self, category, label):
        return ShardWriter(self, category, label)


class ShardWriter:
    def __init__(self, owner, category, label):
        self.owner, self.category, self.label = owner, category, label
        self.index, self.count, self.size = 0, 0, 0
        self.stream, self.path, self.partial = None, None, None
        self.sink, self.encoder = None, None
        self.hasher = hashlib.sha256()
        self.closed = False

    def begin(self):
        if self.owner.compression == "gzip" and self.owner.stored + 18 > self.owner.stored_bytes:
            raise ValueError("Stored shards exceed their explicit byte budget")
        suffix = ".json.gz" if self.owner.compression == "gzip" else ".json"
        self.path = self.owner.output / self.category / f"{self.label}-{self.index:06d}{suffix}"
        self.partial = self.path.with_name(self.path.name + ".partial")
        if self.path.is_symlink() or self.partial.is_symlink():
            raise ValueError("Symlink in shard output")
        if not self.path.exists():
            # An interrupted unsealed shard is rewritten; committed shards are never overwritten.
            self.stream = self.partial.open("wb")
        self.sink = StoredSink(self.owner, self.stream)
        if self.owner.compression == "gzip":
            # Stable gzip header; zlib runtime identity is included in conversion-state.json.
            self.encoder = gzip.GzipFile(filename="", fileobj=self.sink, mode="wb", compresslevel=1, mtime=0)
        self.hasher, self.count, self.size = hashlib.sha256(), 0, 0

    def write(self, line):
        if self.closed:
            raise ValueError("Shard writer closed")
        body = line.encode("utf-8")
        if len(body) > self.owner.shard_bytes:
            raise ValueError("A record exceeds the shard byte limit")
        if self.count and (self.count >= self.owner.rows or self.size + len(body) > self.owner.shard_bytes):
            self.seal()
        if self.owner.total + len(body) > self.owner.data_bytes:
            raise ValueError("Converted data exceeds its explicit byte budget")
        if self.path is None:
            self.begin()
        (self.encoder or self.sink).write(body)
        self.hasher.update(body)
        self.count += 1
        self.size += len(body)
        self.owner.total += len(body)

    def seal(self):
        if self.path is None:
            return
        try:
            if self.encoder:
                self.encoder.close()  # CRC/trailer writes count towards the stored-byte budget too.
                self.encoder = None
            expected = (self.sink.hasher.hexdigest(), self.sink.size)
            if self.stream:
                self.stream.flush()
                os.fsync(self.stream.fileno())
                self.stream.close()
                self.stream = None
                # Single-writer conversion lease plus exclusive publication; never replace another file.
                os.link(self.partial, self.path)
                self.partial.unlink()
                fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            else:
                if digest(self.path) != expected:
                    raise ValueError("Existing shard differs from deterministic replay: " + str(self.path))
                self.owner.reused += 1
        except BaseException:
            self.abort()
            raise
        encoding = {"encoding": {"compression": "gzip", "uncompressedBytes": str(self.size),
                                 "uncompressedSha256": self.hasher.hexdigest()}} if self.owner.compression == "gzip" else {}
        self.owner.files.append({"id": f"{self.category}-{self.label}-{self.index:06d}",
                                 "category": self.category, "label": self.label,
                                 "path": str(self.path.relative_to(self.owner.output)),
                                 "records": str(self.count), "bytes": str(expected[1]), "sha256": expected[0],
                                 **encoding})
        self.index += 1
        self.path, self.partial = None, None

    def close(self):
        self.seal()
        self.closed = True

    def abort(self):
        if self.sink:
            self.sink.discard = True
        if self.encoder:
            self.encoder.close()
            self.encoder = None
        if self.stream:
            self.stream.close()
            self.stream = None
        if self.partial and self.partial.exists():
            self.partial.unlink()
        self.closed = True


class StoredSink:
    """Hash compressed or plain bytes, including replay, without retaining them in memory."""
    def __init__(self, owner, stream):
        self.owner, self.stream = owner, stream
        self.hasher, self.size, self.discard = hashlib.sha256(), 0, False

    def write(self, raw):
        if self.discard:
            return len(raw)
        if self.owner.stored + len(raw) > self.owner.stored_bytes:
            raise ValueError("Stored shards exceed their explicit byte budget")
        if self.stream:
            self.stream.write(raw)
        self.hasher.update(raw)
        self.size += len(raw)
        self.owner.stored += len(raw)
        return len(raw)

    def flush(self):
        if self.stream:
            self.stream.flush()
