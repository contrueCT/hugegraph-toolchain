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

"""Manage immutable LDBC assets: lock, resume, verify, inspect and extract with explicit byte budgets."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.parse
import urllib.request

DEFAULT_CATALOG = Path(__file__).resolve().parent.parent / "benchmarks/ldbc-snb-interactive-v1/candidates/sf100.json"


def digest_file(path, check=None, progress=None):
    digest, size = hashlib.sha256(), 0
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            if check:
                check()
            digest.update(chunk)
            size += len(chunk)
            if progress:
                progress(size)
    return digest.hexdigest(), size


def verification_progress(asset):
    started, previous = time.monotonic(), time.monotonic() - 30
    print(json.dumps({"event": "archive-verification-started", "asset": asset["id"],
                      "expectedBytes": asset.get("bytes")}), file=sys.stderr, flush=True)

    def report(count):
        nonlocal previous
        now = time.monotonic()
        if now - previous >= 30:
            print(json.dumps({"event": "archive-verification-progress", "asset": asset["id"],
                              "bytesRead": count, "elapsedSeconds": round(now - started, 3)}),
                  file=sys.stderr, flush=True)
            previous = now
    return report


class TransferLimits:
    """Invocation-wide budgets, including existing objects, partials and hash verification.

    Disk space is checked every 200 ms during transfer. This is a stop guard, not a filesystem quota;
    another process can consume space between observations. Curl also limits each asset and bandwidth.
    """

    def __init__(self, cache, max_cache_bytes, reserve_free_bytes, timeout_seconds, rate_bytes):
        if any(type(v) is not int or v <= 0 for v in
               (max_cache_bytes, reserve_free_bytes, timeout_seconds, rate_bytes)):
            raise ValueError("All transfer limits must be positive integers")
        self.cache = Path(cache)
        self.max_cache_bytes = max_cache_bytes
        self.reserve_free_bytes = reserve_free_bytes
        self.deadline = time.monotonic() + timeout_seconds
        self.rate_bytes = rate_bytes
        self.next_disk_check = 0

    def check(self):
        now = time.monotonic()
        if now >= self.deadline:
            raise ValueError("Transfer invocation wall-clock budget exceeded; resumable data retained")
        if now < self.next_disk_check:
            return
        self.next_disk_check = now + 0.2
        if self.cache.is_symlink():
            raise ValueError("Cache root must not be a symlink")
        existing = self.cache
        while not existing.exists():
            existing = existing.parent
        if shutil.disk_usage(existing).free < self.reserve_free_bytes:
            raise ValueError("Filesystem free-space reserve reached; resumable data retained")
        total, seen, entries = 0, set(), 0
        for parent, directories, files in os.walk(self.cache, followlinks=False):
            for name in directories + files:
                entries += 1
                if entries > 10000:
                    raise ValueError("Transfer cache contains too many entries")
                info = (Path(parent) / name).lstat()
                if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                    raise ValueError("Transfer cache contains a link or special file")
                identity = (info.st_dev, info.st_ino)
                if stat.S_ISREG(info.st_mode) and identity not in seen:
                    total += info.st_size
                    seen.add(identity)
        if total > self.max_cache_bytes:
            raise ValueError("Combined transfer cache byte budget exceeded; resumable data retained")


def official_url(url):
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname != "datasets.ldbcouncil.org" or
            parsed.username or parsed.password or parsed.port not in (None, 443) or parsed.fragment):
        raise ValueError("Assets must use the official HTTPS dataset host")
    return url


class OfficialRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return super().redirect_request(request, fp, code, msg, headers, official_url(newurl))


def catalog(path, locked=True):
    data = json.loads(Path(path).read_text())
    if data.get("schemaVersion") != 1 or data.get("workload") != "SNB Interactive v1":
        raise ValueError("Unsupported workload catalog")
    ids = set()
    for asset in data["assets"]:
        if not re.fullmatch(r"[a-z0-9.-]+", asset["id"]) or asset["id"] in ids:
            raise ValueError("Invalid or duplicate asset ID")
        ids.add(asset["id"])
        official_url(asset["url"])
        if asset["role"] not in ("initial", "parameters", "updates", "validation"):
            raise ValueError("Unknown asset role")
        if locked and (not re.fullmatch(r"[0-9a-f]{64}", str(asset.get("sha256"))) or
                       type(asset.get("bytes")) is not int or asset["bytes"] <= 0):
            raise ValueError("Unfrozen asset; explicitly capture it before normal use")
    return data


def verified_path(asset, cache, check=None, progress=None):
    path = Path(cache) / (asset["sha256"] + ".tar.zst")
    if path.is_symlink() or not path.is_file():
        raise ValueError("Cached asset missing or not a regular file: " + asset["id"])
    if digest_file(path, check, progress) != (asset["sha256"], asset["bytes"]):
        raise ValueError("Cached content drift: " + asset["id"])
    return path


def acquire(asset, cache, max_bytes, capture=False):
    cache = Path(cache)
    cache.mkdir(parents=True, exist_ok=True)
    if not capture:
        cached = cache / (asset["sha256"] + ".tar.zst")
        if cached.exists() or cached.is_symlink():
            verified_path(asset, cache)
            return asset
        if asset["bytes"] > max_bytes:
            raise ValueError("Asset exceeds the explicit download budget")
    fd, temporary = tempfile.mkstemp(prefix=".partial-", dir=cache)
    try:
        opener = urllib.request.build_opener(OfficialRedirect())
        request = urllib.request.Request(official_url(asset["url"]), headers={"User-Agent": "HG-Bench/0.1"})
        with os.fdopen(fd, "wb") as output, opener.open(request, timeout=30) as response:
            if response.status != 200:
                raise ValueError("Expected a complete HTTP 200 response")
            claimed = response.headers.get("Content-Length")
            if claimed is not None and int(claimed) > max_bytes:
                raise ValueError("Asset exceeds the explicit download budget")
            count = 0
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                count += len(chunk)
                if count > max_bytes:
                    raise ValueError("Download exceeded its byte budget")
                output.write(chunk)
        sha, size = digest_file(temporary)
        if claimed is not None and size != int(claimed):
            raise ValueError("Truncated HTTP response")
        if not capture and (sha != asset["sha256"] or size != asset["bytes"]):
            raise ValueError("Upstream content drift; refusing to replace frozen asset")
        result = {**asset, "sha256": sha, "bytes": size}
        if capture:
            result["checksumAuthority"] = "locally-observed-from-official-https-not-publisher-signed"
        target = cache / (sha + ".tar.zst")
        try:
            os.link(temporary, target)
        except FileExistsError:
            verified_path(result, cache)
        return result
    finally:
        Path(temporary).unlink(missing_ok=True)


def resume_request(asset, partial, etag_file, max_bytes):
    """Use curl's Range implementation; If-Match prevents concatenating different upstream versions."""
    if partial.is_symlink() or etag_file.is_symlink():
        raise ValueError("Download state must not contain symlinks")
    headers = []
    if partial.exists() and partial.stat().st_size:
        if partial.stat().st_size > max_bytes:
            raise ValueError("Partial download exceeds its byte budget")
        etag = etag_file.read_text().strip() if etag_file.exists() else ""
        if not re.fullmatch(r'"[^"\r\n]+"', etag):
            raise ValueError("Resume requires a recorded strong ETag; use a fresh cache for this asset")
        headers = ["--header", "If-Match: " + etag]
    return ["curl", "--http1.1", "--fail", "--silent", "--show-error", "--connect-timeout", "20",
            "--retry", "3", "--retry-delay", "2", "--proto", "=https", "--max-filesize", str(max_bytes),
            "--continue-at", "-", "--etag-save", str(etag_file) + ".incoming", *headers,
            "--write-out", "%{http_code}",
            "--output", str(partial), official_url(asset["url"])]


def transfer_with_digest(asset, partial, etag, max_bytes, limits):
    """Hash an existing prefix, then hash incoming curl bytes while appending; no final reread.

    Curl's explicit -C offset rejects a server that ignores Range. Internal curl retries are disabled:
    an interrupted stdout stream cannot rewind the file or hash state. The next invocation resumes it.
    """
    command = resume_request(asset, partial, etag, max_bytes)
    prefix = partial.stat().st_size if partial.exists() else 0
    previous_etag = etag.read_text().strip() if prefix else None
    digest, count = hashlib.sha256(), 0
    if prefix:
        progress = verification_progress({**asset, "bytes": prefix})
        with partial.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                if limits:
                    limits.check()
                digest.update(chunk)
                count += len(chunk)
                progress(count)
        if count != prefix:
            raise ValueError("Resume prefix changed during hashing")
    command[command.index("--continue-at") + 1] = str(prefix)
    command[command.index("--output") + 1] = "-"
    command[command.index("--retry") + 1] = "0"
    command[command.index("--write-out") + 1] = "%{stderr}HGBENCH_HTTP_STATUS:%{http_code}\n"
    command[1:1] = ["--speed-limit", "1", "--speed-time", "30"]
    if limits:
        command[1:1] = ["--limit-rate", str(limits.rate_bytes)]
    process = None
    descriptor, diagnostic_name = tempfile.mkstemp(prefix=partial.name + ".", suffix=".curl.log", dir=partial.parent)
    diagnostics = Path(diagnostic_name)
    try:
        with os.fdopen(descriptor, "wb") as error_log, partial.open("ab") as destination:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=error_log)
            os.set_blocking(process.stdout.fileno(), False)
            previous = time.monotonic() - 30
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while True:
                    if limits:
                        limits.check()
                    if diagnostics.stat().st_size > 65536:
                        raise ValueError("Curl diagnostics exceeded budget")
                    if not selector.select(timeout=0.2):
                        continue
                    chunk = os.read(process.stdout.fileno(), 1024 * 1024)
                    if not chunk:
                        break
                    if count + len(chunk) > max_bytes:
                        raise ValueError("Download exceeded total asset byte budget")
                    destination.write(chunk)
                    digest.update(chunk)
                    count += len(chunk)
                    now = time.monotonic()
                    if now - previous >= 30:
                        print(json.dumps({"event": "download-and-hash-progress", "asset": asset["id"],
                                          "bytes": count, "resumedFromBytes": prefix}), file=sys.stderr, flush=True)
                        previous = now
            destination.flush()
            code = process.wait(timeout=10)
        incoming = Path(str(etag) + ".incoming")
        observed_etag = incoming.read_text().strip() if incoming.exists() else ""
        # Do not retain a suffix from a different object, even if an upstream ignores If-Match.
        if prefix and observed_etag != previous_etag:
            with partial.open("r+b") as destination:
                destination.truncate(prefix)
            raise ValueError("Resume response ETag changed or is missing; original prefix retained")
        if code:
            raise ValueError(f"curl exited {code}; partial retained at {partial}; see {diagnostics.name}")
        statuses = re.findall(r"^HGBENCH_HTTP_STATUS:([0-9]{3})$", diagnostics.read_text(), re.MULTILINE)
        expected_status = "206" if prefix else "200"
        if statuses != [expected_status]:
            with partial.open("r+b") as destination:
                destination.truncate(prefix)
            raise ValueError("Unexpected transfer HTTP status; original prefix retained")
        if count != partial.stat().st_size or not count:
            raise ValueError("Streamed file size mismatch")
        return digest.hexdigest(), count, expected_status
    finally:
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            process.stdout.close()


def acquire_resumable(asset, cache, max_bytes, capture=False, limits=None, stream_hash=False):
    if not shutil.which("curl"):
        raise ValueError("Resumable transfers require curl")
    cache = Path(cache)
    if limits:
        limits.check()
    cache.mkdir(parents=True, exist_ok=True)
    if not capture:
        target = cache / (asset["sha256"] + ".tar.zst")
        if target.exists() or target.is_symlink():
            verified_path(asset, cache, limits.check if limits else None, verification_progress(asset))
            return asset
        if asset["bytes"] > max_bytes:
            raise ValueError("Asset exceeds the explicit download budget")
    state = cache / "transfers"
    state.mkdir(exist_ok=True)
    key = hashlib.sha256((asset["url"] + str(asset.get("sha256"))).encode()).hexdigest()
    partial, etag = state / (key + ".part"), state / (key + ".etag")
    lease = state / (key + ".lock")
    lease.mkdir()  # One writer per object. A killed controller leaves an explicit lease to inspect.
    process = None
    try:
        resumed_from = partial.stat().st_size if partial.exists() else 0
        if stream_hash:
            sha, size, status = transfer_with_digest(asset, partial, etag, max_bytes, limits)
        else:
            command = resume_request(asset, partial, etag, max_bytes)
            if limits:
                command[1:1] = ["--limit-rate", str(limits.rate_bytes)]
            process = subprocess.Popen(command, stdout=subprocess.PIPE)
            previous = time.monotonic() - 30
            while process.poll() is None:
                if limits:
                    limits.check()
                now = time.monotonic()
                if now - previous >= 30:
                    print(json.dumps({"event": "download-progress", "asset": asset["id"],
                                      "bytes": partial.stat().st_size if partial.exists() else 0}),
                          file=sys.stderr, flush=True)
                    previous = now
                if partial.exists() and partial.stat().st_size > max_bytes:
                    process.terminate()
                    raise ValueError("Download exceeded its total byte budget")
                time.sleep(0.2)
            if process.returncode != 0:
                raise ValueError(f"curl exited {process.returncode}; partial retained at {partial}")
            status = process.stdout.read().decode().strip()
            if status != ("206" if resumed_from else "200"):
                raise ValueError("Expected HTTP 200 or 206; transfer is not promoted")
            sha, size = digest_file(partial, limits.check if limits else None, verification_progress(asset))
        if size == 0 or size > max_bytes:
            raise ValueError("Empty or oversized completed transfer")
        if not capture and (sha, size) != (asset["sha256"], asset["bytes"]):
            raise ValueError("Upstream content drift; retained transfer is not promoted")
        result = {**asset, "sha256": sha, "bytes": size}
        if capture:
            result["checksumAuthority"] = "locally-observed-from-official-https-not-publisher-signed"
        target = cache / (sha + ".tar.zst")
        with partial.open("rb") as completed:
            os.fsync(completed.fileno())
        try:
            os.link(partial, target)
        except FileExistsError:
            verified_path(result, cache, limits.check if limits else None)
        partial.unlink()
        directory_fd = os.open(cache, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        etag.unlink(missing_ok=True)
        print(json.dumps({"event": "download-completed", "asset": asset["id"], "bytes": size,
                          "httpStatus": int(status), "resumedFromBytes": resumed_from, "sha256": sha}),
              file=sys.stderr, flush=True)
        return result
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process is not None and process.stdout is not None:
            process.stdout.close()
        incoming = Path(str(etag) + ".incoming")
        if incoming.exists():
            value = incoming.read_text().strip()
            if partial.exists() and re.fullmatch(r'"[^"\r\n]+"', value):
                old = etag.read_text().strip() if etag.exists() else value
                if old == value:
                    os.replace(incoming, etag)
            incoming.unlink(missing_ok=True)
        lease.rmdir()


def capture_catalog(data, output, cache, max_bytes, downloader=acquire):
    output = Path(output)
    if output.exists():
        raise ValueError("Capture output must be new")
    pending = output.with_name(output.name + ".pending.json")
    if pending.exists():
        previous = catalog(pending, locked=False)
        def descriptors(value):
            return {**value, "assets": [{k: v for k, v in a.items()
                                         if k not in ("sha256", "bytes", "checksumAuthority")}
                                        for a in value["assets"]]}
        if descriptors(previous) != descriptors(data):
            raise ValueError("Pending capture does not match this source catalog")
        for original, completed in zip(data["assets"], previous["assets"]):
            if original.get("sha256") and original != completed:
                raise ValueError("Pending capture cannot replace a frozen asset")
        data = previous
    for index, asset in enumerate(data["assets"]):
        data["assets"][index] = downloader(asset, cache, max_bytes, capture=asset.get("sha256") is None)
        temp = pending.with_name(pending.name + ".tmp")
        with temp.open("w") as stream:
            json.dump(data, stream, indent=2)
            stream.write("\n")
        os.replace(temp, pending)
    with output.open("x") as stream:
        json.dump(data, stream, indent=2)
        stream.write("\n")
    pending.unlink()
    return {"captured": len(data["assets"]), "lock": str(output), "reviewRequired": True}


def merge_captures(data, receipts, cache, output):
    """Merge previously captured identities without rereading multi-GB files; inspection still verifies content."""
    expected = {a["id"]: a for a in data["assets"]}
    found, provenance = {}, []

    def descriptors(asset):
        return {k: v for k, v in asset.items() if k not in ("sha256", "bytes", "checksumAuthority")}

    for file in receipts:
        raw = Path(file).read_bytes()
        receipt = catalog(file, locked=False)
        if {**receipt, "assets": []} != {**data, "assets": []}:
            raise ValueError("Capture receipt belongs to another catalog")
        provenance.append({"path": str(file), "sha256": hashlib.sha256(raw).hexdigest()})
        for asset in receipt["assets"]:
            original = expected.get(asset["id"])
            if original is None or descriptors(asset) != descriptors(original):
                raise ValueError("Capture receipt source differs from requested catalog")
            if asset.get("sha256") is None and asset.get("bytes") is None:
                continue
            if (not re.fullmatch(r"[0-9a-f]{64}", str(asset.get("sha256"))) or
                    type(asset.get("bytes")) is not int or asset["bytes"] <= 0):
                raise ValueError("Capture receipt has an invalid content identity")
            if original.get("sha256") and asset != original:
                raise ValueError("Cannot replace an already frozen identity")
            if asset["id"] in found and found[asset["id"]] != asset:
                raise ValueError("Conflicting capture receipts")
            path = Path(cache) / (asset["sha256"] + ".tar.zst")
            if path.is_symlink() or not path.is_file() or path.stat().st_size != asset["bytes"]:
                raise ValueError("Captured file missing or size changed")
            found[asset["id"]] = asset
    if set(found) != set(expected):
        raise ValueError("Capture receipts do not cover every requested asset")
    result = {**data, "assets": [found[a["id"]] for a in data["assets"]]}
    with Path(output).open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    return {"lock": str(output), "captured": len(found), "receipts": provenance,
            "verification": "metadata-and-file-size-only; content-hashes-from-prior-captures; use-inspect-to-reverify",
            "reviewRequired": True}


def safe_member(name):
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name or not path.parts or ":" in name:
        raise ValueError("Unsafe archive member: " + name)
    return path


def unpack_stream(stream, output, max_bytes, prefix=None, max_files=100000):
    output = Path(output)
    output.mkdir()  # Refuse an existing destination, including interrupted extraction.
    inventory, seen, total = [], set(), 0
    with tarfile.open(fileobj=stream, mode="r|") as archive:
        for entry in archive:
            if entry.name.rstrip("/") in ("", ".") and entry.isdir():
                continue
            path = safe_member(entry.name)
            if not entry.isfile() and not entry.isdir():
                raise ValueError("Links and special archive entries are not supported")
            if entry.isdir():
                continue
            if str(path) in seen:
                raise ValueError("Duplicate archive member")
            seen.add(str(path))
            if len(seen) > max_files:
                raise ValueError("Archive file count exceeded")
            # The budget includes skipped files, bounding decompression work as well as extracted disk bytes.
            total += entry.size
            if entry.size < 0 or total > max_bytes:
                raise ValueError("Expanded archive exceeds byte budget")
            if prefix and not str(path).startswith(prefix):
                continue
            target = output.joinpath(*path.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            digest, count = hashlib.sha256(), 0
            with archive.extractfile(entry) as source, target.open("xb") as destination:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    count += len(chunk)
                    digest.update(chunk)
                    destination.write(chunk)
            if count != entry.size:
                raise ValueError("Truncated archive member")
            inventory.append({"path": str(path), "bytes": count, "sha256": digest.hexdigest()})
    if not inventory:
        raise ValueError("No files selected for extraction")
    return inventory


def extract(asset, cache, output, max_bytes, prefix=None):
    archive = verified_path(asset, cache)
    if prefix:
        safe_member(prefix)
    process = subprocess.Popen(["zstd", "-dc", "--", str(archive)], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        inventory = unpack_stream(process.stdout, output, max_bytes, prefix)
        process.stdout.close()
        if process.wait(timeout=30) != 0:
            raise ValueError("Zstandard decompression failed")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    manifest = {"schemaVersion": 1, "asset": asset, "files": inventory}
    with (Path(output) / "extraction-manifest.json").open("x") as stream:
        json.dump(manifest, stream, indent=2)
        stream.write("\n")
    return {"asset": asset["id"], "files": len(inventory), "expandedBytes": sum(f["bytes"] for f in inventory)}


def inspect_stream(stream, max_bytes, max_files=100000):
    """Count unquoted CsvBasic rows and hash every tar member without materializing the expanded dataset."""
    entities = {"person", "post", "comment", "forum", "place", "organisation", "tag", "tagclass"}
    multi = {"person_email_emailaddress", "person_speaks_language"}
    inventory, tables, seen, total = [], {}, set(), 0
    # Scan through tar end markers so appended valid members cannot escape the inventory.
    with tarfile.open(fileobj=stream, mode="r|", ignore_zeros=True) as archive:
        for entry in archive:
            if entry.name.rstrip("/") in ("", ".") and entry.isdir():
                continue
            path = safe_member(entry.name)
            if not entry.isfile() and not entry.isdir():
                raise ValueError("Links and special archive entries are not supported")
            if entry.isdir():
                continue
            if str(path) in seen or len(seen) >= max_files:
                raise ValueError("Duplicate archive member or excessive file count")
            seen.add(str(path))
            total += entry.size
            if entry.size < 0 or total > max_bytes:
                raise ValueError("Expanded archive exceeds byte budget")
            digest, size, lines, last, header = hashlib.sha256(), 0, 0, b"", b""
            csv_file = path.suffix == ".csv" and not path.name.startswith("._")
            with archive.extractfile(entry) as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
                    size += len(chunk)
                    if csv_file:
                        lines += chunk.count(b"\n")
                        last = chunk[-1:]
                        if b"\n" not in header:
                            header += chunk.split(b"\n", 1)[0] + (b"\n" if b"\n" in chunk else b"")
                            if len(header) > 65536:
                                raise ValueError("Oversized CSV header/first row")
            if size != entry.size:
                raise ValueError("Truncated archive member")
            record = {"path": str(path), "bytes": size, "sha256": digest.hexdigest()}
            if csv_file:
                if last and last != b"\n":
                    lines += 1
                update = path.name.startswith("updateStream_")
                record["rows"] = lines if update else max(0, lines - 1)
                if not update:
                    record["columns"] = header.decode("utf-8").rstrip("\r\n").split("|")
                if path.parent.name in ("static", "dynamic"):
                    match = re.fullmatch(r"(.+)_\d+_\d+\.csv", path.name)
                    if not match:
                        raise ValueError("Unknown graph table filename")
                    table = match[1]
                    if table in entities:
                        kind = "vertex"
                        if not record["columns"] or record["columns"][0] != "id":
                            raise ValueError("Vertex header mismatch")
                    elif table in multi:
                        kind = "multi-property"
                    else:
                        parts = table.split("_")
                        if len(parts) != 3 or parts[0] not in entities or parts[2] not in entities:
                            raise ValueError("Unknown relationship table")
                        kind = "edge"
                        if [c.lower() for c in record["columns"][:2]] != [parts[0] + ".id", parts[2] + ".id"]:
                            raise ValueError("Edge header mismatch")
                    aggregate = tables.setdefault(table, {"kind": kind, "rows": 0, "parts": 0,
                                                          "columns": record["columns"]})
                    if aggregate["columns"] != record["columns"]:
                        raise ValueError("Inconsistent headers across table partitions")
                    aggregate["rows"] += record["rows"]
                    aggregate["parts"] += 1
            inventory.append(record)
    return {"files": inventory, "expandedBytes": total, "tables": tables,
            "vertices": sum(t["rows"] for t in tables.values() if t["kind"] == "vertex"),
            "edges": sum(t["rows"] for t in tables.values() if t["kind"] == "edge"),
            "rowValidation": "header-and-line-count-only-not-field-or-endpoint-validation"}


def inspect_asset(asset, cache, output, max_bytes):
    archive = Path(cache) / (asset["sha256"] + ".tar.zst")
    if archive.is_symlink() or not archive.is_file() or archive.stat().st_size != asset["bytes"]:
        raise ValueError("Cached asset missing, not regular, or size differs from frozen catalog")
    output = Path(output)
    if output.exists():
        raise ValueError("Inspection report must be new")
    # Tee the same compressed bytes to SHA-256 and zstd. A separate pre-hash would read SF100 twice.
    # Publish nothing until both the input identity and every archive member have been verified.
    digest, count, errors = hashlib.sha256(), 0, []
    progress = verification_progress(asset)
    with archive.open("rb") as source:
        process = subprocess.Popen(["zstd", "-dc"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL)

        def feed():
            nonlocal count
            try:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    count += len(chunk)
                    if count > asset["bytes"]:
                        raise ValueError("Compressed asset grew beyond its frozen size")
                    digest.update(chunk)
                    process.stdin.write(chunk)
                    progress(count)
            except Exception as error:
                errors.append(error)
                process.kill()
            finally:
                try:
                    process.stdin.close()
                except BrokenPipeError:
                    pass

        worker = threading.Thread(target=feed, daemon=True)
        worker.start()
        try:
            report = inspect_stream(process.stdout, max_bytes)
            # Finish the decoder even when the tar reader stopped at a partial padding block.
            trailing = 0
            for chunk in iter(lambda: process.stdout.read(65536), b""):
                trailing += len(chunk)
                if trailing > 1024 * 1024 or chunk.strip(b"\0"):
                    raise ValueError("Unexpected trailing archive payload")
            process.stdout.close()
            code = process.wait(timeout=30)
            worker.join(timeout=5)
            if worker.is_alive():
                raise ValueError("Compressed input reader did not terminate")
            if errors:
                raise ValueError("Compressed input read failed") from errors[0]
            if code != 0:
                raise ValueError("Zstandard decompression failed")
            if (digest.hexdigest(), count) != (asset["sha256"], asset["bytes"]):
                raise ValueError("Cached content drift; inspection report not published")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            process.stdout.close()
            worker.join(timeout=5)
    result = {"schemaVersion": 1, "asset": asset, **report, "hugegraphImported": False,
              "archiveVerification": "same-stream-sha256-and-length-before-publication",
              "compressedBytesRead": count}
    with output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    return {"report": str(output), "vertices": report["vertices"], "edges": report["edges"],
            "expandedBytes": report["expandedBytes"], "files": len(report["files"])}


def main():
    def interrupt(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupt)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default=str(DEFAULT_CATALOG), help="Frozen catalog; default is the checked-in official SF100 lock (35.53 GB compressed; large reads/downloads require a reviewed budget)")
    parser.add_argument("--cache", default=".hgbench/ldbc-cache", help="Content-addressed local archive cache")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List asset provenance and sizes without downloading")
    fetch = commands.add_parser("fetch", help="Download missing frozen assets; verify existing cache; no implicit re-lock")
    fetch.add_argument("--id", action="append", help="Select asset ID; repeatable; default all")
    fetch.add_argument("--max-bytes", type=int, default=512 * 1024 * 1024, help="Maximum compressed bytes per asset")
    fetch.add_argument("--resumable", action="store_true", help="Use curl Range + If-Match; retain partial transfers")
    capture = commands.add_parser("capture", help="Maintainer: explicitly capture new upstream bytes into a NEW lock file")
    capture.add_argument("--out", required=True, help="New lock file; interrupted captures keep a sibling .pending.json")
    capture.add_argument("--id", action="append", help="Capture selected IDs into a partial catalog; combine with merge-captures")
    capture.add_argument("--max-bytes", type=int, default=512 * 1024 * 1024,
                         help="Maximum compressed bytes per asset (default: 536870912)")
    capture.add_argument("--resumable", action="store_true", help="Use curl with resumable transfers and progress on stderr")
    for command in (fetch, capture):
        command.add_argument("--stream-hash", action="store_true",
                             help="With --resumable: hash prefix then incoming bytes; avoid a full post-download reread")
        command.add_argument("--max-cache-bytes", type=int, help="Combined archives + partials limit; all four transfer limits required together")
        command.add_argument("--reserve-free-bytes", type=int, help="Filesystem free-space floor; checked every 200 ms, not a disk quota")
        command.add_argument("--timeout-seconds", type=int, help="Total invocation budget, including all assets and hashing")
        command.add_argument("--rate-bytes", type=int, help="Curl maximum transfer bytes/second; requires --resumable")
    merge = commands.add_parser("merge-captures", help="Metadata-only merge of capture receipts; no network or content rehash; inspect before use")
    merge.add_argument("--receipt", required=True, action="append", help="Completed or interrupted capture catalog; repeatable")
    merge.add_argument("--out", required=True, help="New complete catalog; requires all requested assets and exact file sizes")
    commands.add_parser("verify", help="Offline: verify every archive in the selected lock")
    unpack = commands.add_parser("extract", help="Verify then safely extract one archive into a NEW directory; requires zstd")
    unpack.add_argument("--id", required=True)
    unpack.add_argument("--out", required=True)
    unpack.add_argument("--prefix", help="Optional member path prefix; still enforces whole-archive expansion budget")
    unpack.add_argument("--max-bytes", type=int, default=8 * 1024 ** 3, help="Maximum expanded bytes across all archive members")
    inspect = commands.add_parser("inspect", help="Stream a verified archive: per-file hashes, headers and row counts; no expanded copy")
    inspect.add_argument("--id", required=True, help="Asset ID from the selected frozen catalog")
    inspect.add_argument("--out", required=True, help="New JSON report; does not write expanded data or import into HG")
    inspect.add_argument("--max-bytes", type=int, default=8 * 1024 ** 3,
                         help="Maximum total expanded bytes (default: 8589934592); SF100 needs an explicit larger budget")
    args = parser.parse_args()
    try:
        data = catalog(args.catalog, locked=args.command not in ("capture", "merge-captures"))
        downloader = acquire
        if args.command in ("fetch", "capture"):
            downloader = acquire_resumable if args.resumable else acquire
            limits = None
            settings = [args.max_cache_bytes, args.reserve_free_bytes, args.timeout_seconds, args.rate_bytes]
            if any(value is not None for value in settings):
                if not args.resumable or any(value is None for value in settings):
                    raise ValueError("Transfer limits require --resumable and all four limit flags")
                limits = TransferLimits(args.cache, *settings)
                limits.check()
                def downloader(asset, cache, max_bytes, capture=False):
                    return acquire_resumable(asset, cache, max_bytes, capture, limits)
            if args.stream_hash:
                if not args.resumable:
                    raise ValueError("--stream-hash requires --resumable")
                def downloader(asset, cache, max_bytes, capture=False):
                    return acquire_resumable(asset, cache, max_bytes, capture, limits, stream_hash=True)
        if args.command == "list":
            result = data
        elif args.command == "verify":
            for asset in data["assets"]:
                verified_path(asset, args.cache)
            result = {"verified": len(data["assets"]), "scope": "archive-integrity-only"}
        elif args.command == "capture":
            if args.id:
                selected = set(args.id)
                if not selected <= {a["id"] for a in data["assets"]}:
                    raise ValueError("Unknown asset ID")
                data = {**data, "assets": [a for a in data["assets"] if a["id"] in selected]}
            result = capture_catalog(data, args.out, args.cache, args.max_bytes, downloader)
        elif args.command == "merge-captures":
            result = merge_captures(data, args.receipt, args.cache, args.out)
        elif args.command == "fetch":
            wanted = set(args.id or [a["id"] for a in data["assets"]])
            if not wanted <= {a["id"] for a in data["assets"]}:
                raise ValueError("Unknown asset ID")
            result = {"assets": [downloader(a, args.cache, args.max_bytes)
                                 for a in data["assets"] if a["id"] in wanted]}
        else:
            asset = next((a for a in data["assets"] if a["id"] == args.id), None)
            if not asset:
                raise ValueError("Unknown asset ID")
            if args.command == "inspect":
                result = inspect_asset(asset, args.cache, args.out, args.max_bytes)
            else:
                result = extract(asset, args.cache, args.out, args.max_bytes, args.prefix)
        print(json.dumps({"ok": True, "data": result}, indent=2))
    except KeyboardInterrupt:
        print(json.dumps({"ok": False, "error": "interrupted", "resumableTransfersRetained": True}))
        raise SystemExit(130)
    except Exception as error:
        print(json.dumps({"ok": False, "error": str(error)}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
