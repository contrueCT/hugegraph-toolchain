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

import hashlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
import tarfile
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parent.parent / "tools/ldbc-assets.py"
spec = importlib.util.spec_from_file_location("ldbc_assets", MODULE)
assets = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assets)


def archive(entries):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as tar:
        for name, body, kind in entries:
            entry = tarfile.TarInfo(name)
            entry.type = kind
            entry.size = len(body) if kind == tarfile.REGTYPE else 0
            if kind == tarfile.SYMTYPE:
                entry.linkname = "../escape"
            tar.addfile(entry, io.BytesIO(body))
    stream.seek(0)
    return stream


class AssetTests(unittest.TestCase):
    def test_default_catalog_is_frozen_sf100_and_list_has_no_data_io(self):
        data=assets.catalog(assets.DEFAULT_CATALOG)
        self.assertEqual({a['scale'] for a in data['assets']},{'100'})
        self.assertEqual({a['role'] for a in data['assets']},{'initial','parameters','updates'})
        result=subprocess.run(['python3',str(MODULE),'list'],check=True,capture_output=True,text=True)
        self.assertEqual(json.loads(result.stdout)['data'],data)

    def test_capture_receipts_merge_metadata_without_claiming_content_verification(self):
        data = {"schemaVersion": 1, "workload": "SNB Interactive v1", "assets": [
            {"id": name, "role": "initial", "url": f"https://datasets.ldbcouncil.org/{name}",
             "sha256": None, "bytes": None} for name in ["one", "two"]]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipts = []
            for a in data["assets"]:
                digest = hashlib.sha256(a["id"].encode()).hexdigest()
                (root / (digest + ".tar.zst")).write_bytes(a["id"].encode())
                path = root / (a["id"] + ".json")
                path.write_text(json.dumps({**data, "assets": [{**a, "sha256": digest, "bytes": len(a["id"])}]}))
                receipts.append(path)
            with self.assertRaisesRegex(ValueError, "every requested asset"):
                assets.merge_captures(data, receipts[:1], root, root / "incomplete.json")
            with patch.object(assets, "digest_file", side_effect=AssertionError("unexpected data reread")):
                result = assets.merge_captures(data, receipts, root, root / "merged.json")
            self.assertIn("metadata-and-file-size-only", result["verification"])
            self.assertEqual(len(result["receipts"]), 2)
            self.assertEqual([a["id"] for a in assets.catalog(root / "merged.json")["assets"]], ["one", "two"])
            changed = json.loads(receipts[0].read_text())
            changed["assets"][0]["url"] += "-changed"
            receipts[0].write_text(json.dumps(changed))
            with self.assertRaisesRegex(ValueError, "source differs"):
                assets.merge_captures(data, receipts, root, root / "invalid.json")
            self.assertFalse((root / "invalid.json").exists())

    @unittest.skipUnless(shutil.which("curl"), "curl is required for real Range/conditional transfer tests")
    def test_stream_hash_real_curl_resume_and_transport_failures(self):
        body = b"abcdef"
        requests = []
        mode = ["normal"]
        stall_gate = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                offset = int(self.headers.get("Range", "bytes=0-")[6:].split("-", 1)[0])
                requests.append((offset, self.headers.get("If-Match")))
                is_range = self.headers.get("Range") is not None and offset > 0
                if mode[0] == "ignore-range":
                    offset, is_range = 0, False
                self.send_response(206 if is_range else 200)
                self.send_header("ETag", '"different"' if mode[0] == "changed-etag" else '"stable"')
                self.send_header("Content-Length", str(len(body) - offset))
                if is_range:
                    self.send_header("Content-Range", f"bytes {offset}-{len(body)-1}/{len(body)}")
                self.end_headers()
                if mode[0] == "stall":
                    stall_gate.wait(timeout=5)
                try:
                    self.wfile.write(body[offset:3] if mode[0] == "truncate" else body[offset:])
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        original_command = assets.resume_request

        def loopback_command(*args):
            command = original_command(*args)
            command[command.index("--proto") + 1] = "=http"  # Test-only; production remains official HTTPS.
            return command

        record = {"id": "loopback-only", "url": f"http://127.0.0.1:{server.server_port}/asset",
                  "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
        try:
            with patch.object(assets, "official_url", side_effect=lambda url: url), \
                    patch.object(assets, "resume_request", side_effect=loopback_command):
                with tempfile.TemporaryDirectory() as directory:
                    cache = Path(directory)
                    mode[0] = "truncate"
                    with self.assertRaisesRegex(ValueError, "partial retained"):
                        assets.acquire_resumable(record, cache, 100, stream_hash=True)
                    partial = next((cache / "transfers").glob("*.part"))
                    self.assertEqual(partial.read_bytes(), b"abc")
                    for scenario in ["ignore-range", "changed-etag"]:
                        mode[0] = scenario
                        with self.assertRaises(ValueError):
                            assets.acquire_resumable(record, cache, 100, stream_hash=True)
                        self.assertEqual(partial.read_bytes(), b"abc")
                        self.assertFalse(list(cache.glob("*.tar.zst")))
                    mode[0] = "normal"
                    # Success must come from the streaming hash; a post-download digest pass is forbidden here.
                    with patch.object(assets, "digest_file", side_effect=AssertionError("unexpected reread")):
                        assets.acquire_resumable(record, cache, 100, stream_hash=True)
                    self.assertEqual((cache / (record["sha256"] + ".tar.zst")).read_bytes(), body)
                    self.assertEqual(requests[-1], (3, '"stable"'))
                    self.assertFalse(list((cache / "transfers").glob("*.lock")))
                with tempfile.TemporaryDirectory() as directory:
                    wrong = {**record, "sha256": "0" * 64}
                    with self.assertRaisesRegex(ValueError, "content drift"):
                        assets.acquire_resumable(wrong, directory, 100, stream_hash=True)
                    self.assertFalse(list(Path(directory).glob("*.tar.zst")))
                with tempfile.TemporaryDirectory() as directory:
                    mode[0] = "stall"
                    limits = assets.TransferLimits(directory, 100000, 1, 1, 1024)
                    with self.assertRaisesRegex(ValueError, "wall-clock"):
                        assets.acquire_resumable(record, directory, 100, limits=limits, stream_hash=True)
                    self.assertFalse(list(Path(directory).glob("*.tar.zst")))
        finally:
            stall_gate.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_transfer_limits_cover_cache_partials_free_space_and_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            (cache / "archive").write_bytes(b"1234")
            os.link(cache / "archive", cache / "same-inode")
            (cache / "transfers").mkdir()
            partial = cache / "transfers/part"
            partial.write_bytes(b"12")
            with patch.object(assets.time, "monotonic", return_value=100) as clock, \
                    patch.object(assets.shutil, "disk_usage", return_value=SimpleNamespace(free=100)) as disk:
                limits = assets.TransferLimits(cache, 6, 50, 10, 20)
                limits.check()  # Hard links counted once; unfinished objects included.
                clock.return_value = 101
                partial.write_bytes(b"123")
                with self.assertRaisesRegex(ValueError, "Combined transfer cache"):
                    limits.check()
                partial.write_bytes(b"12")
                clock.return_value = 102
                disk.return_value.free = 49
                with self.assertRaisesRegex(ValueError, "free-space reserve"):
                    limits.check()
                clock.return_value = 110
                with self.assertRaisesRegex(ValueError, "wall-clock"):
                    limits.check()
            (cache / "link").symlink_to(cache / "archive")
            with self.assertRaisesRegex(ValueError, "link or special"):
                assets.TransferLimits(cache, 100, 1, 10, 20).check()

    def test_budget_overrun_terminates_curl_retains_partial_and_does_not_publish(self):
        body = b"123456"
        record = {"id": "budget", "url": "https://datasets.ldbcouncil.org/budget.tar.zst",
                  "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}

        class Process:
            returncode = None
            stdout = io.BytesIO(b"200")
            terminated = False

            def poll(self):
                return self.returncode

            def terminate(self):
                self.terminated = True
                self.returncode = -15

            def wait(self, timeout):
                return self.returncode

        process = Process()
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(assets.time, "monotonic", return_value=0) as clock:
            limits = assets.TransferLimits(directory, 5, 1, 30, 20)

            def start(command, **kwargs):
                self.assertEqual(command[command.index("--limit-rate") + 1], "20")
                Path(command[command.index("--output") + 1]).write_bytes(body)
                Path(command[command.index("--etag-save") + 1]).write_text('"same-object"')
                clock.return_value = 1
                return process

            with patch.object(assets.shutil, "which", return_value="curl"), \
                    patch.object(assets.subprocess, "Popen", side_effect=start):
                with self.assertRaisesRegex(ValueError, "cache byte budget"):
                    assets.acquire_resumable(record, directory, 100, limits=limits)
            self.assertTrue(process.terminated)
            self.assertFalse(list(Path(directory).glob("*.tar.zst")))
            self.assertEqual(len(list((Path(directory) / "transfers").glob("*.part"))), 1)
            self.assertEqual(len(list((Path(directory) / "transfers").glob("*.etag"))), 1)
            self.assertFalse(list((Path(directory) / "transfers").glob("*.lock")))

    def test_hash_verification_obeys_invocation_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "archive"
            path.write_bytes(b"x" * (3 * 1024 * 1024))
            with patch.object(assets.time, "monotonic", return_value=0) as clock:
                limits = assets.TransferLimits(directory, 10 * 1024 * 1024, 1, 10, 20)
                clock.side_effect = [1, 2, 10]
                with self.assertRaisesRegex(ValueError, "wall-clock"):
                    assets.digest_file(path, limits.check)

    def test_streaming_inventory_counts_vertices_edges_and_headerless_updates(self):
        entries = [
            ("network/dynamic/person_0_0.csv", b"id|name\n1|a\n2|b", tarfile.REGTYPE),
            ("network/dynamic/person_knows_person_0_0.csv", b"Person.id|Person.id|creationDate\n1|2|3\n", tarfile.REGTYPE),
            ("network/dynamic/person_email_emailaddress_0_0.csv", b"Person.id|email\n1|a@test\n", tarfile.REGTYPE),
            ("network/updateStream_0_0_person.csv", b"1|2|3\n4|5|6\n", tarfile.REGTYPE),
        ]
        result = assets.inspect_stream(archive(entries), max_bytes=1000)
        self.assertEqual(result["vertices"], 2)
        self.assertEqual(result["edges"], 1)
        self.assertEqual(result["tables"]["person_email_emailaddress"]["kind"], "multi-property")
        self.assertEqual(result["files"][3]["rows"], 2)
        self.assertEqual(result["files"][0]["sha256"], hashlib.sha256(entries[0][1]).hexdigest())
        with self.assertRaisesRegex(ValueError, "byte budget"):
            assets.inspect_stream(archive(entries), max_bytes=10)
        mismatched = entries[:1] + [("network/dynamic/person_1_0.csv", b"id|changed\n3|c\n", tarfile.REGTYPE)]
        with self.assertRaisesRegex(ValueError, "Inconsistent headers"):
            assets.inspect_stream(archive(mismatched), max_bytes=1000)

    @unittest.skipUnless(shutil.which("zstd"), "zstd is required for the real archive decoder contract")
    def test_one_pass_archive_inspection_verifies_input_before_publishing(self):
        first = archive([("network/dynamic/person_0_0.csv", b"id|name\n1|a\n", tarfile.REGTYPE)]).getvalue()
        second = archive([("network/dynamic/person_1_0.csv", b"id|name\n2|b\n", tarfile.REGTYPE)]).getvalue()
        raw = subprocess.run(["zstd", "-q", "-c"], input=first + second, capture_output=True, check=True).stdout
        digest = hashlib.sha256(raw).hexdigest()
        record = {"id": "unit-only", "sha256": digest, "bytes": len(raw)}
        with tempfile.TemporaryDirectory() as directory:
            cache, output = Path(directory), Path(directory) / "inventory.json"
            (cache / (digest + ".tar.zst")).write_bytes(raw)
            assets.inspect_asset(record, cache, output, 1000)
            report = json.loads(output.read_text())
            self.assertEqual(report["vertices"], 2)  # Includes members after the first tar end marker.
            self.assertEqual(report["compressedBytesRead"], len(raw))
            self.assertEqual(report["archiveVerification"], "same-stream-sha256-and-length-before-publication")
            wrong = {**record, "sha256": "0" * 64}
            (cache / (wrong["sha256"] + ".tar.zst")).write_bytes(raw)
            rejected = cache / "rejected.json"
            with self.assertRaisesRegex(ValueError, "content drift"):
                assets.inspect_asset(wrong, cache, rejected, 1000)
            self.assertFalse(rejected.exists())
            with self.assertRaisesRegex(ValueError, "byte budget"):
                assets.inspect_asset(record, cache, rejected, 5)
            self.assertFalse(rejected.exists())
            (cache / (digest + ".tar.zst")).write_bytes(raw[:-1])
            with self.assertRaisesRegex(ValueError, "size differs"):
                assets.inspect_asset(record, cache, rejected, 1000)

    def test_resumable_transfer_retains_prefix_and_requires_matching_etag(self):
        body = b"abcdef"
        record = {"id": "resume", "url": "https://datasets.ldbcouncil.org/resume.tar.zst",
                  "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
        attempts = []

        class Process:
            def __init__(self, code, status):
                self.returncode = code
                self.stdout = io.BytesIO(status.encode())

            def poll(self):
                return self.returncode

        def start(command, **kwargs):
            self.assertIn("--http1.1", command)
            partial = Path(command[command.index("--output") + 1])
            incoming = Path(command[command.index("--etag-save") + 1])
            attempts.append(command)
            incoming.write_text('"same-object"')
            if len(attempts) == 1:
                partial.write_bytes(b"abc")
                return Process(18, "200")
            self.assertIn('If-Match: "same-object"', command)
            self.assertEqual(partial.read_bytes(), b"abc")
            with partial.open("ab") as stream:
                stream.write(b"def")
            return Process(0, "206")

        with tempfile.TemporaryDirectory() as directory, \
                patch.object(assets.shutil, "which", return_value="curl"), \
                patch.object(assets.subprocess, "Popen", side_effect=start):
            with self.assertRaisesRegex(ValueError, "partial retained"):
                assets.acquire_resumable(record, directory, 100)
            assets.acquire_resumable(record, directory, 100)
            self.assertEqual(assets.verified_path(record, directory).read_bytes(), body)
            self.assertFalse(list((Path(directory) / "transfers").glob("*.part")))
            self.assertFalse(list((Path(directory) / "transfers").glob("*.lock")))

    def test_resume_rejects_weak_or_missing_etag(self):
        with tempfile.TemporaryDirectory() as directory:
            partial = Path(directory) / "file.part"
            etag = Path(directory) / "file.etag"
            partial.write_bytes(b"abc")
            record = {"url": "https://datasets.ldbcouncil.org/example"}
            for value in ["", 'W/"weak"', '"bad\r\nheader"']:
                etag.write_text(value)
                with self.assertRaisesRegex(ValueError, "strong ETag"):
                    assets.resume_request(record, partial, etag, 100)

    def test_pending_capture_reuses_completed_assets_and_rejects_changed_catalog(self):
        data = {"schemaVersion": 1, "workload": "SNB Interactive v1", "assets": [
            {"id": name, "role": "initial", "url": f"https://datasets.ldbcouncil.org/{name}",
             "sha256": None, "bytes": None} for name in ["one", "two"]]}
        calls = []

        def downloader(asset, cache, budget, capture=False):
            calls.append((asset["id"], capture))
            if len(calls) == 2:
                raise OSError("network interrupted")
            return {**asset, "sha256": "a" * 64, "bytes": 1}

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lock.json"
            with self.assertRaises(OSError):
                assets.capture_catalog(json.loads(json.dumps(data)), path, directory, 100, downloader)
            self.assertFalse(path.exists())
            changed = json.loads(json.dumps(data))
            changed["assets"][0]["url"] += "-changed"
            with self.assertRaisesRegex(ValueError, "does not match"):
                assets.capture_catalog(changed, path, directory, 100, downloader)
            assets.capture_catalog(data, path, directory, 100, downloader)
            self.assertEqual(calls[2], ("one", False))
            self.assertEqual(calls[3], ("two", True))
            self.assertEqual(len(assets.catalog(path)["assets"]), 2)

    def test_cache_requires_exact_locked_bytes_and_rejects_symlink(self):
        body = b"official downloaded bytes"
        record = {"id": "test", "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / (record["sha256"] + ".tar.zst")
            path.write_bytes(body)
            self.assertEqual(assets.verified_path(record, directory), path)
            path.write_bytes(body + b"modified")
            with self.assertRaisesRegex(ValueError, "drift"):
                assets.verified_path(record, directory)
            path.unlink()
            other = Path(directory) / "other"
            other.write_bytes(body)
            path.symlink_to(other)
            with self.assertRaisesRegex(ValueError, "regular file"):
                assets.verified_path(record, directory)

    def test_extract_rejects_traversal_links_duplicate_names_and_expansion_overrun(self):
        cases = [
            [("../escape", b"x", tarfile.REGTYPE)],
            [("/absolute", b"x", tarfile.REGTYPE)],
            [("link", b"", tarfile.SYMTYPE)],
            [("a.csv", b"x", tarfile.REGTYPE), ("./a.csv", b"y", tarfile.REGTYPE)],
            [("a.csv", b"123456", tarfile.REGTYPE)],
        ]
        for entries in cases:
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):
                    assets.unpack_stream(archive(entries), Path(directory) / "out", max_bytes=5)
                self.assertFalse((Path(directory) / "escape").exists())

    def test_extract_hashes_regular_files_and_refuses_existing_output(self):
        entries = [("social_network/static/a.csv", b"id|name\n1|a\n", tarfile.REGTYPE)]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "out"
            manifest = assets.unpack_stream(archive(entries), target, 100)
            self.assertEqual(manifest[0]["sha256"], hashlib.sha256(entries[0][1]).hexdigest())
            with self.assertRaises(FileExistsError):
                assets.unpack_stream(archive(entries), target, 100)

    def test_catalog_rejects_unlocked_assets_and_nonofficial_urls(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "catalog.json"
            file.write_text(json.dumps({"schemaVersion": 1, "workload": "SNB Interactive v1", "assets": [
                {"id": "example", "role": "initial", "url": "https://datasets.ldbcouncil.org/example", "sha256": None}
            ]}))
            with self.assertRaisesRegex(ValueError, "Unfrozen"):
                assets.catalog(file)
        for url in ["http://datasets.ldbcouncil.org/a", "https://example.com/a", "https://user@datasets.ldbcouncil.org/a"]:
            with self.assertRaises(ValueError):
                assets.official_url(url)

    def test_download_checks_size_and_digest_and_does_not_replace_frozen_cache(self):
        body = b"test artifact"
        record = {"id": "example", "url": "https://datasets.ldbcouncil.org/example.tar.zst",
                  "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}

        class Response(io.BytesIO):
            status = 200
            headers = {"Content-Length": str(len(body))}

        class Opener:
            def open(self, *args, **kwargs):
                return Response(body)

        with tempfile.TemporaryDirectory() as directory, patch.object(assets.urllib.request, "build_opener", return_value=Opener()) as opener:
            assets.acquire(record, directory, 100)
            self.assertEqual(opener.call_count, 1)
            assets.acquire(record, directory, 100)
            self.assertEqual(opener.call_count, 1)
            with self.assertRaisesRegex(ValueError, "Upstream content drift"):
                assets.acquire({**record, "sha256": "0" * 64}, directory, 100)
            self.assertFalse((Path(directory) / ("0" * 64 + ".tar.zst")).exists())
            self.assertEqual(len(list(Path(directory).glob(".partial-*"))), 0)


if __name__ == "__main__":
    unittest.main()
