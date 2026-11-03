<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to You
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Portable runtime provenance bundle

`components/http/runtime-provenance-bundle.mjs` packages an independently verified
runtime proof into one portable JSON file. A future experiment can freeze that
file as target evidence, together with its original byte SHA-256 and the proof's
raw-lock content digest. Collection and bundling occur outside measured windows.

```text
original proof directory
  -> local independent verifier
  -> original raw.lock files + raw.lock itself
  -> one canonical JSON, created exclusively

frozen bundle + expected file SHA + expected proof digest
  -> strict bounded content verification
  -> new private temporary directory
  -> existing local independent verifier
  -> original in-memory verified result
  -> remove temporary directory
```

## API

```js
const receipt = await bundleRuntimeProvenance({
    proofDirectory,
    python, // Explicit absolute local interpreter path
    expectedProofDigest, // raw.lock.json contentDigest
    outputFile, // New file; its parent directory already exists
    maxBundleBytes, // Explicit integer, 1 MiB .. 8 MiB
    signal, // Optional AbortSignal
});

const verified = await verifyRuntimeProvenanceBundle({
    file,
    python,
    expectedFileSha256, // Original bundle bytes, including its final LF
    expectedProofDigest,
    signal, // Optional AbortSignal
});
```

Both functions require explicit expected digests. There is no inferred proof
digest or default bundle creation budget. `receipt` records file, bytes, SHA-256,
proofDigest, file count and raw byte count. `verified` is the original frozen
object returned by `verifyRuntimeProvenance`; its private in-memory identity is
preserved for the phase consumer. Serialized CLI output cannot carry that private
identity. Import and call the API when a downstream consumer requires it.

The bundle contains exactly:

```text
schemaVersion: 1
kind: "http-runtime-provenance-bundle"
proofDigest: lowercase SHA-256 raw-lock content digest
files: sorted [{ path, bytes, sha256, base64 }]
```

All JSON object keys are lexicographically sorted, arrays retain their order,
there is no extra whitespace, and one LF terminates the UTF-8 file. Verification
requires those exact canonical bytes after parsing. Duplicate keys, alternative
whitespace, invalid UTF-8, extra schema fields and noncanonical base64 are
rejected. Base64 is independently decoded, re-encoded and checked against each
original byte count and SHA-256.

Every original member in `raw.lock.json.files` is included unchanged, plus
`raw.lock.json` itself. Their member list must exactly reproduce the raw lock and
its expected content digest. Derived `runtime-binding.json`,
`external-expectations.json`, and `recovery/` receipts are excluded. Failed,
incomplete, drifted, incompatible or oversized proofs fail before an output file
is created. The original proof is never rewritten, regenerated or truncated.

The limits are 8 MiB bundle bytes, 16 MiB total decoded raw bytes, 1000 file and
ancestor-directory entries, and eight relative path components. Paths use ASCII
letters, digits, `.`, `_` and `-`; absolute paths, empty components, `.`/`..`,
backslashes, duplicate members and file/directory collisions are rejected.
Regular files are opened with `O_NOFOLLOW` and read through bounded file
descriptors; device/inode/size/mtime/ctime are checked before and after. Parent
member directories must be ordinary directories. Output uses create-exclusive
mode 0600 and cannot be located inside the original proof, including through a
parent-directory symlink.

## Offline replay and cancellation

Verification checks the bundle file SHA, strict schema, canonical encoding and
complete raw lock before creating a temporary directory. It writes only those
validated members, with exclusive files and private directories, and calls the
existing independent adapter. That adapter checks source compatibility and runs
the current local `runtime-provenance.py`. Scripts inside the bundle are never
executed. The temporary directory is removed in `finally` after success, parser
failure or cancellation. The CLI translates SIGINT/SIGTERM to an AbortSignal;
the adapter owns termination of its parser group. SIGKILL or machine failure can
prevent process cleanup and `finally` execution.

Moving the bundle works without access to the original collection directory.
The original `plan.root` remains an audit fact used to reconstruct original
Docker mount/argv semantics. Offline verification starts no Docker container and
does not access the old executable paths. A locally compatible Python interpreter
and the supported local verifier implementation are still required.

The source of trust remains the fixed collector's raw receipts and the separately
frozen expected digests. This container is portable evidence packaging; it adds
no cryptographic host attestation. It retains `futureExperimentsOnly=true`,
`clockAlignment="unverified"` and `performanceAccepted=false` from the verifier.
It cannot qualify an earlier run or prove runtime clock association on its own.

## Fixed component CLI

```sh
node components/http/runtime-provenance-bundle.mjs bundle \
  --proof /absolute/proof --python /absolute/python3 \
  --proof-digest RAW_CONTENT_SHA256 --out /absolute/new-proof.json \
  --max-bundle-bytes 8388608

node components/http/runtime-provenance-bundle.mjs verify \
  --file /absolute/new-proof.json --python /absolute/python3 \
  --file-sha256 ORIGINAL_FILE_SHA256 --proof-digest RAW_CONTENT_SHA256
```

`--help` lists effects and budgets without executing anything. Each command emits
one JSON envelope. Errors exit 6; handled SIGINT/SIGTERM exit 130/143. The CLI
accepts only the fixed flags above and exposes no arbitrary command entry.

`tests-normative/http-runtime-provenance-bundle.test.mjs` reuses the runtime
producer's explicitly synthetic protocol fixture. It tests real local verifier
replay after deleting the original proof, full inventory/encoding/path mutations,
exclusive creation and budgets, foreign script rejection, failed proof
preservation, private result identity, and actual SIGTERM/AbortSignal temporary
directory cleanup. These fixtures do not run Docker or qualify real runtimes.
