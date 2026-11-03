# aru memory

A Windows Add/Search memory service. AMC-ENTRY-1.8.1 retains original messages and derives vector, entity, graph, rule, journal and schedule evidence with separate user storage. This release contains the 202 Python source files matched byte-for-byte to the repaired deployment; private configuration, credentials, runtime data, models and host-assistant history are excluded.

## Current result

The 1.8.1 repair completed a bounded recovery of eighteen previously selected derivations: three prior successes were retained and fifteen remaining derivations succeeded with one new attempt each. It used three new provider calls; the two earlier calls remain recorded, for five total. The recovery helper finished in 31.922 seconds, the supervisor in 33.156 seconds, with no forced stop, no remaining child processes and no retained model owners.

On November 3, 2026, the repaired service was deployed on the existing endpoint. Local and public health returned HTTP 200 with version AMC-ENTRY-1.8.1; authentication and invalid-body checks returned 401 and 422. The original official Smoke task was resumed through the website. In the package-preparation snapshot its Add, Search and answer stages had completed, and scoring was still running at 20/46. It still carries its historical version label; it is not a newly created 1.8 task. **Smoke had not completed and Full had not passed at that snapshot.** See [VALIDATION.md](VALIDATION.md) for the checks and their limits.

Publishing this source does not change the entry's industrial group, grant evaluation quota, or establish compliance with the open-source track's prescribed models. The accepted 1.0 source remains at its original fixed commit and tag. Earlier evidence is retained in [VALIDATION-1.6.md](VALIDATION-1.6.md), [JOB17.md](JOB17.md), [PATCH18.md](PATCH18.md) and their metadata-only reports.

## Runtime

Enable two lanes and shared embedding explicitly. Each lane owns one active user runtime; user data stays separate. Both lanes share one BGE process, with at most two active generations and one actual embedding call at a time. A global admission limit of 32 covers reading, waiting, execution and unsettled owners. These are resource bounds, not throughput guarantees.

Add succeeds only after durable ingestion and publication. Same-ID/same-body retries reuse or resume prior work; changed bodies conflict. Search merges the scoped retrieval paths, verifies current source revisions and returns evidence rather than generating an answer. Original text is not silently truncated: vector slices must fit the configured token, character and chunk limits.

The documented request deadline is 1700 seconds, including body reading and queue time. A tunnel or reverse proxy may impose a shorter limit. Cancellation does not release a user lane until its real work and resources settle. A temporary public hostname is an operational dependency, not a multi-day availability guarantee.

## Windows setup

Use 64-bit Windows, the Python launcher and Python 3.10. The reference interpreter was 3.10.11. Use a short checkout path.

~~~powershell
git clone --branch codex/amc-entry-1.8.1 https://github.com/panglianyu/Aoao-Memory.git
cd Aoao-Memory
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup.ps1 -Dev
~~~

The setup script creates .venv and installs the inherited direct dependency pins, including CPU PyTorch. It does not download models, create credentials, start a service or open a tunnel. A fresh installation of these pins has not been verified for this release. See [DEPENDENCIES.md](DEPENDENCIES.md).

Obtain BAAI/bge-m3 separately under its upstream terms. Provision the exact snapshot once:

~~~powershell
$env:HF_HOME = 'C:/amc-model-cache'
$env:HF_HUB_OFFLINE = '0'
$env:TRANSFORMERS_OFFLINE = '0'
& .\.venv\Scripts\python.exe -c "from huggingface_hub import snapshot_download; snapshot_download('BAAI/bge-m3',revision='5617a9f61b028005a4858fdac845db406aefb181')"
~~~

Keep HF_HOME set when launching. Runtime model loading is offline and binds that revision; the package contains no weights or cache. This is not an independent attestation of every loaded model byte.

## Private configuration and launch

Copy the invalid provider example to a private location outside the checkout and fill its three provider fields. Never put a live Key or Bearer value in Git.

~~~powershell
New-Item -ItemType Directory -Path C:/amc-private -Force | Out-Null
if (-not (Test-Path C:/amc-private/provider.json)) { Copy-Item .\config\provider.example.json C:/amc-private/provider.json }
notepad.exe C:/amc-private/provider.json
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 1700 --shared-embedding --runtime-lanes 2 --check-config
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 1700 --shared-embedding --runtime-lanes 2
~~~

Check-config performs no model call. Normal startup protects the credential directory and uses an OS lease for the data root. Use one service process per root; do not add uvicorn workers against the same state. If a platform binding inherits an existing Bearer token, securely provision that same value before startup. Preserve existing data and storage markers.

Structured extraction explicitly disables thinking only for a validated official DeepSeek HTTPS origin. Other providers retain the existing request shape. The model, prompt hash, output schema and strict finish/JSON validation remain unchanged by this compatibility fix. Private failure diagnostics retain only closed codes, bounded project frames and an allowed finish-reason enum; they do not retain response text or reasoning.

## API example

Public HTTPS forwarding is configured separately. Health exposes only state and version.

~~~powershell
curl.exe --fail http://127.0.0.1:8795/health
$headers = @{ Authorization = 'Bearer ' + (Get-Content C:/amc-data/credentials/memory-system.key -Raw).Trim() }
$body = @{ request_id='demo-1'; user_id='demo-user'; session_id='demo-session'; messages=@(@{role='user'; content='Ada lives in Oslo.'; timestamp=0}) } | ConvertTo-Json -Depth 4
Invoke-RestMethod http://127.0.0.1:8795/add -Method Post -Headers $headers -ContentType application/json -Body $body
$query = @{user_id='demo-user'; query='Where does Ada live?'; top_k=5} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8795/search -Method Post -Headers $headers -ContentType application/json -Body $query
~~~

Use integer Unix milliseconds. Limits include a 2 MiB JSON body, 256 messages, 81,920 characters per message, 32,768 query characters, IDs up to 256 characters and top_k 1..100. Vector slices are capped at 8192 characters and 8192 special-inclusive tokens, with at most ten slices. Multiple-choice hints remain bounded and do not replace the original question or introduce model calls.

## Development

~~~powershell
& .\.venv\Scripts\python.exe -m pytest -q tests/test_memory_grouped_provider.py tests/test_grouped_extractor_diagnostic_reasons.py
~~~

The included tests are synthetic and forbid unplanned model loading by default. Operator-recovery fixtures additionally assume the reference short-path Windows scratch layout; they are audit cases, not a portable unattended suite. Maintenance recovery CLIs contain fixed operator paths and are not normal setup or production launch commands. Do not point them at a live data root.

See [CONTRIBUTING.md](CONTRIBUTING.md) for changes. [SOURCE_MANIFEST.json](SOURCE_MANIFEST.json) identifies the exact runtime files; [PACKAGE_MANIFEST.json](PACKAGE_MANIFEST.json) inventories this package without a self-hash cycle.

## Using with Codex

Codex reads [AGENTS.md](AGENTS.md) for verified commands, architecture and source boundaries. Keep real provider credentials, runtime data and recovery records outside the checkout.

## Remaining limits

Official Smoke completion, Full acceptance and completion, full-workload quality, long synchronous request availability, fresh dependency installation and production privacy acceptance remain unproved. The included historical failures are preserved rather than converted to passes. Packaging ran no new model or evaluation.

## License

First-party code uses the existing MIT license in [LICENSE](LICENSE). Dependencies and BGE-M3 keep their own terms; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
