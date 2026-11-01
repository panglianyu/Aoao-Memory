# aru memory

A Windows Add/Search service built from the independent Aoao Memory source tree. This 1.6 candidate branch contains the selected runtime and packaging materials. It excludes the Aoao OS host assistant, tests, private configuration, runtime data, model weights, and private Aoao OS Git history.

## Candidate status

The selected runtime source matches the frozen 1.6 implementation and its one passing bounded local real-model gate. It remains a candidate branch. The accepted online version is still 1.0, and there is no official Full result for this candidate. Source publication alone does not approve a version change or change the Commercial Products (Industry) division.

The accepted deployment remains AMC-ENTRY-1.0 at fixed commit [e5d5251269d47b27b006aa12df728897591bc90a](https://github.com/panglianyu/Aoao-Memory/commit/e5d5251269d47b27b006aa12df728897591bc90a). Do not use a submitted service's active data root for this candidate. This package makes no claim of passed official Smoke, completed Full, leaderboard score, or proven Full capacity.

SOURCE_MANIFEST.json identifies the 54 copied runtime source files. PACKAGE_MANIFEST.json inventories an explicit whitelist of 54 source files and twelve other package materials, excluding itself. Runtime-generated caches and logs are outside that inventory. VALIDATION.md separates inherited failures, current local evidence, and pending checks. PREPARATION.md describes sanitization coverage and remaining gaps.

## Runtime design

Durable original messages and a request ledger are the write authority. The full memory profile derives vector, entity, graph, rule, journal, and schedule evidence. Search merges scoped retrieval paths and verifies returned source messages; it returns evidence rather than generating a final answer. External user IDs map to separate physical raw and derived storage roots.

The explicitly enabled two-lane mode has one shared BGE process and at most two active embedding generations. Each lane owns one user runtime. Add and Search for the same user keep their lane affinity and FIFO order. A global limit of 32 admitted requests covers body reading, waiting, execution, and unsettled owners. It is a resource bound, not a throughput claim. Shared embedding executes one actual model call at a time.

The documented launch selects a 100-second request deadline. Body reading and queue time consume that same budget. A timed-out HTTP waiter does not prove background work has settled: ownership and admission remain held until actual cleanup. Add returns success only after durable ingestion and current publication. Same-ID/same-body retries resume or reuse progress; changed bodies conflict.

The 1.6 native cleanup fix removes an unnecessary WRITE-access request from directory guards. Sibling snapshot caches can share their common temporary-directory anchor while guards retain READ-only sharing, reject write/delete opens and reparse points, and verify captured object identities. This is a Windows sharing-violation fix; a Python BrokenPipeError from the earlier wrapper did not establish an IPC failure.

Default CLI settings remain one lane without shared embedding. Both flags shown below are required for the two-lane, single-model mode.

## Dependencies and model cache

Use Windows 64-bit, a short checkout path, and Python 3.10 with the Windows Python launcher. The inherited reference interpreter was Python 3.10.11. The dependency pins describe the reference environment; a fresh installation and full transitive lockfile have not been verified for this package. See DEPENDENCIES.md.

~~~powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup.ps1
~~~

The setup script installs dependencies into .venv, including the CPU PyTorch wheel. It does not obtain model weights, create provider credentials, start the service, or open a public tunnel. Its optional -Dev flag installs the inherited developer tooling; this minimal package contains no tests.

Obtain BAAI/bge-m3 separately under its upstream terms. Provision the exact revision once, with network access explicitly enabled for that step:

~~~powershell
$env:HF_HOME = 'C:/amc-model-cache'
$env:HF_HUB_OFFLINE = '0'
$env:TRANSFORMERS_OFFLINE = '0'
& .\.venv\Scripts\python.exe -c "from pathlib import Path; from huggingface_hub import snapshot_download; expected='5617a9f61b028005a4858fdac845db406aefb181'; p=Path(snapshot_download('BAAI/bge-m3',revision=expected)); print('Cached revision:',p.name); raise SystemExit(0 if p.name==expected else 'Revision differs; review the model before starting')"
~~~

Keep HF_HOME set when launching. The CLI forces offline model loading. Both tokenizer and nonformal BGE loaders require revision 5617a9f61b028005a4858fdac845db406aefb181. This code-level binding does not attest the weights actually loaded by the accepted service. Optional formal embedding/vendor audit materials are not included or certified here.

## Private configuration and launch

Choose short, ordinary absolute paths outside the checkout. The example contains an invalid host and placeholder values. Fill the private copy; never put a live credential in this repository.

~~~powershell
New-Item -ItemType Directory -Path C:/amc-private -Force | Out-Null
if (-not (Test-Path C:/amc-private/provider.json)) { Copy-Item .\config\provider.example.json C:/amc-private/provider.json }
notepad.exe C:/amc-private/provider.json
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 100 --shared-embedding --runtime-lanes 2 --check-config
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 100 --shared-embedding --runtime-lanes 2
~~~

Configuration checking is read-only and starts no model or service; it does not authenticate with the provider or prove capacity. Normal startup creates a dedicated Bearer credential in its private data root and protects its new Windows credential directory. Use one externally managed service process per data root. Do not launch extra uvicorn workers against the same state.

## API example

The service listens on loopback. Stable public HTTPS forwarding is a separate deployment responsibility. Health exposes state/version; ready does not prove model throughput or Full capacity.

~~~powershell
curl.exe --fail http://127.0.0.1:8795/health
$headers = @{ Authorization = 'Bearer ' + (Get-Content C:/amc-data/credentials/memory-system.key -Raw).Trim() }
$body = @{ request_id='demo-1'; user_id='demo-user'; session_id='demo-session'; messages=@(@{role='user'; content='Ada lives in Oslo.'; timestamp=0}) } | ConvertTo-Json -Depth 4
Invoke-RestMethod http://127.0.0.1:8795/add -Method Post -Headers $headers -ContentType application/json -Body $body
$query = @{user_id='demo-user'; query='Where does Ada live?'; top_k=5} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8795/search -Method Post -Headers $headers -ContentType application/json -Body $query
~~~

Use integer Unix milliseconds for timestamps. Add returns success plus request/user/session IDs. Search returns data with source id/content and optional created_at. Authentication precedes request-body processing. Queue saturation/busy, conflict, unavailable ownership, and deadlines return explicit errors; a timeout does not erase durable ingestion. Retry only the same request ID and body.

Limits include a 2 MiB JSON body, 256 messages, 81,920 characters per message, 32,768 query characters, IDs up to 256 characters, and top_k 1..100. Original-vector slices each have at most 8,192 characters and 8,192 tokens including special tokens, with at most ten slices. Inputs that do not fit vector limits fail explicitly; tails are not silently discarded.

Optional multiple-choice options supply at most sixteen literal hints and 512 characters. They permit an additional scoped lexical query within the existing retrieval deadline. The original question remains the vector query; options are not stored as facts.

## Validation limits and license

The previous 1.5 real-model gate failed its Search wave. The 1.6 native regression and real Windows worker test with fake embedding/LLM dependencies passed locally. The related 23-file matrix ended with 446 passes, two failures, and four skips; the two failures came from a temporary-directory precondition in the copy probe, before the probe body ran. A separate default-TEMP check of only those two tests passed. This is not a single clean matrix run, and the limited Mypy check still reports inherited errors.

The one 1.6 local real BGE/provider gate then passed: sixteen Add and sixteen Search responses were valid HTTP 200, across sixteen synthetic users/sessions and twenty input messages. The largest measured request times were 83.047 seconds for Add and 26.640 seconds for Search. The recorded runner wall time was 122.469 seconds. Its bounded workload and cleanup checks passed. A minimal-package import check also passed without constructing a model or service.

The implementation owner subsequently started an isolated candidate service and passed seven requests through actual public HTTPS/TLS, including authenticated Add/Search, an unauthorized request, and a same-ID retry. That finite probe leaves the accepted 1.0 service and official binding unchanged. It does not establish 24-hour availability or Full-scale performance. A fresh dependency installation, long-running operation, quality improvement, organizer version/capacity review, and official Smoke/Full remain unproved here. See VALIDATION.md for exact scope and hashes.

First-party source uses the inherited MIT License in LICENSE. Third-party dependencies and BGE-M3 retain their separate terms and attribution; see THIRD_PARTY_NOTICES.md.
