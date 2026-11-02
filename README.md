# aru memory

A Windows memory service built from the independent Aoao Memory source tree. This 1.8 candidate package repairs two bounded recall defects in the shared memory kernel and inherits 1.7's internal durable Add-job core and validation driver. It excludes the Aoao OS host assistant, tests, private configuration, runtime data, model weights, and private Aoao OS Git history.

## Candidate status

The 1.8 patch prevents request boilerplate from occupying all eight Chinese query terms and filters journal/schedule event windows before truncating candidates. Windowed queries use 64-record keyset pages, inspect at most 2048 matching records per domain, and preserve the original absolute deadline and limited trace. It reuses the existing raw-plus-six-domain retrieval and final source/revision checks. See [PATCH18.md](PATCH18.md) and [validation-patch18.json](validation-patch18.json).

The patch passed 58 shared-kernel checks in 9.38 seconds and 25 inherited option-hint checks in the isolated candidate in 9.74 seconds. These are separate checkouts, not one 83-test public-package matrix. Ruff passed for four changed Python files, and independent Python review found no blocking issue. No new model call, official Smoke/Full, 500-question benchmark, or unrelated full-repository matrix was run for this patch.

Final review also checked the original long coffee-preference question and six affected phrase cases: 7 passed in 0.31 seconds, without repeating the earlier matrix. Mypy passed for the two implementations with explicit package bases. In total, 84 distinct related cases were covered across these runs.

The inherited 1.7 internal job core passed one bounded local real-model check: one Add containing twenty messages reached durable SUCCEEDED after 279.593 seconds, all 120 derivations succeeded, and two subsequent Search checks found the first and last message markers. Receipt acceptance took 0.015 seconds; acceptance is not ingestion success. See [JOB17.md](JOB17.md) and the exact metadata-only [validation-job17.json](validation-job17.json).

**1.8 is not deployed and is not Full-ready.** The included HTTP CLI and Add/Search routes retain the 1.6 synchronous protocol and a 100-second request budget. No HTTP 202 job adapter or official job-status schema has been implemented. The complete asynchronous status/poll contract remains unresolved. A local job with a 1700-second budget does not show that the synchronous 100-second HTTP Add can complete the same batch; these recall patches do not resolve that mismatch.

The platform's version-binding dialog offers a new version under the existing Key without another review. Binding still needs real Add/Search endpoints for that version; new Smoke/Full creation has separate eligibility and submission-window checks. Source publication alone does not bind a version, extend a quota, or start an evaluation.

The accepted deployment remains AMC-ENTRY-1.0 at fixed commit [e5d5251269d47b27b006aa12df728897591bc90a](https://github.com/panglianyu/Aoao-Memory/commit/e5d5251269d47b27b006aa12df728897591bc90a). Do not use a submitted service's active data root for this candidate. This package makes no claim of passed official Smoke, completed Full, leaderboard score, or proven Full capacity.

SOURCE_MANIFEST.json identifies the same 57 Python paths as 1.7: the prior explicit 54-file runtime closure, two job modules, and one validation driver. Only kernel/daily_memory_contracts.py and apps/system_memory_god/storage/domain_queries.py change in 1.8; the other 55 source files retain their 1.7 bytes. Source bytes and actual line endings are preserved when copied from the frozen 1.8 candidate. PACKAGE_MANIFEST.json inventories an explicit whitelist and excludes itself to avoid self-hashing. The 193-source before/after digest in the historical real validation report describes the larger private validation tree, not this 57-file publication closure. VALIDATION.md preserves the original 1.6 evidence unchanged; JOB17.md and validation-job17.json preserve the 1.7 result and its limits. PREPARATION.md describes packaging coverage.

## Inherited synchronous HTTP service

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

The 1.6 seven-request HTTPS probe was historical evidence only. A later single Add containing twenty messages returned HTTP 504 after 101.234 seconds, and candidate health was then unavailable. No Search was sent in that failed batch check. The internal 1.7 job result addresses durable ownership and a separately bounded execution budget; it does not establish a working public job adapter or repair a deployed endpoint by publication alone. The 1.8 recall checks do not establish model throughput or official Full capacity. A fresh dependency installation, long-running availability, formal workload scale, quality improvement, asynchronous interface compatibility, and official Smoke/Full remain unproved. See VALIDATION.md and JOB17.md for unchanged historical evidence, and PATCH18.md for the bounded 1.8 changes.

First-party source uses the inherited MIT License in LICENSE. Third-party dependencies and BGE-M3 retain their separate terms and attribution; see THIRD_PARTY_NOTICES.md.
