# aru memory

A Windows Add/Search memory service. AMC-ENTRY-1.8.2 retains original messages and derives vector, entity, graph, rule, journal and schedule evidence with separate user storage. This candidate contains 202 Python source files matched byte-for-byte to the isolated repair checkout. Private configuration, credentials, runtime data, models, operational controls and host-assistant history are excluded.

## Candidate status

This patch addresses two failures found during an official Full attempt. A grouped extraction could repeat the same invalid result across its five target domains. A terminal Raw derivation failure could also remain a nonterminal publication state, making synchronous Add wait until its deadline.

The isolated code now shares a bounded same-source correction budget and propagates current-source terminal failures to the existing Add failure path. Strict validation remains in place: invalid results are not written or acknowledged as success. See [PATCH182.md](PATCH182.md) and [VALIDATION-1.8.2.md](VALIDATION-1.8.2.md).

The original official Smoke subsequently completed 46/46. A Full task was then started and failed during Add; later attempts also encountered an unavailable public ingress. Those facts do not establish that this new candidate has passed Smoke or Full. **1.8.2 is not claimed as deployed, formally accepted, recovered against the official data, or ready to resume Full.** A stable ingress and the original task's fixed-version association still need to be resolved.

Publishing this source does not change the entry's industrial group, grant evaluation quota, or establish compliance with the open-source track's prescribed models. The accepted 1.0 source and 1.8.1 fixed commit remain unchanged. Earlier evidence is retained in [VALIDATION-1.8.1.md](VALIDATION-1.8.1.md), [VALIDATION-1.6.md](VALIDATION-1.6.md), [JOB17.md](JOB17.md), [PATCH18.md](PATCH18.md) and their metadata-only reports.

## Runtime

Enable two lanes and shared embedding explicitly. Each lane owns one active user runtime; user data stays separate. Both lanes share one BGE process, with at most two active generations and one actual embedding call at a time. A global admission limit of 32 covers reading, waiting, execution and unsettled owners. These are resource bounds, not throughput guarantees.

Add succeeds only after durable ingestion and publication. Same-ID/same-body retries reuse or resume prior work; changed bodies conflict. A current, active source with a terminal failed derivation now ends publication waiting with an unavailable state. The API keeps its existing failure response instead of acknowledging incomplete work. Pending work, older revisions and other users or conversations are not promoted to terminal failures.

Search merges the scoped retrieval paths, verifies current source revisions and returns evidence rather than generating an answer. Original text is not silently truncated: vector slices must fit the configured token, character and chunk limits.

The documented request deadline is 1700 seconds, including body reading and queue time. A tunnel or reverse proxy may impose a shorter limit. Cancellation does not release a user lane until its real work and resources settle. A temporary public hostname is an operational dependency, not a multi-day availability guarantee.

## Windows setup

Use 64-bit Windows, the Python launcher and Python 3.10. The reference interpreter was 3.10.11. Use a short checkout path.

~~~powershell
git clone --branch codex/amc-entry-1.8.2 https://github.com/panglianyu/Aoao-Memory.git
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

Structured extraction explicitly disables thinking only for a validated official DeepSeek HTTPS origin. The base system prompt, model, source identities and strict output schema remain unchanged. In 1.8.2, invalid JSON, group shape or domain payload results may trigger a static correction message inside the same-source lock. That message contains only fixed constraints and a closed reason code, never the previous response body. These later requests are not byte-for-byte identical to the old input policy.

The maximum is three started SDK requests per source and content hash within one extractor instance. Calls already started remain counted across cancellation or transport errors. Up to 128 RAM slots retain closed failure metadata; a valid durable cache takes precedence. This is not a persistent, cross-process or cross-restart total-call budget, and it does not authorize re-running failed official data. Private diagnostics still exclude response text and reasoning.

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
& .\.venv\Scripts\python.exe -m pytest -q tests/test_grouped_extractor_repair.py tests/test_memory_terminal_publication.py
~~~

The included tests are synthetic and forbid unplanned model loading by default. Operator-recovery fixtures additionally assume the reference short-path Windows scratch layout; they are audit cases, not a portable unattended suite. Maintenance recovery CLIs contain fixed operator paths and are not normal setup or production launch commands. Do not point them at a live data root.

The two regression modules and their static test-import closure are included. Packaging has checked those imports structurally but has not repeated the implementation checkout's regression matrix. See [CONTRIBUTING.md](CONTRIBUTING.md) for changes. [SOURCE_MANIFEST.json](SOURCE_MANIFEST.json) identifies the candidate's exact runtime files; [PACKAGE_MANIFEST.json](PACKAGE_MANIFEST.json) inventories this package without a self-hash cycle.

## Using with Codex

Codex reads [AGENTS.md](AGENTS.md) for verified commands, architecture and source boundaries. Keep real provider credentials, runtime data and recovery records outside the checkout.

## Remaining limits

Current-candidate deployment, official version association, targeted recovery, stable public ingress, Full completion and full-workload quality remain unproved. The earlier Smoke success belongs to the original resumed task and implementation, not to this candidate. Type-check findings and unrun checks are recorded in [VALIDATION-1.8.2.md](VALIDATION-1.8.2.md). Fresh dependency installation and production privacy acceptance also remain unproved. Packaging ran no new model or evaluation.

## License

First-party code uses the existing MIT license in [LICENSE](LICENSE). Dependencies and BGE-M3 keep their own terms; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
