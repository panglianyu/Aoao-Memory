# Aoao Memory

A Windows Add/Search service for persistent original messages and derived memory. This standalone tree excludes the host assistant, private configuration/data, model weights, and original Git history.

## Versions and scope

The AMC-ENTRY-1.0 tag matches the submitted API source. This codex/amc-entry-1.2 branch is an undeployed candidate. It inherits 1.1's OS-lease and integer-timestamp fixes and adds bounded request waiting, option-assisted retrieval and token-aware original slices. SOURCE_MANIFEST.json identifies its source files; VALIDATION.md records actual checks. The submitted online service remains AMC-ENTRY-1.0.

Publishing source does not change this industrial entry's group or establish compliance with the open-source track's prescribed models.

## Architecture

Original messages and a durable request ledger are the write authority. The full memory profile derives vector, entity, graph, rule, journal, and schedule evidence. Retrieval merges those paths with original-message recall and verifies returned source messages. External user IDs map to separate service-owned scopes and storage roots.

One request is admitted at a time. One user runtime is active; changing users closes and restores the runtime. Add succeeds only after durable ingestion and publication. Same-ID/same-body retries resume or return the prior result; changed bodies conflict. Search returns evidence rather than generating an answer.

## Requirements

Use Windows 64-bit and Python 3.10 with the Python launcher; the reference was 3.10.11. Pins describe that environment, not a verified fresh install or full transitive lockfile.

Install from a short checkout path:

~~~powershell
git clone --branch codex/amc-entry-1.2 https://github.com/panglianyu/Aoao-Memory.git
cd Aoao-Memory
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup.ps1 -Dev
~~~

The script installs CPU PyTorch and Python dependencies into .venv. It does not download models, create credentials, start a service, or change machine settings.

## Prepare the model cache

Obtain BAAI/bge-m3 under its upstream terms. Use a dedicated cache and allow network access only for this explicit provisioning step:

~~~powershell
$env:HF_HOME = 'C:/amc-model-cache'
$env:HF_HUB_OFFLINE = '0'
$env:TRANSFORMERS_OFFLINE = '0'
& .\.venv\Scripts\python.exe -c "from pathlib import Path; from huggingface_hub import snapshot_download; expected='5617a9f61b028005a4858fdac845db406aefb181'; p=Path(snapshot_download('BAAI/bge-m3',revision=expected)); print('Cached revision:',p.name); raise SystemExit(0 if p.name==expected else 'Revision differs; review the model before starting')"
~~~

Stop if provisioning fails or reports a different revision. Candidate 1.2 requires this exact local snapshot for both its fast tokenizer and nonformal BGE workers. This binding is not independent attestation of the submitted service's loaded weights. Keep HF_HOME set when launching. The CLI forces offline model loading. The optional formal embedding/vendor audit configuration is not packaged or certified here.

## Private configuration and launch

Choose short, ordinary absolute paths outside the checkout. The following are example locations. Do not overwrite an existing private configuration.

~~~powershell
New-Item -ItemType Directory -Path C:/amc-private -Force | Out-Null
if (-not (Test-Path C:/amc-private/provider.json)) { Copy-Item .\config\provider.example.json C:/amc-private/provider.json }
notepad.exe C:/amc-private/provider.json
~~~

Fill the three OpenAI-compatible provider fields in that private file. No .env file is loaded. Use integer Unix milliseconds for message timestamps.

~~~powershell
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 100 --check-config
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 100
~~~

Configuration checking is read-only. Normal startup creates a dedicated Bearer credential and protects its new Windows credential directory. Keep the data root and provider configuration private. Version 1.0 requires one externally managed process per data root.

## API example

The service listens on loopback. Public HTTPS forwarding is a separate deployment responsibility. Health contains only state/version; ready does not prove model warmup or Full capacity.

~~~powershell
curl.exe --fail http://127.0.0.1:8795/health
$headers = @{ Authorization = 'Bearer ' + (Get-Content C:/amc-data/credentials/memory-system.key -Raw).Trim() }
$body = @{ request_id='demo-1'; user_id='demo-user'; session_id='demo-session'; messages=@(@{role='user'; content='Ada lives in Oslo.'; timestamp=0}) } | ConvertTo-Json -Depth 4
Invoke-RestMethod http://127.0.0.1:8795/add -Method Post -Headers $headers -ContentType application/json -Body $body
$query = @{user_id='demo-user'; query='Where does Ada live?'; top_k=5} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8795/search -Method Post -Headers $headers -ContentType application/json -Body $query
~~~

Add returns success and the request/user/session IDs. Search returns data containing source id/content and optional created_at. Authentication is required before request processing. Busy requests return 429; conflicts return 409; publication deadlines return 504. A timeout does not erase durable ingestion: retry only the same request ID and body.

Limits: 2 MiB JSON body, 256 messages, 81,920 characters/message, 32,768 characters/query, IDs up to 256 characters, top_k 1..100. Each original-vector slice is limited to 8,192 characters and 8,192 tokens including special tokens, with at most ten slices. Some otherwise-valid raw messages cannot fit those vector limits: derivation fails explicitly and the durable original remains; tails are never silently discarded.

Optional multiple-choice options supply at most sixteen literal hints and 512 characters. They enable one additional scoped raw lexical query of at most 150ms within the existing retrieval deadline. The original question remains unchanged and is the only vector query; options are never stored as facts or treated as instructions. Front-end request timeouts or cancellation retain the active background owner until accepted work settles; new requests receive busy responses in the meantime.

## Validation and limitations

Reference checks covered synthetic tests and a two-user/four-message real smoke; see VALIDATION.md. Improved accuracy, official Smoke/Full, bulk throughput, crash recovery, and production privacy acceptance remain unproved.

Version 1.0's HTTP layer accepts floats but its runtime accepts integers: submit integers. It also lacks a cross-process data-root lease; both 1.1 and this 1.2 candidate fix those issues. User switching still reloads resources. Full-scale batches have not been proved to fit the 100-second deadline. No existing data migration or official version rebinding is included; do not point this candidate at a submitted service's active data root.

## Development and Codex

Use AGENTS.md for commands and architecture. CONTRIBUTING.md describes changes and issue reporting. Codex can read AGENTS.md directly.

## License

First-party code is licensed under the MIT License; see LICENSE. Third-party terms are separate; see THIRD_PARTY_NOTICES.md.

