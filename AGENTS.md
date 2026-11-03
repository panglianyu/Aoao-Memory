# aru memory

Version: AMC-ENTRY-1.8.1 | Port: 8795 | Stack: Windows, Python 3.10, FastAPI, SQLite, Chroma, BGE-M3

## Purpose

Standalone Add/Search memory service with durable originals and scoped six-domain evidence. The published 202-file audit set matches the repaired deployment; it is broader than the minimal service import closure.

## Commands

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup.ps1 -Dev
& .\.venv\Scripts\python.exe -m pytest -q tests/test_memory_grouped_provider.py tests/test_grouped_extractor_diagnostic_reasons.py
```

Follow README.md to provision the pinned model and an external private provider JSON, then:

```powershell
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 1700 --shared-embedding --runtime-lanes 2 --check-config
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 1700 --shared-embedding --runtime-lanes 2
```

Setup installs dependencies only. It creates no model, credential, runtime root, service or tunnel. No Linux setup.sh is offered: this deployment relies on Windows native file/lease behavior.

## Architecture and files

- Tools/serve_memory_challenge.py: explicit config, data-root lease, service lifecycle.
- apps/system_memory_god/challenge_api.py: Bearer authentication, bounded Add/Search/health routes.
- challenge_runtime.py and challenge_lanes.py in that app: user runtime ownership, two lanes, deadlines and settlement.
- grouped_extractor.py: scoped grouped extraction, strict output validation, provider compatibility option.
- memory_factory.py: explicit shared service assembly, no host assistant boot.
- kernel/raw_message_service.py and raw_message_worker.py: durable originals and guarded derivation work.
- kernel/challenge_diagnostics.py: private bounded closed-code diagnostics.
- apps/system_memory_god/storage/: scoped evidence, ledger, vectors and shared embedding ownership.

Add persists originals, runs scoped derivations and waits for current publication. Search merges retrieval paths and revalidates source revisions. One shared BGE process serves two user lanes; storage roots and extraction identities remain separate.

## Boundaries

- Do not edit source manifests to imply different bytes were tested or deployed.
- Preserve request IDs, attempt history, storage markers and source/evidence identities.
- Never put provider config, Bearer/LDBD values, data, model weights or private logs in Git.
- Maintenance recovery CLIs have fixed operator policies; they are not normal startup commands and must not run against a live root.
- Tests deny unplanned real model loading by default. Recovery fixtures additionally require the reference short-path Windows scratch layout.
- Use explicit fakes for offline work. Live calls, deployment changes and evaluations need a defined scope and budget.
- Report coverage and failures honestly; health, binding, Smoke and Full are distinct evidence.

See CONTRIBUTING.md and VALIDATION.md. Existing MIT and third-party terms remain in force.
