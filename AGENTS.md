# aru memory

Version: AMC-ENTRY-1.8.2 candidate | Port: 8795 | Stack: Windows, Python 3.10, FastAPI, SQLite, Chroma, BGE-M3

## Purpose

Standalone Add/Search memory service with durable originals and scoped six-domain evidence. The 202-file audit set matches the isolated repair checkout; it is broader than the minimal service import closure. It is not a deployment or Full-pass certificate.

## Commands

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup.ps1 -Dev
& .\.venv\Scripts\python.exe -m pytest -q tests/test_grouped_extractor_repair.py tests/test_memory_terminal_publication.py
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
- grouped_extractor.py: scoped grouped extraction, strict validation, bounded same-source corrections and provider compatibility.
- kernel/raw_message_store.py and daily_runtime.py: atomic current-source failure metadata and terminal publication propagation.
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
- The three-call extraction budget is RAM state for one instance, not a persistent recovery allowance. Do not silently requeue failed official tasks.
- Preserve the base prompt and strict field contract. New correction requests must not be described as identical to the old request policy.
- Report coverage and failures honestly; health, binding, Smoke and Full are distinct evidence.

See CONTRIBUTING.md, PATCH182.md and VALIDATION-1.8.2.md. VALIDATION.md and VALIDATION-1.8.1.md preserve the historical snapshot. Existing MIT and third-party terms remain in force.
