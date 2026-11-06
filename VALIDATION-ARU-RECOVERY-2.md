# ARU-RECOVERY-2 initial integrated validation

Status: **not ready for deployment**. The results below apply to the frozen input hashes of the first integrated matrix. Some fixture corrections were subsequently made and remain unvalidated at this save point. This working checkpoint is not falsely claimed to reproduce the exact tested tree or to be Full-ready.

The related offline matrix on2026-11-06 used synthetic models, temporary SQLite/Chroma and actual Windows workers. Remaining51 test files selected995 cases: **977 passed,15 failed,3 skipped**, with one further case deselected because it had completed in the interrupted first run. The first run had11 visible passes before its supervisor failed after29.156 seconds; those cases were not repeated. No aggregate all-green claim is made.

The remaining run ended naturally after1082.0 seconds, pytest exit1. Observed child processes retired; frozen204 source and55 test file references remained unchanged. Process-tree sampled peak RSS1,381,412,864 bytes; sampled new data including the first run817,814,017 bytes; minimum available host memory11,038,986,240 bytes. Sampling is not OS-enforced isolation or proof about unobserved descendants.

## Growth findings

| Persisted vectors | Cold publication | First exact interactive search |
| --- | --- | --- |
|1000 ×1024D|3.281 seconds|Timed out in ledger validation at original2-second deadline|
|5002 ×1024D|10.360 seconds|Same deadline failure|
|12000 ×1024D|24.968 seconds|Same deadline failure|
|40000 ×1024D|120.422 seconds; publication unavailable|Not reached|

The12-row fixture passed. Growth fixture construction is not product Add throughput. No larger case completed cold and warm queries, so full-corpus throughput is unproved. Code inspection found repeated whole-page metadata accounting per row and repeated cold index checks/single-ID receipt reads; no stage profile establishes each hotspot's exact share of time.

## Other failures

The new cancellation/replay/cold-restart fixture omitted shared embedding startup and masked its early task failure behind an event timeout. Other failing fixtures referenced removed MAX_ROWS, expected temporary lease pressure or local lane failure to be globally terminal, expected an older exception type/message, or imported a missing tokenizer helper. Two historical recovery tests assumed legacy unknown extraction usage could receive a fresh call allowance; the new persistent-budget policy correctly refuses that assumption, but those recovery tools/tests need an explicit compatible contract.

These11 failures remain recorded as failures until corrected and rechecked; they are not silently converted to passes. The16-user real-worker Add/Search test and the7 real page-fault cases passed.

## Static checks and unrun gates

Modified-file Ruff and whitespace checks passed. Mypy of24 changed/new runtime modules reports33 existing errors in two files; the normalized diagnostic multiset matches the fixed base, with no added diagnostics. This is not a clean type-check result.

The three skipped cases require real models. The later600-second real-model window was not opened because offline performance failed. No new SDK/model calls, formal database changes, deployment, platform resume or email was performed for this validation.

Private artifact digests: result `84178cf79c5d1c5332c642d8ec329e03828d53fc389e6bae9c0c582ba96fc063`; JUnit `f21cf857483c6015c339b6de902c8aca38fb62a5d43fe754d1ea46f1c916f85d`. Operational controls and raw logs are excluded from the public source package.
