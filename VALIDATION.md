# Validation and version correspondence

This repository is a standalone source export. No private application history,
evaluation dataset, model weights, API credentials, memory database or live logs
are distributed.

## AMC-ENTRY-1.0

The 51 runtime Python files and 4 test files in SOURCE_MANIFEST.json are byte-for-byte
copies of the submitted service source. The main branch and AMC-ENTRY-1.0 tag identify
that version. The export omits unrelated host, persona, messaging and plugin modules.

Before export, the entry implementation passed 35 distinct focused checks across
HTTP, runtime, query limits and existing service/factory behavior, plus static checks.
Real integration used two synthetic users with two messages each. Add, publication,
retrieval, retry idempotency, cross-user isolation and switching back to a stored user
succeeded. Initial Add took about 18-20 seconds; switching back took 23.5 seconds.
Authenticated HTTPS Add/Search also succeeded on that small sample.

This is not evidence of Full-scale capacity, a better answer score, arbitrary batch
completion within the configured 100-second deadline, or a fresh-machine install.
No real service-process crash/restart was performed in that integration.

## AMC-ENTRY-1.1 candidate

The codex/amc-entry-1.1 branch contains the separately reviewed candidate. It adds a
real Windows data-root lock, makes HTTP timestamps strict integer milliseconds, and
updates entry version labels. It retains the AMC-ENTRY-1.0 storage marker.

The isolated candidate passed 39 focused checks (8 CLI, 31 HTTP), Ruff and compilation
for its five changed Python files. CLI checks used real Windows child processes for
file locking and explicit fake clients/runtimes for lifecycle failures. Reviewers
inspected code and supplied results without rerunning those tests. No new model
calls were used for that candidate validation. It has not replaced the submitted
online version or been bound to an official evaluation version.

## AMC-ENTRY-1.2 candidate

This branch inherits 1.1 and adds a shared request deadline with retained background
ownership, bounded option-assisted raw retrieval before final source verification,
and token-aware continuous original slices. It retains the 1.0 storage marker but
does not include migration or official version binding for existing data roots.

The isolated candidate passed 99 focused checks in 20.62 seconds across its six
test modules. The checks include actual temporary SQLite and Windows lock processes,
fake service/model dependencies, and one real offline fast-tokenizer coverage test.
After type-narrowing and an explicit CPU constructor check, 27 affected tests passed
in 0.85 seconds. Ruff and compilation passed on all 19 changed/new Python files.
The bounded Mypy check retains only four errors reproduced in the 1.1 baseline:
two HTTP schema annotations and two CLI optional-type annotations. No new Mypy
errors remain; this is not a claim that the full project passes type checking.

Independent Python and security static reviews found no blocking issue in this
candidate scope. No embedding weights were loaded for these tests, no embedding
inference or cloud model calls ran, and no 500-question or official evaluation ran.
Options may improve retrieval coverage, but a score improvement and official batch
capacity have not been established. The submitted online version remains 1.0.

The tokenizer was resolved from the exact local BGE revision recorded in the
manifest. Each vector slice is at most 8192 characters and 8192 tokens including
special tokens, with at most ten slices. Content that needs more slices, or cannot
be represented within non-whitespace payload limits, fails derivation explicitly
and retains its raw original. The tests do not prove actual embedding-weight
loading, inference performance, or arbitrary batch completion.

## Export verification

Source correspondence is recorded by relative path, byte size and SHA-256 in
SOURCE_MANIFEST.json. Packaging checks collect the synthetic tests and inspect the
dependency closure; they do not rerun model calls or the old full-repository matrix.
The original development repository's commits and private metadata are not ancestors
of this repository. Private sanitization reports are excluded from publication.

Model weights and installed packages are external assets. The listed BGE cache
reference is a metadata observation, not independent attestation of the weights
loaded by the online process. Dependency versions describe the existing validation
environment; a clean dependency installation has not been executed by this export.
