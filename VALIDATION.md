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
