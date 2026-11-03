# AMC-ENTRY-1.8.2 candidate validation

This records the isolated implementation checkout's results reported by Root and the implementation owners. Packaging only copied the source audit set, included synthetic test dependencies and checked the package structurally. It did not repeat the matrix, call models, modify official data or start evaluations.

## Regression evidence

- New extraction regressions first reproduced the old behavior: 10 failures and one pass. After implementation, the new module plus the inherited provider and diagnostic modules passed 52 tests. A later first-call guard regression was reproduced, then corrected; four affected guard/cancellation cases passed. The already-green modules were not repeated for that small correction.
- The old publication barrier was reproduced waiting after a real isolated Raw task had failed; a one-second outer wait timed out. The terminal implementation's 16 new cases subsequently passed, as did three relevant existing publisher/metadata-owner cases. Two intermediate failures belonged to new fixture setup and were corrected with affected-only reruns.
- Root ran one matrix across 22 relevant modules with real-model gates disabled: 384 passed, one skipped and one failure in 165.09 seconds. The failure exposed the CLI version constant still reporting 1.8.1. Root corrected that constant and reran only the failing case: one pass. Across those runs, 385 distinct matrix cases passed and one was skipped; the whole matrix was not rerun after the version-only correction.
- Changed Python files passed Ruff and diff checks. Independent Python and security reviews accepted the two business fixes. The new integration cases cover Raw-to-publication behavior, fast ChallengeRuntime failure and request idempotency; the HTTP503 mapping is covered by the inherited API test, not by a new real HTTP/model end-to-end run.

These checks used synthetic extraction/embedding boundaries. No new BGE load, cloud generation, official Smoke, official Full or formal-data recovery was part of this validation.

## Type checking

Limited Mypy checks on Raw/Daily reported six existing `arg-type` findings at unchanged Daily runtime parameter-dictionary calls. The same correctly mapped command against frozen 1.8.1 produced the same six findings; no new Raw/Daily finding was reported. An earlier command was rejected for duplicate module mapping before useful type results were obtained.

The separate extractor/API/CLI check found no extractor or API error and one assignment error at the unchanged CLI client-construction line. The same three-file command and parameters against frozen 1.8.1 produced that same single assignment finding. Together with the six Raw/Daily findings above, these limited checks retain seven existing type-check findings and report no new ones. Mypy is not clean, and this was not a full-repository type check.

## Source and package checks

[SOURCE_MANIFEST.json](SOURCE_MANIFEST.json) identifies 202 candidate runtime Python files. All are copied byte-for-byte from the isolated implementation checkout. The two new regression modules and their static `tests.*` import closure are included. AST, source-manifest and package-manifest verification are packaging checks, not a replacement for execution of tests or a fresh dependency installation.

## Still unverified

- Deployment and continuous survival of 1.8.2 under an independent process supervisor.
- A stable public hostname, long-request behavior and available account quotas for the complete official workload.
- The original Full task's association with the changed implementation and endpoint.
- A separately bounded recovery of its failed work, followed by a legitimate resume of the original task.
- Full completion, score, full-scale capacity and production privacy acceptance.

The earlier official Smoke succeeded 46/46 under the original resumed task, retaining its historical label. The subsequent Full failed during Add. These historical facts do not certify this candidate. [VALIDATION-1.8.1.md](VALIDATION-1.8.1.md) preserves the original package's earlier in-progress snapshot unchanged; the new note does not rewrite those prior observations into later successes.
