# AMC-ENTRY-1.8.1 validation snapshot

This file summarizes actual checks reported by the implementation owner and independently reviewed recovery evidence. Packaging itself did not run models, start services, send evaluation requests or repeat the test matrices.

## Related offline checks

- The earlier repair matrix finished with 412 passed, 1 skipped and 8 failed. Seven failures were missing inherited test helpers and one was a stale version assertion. Restoring the exact helpers and correcting the assertion produced 8 passes in the affected-only rerun. The 412 green cases were not repeated.
- Subsequent API, CLI and diagnostic lifetime checks: 56 passed. Changed implementation Ruff and limited Mypy checks passed; these were not full repository analyses.
- Clone identity and frozen-read checks were verified separately. A copied vector database retains its original file identity until the explicit reviewed migration; ordinary copying is not a safe deployment operation.
- Provider compatibility checks: 53 passed and one test-fixture failure. The fixture alone was corrected and its test passed. Other green tests were not repeated. Ruff, Mypy and diff checks passed.
- Remaining-task recovery checks: 40 passed and one test-fixture failure. The missing synthetic partition registration was corrected and that single case passed in 10.33 seconds. Ruff and limited Mypy passed. Tests covered history preservation, three retained successes, fifteen selected attempts, partial transactions, duplicate authorization, frozen checkpoint checks and abort while another call waits for the model-budget lock.

All these matrix results belong to the isolated implementation checkout. The public package has not repeated them or completed a fresh dependency installation.

## Bounded real recovery

The prior recovery made two provider calls and did not receive normally finished structured responses. Its exact finish reasons were not retained in the old diagnostics. The later successful run supports the compatibility fix; it does not retroactively prove the old finish reason or a network cause.

The new run retained three already successful derivations and selected only the remaining fifteen. All eighteen then succeeded, with zero failures. The retained three had no new attempt; the selected fifteen each gained exactly one. Three new provider calls were used, with the previous two permanently counted: five total.

The helper ended after 31.922 seconds. Its supervisor ended after 33.156 seconds with exit code zero, no forced stop and zero active child processes. Embedding, generation, IPC and model-owner counters settled to zero. The two original incomplete request records remained incomplete; the tool did not manufacture request completion. Normal runtime publication is required when the original request resumes.

Root, independent Python review and independent security review accepted this bounded recovery result for the already reviewed deployment step. It is not a general engineering or Full-capacity certificate.

## Deployment and official task snapshot

On November 3, 2026 at 07:31 UTC, the repaired 1.8.1 service was started in place using the recovered storage root and the existing public endpoint. Local and public health returned HTTP 200 with ready state and version AMC-ENTRY-1.8.1. Unauthenticated and invalid-body checks returned HTTP 401 and 422.

The existing official Smoke task was resumed through its actual website control. In the package-preparation snapshot, Add, Search and answer stages were complete and scoring was running at 20 of 46. Its historical version label and original concurrency settings remained attached to the original task. This is not a newly created 1.8 task or a completed Smoke result.

No passed Smoke, accepted Full, completed Full or leaderboard score is claimed. Endpoint reachability, a successful local recovery and version registration are separate facts.

## Historical evidence

The unchanged earlier validation narrative is retained as VALIDATION-1.6.md. JOB17.md, PATCH18.md and their metadata-only JSON reports retain their earlier results and limitations. Their counts and source closures describe those historical packages, not the new 202-file source inventory.

Private raw data, payloads, credentials, IDs, endpoints, recovery manifests and receipts are not redistributed here. SOURCE_MANIFEST.json contains only relative paths, byte lengths and source digests. This snapshot is bounded evidence, not a promise that every possible defect was found.
