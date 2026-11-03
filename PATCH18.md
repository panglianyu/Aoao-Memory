# AMC-ENTRY-1.8 bounded recall patch

This candidate repairs two concrete recall defects in the shared memory kernel:

1. Request boilerplate could occupy all eight Chinese literal query terms. A finite request-phrase list is now replaced with separators; unique candidates are selected across the whole query, with at most eight terms including both ends. English stop words, the 32768-character boundary and literal fallback remain. This is a bounded lexical heuristic, not a general semantic-understanding claim.
2. Journal/schedule queries previously truncated lexical candidates before filtering their event timestamps. Windowed queries now use memory-ID keyset pages of 64, filter the half-open time window first, and inspect at most 2048 matching records per domain. Result overflow and unproven exhaustion at the scan cap retain the existing limited trace. Page/row processing keeps the caller's original absolute deadline and SQLite progress-handler cleanup.

The existing full profile already combines raw messages with vector, entity, graph, rule, journal and schedule evidence. It retains final source/revision checks. This patch reuses that path; it does not introduce a second fusion architecture or change entity/graph expansion or user isolation.

## Verification performed once after implementation

- Shared-kernel checkout: 58 passed in 9.38 seconds, covering query terms, complete domain-query tests, question-length compatibility, six-domain publication and source revalidation before publication.
- Isolated 1.8 candidate: inherited option-hint compatibility, 25 passed in 9.74 seconds.
- Ruff passed for the two changed implementations and two regression files. Independent Python review found no blocking issue in the bounded patch.
- The earlier implementation was first reproduced as failing. No model, official Smoke/Full, 500-question benchmark or unrelated full-repository matrix was run for this patch.
- A final review added the original long Chinese example about a prior coffee preference. The exact request phrase is separated while the temporal word, coffee and preference remain. Only that new regression and six affected phrase cases were checked: 7 passed in 0.31 seconds; the earlier green matrix was not repeated.
- Mypy passed for the two implementations using explicit package bases. The initial invocation without that repository setting stopped at duplicate module discovery before checking source; no product-code change was needed for that tool invocation.

The 58 checks belong to the shared-kernel checkout; the 25 hint checks belong to the isolated candidate. Source equivalence is checked when packaging. These are separate checks, not a claim of one 83-test public-package run. The portable package still excludes the larger private test tree.

## Limited deployment check and remaining boundary

An independently deployed 1.8 service passed one small public HTTPS fixture from 2026-11-02T12:53:14.6682879Z to 2026-11-02T12:54:05.4892212Z. One Add containing two 164-character synthetic messages returned HTTP 200 in 41.334 seconds with exact ID echoes; first/last marker Search requests returned HTTP 200 in 1.751 and 1.929 seconds and found their markers. Health returned HTTP 200/version 1.8, missing authorization returned 401, and an empty-scope Search returned HTTP 200 with no results. See validation-deployment18.json for metadata only. This was not a repeated recall-test matrix, a twenty-message public batch, official Smoke, or Full.

The synchronous CLI defaults to 1700 seconds and permits values up to 1700. The preceding README selected 100 seconds explicitly; that was a launch configuration, not a source-code ceiling. The current official documentation allows a single Add or Search request to run for up to 30 minutes, so an appropriate long-wait synchronous HTTPS path is possible without inventing a 202 status protocol. The internal 1.7 twenty-message job took 279.593 seconds, but it remains separate internal evidence. A separate diagnostic with a 140-second response delay returned HTTP 502 and did not prove public long-request support. The free hostname may change after several hours. This limited fixture does not establish long-running availability or Full readiness.

Deployment preparation changes only the displayed ENTRY_VERSION constants in the CLI and HTTP health/API module to AMC-ENTRY-1.8. Storage compatibility markers and request/response schemas remain unchanged. The public routes are /add and /search, with unauthenticated /health. The inherited Bearer was privately provisioned in the isolated new data root; no credential or actual endpoint is included here. Original version data was not reused or overwritten.

The platform's version-binding dialog offers a new version under the existing Key without another review. At this deployment-check snapshot the new 1.8 binding is pending and no official Smoke or Full has been started. Smoke/Full creation has separate eligibility and submission-window checks. Source publication alone does not bind a version, extend a quota or start an evaluation. The original 1.0 implementation and historical validation remain unchanged.
