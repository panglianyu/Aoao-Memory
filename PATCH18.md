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

## Remaining deployment boundary

1.8 is a source candidate, not a new live endpoint. It inherits 1.7's internal durable Add-job core and the older synchronous 100-second HTTP CLI. The old real twenty-message job took 279.593 seconds; these two recall patches do not resolve that external Add request-budget mismatch. No asynchronous HTTP adapter is invented here and no official Full-readiness claim is made.

The platform's version-binding dialog offers a new version under the existing Key without another review. Binding requires a real new-version Add/Search endpoint; Smoke/Full creation has separate eligibility and submission-window checks. Source publication alone does not bind a version, extend a quota or start an evaluation. The accepted 1.0 implementation and historical 1.7 validation remain unchanged.
