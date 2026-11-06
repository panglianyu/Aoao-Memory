# ARU-RECOVERY-2

This isolated candidate starts at public commit `46fd94d0df89f89a132ea6d8eb35e66c2b4684db`. It is not an official deployment. Current validation results belong in VALIDATION-ARU-RECOVERY-2.md; historical release results do not establish this candidate's readiness.

## Implementation

- One generation owner coordinates shared embedding, writer and reader deadline handoff. A known unsent busy timeout does not poison unrelated work; unknown sent control results remain unavailable until actual retirement.
- Five structured domains share one persistent source/model/prompt call budget. Calls are preconsumed before the SDK boundary and retained across failures and restarts. Legacy unknown usage cannot become a fresh three-call allowance.
- The strict empty-entity policy preserves rejected-candidate evidence before caching a valid result. Other schema errors remain rejected.
- The challenge service reads persistent Chroma vectors in complete, source-qualified pages and computes exact global cosine TopK. Queries retain generation, scope, sequence, worker and end-of-stream bindings. No partial TopK is returned after a timeout or failed page.
- Readiness retains terminal publication and ownership failures. Replaying a completed Add still requires current Search readiness. Cancellation retains actual owners until their resources settle.

## Resource semantics

The 40 MiB numerical and 2 MiB metadata limits now apply to each active query's controlled working set. They are not whole-corpus capacity claims. Persistent database storage and Chroma's resident index are separate and must be measured. The total rebuildable cache bound remains 768 MiB.

The original two-second interactive query deadline, 1700-second HTTP deadline, two user lanes, one shared embedding process and32-request admission bound are unchanged. Correctness and resource bounds do not imply that large exact queries meet the latency requirement. Growth tests explicitly reject performance above the original deadline.

## Compatibility and evaluation data

Existing Raw records, revisions, user namespaces, idempotency records and failure history are preserved. Legacy snapshot readers remain supported. New extraction cache policy and persistent budget state require a reviewed deployment and rollback procedure; do not delete data or silently give a prior implementation new cache data it cannot understand.

The isolated matrix uses synthetic text and vectors, real temporary SQLite/Chroma stores and actual Windows subprocesses. The inherited real-model admission test is disabled by default and retains host-specific paths; it is not a portable live test or authority to load credentials. No public package contains an evaluation Key, Bearer token, formal evaluation corpus or operational controls.
