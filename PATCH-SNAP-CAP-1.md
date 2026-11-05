# SNAP-CAP-1 capacity repair candidate

Base commit: f07974a26552667b5fedda3a3c2a8f207ecc8d9c (AMC-ENTRY-1.8.4).
Status: undeployed candidate. Publishing this branch does not change a bound evaluation implementation or its accepted version.

## Change

Two runtime files have capacity changes: storage/daily_vector_snapshot.py and storage/supervised_daily_vector.py under apps/system_memory_god. Two additional files only update API/CLI version constants to SNAP-CAP-1 so that new code cannot report the inherited version label. The capture, publication and loader now enforce actual decoded vector bytes and canonical ID directory bytes before admitting or allocating a complete snapshot. Same-ID replacement uses byte deltas; captured receipts are copied to prevent caller mutation. A capacity failure remains sticky and does not rewrite a successful derivation into failure.

The old 5000-row threshold could stop snapshot publication after successful ingestion. It is removed without increasing the existing 40 MiB whole-generation numerical body, 2 MiB whole-generation ID directory or 768 MiB total cache. At 1024 dimensions, 10240 f32 rows occupy exactly 40 MiB; directory or retained-generation limits can reject earlier. This is not a guarantee that an entire evaluation corpus fits.

Snapshot schemas 1/2, the three files, source checks, generation hashes, leases, cleanup, complete-corpus cosine scoring, models, IPC and deadlines remain unchanged. No truncation, approximate retrieval, model replacement or concurrency change is included. It does not incorporate the separate undeployed empty-value candidate.

## Validation

The original 5002-row, 1024-dimensional capture failure was reproduced first. The related isolated matrix finished with 296 distinct passing tests and one intentionally skipped real-model probe. Four test-fixture failures in the first run were retained and individually rechecked after test-only corrections; passing groups were not rerun. The Unicode directory fixture was then changed to a non-emoji supplementary CJK character with the same canonical JSON width, and that single boundary case passed again.

Coverage includes 5000/5001/5002/10240 complete numerical capture, publication, load and a best hit beyond row5000; 2048-dimensional byte boundaries; low-dimensional rows above10240; rejection before over-budget file reads; same-ID accounting; malformed receipts; exact2 MiB and over-budget ID directories; inherited scope, lease, cleanup, checkpoint and API behavior. No live model, official evaluation database, 500-question run or whole-repository rerun was used.

Source and new test Ruff checks passed. Targeted two-module typing retains the same33 diagnostics as the frozen base; it is not fully type-clean. The isolated10240-row path took about1.882 seconds, with observed process RSS about577 MiB and historical peak about626 MiB. These include the test process and dependencies, not a bound for eight scopes or a full evaluation and not an online throughput measurement.

Actual production receipts, resource capacity, version admission and controlled deployment remain separate gates. Existing memory, credentials, request checkpoints and evaluation history must be retained. Keep deployment versions and fixed commits accurate; do not silently substitute this candidate under the old version label.

Existing MIT and third-party terms remain in force. This branch contains source and synthetic regression tests only.
