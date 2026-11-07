# ARU-THROUGHPUT-1 validation

## Result and scope

The scoped optimization substantially reduced redundant status-query work in a synthetic publication barrier. It did not shorten the deliberately fixed waiting period, and no official Full speedup has been measured. The candidate is not deployed.

The 20-file related matrix ran once: **449 passed, 2 failed, 1 skipped**, in245.74seconds. The26 newly added notification cases passed. One failure was a missed runtime version constant; the constant was corrected and two version/restart checks passed in7.21seconds.

The other failure remains recorded: test_thread_dispatch_is_charged_to_the_public_entry_budget expects queue_ms to be populated after a150ms deadline with120ms of injected dispatch delay. If the thread has not acquired the transport before the remaining interval expires, the existing queue timeout path leaves queue_ms unset. The relevant transport sources and test are byte-identical to baseline53987cf and do not call the three changed business classes. A single baseline comparison passed in6.76seconds. This supports a timing-sensitive existing gap; it does not turn the candidate failure into a pass. The related matrix is therefore **not fully green**.

Changed-file Ruff passed. Mypy on the three changed business modules reported five constructor argument errors, identical to the same command on the fixed baseline. The larger inherited type-check debt was not rerun or repaired. Diff whitespace validation passed with core.whitespace=cr-at-eol, preserving existing runtime file bytes.

## One bounded synthetic A/B

Each version used5002 committed synthetic Raw records and outbox results in real temporary SQLite, followed by a30-second unchanged-state interval. MemoryService.status, DailyMemoryRuntime.semantic_publication_state and the Raw publication query were real. Reader/cache/publication completion were controlled synthetic peripherals. Publication was scheduled at30.25seconds for both variants.

| Metric | Baseline | Candidate | Reduction |
|---|---:|---:|---:|
| Status queries in the30-second unchanged interval |399|30|92.48%|
| CPU seconds during the publication barrier |5.859375|0.421875|92.80%|
| Process logical read bytes during the barrier |2233421164|171801628|92.31%|
| Barrier wall seconds |30.250|30.266|No wall-time improvement demonstrated|

Process read counters include cached/logical I/O and are not physical disk traffic. The fixed waiting period intentionally dominates wall time. Both variants returned current with no query errors. A measured zero-tick completion delay is timer resolution, not proof of zero latency. The fixture contains one synthetic entity derivation per source, not a full six-domain production workload.

The first benchmark launcher rejected the legitimate kernel namespace package during initialization after0.329seconds, before fixture creation or measurement. Its evidence was retained. A versioned tool corrected namespace validation, requiring every namespace path to remain inside the selected source root. There was then exactly one complete baseline measurement and one complete candidate measurement.

No cloud SDK call, real BGE loading, official data replay, service restart or platform evaluation submission was performed by this validation.

## Budget and evidence

All test steps and tool recovery remained under the same1200-second absolute budget: final elapsed1161.734seconds. Observed processes exited, frozen inputs remained unchanged, and no resource stop triggered. Peak sampled related-matrix RSS was1170841600bytes; final sampled temporary output was687905077bytes, below the4GiB RSS and2GiB temporary-data limits. Process sampling is not OS isolation or proof about unobserved processes.

Private evidence hashes:

- Matrix result: ed033ad9378648abbba87df5beaca38db5ad02ef7cddb0094be0e61e2aa43abe
- Matrix JUnit: 8dc00abdba4a85f85564c64ce484b3e123f5056420baf913d90e5839f5dcd3e5
- Complete baseline measurement: d8fb939e1337ce74e4ce17836917db437a64c56b4ca8a3ea98b23f97bb32499c
- Complete candidate measurement: 2121eb459d81124ef071af3cc6768c1fc7d9d79e3110bcc7e8d896182575a8e6

Independent Python and security reviews accepted the scoped code and interpretation of results. This is permission to retain the candidate, not a claim that every test passed or that deployment is approved. ARU-RECOVERY-2's inherited large-corpus Search deadline failures and score-reference gap remain unresolved.
