# AMC-ENTRY-1.7 internal durable Add jobs

## Scope

This candidate adds ChallengeJobStore and ChallengeJobs. They provide durable internal Python job ownership while retaining the original messages, request ledger, and six derived memory domains. They do not add a public HTTP 202/status API. The existing HTTP CLI still reports AMC-ENTRY-1.6 and keeps the synchronous Add/Search contract. 1.7 is not deployed and does not claim official Smoke, Full readiness, or a Full score.

A new job receives its first monotonic deadline once, with at most 1700 seconds including queue time; Search keeps 100 seconds. Polling, duplicates, cancellation, and restart do not renew the job budget. The coordinator uses thirty work slots and at most two short concurrent status reads, with one shared model and at most two runtime lanes/generations/IPC operations. The one-job measurement below exercised one generation, not a two-lane throughput claim.

Job receipts distinguish QUEUED/RUNNING/SETTLING from SUCCEEDED/FAILED. Same-user/same-request duplicates return the original job; a different body conflicts. The first received UTC is preserved through the existing ingestion ledger. Receipt acceptance alone never means data is complete.

The admission ticket remains owned until the runtime has actually settled and terminal state is durable. Same-user acceptance is ordered before enqueue. Terminal-write failure keeps ownership and fails closed; observers are woken to an explicit unresolved state. A runtime-root lease plus a fixed jobs-directory OS lock prevents a second owner. Restart marks old unfinished jobs interrupted instead of automatically re-running them. Terminal job payloads are cleared while original raw messages and ingestion checkpoints remain retained.

SQLite uses secure_delete, DELETE journaling, 4096-byte pages and an enforced maximum of 65536 pages (256 MiB); terminal history is limited to 65536 jobs and capacity exhaustion refuses new acceptance rather than deleting history. This is a job-store bound, not a bound on original memory data or overall process RAM.

## Actual bounded evidence

The related isolated implementation matrix completed with **495 passed, 4 skipped in 354.52 seconds**. This package deliberately excludes that private test checkout and its logs. One real BGE/provider job validation then ran from 2026-11-02T06:29:59.858861+00:00 to 2026-11-02T06:34:50.967669+00:00:

| Observation | Actual result |
|---|---|
| Workload | One Add with twenty synthetic messages, 164 characters each |
| Receipt acceptance | 0.015 seconds |
| Job completion | SUCCEEDED in 279.593 seconds |
| Original ingestion | 20/20 messages, request complete |
| Derived memory | 20 succeeded in each of entity, graph, journal, rule, schedule and vector; 120 total |
| Retry evidence | 7 retry events retained |
| Search | First and last message markers found; 0.219 and 0.266 seconds |
| Shared model | Loaded once; model-owner peak 1, generation peak 1, IPC peak 1 |
| Cleanup | Owner idle observed; final managed counts 0, clean_close true, failed false |
| Resource sampling | 528 samples; peak sampled runner/descendant RSS 2829910016 bytes; minimum machine available 5509607424 bytes |
| Observed child processes | 4 observed; 0 still live and 0 unknown after cleanup |
| Runner wall time | 291.125 seconds, including startup and cleanup |

The exact metadata report is [validation-job17.json](validation-job17.json), 2223 bytes, SHA256 **ae3ca693deb3fdc6792d6e40945fbb21357081948015ba6bfc23c5374c539c68**. Its source_before/source_after each record 193 files and digest **7e0a5fc3584fb1df614604dc9b1fc11693d71cc71fc429e91cd817504329c55a**. That is the larger private validation tree. The separate SOURCE_MANIFEST.json pins the 57 copied publication sources; these are different inventories.

## Remaining integration boundary

The prior 1.6 concurrent gate used sixteen Add calls containing twenty messages in total; it did not prove one twenty-message Add. A subsequent single twenty-message HTTPS Add failed with HTTP 504 after 101.234 seconds. Search was not attempted in that failed check, and candidate health was then unavailable. This 1.7 internal result proves completion under its new bounded job budget rather than a sub-100-second synchronous HTTP Add.

The complete organizer job-status/poll protocol has not been established. This candidate therefore does not guess response fields, interpret a 202 response as completion, submit a new official task, overwrite the accepted 1.0 implementation or data, or claim to resume the old task with changed code. External adapter integration and organizer version/capacity acceptance remain necessary before official Full can start.

The copied validation driver is Tools/validate_memory_challenge_batch_job.py. It requires an explicit fresh isolated data root and a private provider configuration, uses the local pinned model cache, writes original messages to its isolated runtime state, invokes real model/provider work, and writes a metadata-only report. It is not an HTTP server and is not run by the setup script. Re-running it incurs work and provider usage; the included report is the completed measurement, not a promise of equal timing on another machine.

Resource measurements are samples, not an OS-enforced memory guarantee. This single short synthetic batch does not prove official dataset scale, maximum input lengths, retrieval quality, long-running availability, fresh installation reproducibility, or Full capacity. The original 1.6 validation history is preserved unchanged in VALIDATION.md.
