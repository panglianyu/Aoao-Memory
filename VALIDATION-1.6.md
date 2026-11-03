# Candidate validation boundary

Package version: AMC-ENTRY-1.6. Runtime source is synchronized to the frozen implementation. Local real-model result review is Go within the limited gate below. This document does not approve deployment, official version binding, or an official evaluation.

## Accepted service and inherited baseline

The accepted API source remains AMC-ENTRY-1.0, fixed public commit e5d5251269d47b27b006aa12df728897591bc90a. This 1.6 package has not replaced that implementation, its data, or its bound endpoints.

The prior 1.5 one-shot local real-model gate completed 16 synthetic-user Adds successfully, followed by one successful Search and fifteen HTTP 503 Search responses. Fourteen failed Search requests did not enter the runtime. That gate failed; its successful Adds were not proof of complete Search behavior, formal capacity, or official Smoke/Full.

## 1.6 change and current local evidence

The first real MemoryService/Chroma/Windows worker reproduction with fake model dependencies failed during service retirement. Snapshot root cleanup requested WRITE access to the common temporary-directory anchor while another cache guard shared only READ. Native sharing violation 32 was wrapped as OSError(32, ...), which could appear as Python BrokenPipeError. That name did not identify an IPC transport failure.

The minimal filesystem change is confined to SnapshotFileOps._hold: every directory guard now uses the existing read-only bound-handle helper, starting from the captured anchor identity. No WRITE/DELETE sharing was added. Reparse, final-path, file identity, retained-handle, and bound-object deletion checks remain in place. The kernel-wide directory helper was not changed for this fix.

The implementation owner reported the four targeted native checks changing from two failures/two passes before the fix to four passes in 6.40 seconds after it. Those checks cover sibling-cache capture/cleanup coexistence and rejection of WRITE and DELETE opens against the shared guard.

The implementation owner also reported one passing real Windows worker test after the fix, taking 66.63 seconds. It exercises the actual MemoryService, DailyMemoryRuntime, Chroma, ASGI boundary, native cleanup, and worker processes, while embedding and LLM dependencies are explicit fakes. All sixteen Add responses and sixteen Search responses were HTTP 200, with own-marker isolation checks passing. The reported maximum HTTP durations were 37.375 seconds for Add and 21.094 seconds for Search.

That fake-model run created 32 services and 32 embedding generations. Its observed shared-owner counters were load_count=1, model_owner_peak=1, active_generation_peak=2, and ipc_peak=2. These are fake-dependency owner observations, not evidence that BGE weights loaded. Cleanup completed, and none of the 95 observed process IDs remained at the final check. This is not a real BGE, cloud API, public-network, quality, or formal-capacity result.

The implementation owner reported the one 23-file matrix finishing in 346.24 seconds with 446 passes, two failures, and four skips. Both failures occurred before the copy-probe body ran: the selected basetemp was outside the system TEMP directory required by an existing helper, producing ValueError. Without changing the tested source, a separate default-TEMP run of only those two tests passed in 13.53 seconds. The full matrix was not rerun. Preserve both observations; do not present them as one clean run.

The limited 13-file Mypy check reported 43 errors in four source files. The owner compared those files with 1.5 and found them unchanged or changed only in their version string; two newly introduced diagnostic typing errors had been fixed. Mypy is not reported as passing.

The implementation owner froze the 219-file implementation at private evidence commit 9a6bb842094bdf3091c0086b8ad6b6bda6b72b54. Its source manifest contains 219 Python files and 3,250,420 bytes; SHA256 is 9A9846B469824F02E6EBA69118FF7E2EB9EC7F6BE16DF3F3F2CC20CA0D714063. All listed frozen file bytes and the three real-gate source pins were verified after the run. This independent package contains only 54 required runtime files, all byte-equal to that freeze. The private evidence commit is not a published package commit and its Git history is not included.

## One local real BGE/provider gate

The unique new-root 1.6 gate completed as one passing pytest case in 122.51 seconds. The original report is 36,890 bytes with SHA256 A6B12C32C57B8E2AE2D033FDBAEE6BBF16BAF13BB26136FA325BB2A80BD60B42. Its UTC strings are 2026-11-01T20:07:52.164612+00:00 through 2026-11-01T20:09:53.727782+00:00. The report records monotonic runner wall time 122.469 seconds, model startup 10.312 seconds, and request-wave work 109.781 seconds; the overall 600-second cooperative budget was not exceeded.

The workload sent 32 ASGI HTTP requests: sixteen Add and sixteen Search for sixteen synthetic users and sessions. Four Add batches contained two messages, for twenty messages total. Every Search used the same 292-character query and top_k=100. All 32 responses were valid HTTP 200, own-marker isolation checks passed, and all request owners were observed idle. Maximum HTTP durations were 83.047 seconds for Add and 26.640 seconds for Search; maximum owner end-to-end durations were 83.047 and 26.625 seconds respectively, within each original 100-second request deadline.

All nine report gates were true. A single BGE owner recorded load_count=1, model_owner_peak=1, active_generation_peak=2, ipc_peak=2, and 32 total generations. Final model owner count, active generation count, IPC in-flight count, and current generation were all zero; active_generations was empty, clean_close=true, and failed=false. Both lane diagnostics were stopped with no failures. Failure, cleanup-failure, report-failure, and orphan-phase fields were empty/zero.

There were 214 valid memory samples: observed peak runner/descendant RSS was 3,063,980,032 bytes, and observed minimum machine-available memory was 8,137,244,672 bytes. There was no sampling error or sampled resource stop. These are sampled observations, not hard OS memory bounds or universal capacity guarantees.

The report hash, request records, counter consistency, cleanup, diagnostic scope, and source pins received an independent read-only native/security ResultGo for this bounded local gate. Report diagnostics contain relative source frames, closed error classes and numeric OS codes; no request text, user IDs, provider configuration or credential values were found in the reviewed report. The report and private logs are not redistributed in this package.

The implementation owner also reported a passing minimal-package import check covering 46 modules, with no project import escaping the package and no model or service constructed. This import check is not a fresh dependency installation or a service startup test.

## Separate isolated HTTPS probe

After the local gate, the implementation owner started a separate candidate service on loopback port 8796 with its own temporary HTTPS tunnel, leaving the accepted service on port 8795 and its tunnel and official binding unchanged. The seven-request probe passed in 24.672 seconds through actual TLS. Health reported AMC-ENTRY-1.6; the unauthorized request returned HTTP 401. Two Add requests completed in 16.125 and 21.204 seconds, the same-ID retry in 0.421 seconds, and two Search requests in 0.672 and 1.094 seconds. Responses and own-marker isolation were validated.

The probe used the [temporary candidate HTTPS endpoint](https://oldest-actual-newest-adding.trycloudflare.com). A Quick Tunnel may change or expire and has no availability guarantee. The probe proves the tested request chain at that time, not 24-hour uptime, formal input sizes, full-dataset throughput, an organizer-approved binding, or an official Smoke/Full result. No credential or synthetic message/marker is included here.

## Checks not completed by this package preparation

- A fresh Windows/Python installation from the included dependency pins; the separate import and startup/HTTPS checks used the existing reference environment.
- Long-running operational behavior and 24-hour public HTTPS availability.
- Formal input sizes, full-dataset storage and throughput, and measured quality improvement.
- Organizer version/capacity review, approved version binding, official Smoke, official Full, and public-board review.

Package preparation itself started no model, provider, service, tunnel, or official evaluation. The local real-model gate above was the separately authorized implementation run. No previous failure was converted into a pass, and the original task/state was not copied into this package.
