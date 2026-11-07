# ARU-THROUGHPUT-1

This candidate replaces repeated publication-state polling with a scoped change notification. A waiting request captures the notification revision, performs the original authoritative Raw/reader/publication checks, and waits only if the revision has not changed. Every wake-up repeats those checks. A maximum one-second fallback covers missed external events; the original absolute request deadline remains in force.

Each configured scope owns one revision and event. Signalling replaces the event and broadcasts to all prior waiters; cancellation affects only the cancelled waiter. Successful derivation uses the existing settlement notification. Failed derivations send a separate notification only after the failure transaction completes. Failure notification does not mark publication dirty, reset attempts, or authorize success. Publisher completion/failure, recovery and shutdown also signal waiters.

The behavioral diff is limited to memory_service.py, daily_runtime.py and raw_message_worker.py. API/CLI/runtime version constants identify this candidate as ARU-THROUGHPUT-1. Models, prompts, CPU float32 embedding, user FIFO, lane count, schemas, extraction budgets, evidence checks, full retrieval and deadlines are unchanged.

The public baseline is commit53987cf97a35eddc9cab825647fbe626b5500997. See VALIDATION-ARU-THROUGHPUT-1.md for measured benefits and remaining failures. This candidate has not been deployed and does not establish a completed official Full.
