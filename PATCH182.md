# AMC-ENTRY-1.8.2 candidate changes

This candidate fixes the isolated extraction and publication failure paths found after the original official Smoke succeeded. It is not a claim that the official Full has resumed or passed.

## Same-source extraction correction

Previously, one invalid grouped response rejected all five structured domains. Only successful responses entered the shared cache, so another target domain could call the model again with the same input. Independent per-domain retries amplified a single source failure.

The extractor now keeps a bounded, instance-local slot keyed by scope, source identity and content hash. It counts each request before entering the SDK. Cancellation and transport exceptions do not refund started calls. Invalid JSON, group shape or domain payload results may receive a static correction instruction, with at most three started SDK calls for that key in the instance. Once exhausted, other domains share closed failure metadata instead of issuing more calls. The slot table is capped at 128; exhausted slots are not evicted to refill budgets. A valid durable cache is checked first and clears a previous RAM slot.

Raw guards remain necessary before calls, failure reuse and output persistence. The original strict five-domain validator remains unchanged. No malformed result, guessed timestamp or silently dropped record is committed as success. The correction instruction includes no previous model response or arbitrary exception text.

The provider, model, base system prompt, base prompt hash and v1 field contract remain unchanged. The new correction message changes the later request policy and is explicitly part of the 1.8.2 implementation. It is not represented as byte-equivalent to the old requests. The RAM limit ends with the extractor instance; real recovery still needs its own durable scope and budget.

## Terminal publication state

The original two-field `partition_state()` ABI remains available. A new atomic `partition_publication_state()` returns generation, resolution and current-source terminal-failure state from the same Raw read transaction. The failure query is scoped to the same agent, user, connector and conversation, active or edited messages, and their current revisions.

The Daily publication barrier returns `unavailable / derived_unavailable / terminal=True` for such a failure after its existing lifecycle and publisher checks. The synchronous Add path therefore stops waiting and uses its existing error response. Pending or leased work, old revisions, retired messages and other scopes do not become terminal. The patch does not reset attempts, recover failed work automatically, change schemas or acknowledge incomplete requests.

## Source and verification boundaries

Five runtime Python files differ from the 1.8.1 source audit set: the extractor, Raw store, Daily runtime and the API/CLI version constants. The 202-file inventory matches the isolated candidate bytes. The added regression modules include the existing synthetic fixture modules they import; those inherited fixture modules are not additional product changes.

See [VALIDATION-1.8.2.md](VALIDATION-1.8.2.md) for actual validation, preserved failures and unverified operational steps. Existing public history, MIT licensing and third-party attribution remain intact. Private deployment controls, credentials and evaluation data are not included.
