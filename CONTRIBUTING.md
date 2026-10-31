# Contributing

Use the Windows/Python setup in README.md and commands in AGENTS.md. The installer is explicit and does not provision models or a provider credential.

Create a focused branch and explain the observed problem, resulting behavior, and relevant validation in the pull request. Separate source changes from model, data, or evaluation changes. Preserve public API compatibility, trusted Scope boundaries, durable idempotency, publication ordering, and lifecycle ownership.

For ordinary changes, use synthetic tests in isolated temporary roots. Run only the affected checks while developing, then the agreed release checks once. Paid-provider calls, model downloads, load tests, and official evaluations need explicit scope.

When reporting a bug, include the source ref, Windows/Python versions, redacted reproduction steps, expected/actual behavior, and a synthetic example. Never attach keys, private provider files, databases, logs containing conversations, or restricted evaluation data.

The submitted 1.0 API and the 1.1 candidate are different version identities. A merged source change does not automatically update a deployed or platform-bound version.

## Using Codex

AGENTS.md describes the layout and actual commands. Ask Codex to inspect the relevant interfaces before editing, preserve unrelated changes, and report what was checked and what remains unverified.

