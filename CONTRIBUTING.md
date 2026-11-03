# Contributing

Use 64-bit Windows and Python 3.10. Follow README.md for setup, the separately provisioned model cache and private configuration. The setup script installs dependencies; it does not contact a model provider or start the service.

Create a focused branch from the appropriate public version. Keep accepted-version tags unchanged. Explain the trigger, resulting behavior, affected interfaces, migration and failure behavior before implementation; review the concrete plan before changing architecture or storage authority.

Use small, reviewable changes, explicit scope and ownership boundaries, monotonic deadlines, strict validation and retained resource owners. Preserve user isolation, idempotency, request/source identities and failure history. Do not turn a missing result or an HTTP timeout into an assumed success.

The README lists two portable offline test modules. Additional included recovery fixtures assume the reference Windows short-path layout. Use fake model dependencies; do not remove the default guard on real model loading to make a unit test pass. Run the affected matrix once, then repeat only checks justified by a changed implementation or unresolved failure. Record skips, dependency limits and unrun checks.

For a pull request, state the problem, behavior change, validation and remaining limits. Include synthetic reproduction data rather than private requests. Never attach credentials, production databases, original messages, private recovery manifests or logs. Report sensitive findings privately rather than in a public issue.

## Using Codex

Codex can read AGENTS.md for commands and source boundaries. Keep it accurate when entrypoints change; do not add hypothetical or unverified commands.
