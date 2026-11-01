# Local package preparation

This is an independent AMC-ENTRY-1.6 candidate runtime package. It contains 54 selected Python runtime files, inherited public licensing/dependency/setup materials, an invalid provider example, and candidate documentation. It contains no tests or private Aoao OS Git history.

## Source and packaging scope

The runtime closure starts at Tools/serve_memory_challenge.py and the independent memory_factory path. It includes function-local imports and multiprocessing worker modules. TYPE_CHECKING-only references do not justify copying the Aoao OS host assistant. Brain, AppBus, channel integrations, MCP/web-research subsystems, old evaluation runners, private configuration, state, logs, and model files were excluded.

An earlier candidate source-final.json described a 1.5-era 216-file tree. It was not copied or relabeled as 1.6. The final implementation freeze has 219 Python files; its exact reference is recorded in VALIDATION.md and SOURCE_MANIFEST.json. The independent package retains only the 54 runtime files, verified byte-equal to that freeze. PACKAGE_MANIFEST.json uses a fixed whitelist: 54 source files plus thirteen hand-prepared materials including the manifest itself. Its 66 entries exclude itself to avoid a self-hash cycle. It does not enumerate the local directory; generated __pycache__, pyc files, data and logs from local runtime checks are excluded.

The inherited LICENSE, requirements files, setup.ps1 and .gitignore are preserved. THIRD_PARTY_NOTICES.md changes the candidate version reference only. The one source file using CRLF was copied by the explicitly authorized raw-byte method because apply_patch normalizes line endings; its bytes are compared directly with the candidate. All other runtime source files are copied without content changes.

## Sanitization coverage

Preparation inspected the selected source closure, Python import edges, hardcoded path locations, and limited recognizable credential/private-key patterns in the candidate Python source. No recognizable provider/GitHub/Slack/AWS token or private-key-header pattern was found in that scan. This is scoped evidence, not a claim that every possible secret or every repository file has been audited.

No private provider configuration, credential file, database, log, benchmark content, installed library source, model weight, cache, or Git history was read or copied to construct this package. The provider example uses provider.invalid and INVALID_EXAMPLE_DO_NOT_USE. README paths are illustrative locations, not copied account details.

## Remaining release gaps

- Any later implementation edit requires a new byte comparison and manifest update; the current inventory identifies the completed frozen-source synchronization.
- The reported 46-module import check passed without constructing models or services. A separate isolated service startup and seven-request public TLS probe also passed in the existing environment. A fresh dependency installation remains untested; these checks do not certify all external-library behavior.
- The complete private tests were deliberately excluded. Some depend on private machine paths or absent probe helpers; they must not be represented as a portable public test suite.
- Third-party license labels and versions come from the prior public reference materials; transitive licenses and current installed distributions were not re-audited here.
- The single bounded local real BGE/provider gate and separate seven-request public TLS probe passed as recorded in VALIDATION.md. Long-running availability, formal input/data scale, and official Smoke/Full remain pending, independently of source publication.

Publishing this candidate source does not change the accepted online 1.0 version, its official binding, or the organizer's version/capacity and Smoke/Full requirements.
