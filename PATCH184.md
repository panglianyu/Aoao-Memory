# AMC-ENTRY-1.8.4 time-representation policy

The isolated implementation is frozen, and its 202-file runtime inventory is copied byte-for-byte into this public candidate. Relevant synthetic checks, scoped code reviews and one bounded real-source representation check have completed. Deployment, official recovery and Full readiness remain unverified.

## Evidence and limits

The prior unsuccessful 1.8.2 check started three SDK calls. A separate diagnostic retained time-error categories and lengths, not the actual strings. A later one-call 1.8.3 validation failed with reason `unknown`; its fence may have obscured the underlying failure. One further diagnostic retained metadata for nine journal time values classified as `other`, each of length 4. Their actual content is not known; length alone does not establish a year or any other literal value.

Those first six started SDK calls comprised three unsuccessful check calls, one diagnostic, one unsuccessful new validation and one later diagnostic. They had not established an accepted valid result or official recovery. Earlier failures, unpublished candidate evidence and the original task history are retained rather than relabeled as successful repairs. No original memory or model response is included here.

The subsequent 1.8.4 first-request observer completed one SDK call and produced a strictly valid grouped result. Root, Python and security result reviews accepted this single-case outcome; its artifact remains private. Ten journal time literals were preserved as text with null timestamps. The cumulative total is seven started SDK calls. The five failed tasks and formal cache remain unchanged, and there has been no deployment or official recovery. The one-case representation result does not establish semantic QA, a new official Smoke or Full success.

## Implemented behavior

Only exact two-field journal records (`text`, `occurred_at`) and schedule records (`text`, `scheduled_at`) are eligible. The original text and a non-null time value must each be nonempty, valid UTF-8 strings of at most 8192 characters. Non-string, blank, invalid-Unicode and overlong values remain rejected under the final contract; malformed arrays, records, fields and other domains are not repaired by this policy.

An existing parseable timezone-aware timestamp remains unchanged. A strict ASCII uppercase RFC3339 `T`/`Z` form, with seconds and optional fractional seconds, may be represented as the equivalent `+00:00` instant only if its complete form, calendar and aware-time parse are valid. This does not infer a time zone.

Other eligible time strings that cannot represent an absolute instant are preserved literally: append one newline followed by every original character to the original text, then use a null timestamp. The combined text must fit the existing 8192-character limit. This includes partial dates, natural language, naive timestamps and invalid calendar strings. Such values are not certified as dates; they are no longer rejected solely for being non-instant time text. There is no stripping, format or calendar repair, truncation, dropped record, guessed day or guessed time zone.

## Policy extension and indexing tradeoff

This is an explicit extension of accepted input representations beyond 1.8.3. The final strict payload validator, base prompt/hash/spec identity, SDK budget and Raw guards remain unchanged; the normalization policy before validation changes. It must not be described as the same input policy as the earlier candidates.

Literal preservation is not an absolute-time index. A null timestamp means that record cannot supply an absolute instant for time-window filtering. Its literal text remains available as evidence, but temporal recall and complete-workload quality still need their own evidence. The retained text does not establish the date's validity, zone or precision.

## Scope and validation

The business edit is limited to grouped parsing and time regressions, alongside release version constants and their version assertion. Existing invalid-timestamp fixtures now keep testing genuinely rejected values such as non-string inputs. One relevant nine-module matrix passed 193 tests in 25.43 seconds without skips or model calls. Seven changed Python files passed Ruff, and Root, Python and security code reviews accepted the source. Limited Mypy retains one existing CLI assignment finding and reports no new finding in its three-source scope.

Relative to public 1.8.2, three runtime files differ logically and twelve differ only in line endings. They are copied unchanged from the frozen candidate; these line endings are not additional business edits. Existing synthetic test helpers are retained with the minimal public test configuration. Packaging checks the source/test structure and byte inventories without repeating the business matrix. It does not run a model, modify official memory, recover failed tasks, commit or push. The ngrok dev domain and original task's endpoint/version association remain unresolved. The inherited budget/publication changes and their historical checks remain documented in [PATCH182.md](PATCH182.md) and [VALIDATION-1.8.2.md](VALIDATION-1.8.2.md). New results belong in [VALIDATION-1.8.4.md](VALIDATION-1.8.4.md). Existing MIT licensing and third-party attribution remain in force.
