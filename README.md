# aru memory

A Windows Add/Search memory service. ARU-RECOVERY-2 is an isolated, undeployed integrated repair candidate based on SNAP-CAP-1 commit `46fd94d0df89f89a132ea6d8eb35e66c2b4684db`. It retains original messages and scoped vector, entity, graph, rule, journal and schedule evidence. Private configuration, credentials, runtime data, models, operational controls and host-assistant history are excluded.

## Candidate status

ARU-RECOVERY-2 combines atomic deadline handoff, persistent shared extraction-call accounting, the strict empty-entity policy, complete paged exact vector search, and readiness that retains terminal failure causes. The default challenge path uses the existing persistent Chroma vectors instead of repeatedly publishing a complete numerical snapshot. Legacy snapshot readers remain available for compatibility.

This deliberately changes resource accounting: 40 MiB bounds each active query's controlled numerical working set and 2 MiB bounds its paged proof metadata; they no longer bound the whole corpus. Persistent storage and Chroma's resident index are separate. The 768 MiB total rebuildable cache limit, two lanes, one shared embedding process, queue limit32, 1700-second HTTP deadline and original two-second interactive query deadline remain. Full-corpus exact retrieval must meet that deadline; timeouts never authorize partial results.

The related matrix and targeted corrections have completed, with remaining growth failures:5002–40000-vector exact queries still exceed the original two-second deadline.40000-vector cold publication now completes in53.218 seconds;1000-vector search returns in0.875 seconds but its strict score-reference comparison remains unresolved. **This candidate is not ready to deploy.** See [VALIDATION-ARU-RECOVERY-2.md](VALIDATION-ARU-RECOVERY-2.md) for actual results. Historical SNAP-CAP-1 evidence is retained in [PATCH-SNAP-CAP-1.md](PATCH-SNAP-CAP-1.md).

The following 1.8.4 notes describe the inherited base and historical validation, not the deployment or acceptance status of this candidate.

The 1.8.4 policy preserves eligible nonempty time strings as literal text when they cannot represent an absolute instant. The complete original string is appended to valid record text and the timestamp becomes null, within the existing text limit. This includes partial dates, natural language, naive timestamps and invalid calendar strings; they are retained as literals, not certified as valid dates. It deliberately expands accepted input representations. See [PATCH184.md](PATCH184.md) and [VALIDATION-1.8.4.md](VALIDATION-1.8.4.md).

Existing parseable timezone-aware instants and null remain unchanged. A strictly recognized, calendar-valid uppercase RFC3339 UTC `Z` form may use its equivalent `+00:00` representation. Other non-string, blank, invalid-Unicode or overlong values, invalid original text and malformed shapes remain rejected. A null timestamp has no absolute-time-window index: retaining the literal in text does not recover a time zone or guarantee temporal recall.

The inherited 1.8.2 same-source correction budget and terminal-publication behavior remain documented in [PATCH182.md](PATCH182.md) and [VALIDATION-1.8.2.md](VALIDATION-1.8.2.md). The existing strict validator, base prompt/hash, SDK budget and Raw guards are unchanged; the accepted representation policy around the validator is expanded explicitly.

The first six SDK calls comprised the prior three-call unsuccessful check, one diagnostic, one unsuccessful 1.8.3 validation and one later diagnostic. The unsuccessful validation's reason remained `unknown`; its one-call fence may have obscured the underlying failure. That diagnostic metadata described nine journal time values as `other` with length 4; actual values were not retained, so their content is not known and must not be guessed as years. At that stage, there was no accepted valid result or official recovery.

The bounded 1.8.4 first-request observer has now completed one SDK call and produced a strictly valid grouped result. Root, Python and security result reviews accepted that single-case result, retained as a private artifact. Ten journal time literals were preserved as text with null timestamps. The cumulative total is seven SDK calls; all earlier failures remain in the record. This establishes one selected source's representation check. Semantic QA, new official Smoke, Full success and recovery of the five failed tasks remain unverified; the formal cache and failed-task state were not changed.

One relevant nine-module synthetic matrix passed 193 tests in 25.43 seconds, without skips or model calls. Changed-file Ruff and independent Python, security and Root code reviews accepted the scoped implementation. Limited Mypy retains one existing CLI finding; these synthetic checks alone do not establish real-source or Full success.

The original official Smoke subsequently completed 46/46. A Full task was then started and failed during Add; later attempts also encountered an unavailable public ingress. Subsequent operational recovery and 1.8.4 deployment are separate from this candidate. The earlier snapshots of endpoint and deployment status in historical validation documents are not current service checks. ARU-RECOVERY-2 has not passed an official Smoke or Full.

Publishing this source does not change the entry's industrial group, grant evaluation quota, or establish compliance with the open-source track's prescribed models. The accepted 1.0 source and 1.8.1 fixed commit remain unchanged. Earlier evidence is retained in [VALIDATION-1.8.1.md](VALIDATION-1.8.1.md), [VALIDATION-1.6.md](VALIDATION-1.6.md), [JOB17.md](JOB17.md), [PATCH18.md](PATCH18.md) and their metadata-only reports.

## Runtime

Enable two lanes and shared embedding explicitly. Each lane owns one active user runtime; user data stays separate. Both lanes share one BGE process, with at most two active generations and one actual embedding call at a time. A global admission limit of 32 covers reading, waiting, execution and unsettled owners. These are resource bounds, not throughput guarantees.

Add succeeds only after durable ingestion and publication. Same-ID/same-body retries reuse or resume prior work; changed bodies conflict. A current, active source with a terminal failed derivation now ends publication waiting with an unavailable state. The API keeps its existing failure response instead of acknowledging incomplete work. Pending work, older revisions and other users or conversations are not promoted to terminal failures.

Search merges the scoped retrieval paths, verifies current source revisions and returns evidence rather than generating an answer. Original text is not silently truncated: vector slices must fit the configured token, character and chunk limits.

The documented request deadline is 1700 seconds, including body reading and queue time. A tunnel or reverse proxy may impose a shorter limit. Cancellation does not release a user lane until its real work and resources settle. A temporary public hostname is an operational dependency, not a multi-day availability guarantee.

## Windows setup

Use 64-bit Windows, the Python launcher and Python 3.10. The reference interpreter was 3.10.11. Use a short checkout path.

~~~powershell
git clone --branch codex/aru-recovery-2 https://github.com/panglianyu/Aoao-Memory.git
cd Aoao-Memory
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup.ps1 -Dev
~~~

The setup script creates .venv and installs the inherited direct dependency pins, including CPU PyTorch. It does not download models, create credentials, start a service or open a tunnel. A fresh installation of these pins has not been verified for this release. See [DEPENDENCIES.md](DEPENDENCIES.md).

Obtain BAAI/bge-m3 separately under its upstream terms. Provision the exact snapshot once:

~~~powershell
$env:HF_HOME = 'C:/amc-model-cache'
$env:HF_HUB_OFFLINE = '0'
$env:TRANSFORMERS_OFFLINE = '0'
& .\.venv\Scripts\python.exe -c "from huggingface_hub import snapshot_download; snapshot_download('BAAI/bge-m3',revision='5617a9f61b028005a4858fdac845db406aefb181')"
~~~

Keep HF_HOME set when launching. Runtime model loading is offline and binds that revision; the package contains no weights or cache. This is not an independent attestation of every loaded model byte.

## Private configuration and launch

Copy the invalid provider example to a private location outside the checkout and fill its three provider fields. Never put a live Key or Bearer value in Git.

~~~powershell
New-Item -ItemType Directory -Path C:/amc-private -Force | Out-Null
if (-not (Test-Path C:/amc-private/provider.json)) { Copy-Item .\config\provider.example.json C:/amc-private/provider.json }
notepad.exe C:/amc-private/provider.json
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 1700 --shared-embedding --runtime-lanes 2 --check-config
& .\.venv\Scripts\python.exe Tools/serve_memory_challenge.py --data-root C:/amc-data --token-file C:/amc-data/credentials/memory-system.key --api-config C:/amc-private/provider.json --port 8795 --add-timeout 1700 --shared-embedding --runtime-lanes 2
~~~

Check-config performs no model call. Normal startup protects the credential directory and uses an OS lease for the data root. Use one service process per root; do not add uvicorn workers against the same state. If a platform binding inherits an existing Bearer token, securely provision that same value before startup. Preserve existing data and storage markers.

Structured extraction explicitly disables thinking only for a validated official DeepSeek HTTPS origin. The base system prompt, model, source identities and strict output schema remain unchanged. In 1.8.2, invalid JSON, group shape or domain payload results may trigger a static correction message inside the same-source lock. That message contains only fixed constraints and a closed reason code, never the previous response body. These later requests are not byte-for-byte identical to the old input policy.

The candidate persistently preconsumes at most three SDK requests per exact source/content/processor/model/prompt identity across its five structured domains, instances and restarts. Started or unknown calls are never refunded. The128 RAM limit covers active owners only; historical terminal records remain in the private ledger. A valid durable cache takes precedence. Old sources with unknown historical usage cannot be reset to zero. This does not authorize replaying failed official data. Private diagnostics exclude response text and reasoning.

## API example

Public HTTPS forwarding is configured separately. Health exposes only state and version.

~~~powershell
curl.exe --fail http://127.0.0.1:8795/health
$headers = @{ Authorization = 'Bearer ' + (Get-Content C:/amc-data/credentials/memory-system.key -Raw).Trim() }
$body = @{ request_id='demo-1'; user_id='demo-user'; session_id='demo-session'; messages=@(@{role='user'; content='Ada lives in Oslo.'; timestamp=0}) } | ConvertTo-Json -Depth 4
Invoke-RestMethod http://127.0.0.1:8795/add -Method Post -Headers $headers -ContentType application/json -Body $body
$query = @{user_id='demo-user'; query='Where does Ada live?'; top_k=5} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8795/search -Method Post -Headers $headers -ContentType application/json -Body $query
~~~

Use integer Unix milliseconds. Limits include a 2 MiB JSON body, 256 messages, 81,920 characters per message, 32,768 query characters, IDs up to 256 characters and top_k 1..100. Vector slices are capped at 8192 characters and 8192 special-inclusive tokens, with at most ten slices. Multiple-choice hints remain bounded and do not replace the original question or introduce model calls.

## Development

~~~powershell
& .\.venv\Scripts\python.exe -m pytest -q tests/test_grouped_partial_dates.py tests/test_grouped_extractor_repair.py tests/test_memory_terminal_publication.py
~~~

The command above covers time-representation and inherited correction/publication regressions. The included tests are synthetic and forbid unplanned model loading by default. Operator-recovery fixtures additionally assume the reference short-path Windows scratch layout; they are audit cases, not a portable unattended suite. Maintenance recovery CLIs contain fixed operator paths and are not normal setup or production launch commands. Do not point them at a live data root.

This candidate includes the complete related regression set plus new deadline, durable budget, paged retrieval, growth and fault tests. See [PATCH-ARU-RECOVERY-2.md](PATCH-ARU-RECOVERY-2.md) for the implementation and resource changes. Source/package manifests are regenerated at the candidate's fixed save point; inherited manifests must not be used to authenticate uncommitted development changes.

## Using with Codex

Codex reads [AGENTS.md](AGENTS.md) for verified commands, architecture and source boundaries. Keep real provider credentials, runtime data and recovery records outside the checkout.

## Remaining limits

Large-corpus performance, this candidate's deployment, official version acceptance, Full completion and full-workload quality require their own evidence. The earlier Smoke success belongs to the original resumed task and implementation. Historical real-source calls are recorded in [VALIDATION-1.8.4.md](VALIDATION-1.8.4.md); they are not new checks of ARU-RECOVERY-2. Changed runtime type checking retains33 existing findings in two files with the same normalized diagnostics as its fixed base. Fresh dependency installation and production privacy acceptance remain unproved. Packaging itself runs no model or evaluation.

## License

First-party code uses the existing MIT license in [LICENSE](LICENSE). Dependencies and BGE-M3 keep their own terms; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
