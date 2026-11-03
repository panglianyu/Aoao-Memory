# 1.8.1 public package preparation

The public baseline is the existing codex/amc-entry-1.8 branch at commit e00405e6c6821c9dc6078e304707c3a23c9d96ce. This work uses a new codex/amc-entry-1.8.1 branch; main, earlier tags and the accepted 1.0 fixed commit are not rewritten.

The source whitelist is the 202 Python files frozen for the actual 1.8.1 repair: 126 kernel files, 53 memory-app files, 16 protocol files and 7 Tools files. Each was copied by explicit relative path and verified against its frozen byte length and SHA256. This is a complete frozen-source audit set, broader than the old 57-file minimum publication closure; unused host-facing type contracts are not additional running services.

Eleven related synthetic test modules and their import setup are included. Model loading is forbidden by the test fixture unless explicitly enabled for a separately authorized live run. The reference recovery tests use a short Windows scratch layout and are not part of the normal setup command.

Existing MIT license, Windows setup script, dependency pins, invalid provider example, attribution notices and historical metadata-only results are retained. Earlier validation text is byte-preserved as VALIDATION-1.6.md. Current README and VALIDATION separate the repaired deployment, running original Smoke task and unproved Full status.

No host configuration, credential, runtime database, message payload, recovery control file, model weight, log, installed library source or private Git history was copied. The runtime/source audit set intentionally preserves the exact maintenance-tool bytes, including their fixed operator path policy; those tools are not portable production entrypoints.

SOURCE_MANIFEST.json lists the 202 exact runtime files. PACKAGE_MANIFEST.json lists explicit public files and excludes itself and the private sanitization report to avoid self-reference or redistributing preparation records. Package equality and syntax checks do not execute the service or model.

Independent sanitization must finish before publication. This package preparation neither pushes Git nor emails the organizers, changes a binding, starts an evaluation, purchases resources or certifies Full readiness.
