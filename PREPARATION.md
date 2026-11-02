# Local package preparation

This independent AMC-ENTRY-1.7 candidate package uses the prior 1.6 PACKAGE_MANIFEST.json explicit whitelist as its base. The 54 prior Python paths were copied from the frozen 1.7 candidate, together with two new job modules and one validation driver. Source bytes and line endings were preserved. Runtime directories were not globbed or copied to construct the package.

The original MIT LICENSE, dependency pins, setup script, invalid provider example, third-party notices, and 1.6 VALIDATION.md are retained. README.md and this preparation note describe the new scope. JOB17.md explains the internal job result. validation-job17.json is the exact 2223-byte metadata-only real report, SHA256 ae3ca693deb3fdc6792d6e40945fbb21357081948015ba6bfc23c5374c539c68.

SOURCE_MANIFEST.json inventories only the 57 published Python files. The actual real job check observed 193 private source files before and after execution; its digest is retained as historical measurement and is not presented as this package's digest. PACKAGE_MANIFEST.json inventories a fixed file list and excludes itself to avoid a self-hash cycle.

No private configuration, credential, database, log, original message payload, model weight, installed library source, private benchmark data, or Aoao OS Git history is included. The provider example uses an invalid host and placeholder values. The publication package contains no tests; the reported related matrix belongs to the isolated implementation checkout, not to a portable public test suite.

Packaging checks compare all listed file sizes and SHA256 values, ensure source equality with the frozen candidate, preserve historical LICENSE/VALIDATION bytes, and inspect the explicit file list for excluded artifacts. The release owner performs an independent sanitization review before publication. These checks are scoped evidence, not a claim that every possible secret or all transitive dependency terms have been audited.

The actual internal batch job passed without a new HTTP adapter. The included HTTP service retains its 1.6 synchronous protocol. 1.7 has not been deployed; official asynchronous status/poll schema, organizer version review, public ingress validation, long-running operation, and official Full remain pending. Publishing source does not change the accepted online 1.0 binding.
