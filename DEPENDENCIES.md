# Dependency materials

The Windows setup and direct dependency pins are inherited from the existing public package. They describe the reference environment, not a fresh-install certificate, vulnerability review or complete transitive lockfile. No dependency installation was performed during this package preparation.

The reference uses 64-bit Python 3.10.11. scripts/setup.ps1 checks Python 3.10 and installs the pinned CPU PyTorch wheel before requirements.txt. The -Dev switch additionally installs requirements-dev.txt.

Runtime direct pins remain FastAPI 0.118.0, Uvicorn 0.37.0, OpenAI Python 1.109.1, PyYAML 6.0.3, ChromaDB 0.4.22, Sentence Transformers 5.2.3, Transformers 5.3.0, PyTorch 2.9.1, NumPy 1.26.4, Pydantic 2.12.5, Hugging Face Hub 1.3.3 and Windows pywin32 311. Development pins are pytest 9.0.3, pytest-asyncio 1.3.0, HTTPX 0.28.1 and Ruff 0.13.2. Mypy is not installed by the development requirements.

The optional maintenance and validation Tools in the full frozen audit set may require additional operator dependencies and a fixed reference layout. They are not automatically executed by setup or the HTTP service. The minimal service commands in README are the supported package entrypoint.

BAAI/bge-m3 revision 5617a9f61b028005a4858fdac845db406aefb181 must be provisioned separately. Runtime loads it offline. Weights, tokenizers, caches, wheels, installed third-party source and benchmark data are not redistributed. Optional formal vendor/material attestation is not packaged or certified.

See THIRD_PARTY_NOTICES.md for attribution. A fresh isolated installation and the production workload remain separate validation work; source-equality and syntax checks do not substitute for them.
