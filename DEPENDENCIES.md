# Dependency materials

This package preserves the direct pins from the previously public Aoao-Memory 1.2 package. They are reference-host version materials, not a new environment audit, fresh-install certificate, vulnerability review, or complete transitive lockfile.

| Distribution | Reference pin | Runtime use |
|---|---|---|
| fastapi | 0.118.0 | Add/Search/health ASGI application |
| uvicorn | 0.37.0 | Loopback ASGI server |
| openai | 1.109.1 | Explicit OpenAI-compatible asynchronous provider client |
| PyYAML | 6.0.3 | Optional YAML provider configuration parsing |
| chromadb | 0.4.22 | Derived numerical vector storage |
| sentence-transformers | 5.2.3 | Offline CPU BGE loader |
| transformers | 5.3.0 | Exact offline BGE tokenizer |
| torch | 2.9.1 | CPU inference; setup.ps1 selects the CPU wheel index |
| numpy | 1.26.4 | Vector and snapshot numerical operations |
| pydantic | 2.12.5 | Inherited explicit framework dependency pin |
| huggingface-hub | 1.3.3 | Exact local snapshot resolution and explicit provisioning |
| pywin32 | 311 on Windows | Native file identity and handle operations |

The model material is BAAI/bge-m3 revision 5617a9f61b028005a4858fdac845db406aefb181. Weights, tokenizers, caches, wheels, installed library source, and benchmark data are not redistributed. Runtime model loading is offline and requires the separately provisioned local snapshot. The optional formal vendor/material attestation path is not packaged.

The inherited requirements-dev.txt additionally pins pytest 9.0.3, pytest-asyncio 1.3.0, HTTPX 0.28.1, and Ruff 0.13.2. No tests are included in this minimal runtime package. The private full worker test matrix also uses psutil; its version is not inferred or silently added here. Mypy and a complete development dependency lock are not provided by the inherited materials.

The source closure was determined through static Python imports, including worker entrypoints and function-local imports, with TYPE_CHECKING-only host references excluded. A fresh isolated installation and runtime import/startup verification are still required after the final source freeze. No dependency installation was performed while preparing this directory.
