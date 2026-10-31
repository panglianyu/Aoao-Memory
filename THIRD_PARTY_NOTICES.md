# Third-party notices

This repository contains project source. It does not redistribute installed third-party library source, wheels, model weights, private datasets, or benchmark data. Dependencies retain their own licenses, copyright notices, and transitive dependency terms.

The following license labels were read from the reference installation's distribution metadata. They are an inventory, not a replacement for upstream license texts or a claim that every transitive package has been audited.

| Distribution | Reference version | License metadata | Upstream package |
|---|---|---|---|
| FastAPI | 0.118.0 | MIT | https://pypi.org/project/fastapi/ |
| Uvicorn | 0.37.0 | BSD-3-Clause | https://pypi.org/project/uvicorn/ |
| OpenAI Python | 1.109.1 | Apache Software License | https://pypi.org/project/openai/ |
| PyYAML | 6.0.3 | MIT | https://pypi.org/project/PyYAML/ |
| Chroma | 0.4.22 | Apache Software License | https://pypi.org/project/chromadb/ |
| Sentence Transformers | 5.2.3 | Apache Software License | https://pypi.org/project/sentence-transformers/ |
| Transformers | 5.3.0 | Apache 2.0 | https://pypi.org/project/transformers/ |
| PyTorch | 2.9.1 | BSD-3-Clause | https://pypi.org/project/torch/ |
| NumPy | 1.26.4 | BSD License | https://pypi.org/project/numpy/ |
| Pydantic | 2.12.5 | MIT | https://pypi.org/project/pydantic/ |
| Hugging Face Hub | 1.3.3 | Apache Software License | https://pypi.org/project/huggingface-hub/ |
| pywin32 | 311 | Python Software Foundation License | https://pypi.org/project/pywin32/ |

Development dependencies are pytest, pytest-asyncio, HTTPX, and Ruff; their installed distributions supply their respective license texts.

## BGE-M3

The embedding model is BAAI/bge-m3, published by the Beijing Academy of Artificial Intelligence. Its upstream model card declares MIT terms: https://huggingface.co/BAAI/bge-m3 . Model weights are obtained separately. The reference cache's main revision was 5617a9f61b028005a4858fdac845db406aefb181; this observation does not certify the live process's loaded weights.

For model background and original work, see the BGE-M3 model card and its linked FlagEmbedding repository/paper. The integration uses the upstream Sentence Transformer embedding interface; it does not claim the BGE-M3 model as original work.

## Design attribution

kernel/message.py records conceptual inspiration from AgentScope's message design. That attribution is retained in the source. AgentScope library source is not vendored in this package.

The first-party MIT license does not relicense any third-party work.

