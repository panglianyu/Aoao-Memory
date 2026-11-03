from typing import Protocol
from kernel.web_research.budget import ResearchBudget


class SearchProvider(Protocol):
    async def search(self, query: str, *, domains: list[str] | None = None,
                     recency_days: int | None = None, budget: ResearchBudget) -> dict: ...

    async def close(self) -> None: ...
