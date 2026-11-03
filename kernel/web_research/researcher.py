"""One task's search, snapshots, evidence audit and bounded research loop."""
from __future__ import annotations
import asyncio
from dataclasses import asdict
from pathlib import Path
from typing import Awaitable, Callable

from kernel.web_research.answer import build_answer
from kernel.web_research.config import ResearchConfig
from kernel.web_research.context import ResearchContext
from kernel.web_research.evidence import independent_sources, plain_text, validate_claim
from kernel.web_research.parser import parse_download
from kernel.web_research.store import SnapshotStore
from kernel.web_research.validation import (
    validate_query,
    validate_snapshot_id,
    validate_source_id,
)
from protocols.web_research import SourceSnapshot, canonical_url


async def settled_io(function, *args):
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    if cancelled:
        # Retrieve exceptions as well, so no owned work is left unobserved.
        if not task.cancelled():
            task.exception()
        raise asyncio.CancelledError
    return task.result()


class ResearchSession:
    def __init__(self, config: ResearchConfig, context: ResearchContext, store: SnapshotStore,
                 provider, fetcher, model, temporary_root: Path, *, parser=parse_download,
                 progress: Callable[[str], Awaitable[None]] | None = None):
        context.validate(network=False)
        self.config, self.context, self.store = config, context, store
        self.provider, self.fetcher, self.model = provider, fetcher, model
        self.temporary_root, self.parser, self.progress = temporary_root, parser, progress
        self.sources: dict[str, dict] = {}
        self.snapshots: dict[str, SourceSnapshot] = {}
        self.queries: dict[str, dict] = {}
        self.state: dict = {"schema_version": 2, "research_id": "research_" + context.scope[:32],
                            "run_id": context.run_id, "status": "planning", "state_revision": 0,
                            "round_index": 0, "errors": [], "subquestions": [], "gaps": [], "claims": [],
                            "conflicts": [], "session_key": context.session_key,
                            "principal_key": context.scope, "deadline": context.budget.deadline}
        self._opened: dict[str, str] = {}
        self._open_locks: dict[str, asyncio.Lock] = {}
        self._parse_slots = asyncio.Semaphore(config.max_fetch_concurrency)
        self._save_lock = asyncio.Lock()
        self._primary: set[str] = set()

    async def _save(self):
        self.state["state_revision"] += 1
        payload = {**self.state, "usage": self.context.budget.snapshot(),
                   "sources": list(self.sources.values()), "snapshot_ids": list(self.snapshots),
                   "completed_queries": list(self.queries)}
        async with self._save_lock:
            await settled_io(self.store.save_state, self.context.scope, payload)

    async def _phase(self, phase: str):
        self.state["status"] = phase
        if self.progress is not None:
            await self.progress(phase)

    async def search(self, queries: list[str], *, domains=None, recency_days=None) -> dict:
        self.context.validate()
        if type(queries) is not list or not 1 <= len(queries) <= self.config.max_queries_per_round:
            raise ValueError("invalid_queries")
        normalized = list(dict.fromkeys(validate_query(q) for q in queries))

        async def one(query):
            # Filter options form part of the cache key; never reuse an unfiltered search.
            import json
            key = json.dumps([query.casefold(), domains, recency_days], ensure_ascii=False, sort_keys=True)
            if key in self.queries:
                self.context.budget.usage["cache_hits"] += 1
                return self.queries[key]
            try:
                result = await self.provider.search(query, domains=domains, recency_days=recency_days,
                                                    budget=self.context.budget)
            except (ValueError, asyncio.TimeoutError) as exc:
                code = str(exc) if isinstance(exc, ValueError) else "timeout"
                result = {"query": query, "results": [], "error": code}
            self.queries[key] = result
            for source in result["results"]:
                self.sources[source["source_id"]] = source
            return result

        results = await asyncio.gather(*(one(q) for q in normalized))
        await self._save()
        return {"schema_version": 1, "queries": results, "results": [s for r in results for s in r["results"]],
                "usage": self.context.budget.snapshot()}

    async def open(self, *, url: str | None = None, source_id: str | None = None,
                   snapshot_id: str | None = None) -> SourceSnapshot:
        if source_id:
            validate_source_id(source_id)
        if snapshot_id:
            validate_snapshot_id(snapshot_id)
        self.context.validate(network=not bool(snapshot_id))
        if snapshot_id:
            snapshot = self.snapshots.get(snapshot_id)
            if snapshot is None:
                snapshot = await settled_io(self.store.load, self.context.scope, snapshot_id)
                self.snapshots[snapshot.snapshot_id] = snapshot
            if source_id and snapshot.source_id != source_id:
                raise ValueError("invalid_source")
            if url and canonical_url(url) != snapshot.final_url:
                raise ValueError("invalid_source")
            self.context.budget.usage["cache_hits"] += 1
            return snapshot
        source = self.sources.get(source_id or "")
        if source_id and source is None:
            raise ValueError("unknown_source")
        if source and url and canonical_url(url) != source["url"]:
            raise ValueError("invalid_source")
        target = canonical_url(url or (source or {}).get("url", ""))
        async with self._open_locks.setdefault(target, asyncio.Lock()):
            cached = self._opened.get(target)
            if cached:
                self.context.budget.usage["cache_hits"] += 1
                return self.snapshots[cached]
            download = await self.fetcher.download(target, self.context.budget, tuple((source or {}).get("domains", [])))
            async with self._parse_slots:
                snapshot = await self.parser(download, temporary_root=self.temporary_root,
                                             timeout=min(15, self.context.budget.remaining()),
                                             max_pages=self.config.max_pdf_pages, max_chars=self.config.max_page_chars)
            await settled_io(self.store.save, self.context.scope, snapshot)
            self.snapshots[snapshot.snapshot_id] = snapshot
            self._opened[target] = snapshot.snapshot_id
            self._opened[snapshot.final_url] = snapshot.snapshot_id
            self.sources.setdefault(snapshot.source_id, {"source_id": snapshot.source_id,
                "title": snapshot.title, "url": snapshot.final_url, "domains": []})
            await self._save()
            return snapshot

    def _model_snapshots(self) -> list[dict]:
        documents = []
        for snapshot in self.snapshots.values():
            # Excerpts are line-preserving windows. The full snapshot remains available to web_find.
            lines: list[dict] = []
            chars = 0
            for line in snapshot.content:
                if chars + len(line.text) > 2500:
                    if not lines:
                        lines.append({**asdict(line), "text": line.text[:2500], "line_truncated": True})
                    break
                lines.append(asdict(line))
                chars += len(line.text)
            documents.append({"source_id": snapshot.source_id, "snapshot_id": snapshot.snapshot_id,
                "title": snapshot.title, "url": snapshot.final_url, "published_at": snapshot.published_at,
                "fetched_at": snapshot.fetched_at, "extraction_status": snapshot.extraction_status,
                "content": lines, "content_hash": snapshot.content_hash,
                "excerpt_truncated": len(lines) != len(snapshot.content) or snapshot.truncated})
        return documents

    async def research(self, question: str, *, mode: str = "auto", research_id: str | None = None) -> dict:
        self.context.validate()
        if research_id is not None:
            await self._load_previous(research_id)
        question = plain_text(question, limit=6000)
        if mode not in {"auto", "quick", "deep"}:
            raise ValueError("invalid_research_mode")
        self.state["original_question"] = question
        self.store.pin(self.context.scope)
        stop_reason = "round_limit"
        claims: list[dict] = []
        gaps: list[str] = []
        complete = False
        try:
            queries, selected_mode, rounds, controversial = await self._plan(question, mode)
            for index in range(rounds):
                self.context.validate()
                self.state["round_index"] = index + 1
                before = len(self.snapshots)
                await self._phase("searching")
                search = await self.search(queries)
                self.state["errors"].extend(r["error"] for r in search["queries"] if r.get("error"))
                await self._phase("reading")
                candidates = self._candidates(search)
                await self._read_candidates(candidates[:3 if selected_mode == "quick" else 4])
                await self._phase("auditing")
                audit = await self.model.complete("audit", {"question": question,
                    "subquestions": self.state["subquestions"], "snapshots": self._model_snapshots(),
                    "previous_claims": claims, "search_errors": self.state["errors"][-12:]}, self.context.budget)
                claims, gaps = self._accept_audit(audit)
                conflicts = self._gaps(audit.get("conflicts", []))
                self.state["conflicts"] = conflicts
                gaps = list(dict.fromkeys(gaps + conflicts))[:12]
                controversial = controversial or audit.get("controversial") is True
                self.state.update(claims=claims, gaps=gaps)
                await self._save()
                if audit.get("complete") is True and claims and not gaps:
                    stop_reason = "evidence_ready"
                    break
                queries = audit.get("queries")
                if not queries:
                    stop_reason = "no_new_queries"
                    break
                if index > 0 and len(self.snapshots) == before:
                    stop_reason = "no_new_evidence"
                    break
            if claims:
                claims, gaps, complete = await self._verify(question, claims, gaps, controversial)
        except asyncio.CancelledError:
            self.state.update(status="cancelled", stop_reason="cancelled")
            await self._save()
            raise
        except (ValueError, asyncio.TimeoutError) as exc:
            stop_reason = str(exc) if isinstance(exc, ValueError) else "timeout"
            # Unreviewed claims never escape if the final model audit did not complete.
            claims = []
            gaps = ["研究在完成最终证据核验前停止，当前无法确认结论。"]
        finally:
            self.store.unpin(self.context.scope)
        answer = build_answer(claims, self.snapshots, gaps=gaps, status="complete" if complete else "partial",
                              stop_reason=stop_reason, usage=self.context.budget.snapshot(),
                              research_id=self.state["research_id"])
        answer["conflicts"] = self.state["conflicts"]
        self.state.update(status=answer["status"], stop_reason=stop_reason, answer=answer)
        await self._save()
        return answer

    async def _load_previous(self, research_id: str) -> None:
        from kernel.web_research.display import find_receipt
        receipt, previous = await settled_io(find_receipt, self.store, self.context.principal,
                                            self.context.session_key, research_id)
        for snapshot_id in receipt["snapshot_ids"]:
            snapshot = await settled_io(self.store.load, previous.scope, snapshot_id)
            await settled_io(self.store.save, self.context.scope, snapshot)
            self.snapshots[snapshot.snapshot_id] = snapshot
            self._opened[snapshot.final_url] = snapshot.snapshot_id
            self.sources[snapshot.source_id] = {"source_id": snapshot.source_id,
                "title": snapshot.title, "url": snapshot.final_url, "domains": []}
        self.context.budget.usage["cache_hits"] += len(receipt["snapshot_ids"])

    @staticmethod
    def _candidates(search: dict) -> list[dict]:
        # Interleave queries and prefer distinct hosts. This is relevance ordering,
        # never an assertion that a domain or title establishes factual authority.
        from urllib.parse import urlsplit
        rows = search["queries"]
        ordered, seen, hosts = [], set(), set()
        for index in range(8):
            for row in rows:
                results = row["results"]
                if index < len(results) and results[index]["source_id"] not in seen:
                    source = results[index]
                    seen.add(source["source_id"])
                    ordered.append(source)
        distinct: list[dict] = []
        repeats: list[dict] = []
        for source in ordered:
            host = urlsplit(source["url"]).hostname
            (repeats if host in hosts else distinct).append(source)
            hosts.add(host)
        return distinct + repeats

    def _accept_audit(self, audit: dict) -> tuple[list[dict], list[str]]:
        rows = audit.get("claims", [])
        if not isinstance(rows, list) or len(rows) > 10:
            raise ValueError("model_response_invalid")
        claims = []
        gaps = self._gaps(audit.get("gaps", []))
        for row in rows:
            try:
                claims.append(validate_claim(row, self.snapshots))
            except (ValueError, TypeError, KeyError):
                gaps.append("有候选结论的来源或引文未通过校验，已排除。")
        self._primary.clear()
        assessments = audit.get("source_assessments", [])
        if isinstance(assessments, list):
            for item in assessments[:16]:
                if isinstance(item, dict) and item.get("primary") is True and item.get("source_id") in self.sources:
                    plain_text(item.get("reason"), limit=400)
                    self._primary.add(item["source_id"])
        return claims, list(dict.fromkeys(gaps))[:12]

    @staticmethod
    def _gaps(values) -> list[str]:
        if not isinstance(values, list) or len(values) > 12:
            raise ValueError("model_response_invalid")
        return [plain_text(v, limit=400) for v in values]

    async def _verify(self, question: str, claims: list[dict], gaps: list[str], controversial: bool):
        result = await self.model.complete("verify", {"question": question, "claims": claims,
            "snapshots": self._model_snapshots(),
            "sources": [{"source_id": s.source_id, "url": s.final_url, "title": s.title,
                         "primary_assessed": s.source_id in self._primary} for s in self.snapshots.values()],
            "subquestions": self.state["subquestions"], "controversial": controversial}, self.context.budget)
        supported = result.get("supported_claim_ids")
        if not isinstance(supported, list) or any(not isinstance(x, str) for x in supported):
            raise ValueError("model_response_invalid")
        validated = []
        final_gaps = self._gaps(result.get("gaps", []))
        for claim in claims:
            if claim["claim_id"] not in supported:
                final_gaps.append("部分候选结论没有通过语义支持核验。")
                continue
            primary = any(e["source_id"] in self._primary for e in claim["evidence"])
            if claim["kind"] == "fact" and (
                (self.config.require_primary_source and not primary)
                or (controversial and independent_sources(claim["evidence"], self.snapshots) < self.config.controversial_topics_min_sources)
            ):
                final_gaps.append("确定性判断缺少所需原始材料或独立来源，已排除。")
                continue
            validated.append(claim)
        covered = {q for claim in validated if claim["kind"] != "unknown" for q in claim["question_ids"]}
        for subquestion in self.state["subquestions"]:
            if subquestion["id"] not in covered:
                final_gaps.append(subquestion["text"])
        final_gaps = list(dict.fromkeys(final_gaps + gaps))[:12]
        complete = result.get("complete") is True and not final_gaps and bool(validated)
        return validated, final_gaps, complete

    async def _plan(self, question: str, mode: str):
        await self._phase("planning")
        admission = self.context.admission
        plan = admission.plan if admission is not None else None
        if plan is None:
            plan = await self.model.complete("plan", {"question": question}, self.context.budget)
        subquestions = plan.get("subquestions")
        if (not isinstance(subquestions, list) or not 1 <= len(subquestions) <= self.config.max_subquestions
                or any(type(q) is not dict or q.get("id") != f"q{i + 1}" for i, q in enumerate(subquestions))):
            raise ValueError("model_response_invalid")
        self.state["subquestions"] = [{"id": q["id"], "text": plain_text(q.get("text"), limit=400)} for q in subquestions]
        queries = plan.get("queries")
        selected_mode = plan.get("mode") if admission is not None and admission.plan is not None \
            else plan.get("mode") if mode == "auto" else mode
        if selected_mode not in {"quick", "deep"}:
            raise ValueError("model_response_invalid")
        if selected_mode == "quick" and isinstance(queries, list):
            queries = queries[:2]
        rounds = 1 if selected_mode == "quick" else self.config.max_rounds
        controversial = plan.get("controversial") is True
        return queries, selected_mode, rounds, controversial

    async def _read_candidates(self, candidates: list[dict]) -> None:
        async def read(source):
            try:
                await self.open(source_id=source["source_id"])
            except (ValueError, asyncio.TimeoutError) as exc:
                self.state["errors"].append(str(exc) if isinstance(exc, ValueError) else "timeout")
        await asyncio.gather(*(read(source) for source in candidates))
