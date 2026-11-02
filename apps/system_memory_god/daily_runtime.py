"""Opt-in isolated daily lifecycle; no configuration loading or default activation."""

from __future__ import annotations

import asyncio
import base64
from dataclasses import asdict
import hashlib
import math
from pathlib import Path
import tempfile
import time

from kernel.memory_pipeline_models import PipelineScope, SourceRevision
from kernel.raw_message_models import Scope
from kernel.raw_message_service import RawMessageService
from kernel.raw_message_profile import RawMessageProfile
from kernel.supervised_daily_retrieval import SupervisedDailyRetrieval
from .derivation import DailyDerivation, Extractor, _RecoveryGenerationChanged, _settle_write
from .storage.scoped_stores import ScopedStores
from .storage.daily_vector_index import MAX_DAILY_VECTOR_PARTITIONS
from .storage.supervised_daily_vector import SupervisedDailyVector
from .storage.daily_vector_worker import _validate_shared_embedding


def _shared_options(profile, embedding, semantic_mode, clients, deadline_ns) -> dict:
    if clients is None and deadline_ns is None:
        return {}
    if profile.formal_embedding is not None:
        raise ValueError("formal and shared embedding cannot be combined")
    if (type(clients) is not dict or set(clients) != {"writer", "query"}
            or clients["writer"] is clients["query"] or semantic_mode != "snapshot"):
        raise ValueError("explicit distinct shared embedding roles required")
    for client in clients.values():
        _validate_shared_embedding(embedding, None, client, deadline_ns)
    return clients


class DailyMemoryRuntime:
    def __init__(self, raw: RawMessageService, profile: RawMessageProfile, bus, *,
                 extractor: Extractor, embedding: str, semantic_mode: str = "disabled",
                 semantic_diagnostics: bool = False, embedding_clients=None,
                 embedding_deadline_ns=None):
        shared_clients = _shared_options(profile, embedding, semantic_mode,
                                         embedding_clients, embedding_deadline_ns)
        if raw.profile is not profile or profile.derived_root is None:
            raise ValueError("explicit matching daily profile required")
        if bus.raw_messages is not raw:
            raise ValueError("daily runtime requires the host Raw service")
        if sum(len(binding.conversations) for binding in profile.connectors) > MAX_DAILY_VECTOR_PARTITIONS:
            raise ValueError("daily vector scope capacity exceeded")
        if semantic_mode not in {"disabled", "snapshot"}:
            raise ValueError("invalid daily semantic mode")
        if (type(semantic_diagnostics) is not bool
                or semantic_diagnostics and semantic_mode != "snapshot"):
            raise ValueError("invalid daily semantic diagnostics")
        if profile.formal_embedding is not None:
            if embedding != "bge-m3" or semantic_mode != "snapshot":
                raise ValueError("formal runtime requires bge-m3 snapshot")
        self._formal_embedding = profile.formal_embedding
        self.raw, self.bus = raw, bus
        self.stores = ScopedStores(profile.derived_root)
        formal_options = {"formal_embedding": profile.formal_embedding} if profile.formal_embedding is not None else {}
        self.vector = SupervisedDailyVector(
            self.stores, embedding=embedding,
            request_timeout=30 if embedding == "bge-m3" else 10,
            enable_snapshot=semantic_mode == "snapshot", **formal_options,
            **({"embedding_client": shared_clients["writer"],
                "embedding_deadline_ns": embedding_deadline_ns} if shared_clients else {}))
        self.derivation = DailyDerivation(self.stores, extractor, self.vector)
        self.semantic_mode = semantic_mode
        self._startup_enabled = semantic_diagnostics
        self._startup_state = {"stage": "created", "error_code": "none", "error_type": "none"}
        self._startup_scopes: dict[PipelineScope, dict] = {}
        self._startup_partitions = tuple(PipelineScope(scope.user_id, scope.agent_id,
            scope.connector_id, scope.conversation_id) for scope in raw.configured_scopes())
        self.snapshot_cache = None
        self.snapshot_reader = None
        if semantic_mode == "snapshot":
            from .storage.daily_vector_snapshot import DailyVectorSnapshot
            from .storage.supervised_daily_vector_snapshot import SupervisedDailyVectorSnapshot
            partitions = tuple(PipelineScope(scope.user_id, scope.agent_id,
                               scope.connector_id, scope.conversation_id) for scope in raw.configured_scopes())
            if profile.formal_embedding is not None:
                self.stores.root.mkdir(parents=True, exist_ok=True)
            self.snapshot_cache = DailyVectorSnapshot(
                (Path(tempfile.mkdtemp(prefix="aoao-daily-semantic-", dir=profile.derived_root))
                 if profile.formal_embedding is not None else Path(tempfile.mkdtemp(prefix="aoao-daily-semantic-"))),
                partitions, embedding=embedding,
                **({"binding_sha256": profile.formal_embedding.binding_sha256} if profile.formal_embedding is not None else {}))
            self.snapshot_reader = SupervisedDailyVectorSnapshot(
                self.snapshot_cache, diagnostics=semantic_diagnostics, **formal_options,
                **({"embedding_client": shared_clients["query"],
                    "embedding_deadline_ns": embedding_deadline_ns} if shared_clients else {}))
        raw_only = semantic_mode == "disabled"
        self.retrieval = SupervisedDailyRetrieval(
            raw.store, self.stores,
            self.snapshot_reader, owns_vector_index=not raw_only,
            raw_only=raw_only, snapshot_vector=not raw_only)
        self._state = "created"
        self._reasons: tuple[str, ...] = ()
        self._lifecycle = asyncio.Lock()
        self._recovery_tasks: set[asyncio.Task] = set()
        self._recovery_slots = asyncio.Semaphore(2)
        self._pending_scopes: set = set()
        self._blocked_scopes: set = set()
        self._close_errors: tuple[BaseException, ...] = ()
        self._semantic_state = "disabled" if raw_only else "preparing"
        self._semantic_dirty: set[PipelineScope] = set()
        self._semantic_wake = asyncio.Event()
        self._semantic_task: asyncio.Task | None = None
        self._semantic_attempts: dict[PipelineScope, int] = {}
        self._semantic_error = ""
        self._semantic_ready: set[PipelineScope] = set()
        self._semantic_failed: set[PipelineScope] = set()
        self._semantic_publications: dict[PipelineScope, tuple[int, str, int]] = {}
        self._semantic_metadata: set[asyncio.Task] = set()

    def set_embedding_deadline(self, deadline_ns: int) -> None:
        """Forward the already registered operation deadline without starting IO."""
        self.vector.set_embedding_deadline(deadline_ns)
        if self.snapshot_reader is not None:
            self.snapshot_reader.set_embedding_deadline(deadline_ns)

    def startup_diagnostic(self) -> dict:
        """Opt-in bounded startup facts; never include source identifiers or text."""
        if not self._startup_enabled:
            return {}
        return {**self._startup_state,
                "scopes": [dict(self._startup_scopes[p]) for p in self._startup_partitions
                           if p in self._startup_scopes],
                "writer": self.vector.startup_diagnostic(),
                "reader": self.snapshot_reader._transport.startup_diagnostic()
                          if self.snapshot_reader is not None else {}}

    def _startup_note(self, stage: str, partition: PipelineScope | None = None, *,
                      code: str = "none", error: BaseException | None = None) -> None:
        if not self._startup_enabled:
            return
        value = {"stage": stage, "error_code": code,
                 "error_type": (type(error).__name__ if type(error) in {
                     RuntimeError, ValueError, OSError, TimeoutError, MemoryError,
                     asyncio.CancelledError} else "OtherError") if error is not None else "none"}
        if partition is None:
            self._startup_state = value
        elif partition in self._startup_partitions:
            self._startup_scopes[partition] = {"slot": self._startup_partitions.index(partition), **value}

    def _startup_failure(self, partition: PipelineScope, code: str, error: BaseException) -> None:
        self._startup_note(self._startup_scopes.get(partition, {}).get("stage", "unknown"),
                           partition, code=code, error=error)

    async def semantic_publication_state(self, partition: PipelineScope, *, deadline: float) -> dict:
        """Check source freshness separately from synchronous component health.

        A timed-out or cancelled metadata read remains owned until it settles.
        No numerical query, model call, or write is performed by this barrier.
        """
        if (type(partition) is not PipelineScope or type(deadline) not in {int, float}
                or not math.isfinite(deadline)):
            raise ValueError("invalid semantic publication request")
        scope = Scope(partition.agent_id, partition.user_id,
                      partition.connector_id, partition.conversation_id)
        if scope not in self.raw.configured_scopes():
            raise ValueError("unknown semantic scope")
        stamp = self._semantic_publications.get(partition)
        result = {"status": "preparing", "reason": "publication_pending",
                  "terminal": False,
                  "raw_generation": None,
                  "published_raw_generation": stamp[0] if stamp else None,
                  "snapshot_generation": stamp[1] if stamp else None}
        if self._state in {"stopping", "stopped", "unavailable"}:
            result.update(status="stopped" if self._state in {"stopping", "stopped"}
                          else "unavailable", reason="runtime_unavailable")
            return result
        if self.snapshot_reader is None or self.snapshot_cache is None:
            result.update(status="disabled", reason="semantic_disabled")
            return result
        remaining = deadline - time.monotonic()
        if remaining <= 0 or len(self._semantic_metadata) >= 8:
            result.update(status="unavailable", reason="metadata_deadline" if remaining <= 0
                          else "metadata_capacity")
            return result
        pending = asyncio.create_task(asyncio.to_thread(self.raw.store.partition_state, scope))
        self._semantic_metadata.add(pending)

        def settled(task: asyncio.Task) -> None:
            self._semantic_metadata.discard(task)
            if not task.cancelled():
                task.exception()

        pending.add_done_callback(settled)
        try:
            generation, resolved = await asyncio.wait_for(asyncio.shield(pending), remaining)
        except (OSError, ValueError, RuntimeError, TimeoutError, asyncio.TimeoutError):
            result.update(status="unavailable", reason="metadata_unavailable")
            return result
        result["raw_generation"] = generation
        if time.monotonic() >= deadline:
            result.update(status="unavailable", reason="metadata_deadline")
            return result
        # Recheck lifecycle and stamp after the await: close/recovery/publication
        # may have progressed while the read was in its owning thread.
        stamp = self._semantic_publications.get(partition)
        result.update(published_raw_generation=stamp[0] if stamp else None,
                      snapshot_generation=stamp[1] if stamp else None)
        reader = self.snapshot_reader
        publisher = self._semantic_task
        publisher_failed = (publisher is not None and publisher.done()
                            and (publisher.cancelled() or publisher.exception() is not None))
        if self._state in {"stopping", "stopped", "unavailable"}:
            result.update(status="stopped" if self._state != "unavailable"
                          else "unavailable", reason="runtime_unavailable")
        elif publisher_failed:
            result.update(status="unavailable", reason="semantic_publisher_failed", terminal=True)
        elif partition in self._semantic_failed and self._semantic_attempts.get(partition, 0) >= 3:
            result.update(status="unavailable", reason="publication_attempts_exhausted", terminal=True)
        elif partition in self._semantic_failed or not reader.available(partition):
            result.update(status="unavailable", reason="reader_unavailable")
        elif stamp is not None and stamp[2] != reader.worker_pid:
            self._semantic_publications.pop(partition, None)
            self._notify_semantic(partition, "reader_recovered")
        elif (resolved and stamp is not None and stamp[0] == generation
              and stamp[1] == reader._ready.get(partition)
              and stamp[1] == self.snapshot_cache._current.get(partition)
              and partition not in self._semantic_dirty and not reader._lifecycle.locked()):
            result.update(status="current", reason="none")
        return result

    def health(self) -> dict:
        read = self.retrieval.health()
        state = self._state
        if state == "ready" and read["state"] != "ready":
            state = read["state"]
        worker_error = self.raw.worker.last_error if self.raw.worker else ""
        if state == "ready" and worker_error:
            state = "degraded"
        semantic_state = self._semantic_state
        if self.snapshot_reader is not None:
            if self._state == "stopped":
                semantic_state = "stopped"
            elif self._semantic_failed:
                semantic_state = "degraded"
            elif self.snapshot_reader.health()["state"] == "recovering":
                semantic_state = "recovering"
            elif (not self._semantic_dirty and self.snapshot_cache is not None
                  and set(self.snapshot_cache.scopes).issubset(self._semantic_ready)
                  and all(self.snapshot_reader.available(scope) for scope in self.snapshot_cache.scopes)):
                semantic_state = "ready"
            elif self._semantic_ready and self.snapshot_reader.health()["state"] == "unavailable":
                semantic_state = "unavailable"
            else:
                semantic_state = "preparing"
            if state == "ready" and semantic_state != "ready":
                state = "degraded"
        result = {"state": state, "reason_codes": self._reasons + read["reason_codes"],
                "raw_state": read["state"],
                "derived_state": ("stopped" if self._state == "stopped" else
                                  self._state if self._state in {"created", "initializing", "stopping", "unavailable"} else
                                  "recovering" if self._pending_scopes else
                                  "degraded" if self._blocked_scopes else
                                  "ready"),
                "online_vector": ("disabled_pending_isolation"
                                  if self.retrieval.raw_only else "snapshot"),
                "semantic_state": semantic_state,
                "semantic_reason": self._semantic_error,
                "semantic_reader_pid": self.snapshot_reader.worker_pid if self.snapshot_reader else None,
                "reader_pid": self.retrieval.worker_pid,
                "reader_recovery_attempts": read["recovery_attempts"],
                "vector_pid": self.vector.worker_pid,
                "vector_recovering": self.vector.recovery_active,
                "vector_recovery_attempts": self.vector.recovery_attempts,
                "worker_error": worker_error}
        if self._formal_embedding is not None:
            result["formal"] = {"binding_sha256": self._formal_embedding.binding_sha256,
                "writer": self.vector.formal_health(),
                "query": self.snapshot_reader._transport.formal_health() if self.snapshot_reader else None,
                "snapshot": getattr(self.snapshot_cache, "formal_snapshot", None)}
        return result

    async def start(self) -> None:
        async with self._lifecycle:
            owner = self.bus._daily_runtime
            if self._state != "created" or not (owner is self or getattr(owner, "runtime", None) is self):
                raise ValueError("daily runtime already started or not bound")
            self._state = "initializing"
            self._startup_note("raw_preparation")
            try:
                await self.raw.initialize_profile()
                scopes = tuple(self.raw.configured_scopes())
                self._startup_note("read_preparation")
                for scope in scopes:
                    if not await self.retrieval.prepare(scope):
                        raise RuntimeError("daily read preparation unavailable")
                self._pending_scopes = set(scopes)
                if self.snapshot_cache is not None:
                    self._semantic_task = asyncio.create_task(self._publish_semantic())
                    self._semantic_task.add_done_callback(self._semantic_finished)
                self.raw.start_worker(self.bus, blocked_scopes=frozenset(scopes),
                    on_settled=self._notify_semantic if self.snapshot_cache is not None else None)
                self._state, self._reasons = "degraded", ("derived_recovering",)
                for scope in scopes:
                    task = asyncio.create_task(self._recover_scope(scope))
                    self._recovery_tasks.add(task)
                    task.add_done_callback(self._recovery_tasks.discard)
                self._startup_note("start_return")
            except BaseException as exc:
                self._startup_note(self._startup_state["stage"], code="startup_failed", error=exc)
                self._state, self._reasons = "unavailable", ("startup_incomplete",)
                await self._close_parts()
                raise

    async def _recover_scope(self, scope) -> None:
        async with self._recovery_slots:
            await self._recover_scope_owned(scope)

    async def _recover_scope_owned(self, scope) -> None:
        partition = PipelineScope(scope.user_id, scope.agent_id,
                                  scope.connector_id, scope.conversation_id)
        ready = False
        self._startup_note("partition_recovery", partition)
        try:
            registry = await asyncio.to_thread(self.stores.partition_registry)
            snapshot = await asyncio.to_thread(registry.snapshot, partition)
            if snapshot is not None:
                for attempt in range(3):
                    try:
                        verified = await self.derivation.recover_partition(
                            partition, self.raw.store, online=True)
                        break
                    except _RecoveryGenerationChanged:
                        if attempt == 2:
                            raise
                        # Keep the scope fenced, retaining its evidence. Only a
                        # fresh completed audit can unblock the writer.
                        await asyncio.sleep((.25, 1.0)[attempt])
                if verified and snapshot["vector"]:
                    self._startup_note("vector_warm", partition)
                    await self.vector.warm_existing(partition)
            else:
                path = await asyncio.to_thread(self.stores.partition, partition)
                verified = not (await asyncio.to_thread(self.raw.store.has_derived_results, scope)
                    or await asyncio.to_thread(self.raw.store.has_succeeded_derivation, scope)
                    or await asyncio.to_thread(lambda: path.exists() and any(path.iterdir())))
            if verified:
                if self.raw.worker is None:
                    raise RuntimeError("derivation worker unavailable")
                await self.raw.worker.allow_recovered_scope(scope)
                if self.snapshot_cache is not None:
                    self._notify_semantic(partition, "recovered")
                ready = True
                self._startup_note("recovered", partition)
            else:
                self._startup_note("partition_recovery", partition, code="recovery_unverified")
        except Exception as exc:
            self._startup_failure(partition, "recovery_failed", exc)
            # Recovery faults keep the scope fenced; expose only a finite state.
            ready = False
        finally:
            self._pending_scopes.discard(scope)
            if not ready:
                self._blocked_scopes.add(scope)
            if self._state not in {"stopping", "stopped", "unavailable"}:
                self._state = "degraded" if self._pending_scopes or self._blocked_scopes else "ready"
                self._reasons = (("derived_recovering",) if self._pending_scopes else
                                 ("derived_recovery_incomplete",) if self._blocked_scopes else ())

    def _notify_semantic(self, partition: PipelineScope, task_id: str) -> None:
        if self.snapshot_cache is None or partition not in self.snapshot_cache.scopes:
            raise ValueError("unknown semantic scope")
        if self._state in {"stopping", "stopped", "unavailable"}:
            return
        if self._semantic_attempts.get(partition, 0) >= 3:
            return
        self._semantic_dirty.add(partition)
        self._semantic_state = "preparing"
        self._semantic_wake.set()

    def _semantic_finished(self, task: asyncio.Task) -> None:
        if (task.cancelled() and self._startup_enabled
                and self._state not in {"stopping", "stopped", "unavailable"}
                and self._startup_state["error_code"] == "none"):
            self._startup_note("publisher", code="cancelled", error=asyncio.CancelledError())
        if not task.cancelled() and task.exception() is not None:
            self._startup_note("publisher", code="publisher_failed", error=task.exception())
            self._semantic_state = "unavailable"
            self._semantic_error = "semantic_publisher_failed"
            if self.snapshot_cache is not None:
                self._semantic_failed.update(self.snapshot_cache.scopes)

    def _verified_snapshot_rows(self, partition: PipelineScope) -> dict[str, bytes]:
        """Approve already captured receipts against existing current sources."""
        scope = Scope(partition.agent_id, partition.user_id,
                      partition.connector_id, partition.conversation_id)
        before = self.raw.store.partition_state(scope)
        captured = self.vector.snapshot_vectors(partition)
        approved: dict[str, bytes] = {}
        if not captured:
            if self.raw.store.partition_state(scope) != before:
                raise _RecoveryGenerationChanged("semantic source changed")
            return approved
        evidence = self.stores.evidence(partition)
        ledger = self.stores.ledger(partition)
        ids = tuple(captured)
        for start in range(0, len(ids), 100):
            page = ids[start:start + 100]
            records = evidence.get_many(page)
            inspected = ledger.inspect_ids(page)
            sources = {key: SourceRevision(**record["source"]) for key, record in records.items()}
            snapshots = self.raw.store.source_snapshots(scope, tuple(dict.fromkeys(
                source.raw_message_id for source in sources.values())))
            for key, record in records.items():
                row, source = captured[key], sources[key]
                proof = snapshots.get(source.raw_message_id)
                if (proof is None or proof[0].revision != source.revision
                        or proof[0].status not in {"active", "edited"}
                        or record["domain"] != "vector"
                        or inspected.get(key) != {"has_item": True, "sources": (source,)}):
                    continue
                if not any(anchor["memory_id"] == key and anchor["store_ref"] == record["store_ref"]
                           and anchor["target_domain"] == "vector" and anchor["task_id"] == source.task_id
                           and anchor["revision"] == source.revision for anchor in proof[1]):
                    continue
                if (record["source"] != asdict(source) or row["payload_hash"] != record["payload_hash"]
                        or row["text_hash"] != hashlib.sha256(record["payload"]["text"].encode()).hexdigest()):
                    raise ValueError("semantic receipt source mismatch")
                approved[key] = base64.b64decode(row["snapshot"]["f32"], validate=True)
        if self.raw.store.partition_state(scope) != before:
            raise _RecoveryGenerationChanged("semantic source changed")
        return approved

    async def _publish_semantic(self) -> None:
        from .storage.daily_vector_snapshot import SnapshotCapacityError
        cache, reader = self.snapshot_cache, self.snapshot_reader
        if cache is None or reader is None:
            raise ValueError("semantic publisher is not configured")
        while True:
            await self._semantic_wake.wait()
            self._semantic_wake.clear()
            while self._semantic_dirty:
                # A finite batch gives every queued scope a turn. Notifications
                # arriving during capture remain queued for the next batch.
                batch = tuple(self._semantic_dirty)
                self._semantic_dirty.difference_update(batch)
                for partition in batch:
                    if self._semantic_attempts.get(partition, 0) >= 3:
                        continue
                    self._semantic_ready.discard(partition)
                    self._startup_note("snapshot_capture", partition)
                    try:
                        scope = Scope(partition.agent_id, partition.user_id,
                                      partition.connector_id, partition.conversation_id)
                        before = await _settle_write(asyncio.to_thread(self.raw.store.partition_state, scope))
                        vectors = await _settle_write(asyncio.to_thread(self._verified_snapshot_rows, partition))
                        if await _settle_write(asyncio.to_thread(self.raw.store.partition_state, scope)) != before:
                            raise _RecoveryGenerationChanged("semantic source changed")
                        self._startup_note("snapshot_publication", partition)
                        generation = await _settle_write(asyncio.to_thread(cache.publish, partition, vectors))
                        self._startup_note("snapshot_reader_warm", partition)
                        await reader.warm_existing(partition)
                        if (await _settle_write(asyncio.to_thread(self.raw.store.partition_state, scope)) != before
                                or not reader.available(partition) or reader.worker_pid is None
                                or reader._ready.get(partition) != generation):
                            raise _RecoveryGenerationChanged("semantic source changed")
                        self._semantic_publications[partition] = (before[0], generation, reader.worker_pid)
                        self._semantic_attempts.pop(partition, None)
                        self._semantic_failed.discard(partition)
                        self._semantic_ready.add(partition)
                        if not self._semantic_failed:
                            self._semantic_error = ""
                        self._semantic_state = "preparing" if self._semantic_dirty else "ready"
                        self._startup_note("ready", partition)
                    except asyncio.CancelledError:
                        raise
                    except _RecoveryGenerationChanged as exc:
                        self._startup_failure(partition, "source_changed", exc)
                        # A moving source requires a fresh proof, not a hardware
                        # fault allowance or a reset of previous hard failures.
                        self._semantic_state = "preparing"
                        self._semantic_dirty.add(partition)
                        await asyncio.sleep(1)
                    except SnapshotCapacityError as exc:
                        self._startup_failure(partition, "publication_capacity", exc)
                        self._semantic_failed.add(partition)
                        self._semantic_state = "degraded"
                        self._semantic_error = "vector_capacity_exceeded"
                        self._semantic_attempts[partition] = 3
                    except (ValueError, RuntimeError, OSError, TimeoutError) as exc:
                        self._startup_failure(partition, "publication_failed", exc)
                        self._semantic_state = "degraded"
                        self._semantic_failed.add(partition)
                        self._semantic_error = "semantic_publication_incomplete"
                        attempts = self._semantic_attempts.get(partition, 0) + 1
                        self._semantic_attempts[partition] = attempts
                        if attempts < 3:
                            await asyncio.sleep((1, 2)[attempts - 1])
                            self._semantic_dirty.add(partition)

    async def _close_parts(self) -> None:
        self._semantic_publications.clear()
        pending = tuple(self._recovery_tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        errors: list[BaseException] = []
        try:
            await self.raw.close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            if self._semantic_task is not None:
                self._semantic_task.cancel()
                results = await asyncio.gather(self._semantic_task, return_exceptions=True)
                errors.extend(result for result in results if isinstance(result, BaseException)
                              and not isinstance(result, asyncio.CancelledError))
            if self._semantic_metadata:
                metadata = await asyncio.gather(*tuple(self._semantic_metadata), return_exceptions=True)
                errors.extend(result for result in metadata if isinstance(result, BaseException))
            try:
                await self.retrieval.close()
            except BaseException as exc:
                errors.append(exc)
            if self.snapshot_cache is not None:
                try:
                    await asyncio.to_thread(self.snapshot_cache.close)
                except BaseException as exc:
                    errors.append(exc)
        # Reader is independent; the vector writer is not. Never force its exit
        # while a Raw owner may still commit a frozen derivation or receipt.
        if self.raw.settlement_status != "unsettled":
            try:
                await self.vector.close()
            except BaseException as exc:
                errors.append(exc)
        elif not errors:
            errors.append(RuntimeError("raw settlement incomplete"))
        if errors:
            self._close_errors = tuple(errors)
            raise errors[0]

    async def close(self) -> None:
        async with self._lifecycle:
            if self._state == "stopped":
                return
            self._state = "stopping"
            self.retrieval.stop_accepting()
            try:
                cleanup = asyncio.create_task(self._close_parts())
                cancelled = False
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        cancelled = True
                cleanup.result()
                if cancelled:
                    raise asyncio.CancelledError
            except BaseException:
                self._state = "unavailable"
                self._reasons = ("shutdown_incomplete",)
                raise
            self._state = "stopped"
            self._reasons = ()
