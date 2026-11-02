"""Partition-scoped SQLite gate shared by derivation writes and purge."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3

from kernel.memory_pipeline_models import PipelineScope
from .pipeline_database import PipelineDatabase


class DailyDerivationGate:
    """Hold one SQLite writer lock across an external write or purge critical section."""

    def __init__(self, root: Path, scope: PipelineScope):
        self._database = PipelineDatabase(root, scope, "derivation_gate.db", "", 0x414C4741)
        self._connection: sqlite3.Connection | None = None

    def held_for(self, scope: PipelineScope) -> bool:
        """Only the entered gate session can authorize a scoped finalization."""
        return (type(scope) is PipelineScope and self._database.scope == scope
                and self._connection is not None and self._connection.in_transaction)

    def _acquire(self) -> sqlite3.Connection:
        database = self._database
        connection = None
        try:
            database._path_check()
            database.path.parent.mkdir(parents=True, exist_ok=True)
            database._path_check()
            connection = sqlite3.connect(database.path, timeout=5, isolation_level=None,
                                         check_same_thread=False)
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            database._recognize(connection)
            return connection
        except BaseException:
            if connection is not None:
                connection.close()
            raise

    @staticmethod
    def _release(connection: sqlite3.Connection) -> None:
        try:
            connection.commit()
        finally:
            connection.close()

    async def _release_settled(self, connection: sqlite3.Connection) -> None:
        pending = asyncio.create_task(asyncio.to_thread(self._release, connection))
        try:
            await asyncio.shield(pending)
        except asyncio.CancelledError:
            while not pending.done():
                try:
                    await asyncio.shield(pending)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
            pending.result()
            raise

    async def __aenter__(self) -> DailyDerivationGate:
        acquisition = asyncio.create_task(asyncio.to_thread(self._acquire))
        try:
            self._connection = await asyncio.shield(acquisition)
        except asyncio.CancelledError:
            while not acquisition.done():
                try:
                    await asyncio.shield(acquisition)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
            try:
                connection = acquisition.result()
            except BaseException:
                pass
            else:
                await self._release_settled(connection)
            raise
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            await self._release_settled(connection)
