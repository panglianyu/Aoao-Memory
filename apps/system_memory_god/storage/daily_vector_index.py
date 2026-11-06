"""Bounded explicit Chroma partitions with an injected embedding boundary.

Indices are not evidence or access control. No default embedding download or
model endpoint is selected; the caller supplies embeddings and scope.
"""

import asyncio
import base64
import hashlib
import math
import json
import threading
import re
import sqlite3
import stat
import struct
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from chromadb.api.models.Collection import Collection

from kernel.memory_pipeline_models import PipelineScope, identity
from kernel.daily_memory_contracts import MAX_MEMORY_QUERY_CHARS
from .pipeline_database import PipelineDatabase, reject_directory_link, resolved_pipeline_path
from .scoped_stores import ScopedStores


_INDEX_DDL = "CREATE TABLE vector_identity(device TEXT NOT NULL, inode TEXT NOT NULL);"
MAX_DAILY_VECTOR_PARTITIONS = 8
_SEGMENT = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
_SQL_FILES = {"chroma.sqlite3", "chroma.sqlite3-journal", "chroma.sqlite3-wal", "chroma.sqlite3-shm"}
_SEGMENT_FILES = {"header.bin", "data_level0.bin", "length.bin", "link_lists.bin", "index_metadata.pickle"}
_CHROMA_TABLES = {
    "migrations", "embeddings_queue", "collection_metadata", "segments",
    "segment_metadata", "tenants", "databases", "collections", "embeddings",
    "embedding_metadata", "max_seq_id", "embedding_fulltext_search",
    "embedding_fulltext_search_data", "embedding_fulltext_search_idx",
    "embedding_fulltext_search_content", "embedding_fulltext_search_docsize",
    "embedding_fulltext_search_config",
}
_CHROMA_MIGRATIONS = {
    ("embeddings_queue", 1, "d3755dfd232be8e8301f4d7fcfb3a486"),
    ("metadb", 1, "2b4cf52c4bb2676e21d6860a4409f856"),
    ("metadb", 2, "12a570f7121b3a8ce750a2a7c36da20f"),
    ("metadb", 3, "f97ad6334aeaa8f419f01110b648b97a"),
    ("sysdb", 1, "38352d725ad1c16074fac420b22b4633"),
    ("sysdb", 2, "2913cb6a503055a95f625448037e8912"),
    ("sysdb", 3, "42d22d0574d31d419c2a0e7f625c93aa"),
    ("sysdb", 4, "048867ce8fcdefe4023c7110e4433591"),
}
_CHROMA_CATALOG_SHA256 = "3dd5bb82a8d6bbb157095a71a769d04c3af1f5fd407a035d7a0ebbd35a25b8cd"
_CHROMA_MIGRATIONS_SHA256 = "d03a521da3a0c9278f134f7c68069513a3ab3e3fd70660b8ea39feb8edc998ba"


def _check_tree(path: Path, *, segment: bool = False) -> None:
    reject_directory_link(path)
    if not path.exists():
        return
    for child in path.iterdir():
        info = child.lstat()
        if getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise ValueError("unsafe vector entry")
        if stat.S_ISDIR(info.st_mode):
            if segment or not _SEGMENT.fullmatch(child.name):
                raise ValueError("unknown vector directory")
            _check_tree(child, segment=True)
        elif (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
              or child.name not in (_SEGMENT_FILES if segment else _SQL_FILES)):
            raise ValueError("unsafe vector file")


def _file_identity(path: Path) -> tuple[str, str]:
    info = path.stat()
    return str(info.st_dev), str(info.st_ino)


def _file_fingerprint(path: Path) -> tuple[int, int, int, int]:
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("vector storage unavailable")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _validate_chroma_readonly(path: Path, scope: PipelineScope,
                              target_ids: tuple[str, ...] = ()) -> None:
    database = path / "chroma.sqlite3"
    if any((path / ("chroma.sqlite3" + suffix)).exists()
           for suffix in ("-journal", "-wal", "-shm")):
        raise ValueError("vector index has unrecognized sidefiles")
    before = _file_fingerprint(database)
    connection = None
    try:
        connection = sqlite3.connect(database.as_uri() + "?mode=ro&immutable=1",
                                     uri=True, timeout=5)
        if connection.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
            raise ValueError("unknown vector journal mode")
        catalog = connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master "
            "WHERE substr(lower(name),1,7) <> 'sqlite_' ORDER BY type,name"
        ).fetchall()
        columns = [(name, connection.execute(
            "PRAGMA table_xinfo(" + json.dumps(name) + ")").fetchall())
            for kind, name, _, _ in catalog if kind == "table"]
        catalog_hash = hashlib.sha256(json.dumps(
            (catalog, columns), sort_keys=True, separators=(",", ":"),
            ensure_ascii=True).encode()).hexdigest()
        if ({name for kind, name, _, _ in catalog if kind == "table"} != _CHROMA_TABLES
                or any(kind not in {"table", "index"} for kind, _, _, _ in catalog)
                or catalog_hash != _CHROMA_CATALOG_SHA256):
            raise ValueError("unknown vector schema")
        migration_rows = connection.execute(
            "SELECT dir,version,filename,sql,hash FROM migrations "
            "ORDER BY dir,version,filename").fetchall()
        migration_hash = hashlib.sha256(json.dumps(
            migration_rows, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True).encode()).hexdigest()
        if (len(migration_rows) != len(_CHROMA_MIGRATIONS)
                or {(row[0], row[1], row[4]) for row in migration_rows}
                != _CHROMA_MIGRATIONS
                or migration_hash != _CHROMA_MIGRATIONS_SHA256):
            raise ValueError("unknown vector migration")
        collection_rows = connection.execute(
            "SELECT id,name,topic,database_id FROM collections").fetchall()
        if len(collection_rows) != 1:
            raise ValueError("unknown vector collection")
        collection_id, name, topic, database_id = collection_rows[0]
        if (name != "daily_evidence" or not _SEGMENT.fullmatch(collection_id)
                or topic != f"persistent://default/default/{collection_id}"
                or database_id != "00000000-0000-0000-0000-000000000000"):
            raise ValueError("vector collection identity changed")
        metadata = connection.execute(
            "SELECT key,str_value,int_value,float_value FROM collection_metadata "
            "WHERE collection_id=?", (collection_id,)).fetchall()
        if (set(metadata) != {("scope", scope.partition_id, None, None),
                              ("schema", None, 2, None),
                              ("hnsw:space", "cosine", None, None)}
                or connection.execute("SELECT COUNT(*) FROM collection_metadata").fetchone()[0]
                != len(metadata)):
            raise ValueError("vector collection identity changed")
        if connection.execute("SELECT id FROM tenants").fetchall() != [("default_tenant",)]:
            raise ValueError("unknown vector tenant")
        if connection.execute("SELECT id,name,tenant_id FROM databases").fetchall() != [
                ("00000000-0000-0000-0000-000000000000", "default_database", "default_tenant")]:
            raise ValueError("unknown vector database")
        segments = connection.execute(
            "SELECT id,type,scope,topic,collection FROM segments").fetchall()
        expected = {("urn:chroma:segment/vector/hnsw-local-persisted", "VECTOR"),
                    ("urn:chroma:segment/metadata/sqlite", "METADATA")}
        if (len(segments) != 2 or {(row[1], row[2]) for row in segments} != expected
                or any(not _SEGMENT.fullmatch(row[0]) or row[3] != topic
                       or row[4] != collection_id for row in segments)):
            raise ValueError("unknown vector segment")
        vector_id = next(row[0] for row in segments if row[2] == "VECTOR")
        if connection.execute(
                "SELECT segment_id,key,str_value,int_value,float_value "
                "FROM segment_metadata").fetchall() != [
                    (vector_id, "hnsw:space", "cosine", None, None)]:
            raise ValueError("unknown vector segment")
        directories = {child.name for child in path.iterdir() if child.is_dir()}
        if not directories <= {vector_id}:
            raise ValueError("unknown vector segment directory")
        metadata_id = next(row[0] for row in segments if row[2] == "METADATA")
        applied = connection.execute(
            "SELECT seq_id FROM max_seq_id WHERE segment_id=?",
            (metadata_id,),
        ).fetchone()
        applied_sequence = (int.from_bytes(applied[0], "big")
                            if applied is not None else 0)
        for start in range(0, len(target_ids), 100):
            batch = target_ids[start:start + 100]
            placeholders = ",".join("?" for _ in batch)
            queued = connection.execute(
                f"SELECT MAX(seq_id) FROM embeddings_queue WHERE id IN ({placeholders})",
                batch,
            ).fetchone()[0]
            if queued is not None and queued > applied_sequence:
                raise RuntimeError("target vector queue has unapplied records")
        if (_file_fingerprint(database) != before or any(
                (path / ("chroma.sqlite3" + suffix)).exists()
                for suffix in ("-journal", "-wal", "-shm"))):
            raise ValueError("vector storage changed during inventory")
    except (sqlite3.Error, OSError):
        raise ValueError("vector storage unavailable") from None
    finally:
        if connection is not None:
            connection.close()


class DailyVectorIndex:
    def __init__(self, stores: ScopedStores, embed: Callable[[list[str]], list[list[float]]], *,
                 max_partitions: int = MAX_DAILY_VECTOR_PARTITIONS):
        if not callable(embed) or type(max_partitions) is not int or not 1 <= max_partitions <= 16:
            raise ValueError("invalid vector configuration")
        self.stores, self.embed, self.max_partitions = stores, embed, max_partitions
        self._collections: dict[str, Collection] = {}
        self._lock = threading.RLock()

    @staticmethod
    def validate_existing(stores: ScopedStores, scope: PipelineScope,
                          target_ids: tuple[str, ...] = ()) -> None:
        for key in target_ids:
            identity(key)
        partition = stores.partition(scope)
        path = partition / "vectors"
        _check_tree(path)
        if not (path / "chroma.sqlite3").is_file():
            raise ValueError("vector identity unavailable")
        def check_identity(db):
            rows = db.execute("SELECT device,inode FROM vector_identity").fetchall()
            if rows != [_file_identity(path / "chroma.sqlite3")]:
                raise ValueError("vector file identity changed")

        authority = PipelineDatabase(stores.root, scope, "vector_index.db", _INDEX_DDL,
                                     0x414C5649)
        authority.validate_existing(check_identity)
        _validate_chroma_readonly(path, scope, target_ids)
        _check_tree(path)
        authority.validate_existing(check_identity)

    def _collection(self, scope: PipelineScope):
        partition = self.stores.partition(scope)
        path = partition / "vectors"
        reject_directory_link(path)
        if resolved_pipeline_path(path).parent != partition:
            raise ValueError("vector partition escaped")
        _check_tree(path)
        authority = PipelineDatabase(self.stores.root, scope, "vector_index.db", _INDEX_DDL, 0x414C5649)
        # The separate authority transaction serializes initialization across processes.
        # If Chroma initialization crashes before commit, keep its files and reject
        # the unrecognized partial index rather than silently adopting or deleting it.
        with authority.transaction() as db:
            _check_tree(path)
            owners = db.execute("SELECT device,inode FROM vector_identity").fetchall()
            if owners:
                if len(owners) != 1 or not (path / "chroma.sqlite3").is_file():
                    raise ValueError("vector identity unavailable")
                if owners[0] != _file_identity(path / "chroma.sqlite3"):
                    raise ValueError("vector file identity changed")
                if scope.partition_id not in self._collections:
                    _validate_chroma_readonly(path, scope)
                    _check_tree(path)
            elif path.exists() and any(path.iterdir()):
                raise ValueError("unmanaged vector index")
            if scope.partition_id not in self._collections:
                if len(self._collections) >= self.max_partitions:
                    raise RuntimeError("vector partition capacity reached")
                from chromadb import PersistentClient
                from chromadb.config import Settings

                path.mkdir(parents=True, exist_ok=True)
                _check_tree(path)
                client = PersistentClient(path=str(path), settings=Settings(anonymized_telemetry=False))
                metadata = {"scope": scope.partition_id, "schema": 2,
                            "hnsw:space": "cosine"}
                collection = (client.get_collection("daily_evidence", embedding_function=None) if owners
                              else client.create_collection("daily_evidence", embedding_function=None, metadata=metadata))
                if collection.metadata != metadata:
                    raise ValueError("vector collection identity changed")
                _check_tree(path)
                if not owners:
                    db.execute("INSERT INTO vector_identity VALUES (?,?)", _file_identity(path / "chroma.sqlite3"))
            else:
                if not owners:
                    raise ValueError("vector identity lost")
                collection = self._collections[scope.partition_id]
        self._collections[scope.partition_id] = collection
        return collection

    async def warm_existing(self, scope: PipelineScope) -> None:
        """Open only an already-authorized index before the first read request."""
        if type(scope) is not PipelineScope:
            raise ValueError("complete vector scope required")

        def warm() -> None:
            with self._lock:
                self.validate_existing(self.stores, scope)
                self._collection(scope)

        await asyncio.to_thread(warm)

    async def retire_worker(self) -> None:
        """An in-process index has no child owner to retire after preparation."""

    async def put(self, scope: PipelineScope, records: list[dict]) -> None:
        def write():
            with self._lock:
                if getattr(self, "_page_read", None) is not None:
                    raise RuntimeError("vector read session owns partition")
                collection = self._collection(scope)
                for record in records:
                    key, text = record["memory_id"], record["payload"]["text"]
                    identity(key)
                    old = collection.get(ids=[key], include=["documents", "metadatas"])
                    if old["ids"]:
                        if (old["documents"] != [text] or
                                old["metadatas"][0].get("payload_hash") != record["payload_hash"]):
                            raise ValueError("vector evidence conflict")
                    else:
                        embeddings = self.embed([text])
                        collection.add(ids=[key], documents=[text], embeddings=embeddings,
                                       metadatas=[{"payload_hash": record["payload_hash"]}])
                    checked = collection.get(ids=[key], include=["documents", "metadatas", "embeddings"])
                    embeddings = checked["embeddings"]
                    if (checked["documents"] != [text]
                            or checked["metadatas"] != [{"payload_hash": record["payload_hash"]}]
                            or embeddings is None or len(embeddings) != 1
                            or len(embeddings[0]) == 0
                            or not all(math.isfinite(float(value)) for value in embeddings[0])):
                        raise RuntimeError("vector receipt unavailable")
        await asyncio.to_thread(write)

    async def receipts(self, scope: PipelineScope, records: list[dict], *,
                       snapshot: bool = False) -> tuple[dict, ...]:
        """Read exact persisted content and metadata for one bounded write batch."""
        if (type(records) is not list or not 1 <= len(records) <= 100
                or type(snapshot) is not bool):
            raise ValueError("invalid vector receipt batch")
        def read():
            with self._lock:
                collection = self._collection(scope)
                receipts = []
                for record in records:
                    key, text = record["memory_id"], record["payload"]["text"]
                    checked = collection.get(ids=[key], include=["documents", "metadatas", "embeddings"])
                    embeddings = checked["embeddings"]
                    if (checked["ids"] != [key] or checked["documents"] != [text]
                            or checked["metadatas"] != [{"payload_hash": record["payload_hash"]}]
                            or embeddings is None or len(embeddings) != 1
                            or len(embeddings[0]) == 0
                            or not all(math.isfinite(float(value)) for value in embeddings[0])):
                        raise RuntimeError("vector receipt unavailable")
                    receipt = {"memory_id": key,
                                     "payload_hash": record["payload_hash"],
                                     "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                                     "embedding_dim": len(embeddings[0])}
                    if snapshot:
                        if not 1 <= len(embeddings[0]) <= 2048:
                            receipt["snapshot"] = {"schema": 1, "unavailable": "vector_capacity_exceeded"}
                        else:
                            body = struct.pack("<" + "f" * len(embeddings[0]), *embeddings[0])
                            receipt["snapshot"] = {"schema": 1,
                                "f32": base64.b64encode(body).decode("ascii"),
                                "sha256": hashlib.sha256(body).hexdigest()}
                    receipts.append(receipt)
                return tuple(receipts)
        return await asyncio.to_thread(read)

    def begin_page_read(self, scope: PipelineScope, request: dict) -> dict:
        from .daily_vector_pages import BINDING_KEYS, PageCursor
        if set(request) != BINDING_KEYS:
            raise ValueError("invalid vector read session")
        with self._lock:
            if getattr(self, "_page_read", None) is not None or scope.partition_id not in self._collections:
                raise RuntimeError("vector page owner unavailable")
            cursor = PageCursor(request)
            self._page_read: tuple[PipelineScope, object] | None = (scope, cursor)
            return cursor.ack()

    def read_numeric_page(self, scope: PipelineScope, request: dict) -> dict:
        from .daily_vector_pages import BINDING_KEYS, PAGE_ROWS, check_deadline, hash_text
        if set(request) != BINDING_KEYS | {"sequence", "last_key", "records"}:
            raise ValueError("invalid vector page request")
        with self._lock:
            active = getattr(self, "_page_read", None)
            if active is None or active[0] != scope:
                raise RuntimeError("vector page owner unavailable")
            cursor = active[1]
            cursor.verify(request)
            records = request["records"]
            if type(records) is not list or not 1 <= len(records) <= PAGE_ROWS:
                raise ValueError("invalid vector page size")
            for record in records:
                if (type(record) is not dict or set(record) != {"memory_id", "payload_hash", "text_hash", "text_bytes"}
                        or type(record["text_bytes"]) is not int or not 0 <= record["text_bytes"] <= 32768):
                    raise ValueError("invalid vector page proof")
                identity(record["memory_id"])
                hash_text(record["payload_hash"])
                hash_text(record["text_hash"])
            ids = [row["memory_id"] for row in records]
            if ids != sorted(set(ids)) or ids[0] <= cursor.last_key:
                raise ValueError("vector page order differs")
            collection = self._collections[scope.partition_id]
            result = []
            start = 0
            while start < len(records):
                check_deadline(cursor.binding["deadline_ns"])
                end, metadata_bytes = start, 0
                while end < len(records):
                    cost = records[end]["text_bytes"] * 8 + 2048
                    if end > start and metadata_bytes + cost > 512 * 1024:
                        break
                    metadata_bytes += cost
                    end += 1
                group = records[start:end]
                fetched = collection.get(ids=[row["memory_id"] for row in group],
                                         include=["documents", "metadatas", "embeddings"])
                found_ids = fetched["ids"]
                if (len(found_ids) != len(group) or len(set(found_ids)) != len(found_ids)
                        or set(found_ids) != {row["memory_id"] for row in group}
                        or fetched["embeddings"] is None):
                    raise ValueError("vector page membership differs")
                positions = {key: index for index, key in enumerate(found_ids)}
                for wanted in group:
                    index = positions[wanted["memory_id"]]
                    text = fetched["documents"][index]
                    meta = fetched["metadatas"][index]
                    vector = fetched["embeddings"][index]
                    if (type(text) is not str or len(text) > 8192
                            or meta != {"payload_hash": wanted["payload_hash"]}
                            or len(text.encode("utf-8")) != wanted["text_bytes"]
                            or hashlib.sha256(text.encode("utf-8")).hexdigest() != wanted["text_hash"]
                            or not 1 <= len(vector) <= 2048
                            or not all(math.isfinite(float(value)) for value in vector)):
                        raise ValueError("vector page source differs")
                    body = struct.pack("<" + "f" * len(vector), *vector)
                    if not all(math.isfinite(value) for value in struct.unpack("<" + "f" * len(vector), body)):
                        raise ValueError("vector page f32 overflow")
                    result.append({"memory_id": wanted["memory_id"], "dimension": len(vector),
                                   "f32": base64.b64encode(body).decode("ascii"),
                                   "sha256": hashlib.sha256(body).hexdigest()})
                del fetched
                start = end
            check_deadline(cursor.binding["deadline_ns"])
            cursor.advance(ids)
            return {"ack": cursor.ack(), "rows": result}

    def end_page_read(self, scope: PipelineScope, request: dict) -> dict:
        from .daily_vector_pages import BINDING_KEYS
        if set(request) != BINDING_KEYS | {"sequence", "last_key", "count"}:
            raise ValueError("invalid vector page EOF")
        with self._lock:
            active = getattr(self, "_page_read", None)
            if active is None or active[0] != scope:
                raise RuntimeError("vector page owner unavailable")
            active[1].verify(request, eof=True)
            result = active[1].ack()
            self._page_read = None
            return result

    async def contains(self, scope: PipelineScope, ids: tuple[str, ...]) -> bool:
        if type(ids) is not tuple or not ids or len(ids) > 100:
            raise ValueError("invalid vector lookup")
        for key in ids:
            identity(key)

        def read():
            with self._lock:
                return set(self._collection(scope).get(ids=list(ids))["ids"]) == set(ids)
        return await asyncio.to_thread(read)

    async def present_ids(self, scope: PipelineScope, ids: tuple[str, ...]) -> tuple[str, ...]:
        if type(ids) is not tuple or not ids or len(ids) > 100:
            raise ValueError("invalid vector lookup")
        for key in ids:
            identity(key)

        def read():
            with self._lock:
                present = set(self._collection(scope).get(ids=list(ids))["ids"])
                return tuple(key for key in ids if key in present)
        return await asyncio.to_thread(read)

    async def search_hits(self, scope: PipelineScope, query: str, *,
                          limit: int = 30) -> tuple[tuple[str, float], ...]:
        """Return ranked candidate IDs and distances, never visibility grants."""
        if (type(scope) is not PipelineScope or type(query) is not str
                or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS
                or type(limit) is not int or not 1 <= limit <= 100):
            raise ValueError("invalid vector search")

        def read():
            with self._lock:
                collection = self._collection(scope)
                embeddings = self.embed([query.strip()])
                rows = collection.query(query_embeddings=embeddings, n_results=limit,
                                        include=["distances"])
                if not rows["ids"]:
                    return ()
                ids, distances = rows["ids"][0], rows["distances"][0]
                if distances is None or len(ids) != len(distances):
                    raise ValueError("vector distances unavailable")
                hits = []
                for key, distance in zip(ids, distances):
                    if (type(distance) not in {int, float} or not math.isfinite(distance)
                            or distance < 0):
                        raise ValueError("invalid vector distance")
                    hits.append((key, float(distance)))
                return tuple(hits)
        return await asyncio.to_thread(read)

    async def search_cosine_hits(self, scope: PipelineScope, query: str, *,
                                 limit: int = 30) -> tuple[tuple[str, float], ...]:
        """Return candidate IDs with scale-independent cosine similarity."""
        if (type(scope) is not PipelineScope or type(query) is not str
                or not query.strip() or len(query) > MAX_MEMORY_QUERY_CHARS
                or type(limit) is not int or not 1 <= limit <= 100):
            raise ValueError("invalid vector search")

        def read():
            with self._lock:
                collection = self._collection(scope)
                query_vector = self.embed([query.strip()])[0]
                query_norm = math.sqrt(sum(float(value) ** 2 for value in query_vector))
                if not math.isfinite(query_norm) or query_norm <= 0:
                    raise ValueError("invalid query embedding")
                rows = collection.query(query_embeddings=[query_vector], n_results=limit,
                                        include=["embeddings"])
                if not rows["ids"]:
                    return ()
                ids = rows["ids"][0]
                vectors = rows["embeddings"]
                if vectors is None or len(vectors) != 1 or len(ids) != len(vectors[0]):
                    raise ValueError("vector embeddings unavailable")
                hits = []
                for key, vector in zip(ids, vectors[0]):
                    if len(vector) != len(query_vector):
                        raise ValueError("vector dimension mismatch")
                    norm = math.sqrt(sum(float(value) ** 2 for value in vector))
                    if not math.isfinite(norm) or norm <= 0:
                        raise ValueError("invalid stored embedding")
                    similarity = sum(float(left) * float(right)
                                     for left, right in zip(query_vector, vector)) / (query_norm * norm)
                    if not math.isfinite(similarity):
                        raise ValueError("invalid vector similarity")
                    hits.append((key, max(-1.0, min(1.0, similarity))))
                hits.sort(key=lambda item: (-item[1], item[0]))
                return tuple(hits[:limit])
        return await asyncio.to_thread(read)

    async def search_ids(self, scope: PipelineScope, query: str, *, limit: int = 30) -> tuple[str, ...]:
        """Keep the ID-only candidate interface for existing callers."""
        return tuple(key for key, _ in await self.search_hits(scope, query, limit=limit))

    async def purge_ids(self, scope: PipelineScope, ids: tuple[str, ...]) -> None:
        """Remove exact vector IDs; source authorization is done by caller."""
        if type(scope) is not PipelineScope or type(ids) is not tuple or len(ids) > 100:
            raise ValueError("invalid vector purge")
        for key in ids:
            identity(key)
        if not ids:
            return

        def remove():
            with self._lock:
                if getattr(self, "_page_read", None) is not None:
                    raise RuntimeError("vector read session owns partition")
                collection = self._collection(scope)
                present = collection.get(ids=list(ids))["ids"]
                if present:
                    collection.delete(ids=present)
                if set(self._collection(scope).get(ids=list(ids))["ids"]):
                    raise RuntimeError("vector purge incomplete")
        await asyncio.to_thread(remove)
