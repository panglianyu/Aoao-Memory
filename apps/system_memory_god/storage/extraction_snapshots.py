"""Scoped successful grouped responses; never an Outbox lease authority."""

from dataclasses import asdict
import hashlib
import json

from kernel.challenge_diagnostics import DiagnosticError
from kernel.memory_pipeline_models import SourceRevision, canonical, identity
from .derived_evidence import validate_payload
from .pipeline_database import PipelineDatabase, paged_rows

STRUCTURED_DOMAINS = ("entity", "graph", "rule", "journal", "schedule")
_DDL = """
CREATE TABLE extraction_snapshots (
 cache_key TEXT PRIMARY KEY, source_json TEXT NOT NULL, domain TEXT NOT NULL,
 content_hash TEXT NOT NULL, outputs_json TEXT NOT NULL, digest TEXT NOT NULL);
"""


def validate_group(outputs: object) -> dict:
    if type(outputs) is not dict or set(outputs) != set(STRUCTURED_DOMAINS):
        raise DiagnosticError("group_shape", "invalid grouped extraction fields")
    for domain, values in outputs.items():
        if type(values) is not list or len(values) > 10:
            raise DiagnosticError("group_shape", "invalid grouped extraction count")
        for payload in values:
            validate_payload(domain, payload)
    if len(canonical(outputs)) > 512000:
        raise DiagnosticError("group_shape", "grouped extraction too large")
    return outputs


def extraction_key(source: SourceRevision) -> str:
    return hashlib.sha256(canonical([source.raw_message_id, source.revision, source.processor,
        source.processor_version, source.model, source.prompt_hash]).encode()).hexdigest()


class ExtractionSnapshots:
    def __init__(self, root, scope):
        self._db = PipelineDatabase(root, scope, "extraction_snapshots.db", _DDL, 0x414C4553)

    def exists(self):
        self._db._path_check()
        if not self._db.path.exists() and any(
                self._db.path.with_name(self._db.path.name + suffix).exists()
                for suffix in ("-journal", "-wal", "-shm")):
            raise DiagnosticError("cache_integrity", "extraction snapshot sidefile unavailable")
        return self._db.path.exists()

    @staticmethod
    def _decode(row):
        key, encoded, domain, content_hash, body, digest = row
        try:
            source = SourceRevision(**json.loads(encoded))
            outputs = validate_group(json.loads(body))
        except (ValueError, TypeError, KeyError):
            raise DiagnosticError("cache_integrity", "extraction snapshot integrity conflict") from None
        if (key != extraction_key(source) or domain not in STRUCTURED_DOMAINS
                or encoded != canonical(asdict(source)) or body != canonical(outputs)
                or len(content_hash) != 64
                or digest != hashlib.sha256(canonical(row[:5]).encode()).hexdigest()):
            raise DiagnosticError("cache_integrity", "extraction snapshot integrity conflict")
        return source, domain, content_hash, outputs

    def get(self, source, content_hash):
        if not self._db.path.exists():
            self._db._path_check()
            if any(self._db.path.with_name(self._db.path.name + suffix).exists()
                   for suffix in ("-journal", "-wal", "-shm")):
                raise DiagnosticError("cache_integrity", "extraction snapshot unavailable")
            return None
        with self._db.readonly_snapshot() as db:
            row = db.execute("SELECT * FROM extraction_snapshots WHERE cache_key=?",
                             (extraction_key(source),)).fetchone()
        if row is None:
            return None
        old_source, _, old_hash, outputs = self._decode(row)
        if extraction_key(old_source) != extraction_key(source) or old_hash != content_hash:
            raise DiagnosticError("cache_integrity", "extraction snapshot content conflict")
        return outputs

    def put(self, source, domain, content_hash, outputs):
        validate_group(outputs)
        values = (extraction_key(source), canonical(asdict(source)), domain, content_hash,
                  canonical(outputs))
        row = (*values, hashlib.sha256(canonical(values).encode()).hexdigest())
        self._decode(row)
        with self._db.transaction() as db:
            previous = db.execute("SELECT * FROM extraction_snapshots WHERE cache_key=?",
                                  (row[0],)).fetchone()
            if previous is not None:
                _, _, old_hash, old_outputs = self._decode(previous)
                if old_hash != content_hash or old_outputs != outputs:
                    raise DiagnosticError("cache_integrity", "extraction snapshot conflict")
            else:
                db.execute("INSERT INTO extraction_snapshots VALUES(?,?,?,?,?,?)", row)

    def audit(self, validate_sources):
        with self._db.readonly_snapshot() as db:
            batch = []
            for row in paged_rows(db, "SELECT * FROM extraction_snapshots ORDER BY cache_key"):
                source, domain, _, _ = self._decode(row)
                batch.append((source, domain))
                if len(batch) == 256:
                    validate_sources(tuple(batch))
                    batch.clear()
            if batch:
                validate_sources(tuple(batch))

    def purge(self, raw_message_id):
        identity(raw_message_id)
        with self._db.transaction() as db:
            db.execute("PRAGMA secure_delete=ON")
            db.execute("DELETE FROM extraction_snapshots WHERE "
                       "json_extract(source_json,'$.raw_message_id')=?", (raw_message_id,))
            return db.execute("SELECT count(*) FROM extraction_snapshots WHERE "
                "json_extract(source_json,'$.raw_message_id')=?", (raw_message_id,)).fetchone()[0] == 0
