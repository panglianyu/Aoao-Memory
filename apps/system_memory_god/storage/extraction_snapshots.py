"""Scoped successful grouped responses; never an Outbox lease authority."""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from kernel.challenge_diagnostics import (DiagnosticError, _pinned_directories, _windows_api,
    check_private_path, ensure_private_directory, open_private_file)
from kernel.memory_pipeline_models import SourceRevision, canonical, identity
from .derived_evidence import validate_payload
from .pipeline_database import PipelineDatabase, paged_rows

STRUCTURED_DOMAINS = ("entity", "graph", "rule", "journal", "schedule")
ADMISSION_POLICY = "empty-entity-value/v1"
MAX_ADMISSION_BYTES = 1048576
_HASH = re.compile(r"[0-9a-f]{64}\Z")
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


def _sha(value):
    return hashlib.sha256(canonical(value).encode("ascii")).hexdigest()


def _conflict():
    return DiagnosticError("cache_integrity", "extraction admission integrity conflict")


def _io_path(path):
    """Equivalent long Win32 spelling only at the private filesystem boundary."""
    if os.name != "nt":
        return path
    spelling = str(path)
    if spelling.startswith("\\\\?\\"):
        return path
    return Path("\\\\?\\UNC\\" + spelling[2:] if spelling.startswith("\\\\") else "\\\\?\\" + spelling)


def validate_admission(outputs, admission):
    """Reconstruct every excluded candidate; never relax accepted evidence checks."""
    try:
        validate_group(outputs)
        if (type(admission) is not dict or set(admission) != {
                "policy", "input_sha256", "output_sha256", "rejected"}
                or admission["policy"] != ADMISSION_POLICY
                or any(type(admission[key]) is not str or not _HASH.fullmatch(admission[key])
                       for key in ("input_sha256", "output_sha256"))
                or admission["output_sha256"] != _sha(outputs)
                or type(admission["rejected"]) is not list
                or not 1 <= len(admission["rejected"]) <= 9
                or not outputs["entity"]
                or len(outputs["entity"]) + len(admission["rejected"]) > 10):
            raise _conflict()
        restored = {**outputs, "entity": list(outputs["entity"])}
        previous = -1
        for item in admission["rejected"]:
            if (type(item) is not dict or set(item) != {"index", "candidate", "sha256", "reason"}
                    or type(item["index"]) is not int or not previous < item["index"] <= 9
                    or item["index"] > len(restored["entity"])
                    or item["reason"] != "empty_value"):
                raise _conflict()
            candidate = item["candidate"]
            if (type(candidate) is not dict or set(candidate) != {"subject", "attribute", "value"}
                    or type(candidate["value"]) is not str or candidate["value"] != ""
                    or item["sha256"] != _sha(candidate)):
                raise _conflict()
            validate_payload("entity", {**candidate, "value": "valid"})
            previous = item["index"]
            restored["entity"].insert(previous, dict(candidate))
        if len(canonical(restored)) > 512000 or _sha(restored) != admission["input_sha256"]:
            raise _conflict()
        return restored
    except (ValueError, TypeError, KeyError, UnicodeError, OverflowError):
        raise _conflict() from None


class ExtractionSnapshots:
    def __init__(self, root, scope):
        self._db = PipelineDatabase(root, scope, "extraction_snapshots.db", _DDL, 0x414C4553)

    def exists(self):
        self._db._path_check()
        present = self._db.path.exists()
        if not present:
            if any(self._db.path.with_name(self._db.path.name + suffix).exists()
                   for suffix in ("-journal", "-wal", "-shm")):
                raise DiagnosticError("cache_integrity", "extraction snapshot sidefile unavailable")
            try:
                _io_path(self._db.path.parent / "extraction-admission-v185").lstat()
            except FileNotFoundError:
                pass
            else:
                # A missing database does not erase its private sidecar evidence.
                raise _conflict()
        return present

    @staticmethod
    def _decode(row, *, receipt=None, scope=None):
        try:
            key, encoded, domain, content_hash, body, digest = row
            source = SourceRevision(**json.loads(encoded))
            outputs = validate_group(json.loads(body))
            if (key != extraction_key(source) or domain not in STRUCTURED_DOMAINS
                    or encoded != canonical(asdict(source)) or body != canonical(outputs)
                    or type(content_hash) is not str or len(content_hash) != 64):
                raise _conflict()
            if digest == _sha(row[:5]):
                return source, domain, content_hash, outputs
            if (type(receipt) is not dict or set(receipt) != {"schema", "scope", "source", "content_hash",
                    "policy", "input_sha256", "output_sha256", "rejected"}
                    or receipt["schema"] != "extraction-admission-185-v1"
                    or scope is None or receipt["scope"] != asdict(scope)
                    or receipt["source"] != asdict(source) or receipt["content_hash"] != content_hash):
                raise _conflict()
            admission = {name: receipt[name] for name in (
                "policy", "input_sha256", "output_sha256", "rejected")}
            validate_admission(outputs, admission)
            receipt_body = canonical(receipt).encode("ascii")
            if (len(receipt_body) > MAX_ADMISSION_BYTES
                    or digest != _sha([ADMISSION_POLICY, row[:5], hashlib.sha256(receipt_body).hexdigest()])):
                raise _conflict()
        except (ValueError, TypeError, KeyError, UnicodeError, OverflowError):
            raise DiagnosticError("cache_integrity", "extraction snapshot integrity conflict") from None
        return source, domain, content_hash, outputs

    def _receipt_path(self, source, digest):
        if type(source) is not SourceRevision or type(digest) is not str or not _HASH.fullmatch(digest):
            raise _conflict()
        raw_hash = hashlib.sha256(source.raw_message_id.encode("utf-8")).hexdigest()
        return self._db.path.parent / "extraction-admission-v185" / raw_hash / (digest + ".json")

    def _read_receipt(self, path, *, durable=False):
        """Read existing bytes; reuse may re-fsync the same owned handle."""
        descriptor = None
        try:
            with _pinned_directories(_io_path(path.parent)):
                check_private_path(_io_path(path))
                before = _io_path(path).lstat()
                if before.st_size > MAX_ADMISSION_BYTES:
                    raise _conflict()
                if os.name == "nt":
                    import msvcrt
                    ctypes, _, kernel, _ = _windows_api()
                    access = 0xC0000000 if durable else 0x80000000
                    handle = kernel.CreateFileW(str(_io_path(path)), access, 1, None, 3, 0x00200080, None)
                    if handle == ctypes.c_void_p(-1).value:
                        raise _conflict()
                    try:
                        descriptor = msvcrt.open_osfhandle(handle,
                            (os.O_RDWR if durable else os.O_RDONLY) | os.O_BINARY)
                    except BaseException:
                        kernel.CloseHandle(handle)
                        raise
                else:
                    descriptor = os.open(path, (os.O_RDWR if durable else os.O_RDONLY)
                                         | getattr(os, "O_NOFOLLOW", 0))
                os.set_inheritable(descriptor, False)
                opened, named = os.fstat(descriptor), _io_path(path).lstat()
                check_private_path(_io_path(path))
                if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                        or (opened.st_dev, opened.st_ino, opened.st_size) !=
                        (before.st_dev, before.st_ino, before.st_size)
                        or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)):
                    raise _conflict()
                with os.fdopen(descriptor, "r+b" if durable else "rb") as stream:
                    descriptor = None
                    body = stream.read(MAX_ADMISSION_BYTES + 1)
                    if len(body) != opened.st_size or len(body) > MAX_ADMISSION_BYTES:
                        raise _conflict()
                    decoded = json.loads(body.decode("ascii"))
                    if canonical(decoded).encode("ascii") != body:
                        raise _conflict()
                    if durable:
                        os.fsync(stream.fileno())
                return decoded
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, OverflowError, RecursionError):
            raise _conflict() from None
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _write_receipt(self, source, digest, receipt):
        path = self._receipt_path(source, digest)
        body = canonical(receipt).encode("ascii")
        if len(body) > MAX_ADMISSION_BYTES:
            raise _conflict()
        self._db._path_check()
        # PipelineDatabase creates its scope directory only on a transaction.
        # Keep these files inside that same explicit partition.
        ensure_private_directory(_io_path(self._db.path.parent))
        ensure_private_directory(_io_path(path.parent.parent))
        ensure_private_directory(_io_path(path.parent))
        try:
            descriptor = open_private_file(_io_path(path), exclusive=True)
        except FileExistsError:
            if canonical(self._read_receipt(path, durable=True)).encode("ascii") != body:
                raise _conflict() from None
        else:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def _decode_row(self, row):
        try:
            receipt = None
            if row[5] != _sha(row[:5]):
                source = SourceRevision(**json.loads(row[1]))
                receipt = self._read_receipt(self._receipt_path(source, row[5]))
            return self._decode(row, receipt=receipt, scope=self._db.scope)
        except (ValueError, TypeError, KeyError, IndexError, UnicodeError, OverflowError, RecursionError):
            raise _conflict() from None

    def get(self, source, content_hash):
        if not self.exists():
            return None
        with self._db.readonly_snapshot() as db:
            row = db.execute("SELECT * FROM extraction_snapshots WHERE cache_key=?",
                             (extraction_key(source),)).fetchone()
        if row is None:
            return None
        old_source, _, old_hash, outputs = self._decode_row(row)
        if extraction_key(old_source) != extraction_key(source) or old_hash != content_hash:
            raise DiagnosticError("cache_integrity", "extraction snapshot content conflict")
        return outputs

    def put(self, source, domain, content_hash, outputs, *, admission=None):
        validate_group(outputs)
        values = (extraction_key(source), canonical(asdict(source)), domain, content_hash,
                  canonical(outputs))
        receipt = None
        digest = _sha(values)
        if admission is not None:
            validate_admission(outputs, admission)
            receipt = {"schema": "extraction-admission-185-v1", "scope": asdict(self._db.scope),
                       "source": asdict(source), "content_hash": content_hash, **admission}
            body = canonical(receipt).encode("ascii")
            if len(body) > MAX_ADMISSION_BYTES:
                raise _conflict()
            digest = _sha([ADMISSION_POLICY, values, hashlib.sha256(body).hexdigest()])
        row = (*values, digest)
        self._decode(row, receipt=receipt, scope=self._db.scope)
        with self._db.transaction() as db:
            previous = db.execute("SELECT * FROM extraction_snapshots WHERE cache_key=?",
                                  (row[0],)).fetchone()
            if previous is not None:
                _, _, old_hash, old_outputs = self._decode_row(previous)
                if old_hash != content_hash or old_outputs != outputs:
                    raise DiagnosticError("cache_integrity", "extraction snapshot conflict")
                if admission is not None:
                    old_receipt = self._read_receipt(self._receipt_path(
                        SourceRevision(**json.loads(previous[1])), previous[5]))
                    if any(old_receipt[name] != admission[name] for name in admission):
                        raise _conflict()
            else:
                if receipt is not None:
                    self._write_receipt(source, digest, receipt)
                db.execute("INSERT INTO extraction_snapshots VALUES(?,?,?,?,?,?)", row)

    def audit(self, validate_sources):
        with self._db.readonly_snapshot() as db:
            batch = []
            for row in paged_rows(db, "SELECT * FROM extraction_snapshots ORDER BY cache_key"):
                source, domain, _, _ = self._decode_row(row)
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
            absent = db.execute("SELECT count(*) FROM extraction_snapshots WHERE "
                "json_extract(source_json,'$.raw_message_id')=?", (raw_message_id,)).fetchone()[0] == 0
        parent = self._db.path.parent / "extraction-admission-v185"
        directory = parent / hashlib.sha256(raw_message_id.encode("utf-8")).hexdigest()
        check_private_path(_io_path(parent), directory=True)
        if _io_path(parent).exists():
            check_private_path(_io_path(directory), directory=True)
        if _io_path(directory).exists():
            with _pinned_directories(_io_path(directory)):
                for path in _io_path(directory).iterdir():
                    if not re.fullmatch(r"[0-9a-f]{64}\.json", path.name):
                        raise _conflict()
                    check_private_path(path)
                    _io_path(path).unlink()
            _io_path(directory).rmdir()
        return absent and not _io_path(directory).exists()
