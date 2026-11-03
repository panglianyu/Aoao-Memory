"""Restore the missing baseline fixture used by the daily-online matrix.

The fixture is copied from the main repository's existing isolation tests.
It introduces no production behavior and performs only synthetic SQLite IO.
"""

from contextlib import contextmanager
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace


@contextmanager
def first_registry_creation(monkeypatch, root):
    """Schedule real missing-main then live-journal observations, without fake IO."""
    main = root / "daily_partition_registry.db"
    journal = main.with_name(main.name + "-journal")
    selected = None
    selection = threading.Lock()
    missing, writing, sampled = (threading.Event() for _ in range(3))
    original_stat, original_connect = Path.lstat, sqlite3.connect

    class HeldInitialization(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            result = super().execute(sql, *args, **kwargs)
            if sql == "PRAGMA user_version=2":
                writing.set()
                if not sampled.wait(3):
                    raise AssertionError("registry observation barrier exhausted")
            return result

    def connect(database, *args, **kwargs):
        if database == main.as_uri() + "?mode=rwc":
            kwargs["factory"] = HeldInitialization
        return original_connect(database, *args, **kwargs)

    def lstat(path, *args, **kwargs):
        nonlocal selected
        try:
            info = original_stat(path, *args, **kwargs)
        except FileNotFoundError:
            if path == main:
                with selection:
                    observe = selected is None
                    if observe:
                        selected = threading.get_ident()
                if observe:
                    missing.set()
                    if not writing.wait(3):
                        raise AssertionError("registry observation barrier exhausted")
            raise
        if path == journal and threading.get_ident() == selected:
            sampled.set()
        return info

    with monkeypatch.context() as boundary:
        boundary.setattr(Path, "lstat", lstat)
        boundary.setattr(sqlite3, "connect", connect)
        try:
            yield SimpleNamespace(missing=missing, writing=writing, sampled=sampled)
        finally:
            sampled.set()
