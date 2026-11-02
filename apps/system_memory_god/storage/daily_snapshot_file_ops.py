"""Windows deletion of fixed numerical-cache objects through bound handles."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
import ctypes
from ctypes import wintypes
from dataclasses import dataclass, field
import hashlib
from io import BytesIO
import os
from pathlib import Path
import re
import stat
from typing import Callable, Iterator

from kernel.project_workspace_io import publish_opened_file

_HASH = re.compile(r"[0-9a-f]{64}")
_TEMP = re.compile(r"(?:staging|pointer)-[0-9a-f]{32}")
_FILES = frozenset({"vectors.f32", "ids.json", "manifest.json"})
_LIMITS = {"vectors.f32": 40 * 1024**2, "ids.json": 2 * 1024**2, "manifest.json": 4096}


def _ordinary(path: Path) -> Path:
    text = str(path)
    if text.startswith("\\\\?\\UNC\\"):
        text = "\\\\" + text[8:]
    elif text.startswith("\\\\?\\"):
        text = text[4:]
    return Path(text)


def _identity(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


def _extended(path: Path) -> Path:
    text = str(_ordinary(path))
    return Path("\\\\?\\UNC\\" + text[2:] if text.startswith("\\\\") else "\\\\?\\" + text)


class _RetainedHandle:
    def __init__(self, kernel, handle):
        self.kernel, self.handle = kernel, handle

    def close(self) -> None:
        if self.handle is not None:
            if not self.kernel.CloseHandle(self.handle):
                error = OSError(ctypes.get_last_error(), "cannot close bound directory")
                setattr(error, "retained_owner", self)
                raise error
            self.handle = None


def _verify_bound_file(kernel, handle, info, before: os.stat_result,
                       expected_hash: str, maximum: int) -> None:
    """Read through the delete handle, keeping its no-write sharing intact."""
    written = (info.written.dwHighDateTime << 32) | info.written.dwLowDateTime
    expected_written = before.st_mtime_ns // 100 + 116444736000000000
    if ((info.size_high << 32) | info.size_low) != before.st_size or written != expected_written:
        raise ValueError("snapshot cleanup content changed")
    read = kernel.ReadFile
    read.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD,
                     ctypes.POINTER(wintypes.DWORD), ctypes.c_void_p]
    read.restype = wintypes.BOOL
    buffer = ctypes.create_string_buffer(64 * 1024)
    received = wintypes.DWORD()
    digest = hashlib.sha256()
    total = 0
    while True:
        if not read(handle, buffer, len(buffer), ctypes.byref(received), None):
            raise OSError(ctypes.get_last_error(), "cannot read bound snapshot")
        if not received.value:
            break
        total += received.value
        if total > maximum:
            raise ValueError("snapshot cleanup file too large")
        digest.update(buffer.raw[:received.value])
    after = type(info)()
    if not kernel.GetFileInformationByHandle(handle, ctypes.byref(after)):
        raise OSError(ctypes.get_last_error(), "cannot inspect bound snapshot")
    stable_fields = ("attributes", "volume", "size_high", "size_low", "links",
                     "index_high", "index_low")
    if (total != before.st_size or digest.hexdigest() != expected_hash
            or any(getattr(after, name) != getattr(info, name) for name in stable_fields)
            or after.written.dwHighDateTime != info.written.dwHighDateTime
            or after.written.dwLowDateTime != info.written.dwLowDateTime):
        raise ValueError("snapshot cleanup content changed")


@contextmanager
def _bound(path: Path, before: os.stat_result | tuple[int, int], *, directory: bool,
           deleting: bool = True, allow_writes: bool = False,
           expected_hash: str | None = None) -> Iterator[Callable[[], None]]:
    """The caller holds the full parent chain. No path-based delete fallback."""
    if os.name != "nt":
        raise OSError("bound snapshot deletion requires Windows")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                       ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes, close.restype = [wintypes.HANDLE], wintypes.BOOL
    handle = create(str(_extended(path)), 0x80000000 | (0x10000 if deleting else 0),
                    3 if allow_writes else 1, None, 3,
                    0x00200000 | (0x02000000 if directory else 0), None)
    if handle == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_last_error(), "cannot bind snapshot deletion")
    try:
        class FileInfo(ctypes.Structure):
            _fields_ = [("attributes", wintypes.DWORD), ("created", wintypes.FILETIME),
                        ("accessed", wintypes.FILETIME), ("written", wintypes.FILETIME),
                        ("volume", wintypes.DWORD), ("size_high", wintypes.DWORD),
                        ("size_low", wintypes.DWORD), ("links", wintypes.DWORD),
                        ("index_high", wintypes.DWORD), ("index_low", wintypes.DWORD)]

        info = FileInfo()
        get_info = kernel.GetFileInformationByHandle
        get_info.argtypes, get_info.restype = [wintypes.HANDLE, ctypes.POINTER(FileInfo)], wintypes.BOOL
        if not get_info(handle, ctypes.byref(info)):
            raise OSError(ctypes.get_last_error(), "cannot inspect bound snapshot")
        # stat_result subclasses tuple, so test that type before treating an
        # explicit (device, inode) tuple as the already captured identity.
        expected_identity = _identity(before) if isinstance(before, os.stat_result) else before
        if (info.attributes & 0x400 or bool(info.attributes & 0x10) != directory
                or (not directory and info.links != 1)
                or (info.volume, (info.index_high << 32) | info.index_low) !=
                expected_identity):
            raise ValueError("snapshot deletion identity changed")
        getter = kernel.GetFinalPathNameByHandleW
        getter.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
        getter.restype = wintypes.DWORD
        size = getter(handle, None, 0, 0)
        if not size:
            raise OSError(ctypes.get_last_error(), "cannot inspect snapshot handle path")
        buffer = ctypes.create_unicode_buffer(size + 1)
        written = getter(handle, buffer, len(buffer), 0)
        if not written or written >= len(buffer):
            raise OSError("invalid snapshot handle path")
        if os.path.normcase(str(_ordinary(Path(buffer.value)))) != os.path.normcase(str(path)):
            raise ValueError("snapshot deletion escaped bound parent")
        if expected_hash is not None:
            if directory or not isinstance(before, os.stat_result):
                raise ValueError("file proof requires a captured file")
            _verify_bound_file(kernel, handle, info, before, expected_hash, _LIMITS.get(path.name, 256))

        class Disposition(ctypes.Structure):
            _fields_ = [("delete_file", wintypes.BOOL)]

        disposition = Disposition(True)
        setter = kernel.SetFileInformationByHandle
        setter.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        setter.restype = wintypes.BOOL
        def remove() -> None:
            if not deleting:
                raise ValueError("directory guard cannot delete")
            if not setter(handle, 4, ctypes.byref(disposition), ctypes.sizeof(disposition)):
                raise OSError(ctypes.get_last_error(), "cannot delete bound snapshot object")
        yield remove
    finally:
        _RetainedHandle(kernel, handle).close()


class OwnedDirectory:
    """Pin a captured root and its anchor while permitting bounded file IO."""

    def __init__(self, root: Path, *, expected: tuple[int, int] | None = None):
        self.root = _ordinary(root)
        anchor = self.root.parent
        if (not self.root.is_absolute() or ".." in self.root.parts
                or self.root in {Path(self.root.anchor), Path.home()}
                or anchor in {Path(self.root.anchor), Path.home()}):
            raise ValueError("explicit owned directory required")
        self.identity = expected or _identity(_extended(self.root).lstat())
        self._stack = ExitStack()
        self._retained: list[_RetainedHandle] = []
        self._active = False
        try:
            self._stack.enter_context(_bound(anchor, _extended(anchor).lstat(),
                directory=True, deleting=False, allow_writes=True))
            self._stack.enter_context(_bound(self.root, self.identity,
                directory=True, deleting=False, allow_writes=True))
            self._active = True
        except BaseException:
            self.close()
            raise

    def publish(self, name: str, body: bytes, *, replace: bool = False) -> None:
        if (not self._active or not re.fullmatch(r"[a-z][a-z0-9.-]{0,63}", name)
                or type(body) is not bytes or not 0 < len(body) <= 64 * 1024):
            raise ValueError("invalid owned publication")
        destination = self.root / name
        if not replace and destination.exists():
            raise FileExistsError("owned publication already exists")
        publish_opened_file(BytesIO(body), destination, root=self.root,
                            max_bytes=64 * 1024, replace=replace)

    def close(self) -> None:
        self._active = False
        for handle in tuple(self._retained):
            handle.close()
            self._retained.remove(handle)
        try:
            self._stack.close()
        except OSError as error:
            retained = getattr(error, "retained_owner", None)
            if retained is not None:
                self._retained.append(retained)
            setattr(error, "retained_owner", self)
            raise

    def __enter__(self) -> OwnedDirectory:
        if not self._active:
            raise ValueError("owned directory closed")
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


@dataclass
class _DeleteProgress:
    before: os.stat_result | tuple[int, int]
    requested: bool = False
    content_hash: str | None = None


@dataclass
class _GenerationProgress:
    directory: _DeleteProgress
    files: dict[str, tuple[os.stat_result, str]]
    requested_files: set[str] = field(default_factory=set)


def _file_proof(path: Path, *, maximum: int | None = None) -> tuple[os.stat_result, str]:
    before = _extended(path).lstat()
    maximum = _LIMITS[path.name] if maximum is None else maximum
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > maximum
            or getattr(before, "st_file_attributes", 0) & 0x400):
        raise ValueError("unsafe snapshot cleanup file")
    digest = hashlib.sha256()
    with _extended(path).open("rb") as handle:
        if _identity(os.fstat(handle.fileno())) != _identity(before):
            raise ValueError("snapshot cleanup file changed")
        size = 0
        while chunk := handle.read(64 * 1024):
            size += len(chunk)
            if size > maximum:
                raise ValueError("snapshot cleanup file too large")
            digest.update(chunk)
        after = os.fstat(handle.fileno())
    current = _extended(path).lstat()
    if (size != before.st_size or _file_stamp(after) != _file_stamp(before)
            or _file_stamp(current) != _file_stamp(before)):
        raise ValueError("snapshot cleanup file changed")
    return before, digest.hexdigest()


def _file_stamp(info: os.stat_result) -> tuple:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_nlink,
            getattr(info, "st_file_attributes", 0))


def _deleted(path: Path) -> None:
    # Only called for this owner's successful delete disposition after its
    # retained native handles have settled, never for arbitrary missing files.
    try:
        _extended(path).lstat()
    except FileNotFoundError:
        return
    raise ValueError("deleted snapshot object remains or was replaced")


class SnapshotFileOps:
    """Only the root and fixed descendants owned by one cache are eligible."""

    def __init__(self, root: Path):
        if os.name != "nt":
            raise OSError("bound snapshot deletion requires Windows")
        self.root = _ordinary(root)
        self.anchor = self.root.parent
        if (not self.root.is_absolute() or self.anchor == Path(self.root.anchor)
                or self.anchor == Path.home()):
            raise ValueError("explicit owned snapshot parent required")
        self.anchor_identity = _identity(_extended(self.anchor).lstat())
        self.root_identity = _identity(_extended(self.root).lstat())
        self._partitions: dict[str, tuple[int, int] | None] = {}
        self._generations: dict[Path, _GenerationProgress] = {}
        self._pointers: dict[Path, _DeleteProgress] = {}
        self._empty: dict[Path, _DeleteProgress] = {}
        self._retained: list[_RetainedHandle] = []

    def _retain(self, error: BaseException) -> None:
        seen: set[int] = set()
        current: BaseException | None = error
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            owner = getattr(current, "retained_owner", None)
            if isinstance(owner, _RetainedHandle) and owner not in self._retained:
                self._retained.append(owner)
            current = current.__context__
        if self._retained:
            setattr(error, "retained_owner", self)

    def close(self) -> None:
        """Settle retained native handles; deletion callers then resume receipts."""
        try:
            for owner in tuple(self._retained):
                owner.close()
                self._retained.remove(owner)
        except BaseException as error:
            self._retain(error)
            raise

    def capture_partition(self, partition: str) -> None:
        if not _HASH.fullmatch(partition):
            raise ValueError("invalid snapshot partition")
        if partition in self._partitions and self._partitions[partition] is None:
            raise ValueError("snapshot partition ownership unknown")
        previous = self._partitions.setdefault(partition, None)
        info = _extended(self.root / partition).lstat()
        if not stat.S_ISDIR(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("unsafe snapshot partition")
        captured = _identity(info)
        if previous is not None and captured != previous:
            raise ValueError("snapshot partition identity changed")
        self._partitions[partition] = captured
        self.close()
        try:
            with self._hold(self.root / partition):
                pass
        except BaseException as error:
            self._retain(error)
            raise

    @contextmanager
    def _hold(self, directory: Path) -> Iterator[None]:
        if os.name != "nt":
            raise OSError("bound snapshot deletion requires Windows")
        # Sibling caches may share this anchor. Guards need READ access only;
        # requesting WRITE here conflicts with another guard's READ-only share.
        # _bound keeps no-write/no-delete sharing and supports extended paths.
        directory = _ordinary(directory)
        if not directory.is_relative_to(self.anchor):
            raise ValueError("snapshot cleanup outside owned anchor")
        with ExitStack() as handles:
            current = self.anchor
            # Never climb outside the captured owner, including long paths.
            handles.enter_context(_bound(current, self.anchor_identity,
                                         directory=True, deleting=False))
            for part in directory.relative_to(current).parts:
                current = current / part
                handles.enter_context(_bound(current, _extended(current).lstat(),
                                             directory=True, deleting=False))
            if _identity(_extended(self.anchor).lstat()) != self.anchor_identity:
                raise ValueError("snapshot anchor identity changed")
            try:
                root_identity = _identity(_extended(self.root).lstat())
            except FileNotFoundError:
                receipt = self._empty.get(self.root)
                if receipt is None or not receipt.requested:
                    raise
            else:
                if root_identity != self.root_identity:
                    raise ValueError("snapshot root identity changed")
            if directory.is_relative_to(self.root) and directory != self.root:
                partition = directory.relative_to(self.root).parts[0]
                captured = _identity(_extended(self.root / partition).lstat())
                if captured != self._partitions.setdefault(partition, captured):
                    raise ValueError("snapshot partition identity changed")
            yield

    def remove_generation(self, partition: str, generation: str,
                          validate: Callable[[], object] | None = None, *,
                          before: os.stat_result | None = None) -> None:
        if not _HASH.fullmatch(partition) or not (
                _HASH.fullmatch(generation) or generation.startswith("staging-") and _TEMP.fullmatch(generation)):
            raise ValueError("invalid managed snapshot directory")
        path = self.root / partition / generation
        self.close()
        try:
            with self._hold(path.parent):
                progress = self._generations.get(path)
                if progress is not None and progress.directory.requested:
                    _deleted(path)
                else:
                    captured_before = progress.directory.before if progress else before or _extended(path).lstat()
                    with _bound(path, captured_before, directory=True) as remove:
                        names = {entry.name for entry in _extended(path).iterdir()}
                        if progress is None:
                            if not names.issubset(_FILES):
                                raise ValueError("unknown snapshot cleanup entry")
                            proofs = {name: _file_proof(path / name) for name in names}
                            if validate is not None:
                                validate()
                            progress = _GenerationProgress(_DeleteProgress(captured_before), proofs)
                            self._generations[path] = progress
                        for name in progress.requested_files:
                            _deleted(path / name)
                        if names != set(progress.files) - progress.requested_files:
                            raise ValueError("snapshot cleanup entries changed")
                        for name in sorted(names):
                            item = path / name
                            info, digest = progress.files[name]
                            with _bound(item, info, directory=False, expected_hash=digest) as remove_file:
                                remove_file()
                                progress.requested_files.add(name)
                        remove()
                        progress.directory.requested = True
            del self._generations[path]
        except BaseException as error:
            self._retain(error)
            raise

    def capture_generation(self, partition: str, generation: str, before: os.stat_result,
                           files: dict[str, tuple[os.stat_result, bytes]]) -> None:
        """Pin only an uncommitted generation and files this publication created."""
        if not _HASH.fullmatch(partition) or not (
                _HASH.fullmatch(generation) or generation.startswith("staging-") and _TEMP.fullmatch(generation)):
            raise ValueError("invalid managed snapshot directory")
        path = self.root / partition / generation
        self.close()
        try:
            with self._hold(path.parent):
                progress = self._generations.get(path)
                if progress is not None:
                    captured = (_identity(progress.directory.before) if isinstance(
                        progress.directory.before, os.stat_result) else progress.directory.before)
                    if captured != _identity(before):
                        raise ValueError("snapshot rollback identity changed")
                    return
                with _bound(path, before, directory=True, deleting=False):
                    names = {entry.name for entry in _extended(path).iterdir()}
                    if not names.issubset(_FILES) or names != set(files):
                        raise ValueError("snapshot rollback entries changed")
                    proofs = {}
                    for name in names:
                        created, contents = files[name]
                        info, digest = _file_proof(path / name)
                        # A failed write may have left only a prefix. It must
                        # still be the opened file and our intended bytes.
                        if (_identity(info) != _identity(created) or info.st_size > len(contents)
                                or digest != hashlib.sha256(contents[:info.st_size]).hexdigest()):
                            raise ValueError("snapshot rollback content changed")
                        proofs[name] = (info, digest)
                    self._generations[path] = _GenerationProgress(_DeleteProgress(before), proofs)
        except BaseException as error:
            self._retain(error)
            raise

    def remove_pointer(self, partition: str, name: str,
                       validate: Callable[[], object], *, before: os.stat_result | None = None,
                       contents: bytes | None = None) -> None:
        if not _HASH.fullmatch(partition) or not (name == "current.json" or
                name.startswith("pointer-") and _TEMP.fullmatch(name)):
            raise ValueError("invalid snapshot pointer")
        path = self.root / partition / name
        self.close()
        try:
            with self._hold(path.parent):
                progress = self._pointers.get(path)
                if progress is not None and progress.requested:
                    _deleted(path)
                else:
                    if progress is None:
                        info, digest = _file_proof(path, maximum=256)
                        if before is not None and _identity(info) != _identity(before):
                            raise ValueError("snapshot rollback pointer changed")
                        if contents is not None and (info.st_size > len(contents)
                                or digest != hashlib.sha256(contents[:info.st_size]).hexdigest()):
                            raise ValueError("snapshot rollback pointer content changed")
                        progress = _DeleteProgress(info, content_hash=digest)
                    validate()
                    self._pointers[path] = progress
                    with _bound(path, progress.before, directory=False,
                                expected_hash=progress.content_hash) as remove:
                        remove()
                        progress.requested = True
            self._pointers.pop(path)
        except BaseException as error:
            self._retain(error)
            raise

    def remove_empty(self, partition: str | None = None) -> None:
        if partition is not None and not _HASH.fullmatch(partition):
            raise ValueError("invalid snapshot partition")
        path = self.root if partition is None else self.root / partition
        self.close()
        try:
            with self._hold(path.parent):
                progress = self._empty.get(path)
                if progress is not None and progress.requested:
                    _deleted(path)
                else:
                    before = self.root_identity if partition is None else self._partitions.get(partition)
                    if before is None:
                        raise ValueError("snapshot partition ownership unknown")
                    progress = self._empty.setdefault(path, _DeleteProgress(before))
                    with _bound(path, progress.before, directory=True) as remove:
                        remove()
                        progress.requested = True
            self._empty.pop(path)
            if partition is not None:
                self._partitions.pop(partition, None)
        except BaseException as error:
            self._retain(error)
            raise
