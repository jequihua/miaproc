"""Conservative run publication for local and mounted (FUSE) storage (M002-S03, D005/C4).

One invocation owns one fresh run directory. Artifacts are written and
validated in invocation-owned local scratch, then copied sequentially to
fresh destinations in the run directory (exclusive create, explicit flush,
checked close). A small versioned completion manifest is written LAST; the
closed, valid manifest is the publication point. Consumers call
:func:`validate_run` and accept nothing else.

Deliberately not used: rename/replace, file locks, append, in-place
overwrite, fsync-as-commit and any cloud SDK. An existence check is not a
distributed lock: callers/orchestrators own exclusive run names. Cleanup is
bounded to the scratch directory this invocation created and the files it
created itself; prior runs and shared prefixes are never deleted.

The scratch byte limit bounds the logical bytes of every file this
invocation writes into its scratch (tables, diagnostics, run metadata and
the staged manifest). It is charged as bytes are written, so scratch never
holds more than the limit; it is not a cap on Python/R/Arrow memory, process
RSS, filesystem allocation overhead or mount caches.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import platform
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Optional

logger = logging.getLogger(__name__)

MANIFEST_NAME = "_MIAPROC_RUN_COMPLETE.json"
MANIFEST_KIND = "miaproc.run-manifest"
MANIFEST_VERSION = 1
MANIFEST_MAX_BYTES = 8 * 1024 * 1024
DEFAULT_SCRATCH_MAX_BYTES = 4 * 1024 ** 3
SCRATCH_DIR_ENV = "MIAPROC_SCRATCH_DIR"
SCRATCH_MAX_BYTES_ENV = "MIAPROC_SCRATCH_MAX_BYTES"
_CHUNK = 1024 * 1024


class RunValidationError(ValueError):
    """A run directory is missing, incomplete or does not match its manifest."""


class PublicationError(RuntimeError):
    """An observed failure before the publication point (nothing consumable)."""


class ScratchBudgetExceeded(PublicationError):
    """Writing more staged bytes would exceed the scratch byte limit (nothing published)."""


class AmbiguousPublicationError(RuntimeError):
    """The completion manifest may exist, but the producer cannot confirm a clean finish.

    Either the manifest was written and validates (the run IS published), or
    its write/close or readback failed in a way that cannot prove the run was
    not published. The producer reports a distinct nonzero status and never
    deletes the run; consumers decide with :func:`validate_run`. Retries must
    use a fresh run name.
    """


def _resolve(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def _inside(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def safe_relative_name(name: str) -> str:
    """Return ``name`` if it is a safe relative POSIX artifact name, else raise."""
    if not isinstance(name, str) or not name or "\\" in name or "\x00" in name:
        raise RunValidationError(f"unsafe artifact name {name!r}")
    pure = PurePosixPath(name)
    if (pure.is_absolute() or pure.as_posix() != name or ":" in pure.parts[0]
            or any(p in ("", ".", "..") for p in pure.parts)):
        raise RunValidationError(f"unsafe or non-canonical artifact name {name!r}")
    return pure.as_posix()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def table_identity(path: Path, columns: Optional[list[str]] = None) -> dict[str, Any]:
    """Row count and schema identity of a staged table, read back from bytes."""
    if path.suffix.lower() in (".parquet", ".pq"):
        import pyarrow.parquet as pq

        meta = pq.ParquetFile(path).metadata
        schema = meta.schema.to_arrow_schema()
        text = schema.remove_metadata().to_string(show_field_metadata=False)
        return {"rows": int(meta.num_rows), "columns": len(schema.names),
                "schema_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest()}
    import pandas as pd

    frame = pd.read_csv(path)
    names = [str(c) for c in frame.columns]
    return {"rows": int(len(frame)), "columns": len(names),
            "schema_sha256": hashlib.sha256("\n".join(names).encode("utf-8")).hexdigest()}


def _open_dest(path: Path):
    """Seam for fault injection: open a fresh destination (exclusive create)."""
    return open(path, "xb")


def _open_scratch(path: Path):
    """Seam for observation: open a fresh scratch file (exclusive create, unbuffered)."""
    return open(path, "xb", buffering=0)


def copy_closed(src: Path, dst: Path, *, progress: Optional[dict[str, bool]] = None) -> None:
    """Copy ``src`` to the fresh ``dst``: sequential writes, flush, checked close.

    ``dst`` must not exist (no overwrite). Write/flush/close errors propagate;
    a close error is never swallowed because FUSE drivers report upload
    failures at close. When given, ``progress["opened"]`` records that the
    destination was created; from then on any byte may have reached storage,
    and the error-path close may still finalize it.
    """
    fout = _open_dest(dst)
    if progress is not None:
        progress["opened"] = True
    try:
        with open(src, "rb") as fin:
            for block in iter(lambda: fin.read(_CHUNK), b""):
                fout.write(block)
        fout.flush()
    except BaseException:
        try:
            fout.close()
        except Exception:  # noqa: BLE001 - the original error wins
            pass
        raise
    fout.close()


class _ScratchBudget:
    """Aggregate logical bytes written to invocation-owned scratch files.

    Each file is charged for the furthest extent it reaches (rewriting bytes
    below that extent, e.g. a local seek/backpatch, adds nothing). A write
    that would raise the total above the limit is refused before any of its
    bytes reach the file.
    """

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.total = 0
        self._extents: dict[str, int] = {}

    def charge(self, key: str, end: int, *, name: str) -> None:
        old = self._extents.get(key, 0)
        if end <= old:
            return
        total = self.total + (end - old)
        if total > self.limit:
            raise ScratchBudgetExceeded(
                f"staging {name} would write {total} bytes into scratch, over the scratch byte limit "
                f"{self.limit}; raise --scratch-max-bytes or MIAPROC_SCRATCH_MAX_BYTES within the "
                "task's scratch/memory budget. Nothing was published.")
        self._extents[key] = end
        self.total = total


class _BudgetedScratchFile(io.RawIOBase):
    """A fresh scratch file whose written extent is charged to a :class:`_ScratchBudget`."""

    def __init__(self, path: Path, budget: _ScratchBudget, name: str) -> None:
        super().__init__()
        self._budget, self._key, self._name = budget, str(path), name
        self._fh = _open_scratch(path)

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._fh.tell()

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._fh.seek(offset, whence)

    def write(self, data) -> int:
        size = memoryview(data).nbytes
        self._budget.charge(self._key, self._fh.tell() + size, name=self._name)
        return self._fh.write(data)

    def close(self) -> None:
        if not self.closed:
            try:
                self._fh.close()
            finally:
                super().close()


def resolve_scratch(scratch_dir: Optional[Path], max_bytes: Optional[int]) -> tuple[Path, int]:
    base = scratch_dir or (Path(os.environ[SCRATCH_DIR_ENV]) if os.environ.get(SCRATCH_DIR_ENV) else None)
    base = _resolve(base) if base is not None else _resolve(Path(tempfile.gettempdir()))
    if max_bytes is None:
        env = os.environ.get(SCRATCH_MAX_BYTES_ENV)
        try:
            max_bytes = int(env) if env else DEFAULT_SCRATCH_MAX_BYTES
        except ValueError as exc:
            raise ValueError(f"{SCRATCH_MAX_BYTES_ENV} must be a positive integer byte count "
                             f"(got {env!r}).") from exc
    if max_bytes <= 0:
        raise ValueError(f"scratch byte budget must be positive (got {max_bytes}).")
    return base, int(max_bytes)


class RunPublisher:
    """Stage artifacts in owned scratch, then publish them to one fresh run directory."""

    def __init__(self, *, stage: str, run_dir: Path, outputs: dict[str, Path], inputs: list[Path],
                 scratch_dir: Optional[Path] = None, scratch_max_bytes: Optional[int] = None) -> None:
        self.stage = stage
        self.run_dir_arg = Path(run_dir)
        self.run_dir = _resolve(run_dir)
        if self.run_dir.exists():
            raise ValueError(
                f"run directory already exists: {run_dir}. Each invocation needs a fresh run "
                "directory (choose a new run name); existing runs are never reused or overwritten.")
        if self.run_dir.parent == self.run_dir:
            raise ValueError(f"run directory cannot be a filesystem root: {run_dir}")
        self.scratch_base, self.scratch_max_bytes = resolve_scratch(scratch_dir, scratch_max_bytes)
        for raw in inputs:
            inp = _resolve(raw)
            if _inside(self.run_dir, inp) or _inside(inp, self.run_dir):
                raise ValueError(f"run directory {run_dir} overlaps input {raw}; inputs are immutable.")
            if _inside(self.scratch_base, inp):
                raise ValueError(f"scratch directory {self.scratch_base} lies inside input {raw}.")
        if _inside(self.scratch_base, self.run_dir):
            raise ValueError("scratch directory must not lie inside the run directory.")
        self.names: dict[str, str] = {}
        for flag, path in outputs.items():
            self.names[flag] = self.relative(path, flag=flag)
        seen: dict[str, str] = {}
        for flag, rel in self.names.items():
            if rel.casefold() in seen:
                raise ValueError(f"{flag} and {seen[rel.casefold()]} name the same run artifact {rel!r}.")
            seen[rel.casefold()] = flag
        self._scratch: Optional[Path] = None
        self._artifacts: dict[str, dict[str, Any]] = {}
        self._budget = _ScratchBudget(self.scratch_max_bytes)

    @property
    def staged_bytes(self) -> int:
        """Logical bytes written into this invocation's scratch so far."""
        return self._budget.total

    # -- staging -----------------------------------------------------------
    def relative(self, path: Path, *, flag: str = "output") -> str:
        resolved = _resolve(path)
        if resolved == self.run_dir or not _inside(resolved, self.run_dir):
            raise ValueError(f"{flag} {path} must lie inside the run directory {self.run_dir_arg} "
                             "(no traversal outside the run).")
        return safe_relative_name(resolved.relative_to(self.run_dir).as_posix())

    @property
    def scratch(self) -> Path:
        if self._scratch is None:
            self.scratch_base.mkdir(parents=True, exist_ok=True)
            self._scratch = Path(tempfile.mkdtemp(prefix=f"miaproc-{self.stage}-", dir=self.scratch_base))
        return self._scratch

    def staged(self, path: Path, *, role: str, **extra: Any) -> Path:
        """Register ``path`` (inside the run dir) as an artifact; return its scratch path."""
        rel = self.relative(path)
        key = rel.casefold()
        if key in self._artifacts:
            raise ValueError(f"artifact name collision inside the run directory: {rel!r}")
        self._artifacts[key] = {"path": rel, "role": role, **extra}
        dest = self.scratch.joinpath(*PurePosixPath(rel).parts)
        dest.parent.mkdir(parents=True, exist_ok=True)
        return dest

    def _scratch_name(self, path: Path) -> str:
        resolved = _resolve(path)
        if self._scratch is None or not _inside(resolved, _resolve(self._scratch)):
            raise ValueError(f"{path} is not inside this invocation's scratch directory")
        return resolved.relative_to(_resolve(self._scratch)).as_posix()

    def open_staged(self, path: Path) -> io.BufferedWriter:
        """Open the fresh scratch file ``path`` (from :meth:`staged`) for binary writing.

        Every byte written is charged to the scratch byte limit before it
        reaches the file; exceeding it raises :class:`ScratchBudgetExceeded`.
        """
        raw = _BudgetedScratchFile(path, self._budget, self._scratch_name(path))
        return io.BufferedWriter(raw)

    def open_staged_text(self, path: Path, *, newline: Optional[str] = None) -> io.TextIOWrapper:
        """Text (UTF-8) counterpart of :meth:`open_staged`."""
        return io.TextIOWrapper(self.open_staged(path), encoding="utf-8", newline=newline)

    def cleanup_scratch(self) -> None:
        if self._scratch is not None:
            shutil.rmtree(self._scratch, ignore_errors=True)
            self._scratch = None

    # -- publication -------------------------------------------------------
    def _validate_staged(self) -> list[dict[str, Any]]:
        total = 0
        records = []
        for key in sorted(self._artifacts):
            rec = dict(self._artifacts[key])
            src = self.scratch.joinpath(*PurePosixPath(rec["path"]).parts)
            if not src.is_file():
                raise PublicationError(f"staged artifact missing: {rec['path']}")
            rec["size"] = src.stat().st_size
            total += rec["size"]
            if total > self.scratch_max_bytes:
                raise PublicationError(
                    f"staged artifacts exceed the scratch budget ({total} > {self.scratch_max_bytes} bytes); "
                    "raise --scratch-max-bytes or MIAPROC_SCRATCH_MAX_BYTES within the task memory/disk budget.")
            rec["sha256"] = sha256_file(src)
            fmt = src.suffix.lower().lstrip(".")
            rec["format"] = "parquet" if fmt in ("parquet", "pq") else fmt
            if rec["format"] in ("parquet", "csv"):
                ident = table_identity(src)
                expected = rec.pop("expected_rows", None)
                if expected is not None and ident["rows"] != expected:
                    raise PublicationError(
                        f"staged table {rec['path']} reads back {ident['rows']} rows, expected {expected}.")
                rec.update(ident)
            elif rec["format"] == "json":
                with open(src, encoding="utf-8") as fh:
                    json.load(fh)
            records.append(rec)
        return records

    def publish(self, *, run_metadata: Path, diagnostics: Optional[Path] = None) -> dict[str, Any]:
        """Publish staged artifacts, manifest last. Returns the manifest.

        Raises :class:`PublicationError` before the publication point (after
        bounded cleanup of files this call created) and
        :class:`AmbiguousPublicationError` when the manifest may have been
        published but the finish cannot be confirmed (never deleted).
        """
        records = self._validate_staged()
        from miaproc import __version__ as miaproc_version

        manifest = {
            "manifest_kind": MANIFEST_KIND,
            "manifest_version": MANIFEST_VERSION,
            "stage": self.stage,
            "run_name": self.run_dir.name,
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "producer": {"miaproc": miaproc_version, "python": platform.python_version()},
            "run_metadata": self.relative(run_metadata),
            "diagnostics": self.relative(diagnostics) if diagnostics is not None else None,
            "artifacts": records,
        }
        staged_manifest = self.scratch / MANIFEST_NAME
        with self.open_staged_text(staged_manifest) as fh:
            fh.write(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

        created: list[Path] = []
        made_dirs: list[Path] = []
        try:
            try:
                self.run_dir.parent.mkdir(parents=True, exist_ok=True)
                self.run_dir.mkdir(exist_ok=False)
            except FileExistsError as exc:
                raise PublicationError(f"run directory appeared concurrently: {self.run_dir_arg}") from exc
            made_dirs.append(self.run_dir)
            for rec in records:
                dst = self.run_dir.joinpath(*PurePosixPath(rec["path"]).parts)
                for parent in reversed(dst.parents):
                    if _inside(parent, self.run_dir) and parent != self.run_dir and not parent.exists():
                        parent.mkdir()
                        made_dirs.append(parent)
                src = self.scratch.joinpath(*PurePosixPath(rec["path"]).parts)
                created.append(dst)
                copy_closed(src, dst)
                if dst.stat().st_size != rec["size"] or sha256_file(dst) != rec["sha256"]:
                    raise PublicationError(f"published artifact {rec['path']} does not match its staged bytes")
        except BaseException as exc:
            self._cleanup_owned(created, made_dirs)
            if isinstance(exc, Exception) and not isinstance(exc, PublicationError):
                raise PublicationError(f"publication failed before the manifest: {exc}") from exc
            raise

        manifest_path = self.run_dir / MANIFEST_NAME
        progress: dict[str, bool] = {}
        try:
            copy_closed(staged_manifest, manifest_path, progress=progress)
        except BaseException as exc:
            outcome = self._manifest_outcome(progress)
            if outcome == "not_published":
                self._cleanup_owned(created, made_dirs)
                if isinstance(exc, Exception):
                    raise PublicationError(f"completion manifest could not be created: {exc}") from exc
                raise
            if not isinstance(exc, Exception):
                raise  # an interruption wins; the possibly published run is kept
            if outcome == "published":
                raise AmbiguousPublicationError(
                    f"completion manifest write reported {type(exc).__name__}: {exc}, yet the run validates; "
                    f"the run at {self.run_dir_arg} IS published. Do not reuse its name.") from exc
            raise AmbiguousPublicationError(
                f"completion manifest write reported {type(exc).__name__}: {exc} after the manifest was "
                f"created; whether the run at {self.run_dir_arg} was published cannot be confirmed, so "
                "nothing was deleted. Run 'miaproc eddy validate-run' before use; retry only under a fresh "
                "run name.") from exc
        # Publication point passed. Anything below is post-publication.
        try:
            return validate_run(self.run_dir)
        except Exception as exc:  # noqa: BLE001
            raise AmbiguousPublicationError(
                f"manifest written at {manifest_path} but published readback failed: {exc}. "
                "The run is not deleted; consumers validate it before use.") from exc

    def _manifest_outcome(self, progress: dict[str, bool]) -> str:
        """Classify a failed manifest copy as ``published``, ``not_published`` or ``unknown``.

        Non-publication is established only when the manifest destination was
        never created. Once it exists, a write, flush or close error may follow
        a complete write that the error-path close still finalizes, and any
        readback (failed, missing, stale or mismatching) proves nothing: the
        run is kept and reported as ``unknown`` unless it validates.
        """
        if not progress.get("opened"):
            return "not_published"
        try:
            validate_run(self.run_dir)
            return "published"
        except Exception:  # noqa: BLE001 - a failed readback is not evidence by itself
            return "unknown"

    @staticmethod
    def _cleanup_owned(files: list[Path], dirs: list[Path]) -> None:
        """Best-effort removal of files/dirs this invocation created (never others)."""
        for path in reversed(files):
            try:
                if path.is_file():
                    path.unlink()
            except OSError as exc:
                logger.warning("could not remove invocation-owned partial file %s: %s", path, exc)
        for directory in reversed(dirs):
            try:
                directory.rmdir()
            except OSError:
                pass


def find_run_dir(artifact: Path) -> Optional[Path]:
    """Nearest ancestor of ``artifact`` holding a completion manifest, or None."""
    for parent in _resolve(artifact).parents:
        if (parent / MANIFEST_NAME).is_file():
            return parent
    return None


def validate_run(run_dir: Path, *, stage: Optional[str] = None) -> dict[str, Any]:
    """Validate a published run: manifest identity and every artifact's size/checksum."""
    root = _resolve(run_dir)
    manifest_path = root / MANIFEST_NAME
    if not manifest_path.is_file():
        raise RunValidationError(f"{run_dir} has no completion manifest {MANIFEST_NAME}; the run is "
                                 "incomplete or not a published miaproc run.")
    size = manifest_path.stat().st_size
    if size > MANIFEST_MAX_BYTES:
        raise RunValidationError(f"manifest is implausibly large ({size} bytes)")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunValidationError(f"manifest is truncated or corrupt: {exc}") from exc
    if not isinstance(manifest, dict) or manifest.get("manifest_kind") != MANIFEST_KIND:
        raise RunValidationError("manifest kind is not a miaproc run manifest")
    if manifest.get("manifest_version") != MANIFEST_VERSION:
        raise RunValidationError(f"unsupported manifest_version {manifest.get('manifest_version')!r}")
    if stage is not None and manifest.get("stage") != stage:
        raise RunValidationError(f"run stage is {manifest.get('stage')!r}, expected {stage!r}")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise RunValidationError("manifest lists no artifacts")
    seen: set[str] = set()
    for rec in artifacts:
        if not isinstance(rec, dict):
            raise RunValidationError("malformed artifact record")
        rel = safe_relative_name(rec.get("path"))
        if rel.casefold() in seen or rel == MANIFEST_NAME:
            raise RunValidationError(f"duplicate or reserved artifact name {rel!r}")
        seen.add(rel.casefold())
        path = root.joinpath(*PurePosixPath(rel).parts)
        if not path.is_file():
            raise RunValidationError(f"artifact {rel} is missing")
        if path.stat().st_size != rec.get("size"):
            raise RunValidationError(f"artifact {rel} size {path.stat().st_size} != manifest {rec.get('size')}")
        if sha256_file(path) != rec.get("sha256"):
            raise RunValidationError(f"artifact {rel} checksum does not match the manifest")
    for ref in ("run_metadata", "diagnostics"):
        value = manifest.get(ref)
        if value is not None and safe_relative_name(value).casefold() not in seen:
            raise RunValidationError(f"manifest {ref} reference {value!r} is not a listed artifact")
    return manifest


def artifact_record(manifest: dict[str, Any], run_dir: Path, path: Path) -> dict[str, Any]:
    """Return the manifest record for ``path`` (inside ``run_dir``) or raise."""
    root, target = _resolve(run_dir), _resolve(path)
    if not _inside(target, root):
        raise RunValidationError(f"{path} is outside run {run_dir}")
    rel = target.relative_to(root).as_posix()
    for rec in manifest["artifacts"]:
        if rec["path"] == rel:
            return rec
    raise RunValidationError(f"{rel} is not listed in the run manifest of {run_dir}")
