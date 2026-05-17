"""
R runtime preflight gate for the ``reddyproc-rpy2`` backend.

Milestone 5 must begin with a Python-side check that answers:

1. What R runtime would ``rpy2`` bind to?
2. Which R library paths would be used?
3. Is ``REddyProc`` installed in that R environment?
4. Which ``rpy2`` version is in use?
5. Is the discovered R environment project-scoped or explicitly approved?
6. If not, does the preflight stop before scientific comparison?

Per Decision 010 and risk R11, this module is the mechanism for
answering those questions **before** any live REddyProc reference
output is generated.

Safety contract:

- Importing this module does **not** import ``rpy2`` or bind to R.
- ``rpy2`` is lazily imported inside :func:`_discover_r_runtime` only
  when :func:`preflight_reddyproc_r_environment` is actually called.
- No case-study data is touched. No ``postproc()`` or
  ``run_reddyproc_engine()`` is called. No REddyProc reference
  output is produced.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional


# Kept in sync with engine_reddyproc._INSTALL_HINT in spirit, but
# duplicated here so importing ``r_preflight`` does not drag in the
# REddyProc engine module.
_INSTALL_HINT = (
    "REddyProc rpy2 backend requires optional dependencies. Install "
    "Python extra miaproc[reddyproc], install R, and install the R "
    "package REddyProc."
)


# ----------------------------------------------------------------------
# Dataclasses
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RRuntimePreflightPolicy:
    """Policy controlling whether a discovered R runtime is approved.

    Defaults are deliberately conservative: global/personal R is not
    approved unless a caller explicitly permits it (via
    ``allow_global_r=True`` or the ``MIAPROC_ALLOW_GLOBAL_R=1``
    environment variable). Project-scoped approval (``renv.lock`` +
    library under the repo root) and explicit R-home / executable
    matching are stronger and record the approval source.

    ``require_approval`` semantics
    ------------------------------

    - ``True`` (default, safe): the full approval ladder is applied
      (project-scoped → explicit → global override). If none of those
      succeed, the result is ``status="unapproved"`` and
      ``approved=False``. **This is the only mode that qualifies for
      M5 reference-output generation.**
    - ``False`` (explicit opt-out): when R, ``rpy2`` and ``REddyProc``
      are all present, the result is ``status="ok"`` and
      ``approved=True`` with
      ``approval_source="approval-not-required"``. A warning is always
      emitted stating that this mode **must not** be used to generate
      M5 reference output. Intended for local exploration only.
    """

    require_approval: bool = True
    allow_global_r: bool = False
    approved_r_home: Optional[str] = None
    approved_r_executable: Optional[str] = None
    repo_root: Optional[str] = None   # str for JSON-safety; Path coerced


@dataclass(frozen=True)
class _DiscoveredRMetadata:
    """Internal payload produced by live discovery. Tests pass instances
    of this class directly into :func:`_evaluate_r_runtime_policy` so
    the policy layer can be exercised without ``rpy2``.
    """

    rpy2_available: bool = False
    rpy2_version: Optional[str] = None
    r_available: bool = False
    r_executable: Optional[str] = None
    r_home: Optional[str] = None
    r_version: Optional[str] = None
    r_lib_paths: tuple[str, ...] = ()
    reddyproc_version: Optional[str] = None
    discovery_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class RRuntimePreflightResult:
    """Structured preflight outcome. JSON-safe via :meth:`to_dict`."""

    status: str
    approved: bool
    is_project_scoped: bool
    approval_source: Optional[str]
    r_executable: Optional[str]
    r_home: Optional[str]
    r_version: Optional[str]
    r_lib_paths: tuple[str, ...]
    reddyproc_version: Optional[str]
    rpy2_version: Optional[str]
    repo_root: Optional[str]
    warnings: tuple[str, ...] = field(default_factory=tuple)
    errors: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dict with only JSON-safe scalars / lists."""
        d = asdict(self)
        d["r_lib_paths"] = list(self.r_lib_paths)
        d["warnings"] = list(self.warnings)
        d["errors"] = list(self.errors)
        return d


# Valid values for ``RRuntimePreflightResult.status``.
_STATUS_OK = "ok"
_STATUS_MISSING_RPY2 = "missing_rpy2"
_STATUS_MISSING_R = "missing_r"
_STATUS_MISSING_REDDYPROC = "missing_reddyproc"
_STATUS_UNAPPROVED = "unapproved"
_STATUS_ERROR = "error"


# ----------------------------------------------------------------------
# Path helpers
# ----------------------------------------------------------------------


def _paths_equivalent(a: Optional[str], b: Optional[str]) -> bool:
    """Compare two path-like strings with normalization + case folding.

    Behavior:

    - Returns ``False`` when either side is falsy (``None``, ``""``).
    - Resolves each side with ``Path.resolve(strict=False)`` so that
      ``.`` / ``..`` are normalized and non-existent paths do not raise.
    - Applies ``os.path.normcase`` so Windows paths compare
      case-insensitively (``"C:/Temp/R"`` ≡ ``"c:/temp/r"``); on POSIX
      ``normcase`` is identity.
    - Any unexpected exception during resolution falls back to
      ``os.path.normcase(os.path.normpath(...))`` on the raw strings.
    """
    if not a or not b:
        return False

    def _norm(p: str) -> str:
        try:
            resolved = str(Path(p).resolve(strict=False))
        except (OSError, RuntimeError, ValueError):
            resolved = os.path.normpath(str(p))
        return os.path.normcase(resolved)

    try:
        return _norm(a) == _norm(b)
    except Exception:
        return False


def _is_under_repo(child: Optional[str], repo_root: Optional[str]) -> bool:
    """True iff ``child`` resolves to a path inside ``repo_root``."""
    if not child or not repo_root:
        return False
    try:
        child_p = Path(child).resolve()
        root_p = Path(repo_root).resolve()
    except Exception:
        return False
    return root_p == child_p or root_p in child_p.parents


# ----------------------------------------------------------------------
# Pure policy evaluation (no rpy2 / no R)
# ----------------------------------------------------------------------


def _result_from_discovered(
    discovered: _DiscoveredRMetadata,
    *,
    status: str,
    approved: bool,
    is_project_scoped: bool,
    approval_source: Optional[str],
    repo_root: Optional[str],
    warnings: tuple[str, ...],
    extra_errors: tuple[str, ...] = (),
) -> RRuntimePreflightResult:
    return RRuntimePreflightResult(
        status=status,
        approved=approved,
        is_project_scoped=is_project_scoped,
        approval_source=approval_source,
        r_executable=discovered.r_executable,
        r_home=discovered.r_home,
        r_version=discovered.r_version,
        r_lib_paths=tuple(discovered.r_lib_paths),
        reddyproc_version=discovered.reddyproc_version,
        rpy2_version=discovered.rpy2_version,
        repo_root=repo_root,
        warnings=tuple(warnings),
        errors=tuple(discovered.discovery_errors) + tuple(extra_errors),
    )


def _check_project_scoped(
    policy: RRuntimePreflightPolicy,
    discovered: _DiscoveredRMetadata,
) -> Optional[str]:
    """Return an approval-source string if project-scoped, else None.

    Project-scoped means: ``repo_root`` contains ``renv.lock`` AND at
    least one ``.libPaths()`` entry resolves to a path under
    ``repo_root``. Decision 010 prefers this approval form.
    """
    if not policy.repo_root:
        return None
    root = str(policy.repo_root)
    renv_lock = Path(root) / "renv.lock"
    if not renv_lock.exists():
        return None
    repo_libs = [p for p in discovered.r_lib_paths if _is_under_repo(p, root)]
    if not repo_libs:
        return None
    return f"project-scoped (renv.lock in {root!r}; R library under repo)"


def _check_explicit_match(
    policy: RRuntimePreflightPolicy,
    discovered: _DiscoveredRMetadata,
) -> Optional[str]:
    """Return an approval-source string for an explicit policy/env match."""
    env_home = os.environ.get("MIAPROC_APPROVED_R_HOME")
    env_exe = os.environ.get("MIAPROC_APPROVED_R_EXECUTABLE")

    candidates = [
        (policy.approved_r_home, discovered.r_home, "policy.approved_r_home"),
        (env_home, discovered.r_home, "MIAPROC_APPROVED_R_HOME env var"),
        (
            policy.approved_r_executable,
            discovered.r_executable,
            "policy.approved_r_executable",
        ),
        (
            env_exe,
            discovered.r_executable,
            "MIAPROC_APPROVED_R_EXECUTABLE env var",
        ),
    ]
    for approved_value, discovered_value, source in candidates:
        if approved_value and _paths_equivalent(approved_value, discovered_value):
            return source
    return None


def _check_global_override(
    policy: RRuntimePreflightPolicy,
) -> Optional[str]:
    """Return an approval-source string for a global-override allowance."""
    if policy.allow_global_r:
        return "allow_global_r policy flag"
    if os.environ.get("MIAPROC_ALLOW_GLOBAL_R") == "1":
        return "MIAPROC_ALLOW_GLOBAL_R=1 env var"
    return None


def _evaluate_r_runtime_policy(
    discovered: _DiscoveredRMetadata,
    policy: RRuntimePreflightPolicy,
) -> RRuntimePreflightResult:
    """Decide whether the discovered R runtime is approved.

    This function contains all policy logic and no rpy2 calls.
    """
    repo_root = str(policy.repo_root) if policy.repo_root else None

    # 1) rpy2 missing -> nothing else can be decided.
    if not discovered.rpy2_available:
        return _result_from_discovered(
            discovered,
            status=_STATUS_MISSING_RPY2,
            approved=False,
            is_project_scoped=False,
            approval_source=None,
            repo_root=repo_root,
            warnings=(),
            extra_errors=(
                "rpy2 is not importable. " + _INSTALL_HINT,
            ),
        )

    # 2) rpy2 loaded but the R runtime did not initialize.
    if not discovered.r_available:
        return _result_from_discovered(
            discovered,
            status=_STATUS_MISSING_R,
            approved=False,
            is_project_scoped=False,
            approval_source=None,
            repo_root=repo_root,
            warnings=(),
            extra_errors=(
                "rpy2 loaded but R runtime could not be initialized. "
                + _INSTALL_HINT,
            ),
        )

    # 3) R available but REddyProc R package missing.
    if discovered.reddyproc_version is None:
        return _result_from_discovered(
            discovered,
            status=_STATUS_MISSING_REDDYPROC,
            approved=False,
            is_project_scoped=False,
            approval_source=None,
            repo_root=repo_root,
            warnings=(),
            extra_errors=(
                "R is initialized but the REddyProc R package is not "
                "installed in the discovered R library. " + _INSTALL_HINT,
            ),
        )

    # 4) R + rpy2 + REddyProc all present. Evaluate approval.
    warnings: list[str] = []
    approval_source: Optional[str] = None
    is_project_scoped = False

    # Short-circuit: ``require_approval=False`` is an explicit opt-out.
    # The runtime is marked approved but the approval_source records
    # the opt-out and a hard warning flags this mode as unsuitable for
    # M5 reference-output generation. See ``RRuntimePreflightPolicy``
    # docstring and Decision 010.
    if not policy.require_approval:
        return _result_from_discovered(
            discovered,
            status=_STATUS_OK,
            approved=True,
            is_project_scoped=False,
            approval_source="approval-not-required",
            repo_root=repo_root,
            warnings=(
                "require_approval=False: approval ladder bypassed. This "
                "mode MUST NOT be used to generate Milestone 5 REddyProc "
                "reference output. Use project-scoped approval (renv.lock "
                "+ in-repo library) or an explicit approved_r_home / "
                "approved_r_executable for reference runs.",
            ),
        )

    project_scope_source = _check_project_scoped(policy, discovered)
    if project_scope_source is not None:
        approval_source = project_scope_source
        is_project_scoped = True
    else:
        explicit_source = _check_explicit_match(policy, discovered)
        if explicit_source is not None:
            approval_source = explicit_source
        else:
            global_source = _check_global_override(policy)
            if global_source is not None:
                approval_source = global_source
                warnings.append(
                    "Global-R override in effect. This is weaker than "
                    "project-scoped approval; prefer renv or explicit "
                    "approved_r_home for reproducibility."
                )

    if approval_source is None:
        return _result_from_discovered(
            discovered,
            status=_STATUS_UNAPPROVED,
            approved=False,
            is_project_scoped=False,
            approval_source=None,
            repo_root=repo_root,
            warnings=tuple(warnings),
            extra_errors=(
                "Discovered R runtime is not project-scoped and not "
                "explicitly approved. Set approved_r_home, "
                "approved_r_executable, pass allow_global_r=True, or "
                "create renv.lock + project-scoped R library before "
                "accepting REddyProc reference output.",
            ),
        )

    return _result_from_discovered(
        discovered,
        status=_STATUS_OK,
        approved=True,
        is_project_scoped=is_project_scoped,
        approval_source=approval_source,
        repo_root=repo_root,
        warnings=tuple(warnings),
    )


# ----------------------------------------------------------------------
# Live discovery (rpy2 lazy-imported here)
# ----------------------------------------------------------------------


def _discover_rpy2_version(rpy2_module: Any) -> Optional[str]:
    """Return the installed rpy2 version or ``None`` if neither source works.

    rpy2 3.5.x exposed a top-level ``__version__`` attribute; rpy2 3.6.x
    dropped it in favor of the standard ``importlib.metadata`` lookup. Try
    both so the preflight report reflects an importable rpy2 regardless of
    series.
    """
    version = getattr(rpy2_module, "__version__", None)
    if version:
        return str(version)
    try:
        from importlib.metadata import PackageNotFoundError, version as _pkg_version
    except Exception:
        return None
    try:
        return str(_pkg_version("rpy2"))
    except PackageNotFoundError:
        return None
    except Exception:
        return None


def _discover_r_runtime() -> _DiscoveredRMetadata:
    """Best-effort live discovery of the R runtime that ``rpy2`` binds to.

    Never raises. On any failure the corresponding field is left None /
    empty and a message is added to ``discovery_errors``. The caller is
    expected to pass the result to :func:`_evaluate_r_runtime_policy`
    which converts the payload into an actionable
    :class:`RRuntimePreflightResult`.
    """
    errors: list[str] = []

    # rpy2 import
    try:
        import rpy2
    except Exception as exc:
        return _DiscoveredRMetadata(
            rpy2_available=False,
            discovery_errors=(f"rpy2 import failed: {exc!r}",),
        )

    rpy2_version = _discover_rpy2_version(rpy2)

    try:
        import rpy2.robjects as ro
    except Exception as exc:
        errors.append(f"rpy2.robjects import failed: {exc!r}")
        return _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version=rpy2_version,
            discovery_errors=tuple(errors),
        )

    # R initialization check (R.home())
    r_home: Optional[str] = None
    r_available = False
    try:
        r_home = str(ro.r("R.home()")[0])
        r_available = True
    except Exception as exc:
        errors.append(f"R.home() call failed: {exc!r}")

    if not r_available:
        return _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version=rpy2_version,
            r_available=False,
            discovery_errors=tuple(errors),
        )

    r_version: Optional[str] = None
    try:
        r_version = str(ro.r("R.version.string")[0])
    except Exception as exc:
        errors.append(f"R.version.string failed: {exc!r}")

    r_executable: Optional[str] = None
    # Try R-side resolution first; fall back to Sys.which("R"); fall back
    # to the R_HOME env var. On Windows the binary is R.exe.
    _exe_name = "R.exe" if os.name == "nt" else "R"
    _scripts = (
        f'normalizePath(file.path(R.home("bin"), "{_exe_name}"))',
        'Sys.which("R")[[1]]',
    )
    for script in _scripts:
        try:
            candidate = str(ro.r(script)[0])
        except Exception as exc:
            errors.append(f"R executable discovery ({script!r}) failed: {exc!r}")
            continue
        if candidate:
            r_executable = candidate
            break
    if not r_executable:
        env_r_home = os.environ.get("R_HOME")
        if env_r_home:
            r_executable = str(
                Path(env_r_home) / "bin" / ("R.exe" if os.name == "nt" else "R")
            )

    r_lib_paths: tuple[str, ...] = ()
    try:
        raw = ro.r(".libPaths()")
        r_lib_paths = tuple(str(p) for p in raw)
    except Exception as exc:
        errors.append(f".libPaths() failed: {exc!r}")

    reddyproc_version: Optional[str] = None
    try:
        reddyproc_version = str(
            ro.r('as.character(packageVersion("REddyProc"))')[0]
        )
    except Exception as exc:
        # Missing REddyProc is a recoverable state handled by the policy.
        errors.append(f"REddyProc version discovery failed: {exc!r}")

    return _DiscoveredRMetadata(
        rpy2_available=True,
        rpy2_version=rpy2_version,
        r_available=True,
        r_executable=r_executable,
        r_home=r_home,
        r_version=r_version,
        r_lib_paths=r_lib_paths,
        reddyproc_version=reddyproc_version,
        discovery_errors=tuple(errors),
    )


# ----------------------------------------------------------------------
# Public entry point
# ----------------------------------------------------------------------


def preflight_reddyproc_r_environment(
    *,
    policy: Optional[RRuntimePreflightPolicy] = None,
) -> RRuntimePreflightResult:
    """Run a conservative preflight check of the R runtime.

    Safe to call at any time:

    - imports ``rpy2`` only if it is available;
    - never calls ``postproc()`` or ``run_reddyproc_engine()``;
    - never loads case-study data;
    - never writes to disk.

    Returns an :class:`RRuntimePreflightResult`. If ``result.approved``
    is ``False``, Milestone 5 scientific comparison must not proceed
    until approval is recorded.
    """
    if policy is None:
        policy = RRuntimePreflightPolicy()
    discovered = _discover_r_runtime()
    return _evaluate_r_runtime_policy(discovered, policy)


# ----------------------------------------------------------------------
# Markdown report rendering
# ----------------------------------------------------------------------


def _format_lib_paths(paths: tuple[str, ...]) -> str:
    if not paths:
        return "_none discovered_"
    return "\n".join(f"- `{p}`" for p in paths)


def _format_messages(messages: tuple[str, ...]) -> str:
    if not messages:
        return "_none_"
    return "\n".join(f"- {m}" for m in messages)


def render_r_preflight_report(result: RRuntimePreflightResult) -> str:
    """Render a markdown report for a preflight result.

    The report names the R runtime ``rpy2`` would bind to, the R library
    paths that would be used, the ``REddyProc`` version found (if any),
    the ``rpy2`` version, approval status, approval source, and any
    warnings/errors. A trailing line makes it explicit that no
    REddyProc case-study reference output was generated by this
    preflight call.
    """
    approval_line = "**Approved**" if result.approved else "**NOT approved**"
    scope = "project-scoped" if result.is_project_scoped else "not project-scoped"
    approval_source = result.approval_source or "_none_"

    lines = [
        "# REddyProc R Environment Preflight",
        "",
        f"- Status: `{result.status}`",
        f"- {approval_line} ({scope})",
        f"- Approval source: {approval_source}",
        f"- Repo root: `{result.repo_root}`" if result.repo_root else "- Repo root: _not set_",
        "",
        "## R runtime",
        f"- Executable / launch path: `{result.r_executable}`"
        if result.r_executable
        else "- Executable / launch path: _not discovered_",
        f"- `R.home()`: `{result.r_home}`" if result.r_home else "- `R.home()`: _not discovered_",
        f"- R version: `{result.r_version}`" if result.r_version else "- R version: _not discovered_",
        "",
        "### `.libPaths()`",
        _format_lib_paths(result.r_lib_paths),
        "",
        "## Packages",
        f"- `REddyProc`: `{result.reddyproc_version}`"
        if result.reddyproc_version
        else "- `REddyProc`: _not installed in the discovered R library_",
        f"- `rpy2`: `{result.rpy2_version}`"
        if result.rpy2_version
        else "- `rpy2`: _not importable_",
        "",
        "## Warnings",
        _format_messages(result.warnings),
        "",
        "## Errors",
        _format_messages(result.errors),
        "",
        "---",
        "",
        "No REddyProc case-study reference output was generated by this "
        "preflight.",
    ]
    return "\n".join(lines)


# ----------------------------------------------------------------------
# Optional CLI
# ----------------------------------------------------------------------


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m miaproc.eddy.r_preflight",
        description=(
            "Run the REddyProc R-environment preflight and print a "
            "markdown report. Exits non-zero when the discovered R "
            "runtime is not approved, unless --report-only is given."
        ),
    )
    p.add_argument(
        "--repo-root",
        default=None,
        help="Repository root used for project-scoped (renv) approval.",
    )
    p.add_argument(
        "--approved-r-home",
        default=None,
        help="Approved value for R.home() (overrides MIAPROC_APPROVED_R_HOME).",
    )
    p.add_argument(
        "--approved-r-executable",
        default=None,
        help=(
            "Approved R executable / launch path (overrides "
            "MIAPROC_APPROVED_R_EXECUTABLE)."
        ),
    )
    p.add_argument(
        "--allow-global-r",
        action="store_true",
        help=(
            "Allow approval via a global/personal R runtime. Weaker than "
            "project scoping; prefer --repo-root + renv.lock."
        ),
    )
    p.add_argument(
        "--report-only",
        action="store_true",
        help="Always exit 0 and print the report, even when not approved.",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_argparser().parse_args(argv)
    policy = RRuntimePreflightPolicy(
        repo_root=args.repo_root,
        approved_r_home=args.approved_r_home,
        approved_r_executable=args.approved_r_executable,
        allow_global_r=bool(args.allow_global_r),
    )
    result = preflight_reddyproc_r_environment(policy=policy)
    sys.stdout.write(render_r_preflight_report(result) + "\n")
    if args.report_only:
        return 0
    return 0 if result.approved else 1
