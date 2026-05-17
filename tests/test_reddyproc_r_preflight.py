"""Python-only tests for the REddyProc R-environment preflight gate.

These tests never import ``rpy2`` and never call
:func:`miaproc.eddy.preflight_reddyproc_r_environment` directly with a
live discovery. They exercise the policy logic via
``_evaluate_r_runtime_policy(discovered, policy)`` with fake
:class:`miaproc.eddy.r_preflight._DiscoveredRMetadata` payloads.

The live-discovery path has an opt-in counterpart in
``test_eddy_reddyproc_live.py`` guarded by
``MIAPROC_RUN_R_PREFLIGHT=1``.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

import os

from miaproc.eddy.r_preflight import (
    RRuntimePreflightPolicy,
    RRuntimePreflightResult,
    _DiscoveredRMetadata,
    _discover_rpy2_version,
    _evaluate_r_runtime_policy,
    _paths_equivalent,
    preflight_reddyproc_r_environment,
    render_r_preflight_report,
)


# ----------------------------------------------------------------------
# Import safety
# ----------------------------------------------------------------------


class TestImportSafety:
    def test_importing_miaproc_does_not_import_rpy2(self):
        """Default package import must be safe on machines without rpy2.
        Running this assertion inside a pytest process is not a perfect
        proof (something earlier in collection could have imported rpy2),
        but it surfaces any regression that chains rpy2 through
        ``miaproc.eddy.__init__``."""
        # If rpy2 is installed we cannot assert absence, but a fresh
        # ``import miaproc`` call must not *re*-import it as a side
        # effect. Use a subprocess for a clean check.
        import subprocess

        code = (
            "import sys\n"
            "assert 'rpy2' not in sys.modules\n"
            "import miaproc\n"
            "assert 'rpy2' not in sys.modules, sorted(m for m in sys.modules if 'rpy2' in m)\n"
            "from miaproc.eddy import preflight_reddyproc_r_environment, r_preflight\n"
            "assert 'rpy2' not in sys.modules, sorted(m for m in sys.modules if 'rpy2' in m)\n"
            "print('ok')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, (
            f"subprocess import check failed:\nSTDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )
        assert result.stdout.strip() == "ok"

    def test_r_preflight_module_is_importable_without_rpy2(self):
        """A direct import of ``r_preflight`` must also not trigger
        rpy2."""
        import subprocess

        code = (
            "import sys\n"
            "from miaproc.eddy import r_preflight\n"
            "assert 'rpy2' not in sys.modules\n"
            "print('ok')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "ok"


# ----------------------------------------------------------------------
# Policy: missing dependencies
# ----------------------------------------------------------------------


class TestMissingDependencies:
    def test_missing_rpy2_returns_actionable_error(self):
        discovered = _DiscoveredRMetadata(
            rpy2_available=False,
            discovery_errors=("rpy2 import failed: ModuleNotFoundError(...)",),
        )
        policy = RRuntimePreflightPolicy()
        result = _evaluate_r_runtime_policy(discovered, policy)
        assert result.status == "missing_rpy2"
        assert result.approved is False
        assert result.is_project_scoped is False
        assert result.approval_source is None
        # Error must name all three install prerequisites.
        joined = " ".join(result.errors)
        assert "rpy2" in joined
        assert "R" in joined
        assert "REddyProc" in joined

    def test_missing_r_runtime_after_rpy2_loaded(self):
        discovered = _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=False,
            discovery_errors=("R.home() call failed: ...",),
        )
        result = _evaluate_r_runtime_policy(discovered, RRuntimePreflightPolicy())
        assert result.status == "missing_r"
        assert result.approved is False
        assert any("R runtime" in e for e in result.errors)

    def test_missing_reddyproc_with_r_available(self):
        discovered = _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=True,
            r_home="/usr/lib/R",
            r_version="R version 4.3.2",
            r_lib_paths=("/usr/lib/R/library",),
            reddyproc_version=None,
        )
        result = _evaluate_r_runtime_policy(discovered, RRuntimePreflightPolicy())
        assert result.status == "missing_reddyproc"
        assert result.approved is False
        assert result.r_home == "/usr/lib/R"
        assert result.reddyproc_version is None


# ----------------------------------------------------------------------
# Policy: global R is not silently approved
# ----------------------------------------------------------------------


class TestUnapprovedGlobalR:
    def test_global_r_with_no_policy_overrides_is_unapproved(self, monkeypatch):
        # Scrub the approval env vars.
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        discovered = _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=True,
            r_executable="C:/Program Files/R/R-4.3.2/bin/R.exe",
            r_home="C:/Program Files/R/R-4.3.2",
            r_version="R version 4.3.2",
            r_lib_paths=("C:/Program Files/R/R-4.3.2/library",),
            reddyproc_version="1.3.3",
        )
        policy = RRuntimePreflightPolicy()
        result = _evaluate_r_runtime_policy(discovered, policy)
        assert result.status == "unapproved"
        assert result.approved is False
        assert result.is_project_scoped is False
        assert result.approval_source is None
        # Error message must mention the mitigation paths.
        joined = " ".join(result.errors)
        assert "project-scoped" in joined or "approved_r_home" in joined

    def test_allow_global_r_policy_flag_approves_with_warning(self, monkeypatch):
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        discovered = _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=True,
            r_home="/usr/lib/R",
            r_version="R version 4.3.2",
            r_lib_paths=("/usr/lib/R/library",),
            reddyproc_version="1.3.3",
        )
        result = _evaluate_r_runtime_policy(
            discovered, RRuntimePreflightPolicy(allow_global_r=True)
        )
        assert result.status == "ok"
        assert result.approved is True
        assert result.is_project_scoped is False
        assert "allow_global_r" in (result.approval_source or "")
        # Global override must emit a warning about weaker approval.
        assert any("Global" in w or "weaker" in w.lower() for w in result.warnings)

    def test_miaproc_allow_global_r_env_approves_with_warning(self, monkeypatch):
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.setenv("MIAPROC_ALLOW_GLOBAL_R", "1")

        discovered = _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=True,
            r_home="/usr/lib/R",
            r_lib_paths=("/usr/lib/R/library",),
            reddyproc_version="1.3.3",
        )
        result = _evaluate_r_runtime_policy(discovered, RRuntimePreflightPolicy())
        assert result.approved is True
        assert (result.approval_source or "").startswith("MIAPROC_ALLOW_GLOBAL_R")
        assert result.warnings   # non-empty


# ----------------------------------------------------------------------
# Policy: explicit approval records the source
# ----------------------------------------------------------------------


class TestExplicitApproval:
    def _base_discovered(self, r_home: str, r_exe: str) -> _DiscoveredRMetadata:
        return _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=True,
            r_home=r_home,
            r_executable=r_exe,
            r_version="R version 4.3.2",
            r_lib_paths=(f"{r_home}/library",),
            reddyproc_version="1.3.3",
        )

    def test_approved_r_home_matches(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        # Use tmp_path so the resolved path is real and stable.
        r_home = str(tmp_path / "R-4.3.2")
        (tmp_path / "R-4.3.2").mkdir()
        discovered = self._base_discovered(r_home, f"{r_home}/bin/R")
        policy = RRuntimePreflightPolicy(approved_r_home=r_home)
        result = _evaluate_r_runtime_policy(discovered, policy)
        assert result.approved is True
        assert result.status == "ok"
        assert result.approval_source == "policy.approved_r_home"
        assert result.is_project_scoped is False   # not renv

    def test_MIAPROC_APPROVED_R_HOME_env_matches(
        self, tmp_path: Path, monkeypatch
    ):
        r_home = str(tmp_path / "R-4.3.2")
        (tmp_path / "R-4.3.2").mkdir()
        monkeypatch.setenv("MIAPROC_APPROVED_R_HOME", r_home)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        discovered = self._base_discovered(r_home, f"{r_home}/bin/R")
        result = _evaluate_r_runtime_policy(discovered, RRuntimePreflightPolicy())
        assert result.approved is True
        assert "MIAPROC_APPROVED_R_HOME" in (result.approval_source or "")

    def test_approved_r_executable_policy_matches(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        r_home_dir = tmp_path / "R-4.3.2"
        bin_dir = r_home_dir / "bin"
        bin_dir.mkdir(parents=True)
        r_exe = bin_dir / "R"
        r_exe.write_text("#!fake")

        discovered = self._base_discovered(str(r_home_dir), str(r_exe))
        result = _evaluate_r_runtime_policy(
            discovered,
            RRuntimePreflightPolicy(approved_r_executable=str(r_exe)),
        )
        assert result.approved is True
        assert result.approval_source == "policy.approved_r_executable"


# ----------------------------------------------------------------------
# Policy: project-scoped renv approval
# ----------------------------------------------------------------------


class TestProjectScopedApproval:
    def test_renv_lock_plus_library_under_repo(
        self, tmp_path: Path, monkeypatch
    ):
        # Scrub env vars so only the renv path can approve.
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        repo_root = tmp_path
        (repo_root / "renv.lock").write_text("{}")
        renv_lib = repo_root / "renv" / "library" / "R-4.3" / "x86_64"
        renv_lib.mkdir(parents=True)

        discovered = _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=True,
            r_home="/some/global/R",
            r_executable="/some/global/R/bin/R",
            r_version="R version 4.3.2",
            r_lib_paths=(str(renv_lib), "/some/global/R/library"),
            reddyproc_version="1.3.3",
        )
        policy = RRuntimePreflightPolicy(repo_root=str(repo_root))
        result = _evaluate_r_runtime_policy(discovered, policy)
        assert result.approved is True
        assert result.status == "ok"
        assert result.is_project_scoped is True
        assert "project-scoped" in (result.approval_source or "")
        # Repo root is reported in the result for reviewer visibility.
        assert result.repo_root == str(repo_root)

    def test_renv_lock_without_repo_library_is_not_project_scoped(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        repo_root = tmp_path
        (repo_root / "renv.lock").write_text("{}")
        # No library path under the repo.

        discovered = _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=True,
            r_home="/some/global/R",
            r_executable="/some/global/R/bin/R",
            r_lib_paths=("/some/global/R/library",),
            reddyproc_version="1.3.3",
        )
        result = _evaluate_r_runtime_policy(
            discovered, RRuntimePreflightPolicy(repo_root=str(repo_root))
        )
        assert result.approved is False
        assert result.is_project_scoped is False
        assert result.status == "unapproved"


# ----------------------------------------------------------------------
# Markdown report
# ----------------------------------------------------------------------


class TestMarkdownReport:
    def _approved_result(self) -> RRuntimePreflightResult:
        return RRuntimePreflightResult(
            status="ok",
            approved=True,
            is_project_scoped=True,
            approval_source="project-scoped (renv.lock in '/repo'; R library under repo)",
            r_executable="/repo/renv/R/bin/R",
            r_home="/repo/renv/R",
            r_version="R version 4.3.2",
            r_lib_paths=("/repo/renv/library/R-4.3", "/some/global/lib"),
            reddyproc_version="1.3.3",
            rpy2_version="3.5.15",
            repo_root="/repo",
            warnings=("example warning",),
            errors=(),
        )

    def test_report_contains_required_fields(self):
        text = render_r_preflight_report(self._approved_result())
        assert "ok" in text
        assert "Approved" in text
        assert "project-scoped" in text
        assert "/repo/renv/R/bin/R" in text
        assert "/repo/renv/R" in text
        assert "R version 4.3.2" in text
        assert "/repo/renv/library/R-4.3" in text
        assert "1.3.3" in text
        assert "3.5.15" in text
        assert "example warning" in text

    def test_report_states_no_reference_output_generated(self):
        text = render_r_preflight_report(self._approved_result())
        assert (
            "No REddyProc case-study reference output was generated by this "
            "preflight." in text
        )

    def test_report_does_not_contain_python_object_repr_noise(self):
        text = render_r_preflight_report(self._approved_result())
        # No Python object repr leakage from tuples / dataclasses.
        assert "RRuntimePreflightResult(" not in text
        assert "<object" not in text
        assert "at 0x" not in text

    def test_report_for_missing_rpy2(self):
        result = RRuntimePreflightResult(
            status="missing_rpy2",
            approved=False,
            is_project_scoped=False,
            approval_source=None,
            r_executable=None,
            r_home=None,
            r_version=None,
            r_lib_paths=(),
            reddyproc_version=None,
            rpy2_version=None,
            repo_root=None,
            warnings=(),
            errors=("rpy2 is not importable. Install miaproc[reddyproc], ...",),
        )
        text = render_r_preflight_report(result)
        assert "NOT approved" in text
        assert "missing_rpy2" in text
        assert "not importable" in text


# ----------------------------------------------------------------------
# JSON-safe serialization
# ----------------------------------------------------------------------


class TestJsonSafeSerialization:
    def test_to_dict_is_json_dumps_safe(self):
        result = RRuntimePreflightResult(
            status="ok",
            approved=True,
            is_project_scoped=True,
            approval_source="policy.approved_r_home",
            r_executable="/r/bin/R",
            r_home="/r",
            r_version="R version 4.3.2",
            r_lib_paths=("/r/library",),
            reddyproc_version="1.3.3",
            rpy2_version="3.5.15",
            repo_root="/repo",
            warnings=("w1", "w2"),
            errors=(),
        )
        d = result.to_dict()
        # json.dumps raises if anything is non-JSON-safe.
        s = json.dumps(d, sort_keys=True)
        # Round-trip and spot-check fields.
        reloaded = json.loads(s)
        assert reloaded["status"] == "ok"
        assert reloaded["approved"] is True
        assert reloaded["r_lib_paths"] == ["/r/library"]
        assert reloaded["warnings"] == ["w1", "w2"]
        assert reloaded["errors"] == []


# ----------------------------------------------------------------------
# End-to-end through the public entry point (still no real rpy2)
# ----------------------------------------------------------------------


class TestRequireApprovalSemantics:
    """``require_approval`` was previously exposed without wired-up
    behavior. The M5 preflight polish pass made the field explicit:
    ``True`` (default) applies the full approval ladder; ``False`` is an
    explicit opt-out that approves but emits a warning flagging the
    runtime as unsuitable for M5 reference-output generation."""

    def _global_r_discovered(self) -> _DiscoveredRMetadata:
        return _DiscoveredRMetadata(
            rpy2_available=True,
            rpy2_version="3.5.15",
            r_available=True,
            r_home="/usr/lib/R",
            r_executable="/usr/lib/R/bin/R",
            r_version="R version 4.3.2",
            r_lib_paths=("/usr/lib/R/library",),
            reddyproc_version="1.3.3",
        )

    def test_require_approval_false_short_circuits_and_warns(self, monkeypatch):
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        discovered = self._global_r_discovered()
        policy = RRuntimePreflightPolicy(require_approval=False)
        result = _evaluate_r_runtime_policy(discovered, policy)

        assert result.status == "ok"
        assert result.approved is True
        assert result.is_project_scoped is False
        assert result.approval_source == "approval-not-required"
        assert result.warnings, "require_approval=False must always warn"
        joined = " ".join(result.warnings)
        assert "MUST NOT" in joined.upper() or "must not" in joined
        assert "Milestone 5" in joined or "reference output" in joined

    def test_require_approval_true_still_unapproved_for_global_r(
        self, monkeypatch
    ):
        """Sanity regression: the default path is unchanged."""
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        discovered = self._global_r_discovered()
        result = _evaluate_r_runtime_policy(
            discovered, RRuntimePreflightPolicy(require_approval=True)
        )
        assert result.status == "unapproved"
        assert result.approved is False
        assert result.approval_source is None

    def test_require_approval_false_still_reports_missing_dependencies(
        self, monkeypatch
    ):
        """The opt-out must not mask missing-dependency states."""
        monkeypatch.delenv("MIAPROC_APPROVED_R_HOME", raising=False)
        monkeypatch.delenv("MIAPROC_APPROVED_R_EXECUTABLE", raising=False)
        monkeypatch.delenv("MIAPROC_ALLOW_GLOBAL_R", raising=False)

        discovered = _DiscoveredRMetadata(rpy2_available=False)
        result = _evaluate_r_runtime_policy(
            discovered, RRuntimePreflightPolicy(require_approval=False)
        )
        # Missing rpy2 still blocks approval even with the opt-out.
        assert result.status == "missing_rpy2"
        assert result.approved is False


class TestPathsEquivalent:
    """Direct unit tests for ``_paths_equivalent``. The review asked for
    explicit coverage so the function's behavior is no longer
    speculative."""

    def test_none_or_empty_is_never_equivalent(self):
        assert _paths_equivalent(None, None) is False
        assert _paths_equivalent("", "") is False
        assert _paths_equivalent(None, "/tmp/x") is False
        assert _paths_equivalent("/tmp/x", None) is False
        assert _paths_equivalent("", "/tmp/x") is False

    def test_dot_dot_normalization_is_equivalent(self, tmp_path):
        """Two paths that resolve to the same location after ``..``
        normalization are equivalent. Uses ``tmp_path`` so the resolved
        form is stable on both Windows and POSIX."""
        base = tmp_path
        a = str(base / "sub" / ".." / "sub")
        b = str(base / "sub")
        assert _paths_equivalent(a, b) is True

    @pytest.mark.skipif(
        os.name != "nt",
        reason="Case-insensitive path comparison is a Windows semantic.",
    )
    def test_case_different_windows_paths_are_equivalent(self, tmp_path):
        base = str(tmp_path)
        upper = base.upper()
        lower = base.lower()
        assert _paths_equivalent(upper, lower) is True

    def test_non_existent_paths_do_not_raise(self, tmp_path):
        ghost_a = tmp_path / "definitely_not_there"
        ghost_b = tmp_path / "also_absent" / "x"
        # No exception; just a boolean result.
        result = _paths_equivalent(str(ghost_a), str(ghost_b))
        assert result is False

    def test_non_existent_identical_paths_are_equivalent(self, tmp_path):
        """Two non-existent paths that are textually identical after
        normalization must still compare equal. This matters for
        ``MIAPROC_APPROVED_R_EXECUTABLE`` matching against a discovered
        executable path that may not exist on disk from Python's
        perspective (e.g. a path reported by ``Sys.which('R')`` inside
        the R runtime)."""
        ghost = tmp_path / "R-4.3" / "bin" / "R"
        assert _paths_equivalent(str(ghost), str(ghost)) is True

    def test_clearly_different_paths_are_not_equivalent(self, tmp_path):
        a = str(tmp_path / "left")
        b = str(tmp_path / "right")
        assert _paths_equivalent(a, b) is False


class TestRpy2VersionDiscovery:
    """rpy2 3.6.x dropped ``__version__``; the preflight must still report
    a version string via ``importlib.metadata.version("rpy2")``."""

    def test_top_level_attribute_wins_when_present(self):
        class _Fake:
            __version__ = "3.5.15"

        assert _discover_rpy2_version(_Fake()) == "3.5.15"

    def test_falls_back_to_importlib_metadata_for_rpy2_36(self, monkeypatch):
        """rpy2 3.6.7 exposes its version through ``importlib.metadata``.

        Simulate a rpy2 module without a top-level ``__version__`` and
        confirm the preflight falls back to the package metadata lookup
        and returns a non-empty string.
        """
        class _NoVersionRpy2:
            pass

        import importlib.metadata as _md

        def _fake_version(name: str) -> str:
            assert name == "rpy2"
            return "3.6.7"

        monkeypatch.setattr(_md, "version", _fake_version)
        assert _discover_rpy2_version(_NoVersionRpy2()) == "3.6.7"

    def test_returns_none_when_package_not_found(self, monkeypatch):
        class _NoVersionRpy2:
            pass

        import importlib.metadata as _md

        def _raise(name: str):
            raise _md.PackageNotFoundError(name)

        monkeypatch.setattr(_md, "version", _raise)
        assert _discover_rpy2_version(_NoVersionRpy2()) is None


class TestPublicEntryPointWithoutRpy2:
    def test_public_function_returns_missing_rpy2_without_real_rpy2(
        self, monkeypatch
    ):
        """``preflight_reddyproc_r_environment`` must gracefully return
        ``status="missing_rpy2"`` when rpy2 cannot be imported, without
        raising. This simulates absence even if rpy2 happens to be
        installed in the reviewer's environment."""
        monkeypatch.setitem(sys.modules, "rpy2", None)
        monkeypatch.setitem(sys.modules, "rpy2.robjects", None)

        result = preflight_reddyproc_r_environment()
        assert result.status == "missing_rpy2"
        assert result.approved is False
        assert "rpy2" in " ".join(result.errors)
