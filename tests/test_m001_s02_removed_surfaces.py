"""M001-S02 negative checks: the biomass product surface is removed.

These checks go beyond text search. They exercise the import system, the
argparse CLI (in process and as a separate interpreter process), the package
metadata and packaging configuration in ``pyproject.toml``, the packages and
files setuptools would collect from ``src/``, and the Docker install extras.
PyArrow must stay an unconditional runtime dependency for the eddy Parquet
file workflow (no biomass or BigQuery extra needed).
"""
from __future__ import annotations

import argparse
import importlib
import importlib.util
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

import miaproc
from miaproc import cli

# Candidate source under test (src layout), independent of where this test
# module itself lives.
SRC = Path(miaproc.__file__).resolve().parents[1]
PKG_ROOT = SRC.parent

REMOVED_ARGV = (
    ["biomass"],
    ["biomass", "enrich-table", "--help"],
    ["biomass", "run-bigquery", "--help"],
)


def _pyproject() -> dict:
    return tomllib.loads((PKG_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _run_cli_process(*argv: str, cwd: Path) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in (str(SRC), env.get("PYTHONPATH", "")) if p)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "miaproc.cli", *argv],
        cwd=cwd, env=env, capture_output=True, text=True, timeout=120, check=False,
    )


class TestImportRemoved:
    def test_biomass_package_has_no_import_spec(self):
        assert importlib.util.find_spec("miaproc.biomass") is None

    def test_import_biomass_fails(self):
        with pytest.raises(ModuleNotFoundError, match=r"No module named 'miaproc\.biomass'"):
            importlib.import_module("miaproc.biomass")


class TestCliRemoved:
    def test_top_level_commands_are_eddy_only(self):
        parser = cli._build_parser()
        sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        assert list(sub.choices) == ["run", "eddy"]

    def test_top_level_help_does_not_offer_biomass(self, capsys):
        with pytest.raises(SystemExit) as exc:
            cli.main(["--help"])
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "eddy" in out
        assert "biomass" not in out.lower()

    @pytest.mark.parametrize("argv", REMOVED_ARGV, ids=" ".join)
    def test_biomass_commands_fail_as_invalid_choice(self, argv, capsys):
        with pytest.raises(SystemExit) as exc:
            cli.main(argv)
        assert exc.value.code == 2
        assert "invalid choice: 'biomass'" in capsys.readouterr().err

    def test_cli_process_rejects_biomass_and_help_omits_it(self, tmp_path):
        rejected = _run_cli_process("biomass", "run-bigquery", "--help", cwd=tmp_path)
        assert rejected.returncode == 2
        assert "invalid choice: 'biomass'" in rejected.stderr
        helped = _run_cli_process("--help", cwd=tmp_path)
        assert helped.returncode == 0
        assert "biomass" not in helped.stdout.lower()


class TestPackagingRemoved:
    def test_no_biomass_extra(self):
        extras = _pyproject()["project"]["optional-dependencies"]
        assert "biomass" not in extras

    def test_pyarrow_is_an_unconditional_dependency(self):
        assert "pyarrow>=14" in _pyproject()["project"]["dependencies"]

    def test_no_package_data_declaration(self):
        assert "package-data" not in _pyproject()["tool"]["setuptools"]

    def test_source_packages_are_eddy_only(self):
        packages = sorted(
            ".".join(p.parent.relative_to(SRC).parts) for p in SRC.rglob("__init__.py")
            if "__pycache__" not in p.parts
        )
        assert packages == ["miaproc", "miaproc.eddy", "miaproc.eddy.r_preflight"]

    def test_no_packaged_data_assets(self):
        assets = sorted(
            p.relative_to(SRC).as_posix() for p in (SRC / "miaproc").rglob("*")
            if p.is_file() and p.suffix != ".py" and "__pycache__" not in p.parts
        )
        assert assets == []

    def test_docker_install_extras_are_declared(self):
        dockerfile = (PKG_ROOT / "docker" / "Dockerfile.miaproc-r45-reddyproc").read_text(encoding="utf-8")
        installs = re.findall(r'pip install -e "/app\[([^\]]*)\]"', dockerfile)
        assert len(installs) == 1
        requested = set(installs[0].split(","))
        assert requested <= set(_pyproject()["project"]["optional-dependencies"])
        assert "biomass" not in requested
