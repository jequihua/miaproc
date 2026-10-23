"""M002-S04 negative checks: the BigQuery transport surface is removed.

Beyond text search: the import system (also with every Google SDK made
unimportable in a separate interpreter), the argparse CLI in process and
as a separate process, the package exports, ``pyproject.toml`` and the
Docker install extras. Removed commands/flags fail clearly (exit 3, a
pointer to the file workflow and migration guide) and write nothing.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import pytest

import miaproc
import miaproc.eddy
from miaproc import cli

SRC = Path(miaproc.__file__).resolve().parents[1]
PKG_ROOT = SRC.parent
GUIDE = "docs/MIGRATING_FROM_BIGQUERY.md"
BQ_NAME = re.compile(r"bigquery|watermark|writeback", re.IGNORECASE)
GOOGLE_REQ = re.compile(r"google|bigquery|db-dtypes|grpc", re.IGNORECASE)

# Meta-path finder that makes the Google SDK stack unimportable.
BLOCK_GOOGLE = textwrap.dedent("""
    import sys
    BLOCKED = ("google", "google_crc32c", "grpc", "db_dtypes", "proto")
    class _Block:
        def find_spec(self, name, path=None, target=None):
            if name.split(".")[0] in BLOCKED:
                raise ModuleNotFoundError(f"blocked for the Google-free check: {name}")
            return None
    sys.meta_path.insert(0, _Block())
""")


def _env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("GOOGLE_", "CLOUDSDK_"))}
    env["PYTHONPATH"] = os.pathsep.join(p for p in (str(SRC), env.get("PYTHONPATH", "")) if p)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _python(code: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-c", code], cwd=cwd, env=_env(),
                          capture_output=True, text=True, timeout=120, check=False)


def _subparsers(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    action = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return dict(action.choices)


def _all_parsers() -> dict[str, argparse.ArgumentParser]:
    top = cli._build_parser()
    out = {"": top}
    for name, sub in _subparsers(top).items():
        out[name] = sub
        if name == "eddy":
            for eddy_name, eddy_sub in _subparsers(sub).items():
                out[f"eddy {eddy_name}"] = eddy_sub
    return out


class TestImportRemoved:
    @pytest.mark.parametrize("module", ["miaproc.eddy.bigquery_runner", "miaproc.eddy.bigquery_writeback"])
    def test_bigquery_modules_have_no_import_spec(self, module):
        assert importlib.util.find_spec(module) is None

    def test_no_source_file_names_bigquery(self):
        assert [p for p in (SRC / "miaproc").rglob("*")
                if "bigquery" in p.name.lower() and "__pycache__" not in p.parts] == []

    def test_package_exports_no_bigquery_names(self):
        assert [n for n in miaproc.eddy.__all__ if BQ_NAME.search(n)] == []
        assert [n for n in dir(miaproc.eddy) if BQ_NAME.search(n)] == []

    def test_import_and_help_without_any_google_sdk(self, tmp_path):
        code = BLOCK_GOOGLE + textwrap.dedent("""
            import contextlib, io, json
            import miaproc, miaproc.eddy, miaproc.eddy.payload, miaproc.cli
            codes = {}
            for argv in (["--help"], ["eddy", "--help"]):
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    try:
                        miaproc.cli.main(argv)
                    except SystemExit as exc:
                        codes[" ".join(argv)] = exc.code
            leaked = sorted(m for m in sys.modules
                            if m.split(".")[0] in ("google", "grpc", "db_dtypes", "google_crc32c"))
            print(json.dumps({"codes": codes, "leaked": leaked}))
        """)
        proc = _python(code, tmp_path)
        assert proc.returncode == 0, proc.stderr
        result = json.loads(proc.stdout.strip().splitlines()[-1])
        assert result == {"codes": {"--help": 0, "eddy --help": 0}, "leaked": []}

    def test_blocker_positive_control(self, tmp_path):
        proc = _python(BLOCK_GOOGLE + "import google.cloud.bigquery\n", tmp_path)
        assert proc.returncode != 0
        assert "blocked for the Google-free check" in proc.stderr


class TestCliRemoved:
    def test_eddy_subcommands_are_file_workflow_only(self):
        eddy = _subparsers(cli._build_parser())["eddy"]
        assert list(_subparsers(eddy)) == ["run-silver", "run-gold", "validate-run"]

    def test_no_parser_offers_a_removed_flag(self):
        for name, parser in _all_parsers().items():
            for action in parser._actions:
                for opt in action.option_strings:
                    assert not opt.startswith(("--bq-", "--stage-payload-dry-run-dir")), (name, opt)

    @pytest.mark.parametrize("name", ["", "run", "eddy", "eddy run-silver", "eddy run-gold", "eddy validate-run"])
    def test_help_mentions_no_bigquery_or_google(self, name):
        text = _all_parsers()[name].format_help().lower()
        assert "bigquery" not in text and "google" not in text

    @pytest.mark.parametrize("argv, token", [
        (["eddy", "run-bigquery"], "run-bigquery"),
        (["eddy", "run-bigquery", "--help"], "run-bigquery"),
        (["eddy", "run-bigquery-silver", "--help"], "run-bigquery-silver"),
        (["eddy", "run-bigquery-gold"], "run-bigquery-gold"),
        (["eddy", "run-silver", "--flux-dir", "f", "--biomet-dir", "b", "--bq-stage-table", "x"], "--bq-stage-table"),
        (["eddy", "run-gold", "--silver-table", "s.parquet", "--bq-start-timestamp=2025-01-01"], "--bq-start-timestamp"),
        (["eddy", "run-silver", "--stage-payload-dry-run-dir", "d"], "--stage-payload-dry-run-dir"),
    ], ids=lambda v: " ".join(v) if isinstance(v, list) else None)
    def test_removed_surface_fails_clearly_and_writes_nothing(self, argv, token, tmp_path, capsys):
        run_dir = tmp_path / "run"
        assert cli.main([*argv, "--run-dir", str(run_dir)]) == cli.VALIDATION_EXIT == 3
        err = capsys.readouterr().err
        assert repr(token) in err and "removed in M002-S04" in err and GUIDE in err
        assert "eddy run-silver" in err and "validate-run" in err
        assert not run_dir.exists()

    @pytest.mark.parametrize("argv", [
        ["eddy", "run-silver", "--run-dir", "run-bigquery"],
        ["eddy", "run-gold", "--silver-table", "run-bigquery-silver", "--output-table", "run-bigquery-gold"],
        ["run", "--flux-dir", "run-bigquery", "--biomet-dir", "eddy"],
    ], ids=lambda v: " ".join(v))
    def test_removed_command_names_match_only_the_eddy_subcommand_position(self, argv):
        # M002-S04-F7: option values that spell a removed command are not refused.
        assert cli._removed_bigquery_token(argv) is None
        assert cli._removed_bigquery_token(["eddy", "run-bigquery-gold", "--run-dir", "x"]) == "run-bigquery-gold"

    def test_run_dir_named_like_a_removed_command_reaches_the_parser(self, capsys):
        with pytest.raises(SystemExit) as exc:
            cli.main(["eddy", "run-silver", "--run-dir", "run-bigquery", "--help"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        assert "--flux-parquet" in captured.out and "removed in M002-S04" not in captured.err

    def test_removed_command_fails_in_a_separate_process(self, tmp_path):
        proc = subprocess.run([sys.executable, "-m", "miaproc.cli", "eddy", "run-bigquery", "--help"],
                              cwd=tmp_path, env=_env(), capture_output=True, text=True, timeout=120, check=False)
        assert proc.returncode == 3
        assert "'run-bigquery' was removed in M002-S04" in proc.stderr and GUIDE in proc.stderr
        assert list(tmp_path.iterdir()) == []


class TestPackagingRemoved:
    def _pyproject(self) -> dict:
        return tomllib.loads((PKG_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    def test_no_bigquery_extra_or_google_requirement(self):
        project = self._pyproject()["project"]
        assert "bigquery" not in project["optional-dependencies"]
        reqs = list(project["dependencies"]) + [r for rs in project["optional-dependencies"].values() for r in rs]
        assert [r for r in reqs if GOOGLE_REQ.search(r)] == []
        assert "pyarrow>=14" in project["dependencies"]

    def test_docker_install_extras_exclude_bigquery(self):
        dockerfile = (PKG_ROOT / "docker" / "Dockerfile.miaproc-r45-reddyproc").read_text(encoding="utf-8")
        installs = re.findall(r'pip install -e "/app\[([^\]]*)\]"', dockerfile)
        assert installs == ["dev,hesseflux,reddyproc"]


class TestValidateRunMalformedManifest:
    """M002-S03-F8: a manifest missing required keys is exit 3, not a crash."""

    def test_manifest_missing_stage_key_exits_3(self, tmp_path, capsys):
        from miaproc.eddy import publish

        run_dir = tmp_path / "run"
        run_dir.mkdir()
        table = run_dir / "silver.csv"
        table.write_text("a\n1\n", encoding="utf-8")
        manifest = {
            "manifest_kind": publish.MANIFEST_KIND, "manifest_version": publish.MANIFEST_VERSION,
            "artifacts": [{"path": "silver.csv", "role": "table", "size": table.stat().st_size,
                           "sha256": publish.sha256_file(table)}],
        }
        (run_dir / publish.MANIFEST_NAME).write_text(json.dumps(manifest), encoding="utf-8")
        assert cli.main(["eddy", "validate-run", "--run-dir", str(run_dir)]) == 3
        out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert out["ok"] is False and "stage" in out["error"]
