"""M002-S03: conservative run publication on local and (simulated) mounted storage.

C4 / D005 invariants exercised through the real ``eddy run-silver`` /
``eddy run-gold`` / ``eddy validate-run`` entry points:

* each invocation owns a fresh run directory; reuse, input/output overlap,
  traversal and unsafe artifact names are rejected before any work;
* artifacts are staged in invocation-owned scratch (cleaned on success and
  failure, bounded by a byte budget), copied sequentially to fresh
  destinations with checked flush/close, and a versioned manifest is
  written last - the publication point;
* serialization/copy/flush/close/manifest faults, denied writes, ENOSPC and
  interruptions before the manifest leave no consumable run and return
  nonzero; after a valid manifest an ambiguous close or crash leaves a valid
  run with a distinct status and the run is never deleted;
* consumers (gold, ``validate-run``) reject incomplete, corrupt, truncated or
  mismatched runs; inputs and prior runs are byte-for-byte unchanged and a
  retry with a fresh run name succeeds;
* FUSE limits (no rename/replace, no append, no locks, close-time errors)
  are simulated by patching, not by mounting.
"""
from __future__ import annotations

import builtins
import errno
import hashlib
import io
import json
import os
from pathlib import Path

import pytest

from miaproc import cli
from miaproc.eddy import publish
from test_m002_s02_parquet_io import _typed_inputs, _write_inputs, fake_engine  # noqa: F401

MANIFEST = publish.MANIFEST_NAME


def _tree_digest(root: Path) -> dict[str, str]:
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def _make_inputs(tmp_path, sites):
    flux, biomet = _typed_inputs(days=1, sites=sites)
    src = tmp_path / "in"
    src.mkdir()
    return _write_inputs(src, flux, biomet)


@pytest.fixture
def inputs(tmp_path):
    return _make_inputs(tmp_path, ("S1",))


@pytest.fixture
def inputs2(tmp_path):
    """Two sites, one with a filename-unsafe label (grouped runs)."""
    return _make_inputs(tmp_path, ("S1", "S/2"))


@pytest.fixture
def scratch(tmp_path):
    base = tmp_path / "scratch"
    base.mkdir()
    return base


def _silver(tmp_path, fp, bp, scratch, run="srun", *extra):
    run_dir = tmp_path / run
    return ["eddy", "run-silver", "--flux-parquet", str(fp), "--biomet-parquet", str(bp),
            "--output-table", str(run_dir / "silver.parquet"), "--output-run-json", str(run_dir / "silver_run.json"),
            "--scratch-dir", str(scratch), *extra]


def _gold(tmp_path, silver, scratch, run="grun", *extra):
    run_dir = tmp_path / run
    return ["eddy", "run-gold", "--silver-table", str(silver), "--engine", "hesseflux-native",
            "--output-table", str(run_dir / "gold.parquet"),
            "--output-diagnostics-json", str(run_dir / "gold_diag.json"),
            "--output-run-json", str(run_dir / "gold_run.json"), "--scratch-dir", str(scratch), *extra]


class _FaultyFile:
    """A destination file object that fails at one step (FUSE-style faults)."""

    def __init__(self, fh, step: str, exc: BaseException):
        self.fh, self.step, self.exc = fh, step, exc

    def write(self, data):
        if self.step == "write":
            raise self.exc
        written = self.fh.write(data)
        if self.step == "write_ack":
            raise self.exc  # the bytes were written; only the acknowledgement fails
        return written

    def flush(self):
        if self.step == "flush":
            self.fh.truncate(0)  # the buffered bytes never reach storage
            raise self.exc
        self.fh.flush()
        if self.step == "flush_ack":
            raise self.exc  # the bytes were flushed; only the acknowledgement fails

    def close(self):
        self.fh.close()  # bytes reach storage; only the acknowledgement fails
        if self.step == "close":
            raise self.exc


def _inject(monkeypatch, target: str, step: str, exc: BaseException):
    real = publish._open_dest

    def fake_open(path):
        if Path(path).name != target:
            return real(path)
        if step == "open":
            raise exc
        return _FaultyFile(real(path), step, exc)

    monkeypatch.setattr(publish, "_open_dest", fake_open)
    return lambda: monkeypatch.setattr(publish, "_open_dest", real)


# ---------------------------------------------------------------------------
# Successful publication and the manifest contract
# ---------------------------------------------------------------------------


class TestPublishedRun:
    def test_grouped_silver_and_gold_publish_manifest_last(self, tmp_path, inputs2, scratch, fake_engine,
                                                           monkeypatch, capsys):
        inputs = inputs2
        order = []
        real_copy = publish.copy_closed

        def spy(src, dst, **kw):
            order.append(Path(dst).name)
            real_copy(src, dst, **kw)

        monkeypatch.setattr(publish, "copy_closed", spy)
        fp, bp = inputs
        before = {p: p.read_bytes() for p in inputs}
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "srun", "--group-column", "site_id")) == 0
        assert order[-1] == MANIFEST and order.count(MANIFEST) == 1
        srun = tmp_path / "srun"
        manifest = json.loads((srun / MANIFEST).read_text(encoding="utf-8"))
        assert manifest["manifest_kind"] == publish.MANIFEST_KIND and manifest["manifest_version"] == 1
        assert manifest["stage"] == "silver" and manifest["run_metadata"] == "silver_run.json"
        names = {r["path"]: r for r in manifest["artifacts"]}
        assert set(names) == {"silver.parquet", "silver_run.json", "silver__groups/s1__silver.parquet",
                              "silver__groups/s_2__silver.parquet"}
        for rel, rec in names.items():
            path = srun / rel
            assert not Path(rel).is_absolute() and ".." not in rel
            assert rec["size"] == path.stat().st_size
            assert rec["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        table = names["silver.parquet"]
        assert table["role"] == "table" and table["rows"] == 2 * 48 and len(table["schema_sha256"]) == 64
        assert table["columns"] > 10 and names["silver__groups/s1__silver.parquet"]["rows"] == 48
        run = json.loads((srun / "silver_run.json").read_text(encoding="utf-8"))
        assert run["publication"]["manifest"] == MANIFEST and run["publication"]["manifest_version"] == 1
        assert list(scratch.iterdir()) == []  # invocation-owned scratch removed
        assert {p: p.read_bytes() for p in inputs} == before  # inputs immutable

        order.clear()
        assert cli.main(_gold(tmp_path, srun / "silver.parquet", scratch, "grun", "--group-column", "site_id")) == 0
        assert order[-1] == MANIFEST
        gold_manifest = publish.validate_run(tmp_path / "grun", stage="gold")
        assert gold_manifest["diagnostics"] == "gold_diag.json"
        roles = sorted(r["role"] for r in gold_manifest["artifacts"])
        assert roles == ["diagnostics", "group_diagnostics", "group_diagnostics", "group_table", "group_table",
                         "run_metadata", "table"]
        gold_run = json.loads((tmp_path / "grun" / "gold_run.json").read_text(encoding="utf-8"))
        assert gold_run["inputs"]["silver_publication"]["mode"] == "published-run"
        assert gold_run["inputs"]["silver_publication"]["sha256"] == table["sha256"]
        assert list(scratch.iterdir()) == []

        capsys.readouterr()
        assert cli.main(["eddy", "validate-run", "--run-dir", str(srun), "--stage", "silver"]) == 0
        summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert summary["ok"] is True and len(summary["artifacts"]) == 4
        assert cli.main(["eddy", "validate-run", "--run-dir", str(srun), "--stage", "gold"]) == cli.VALIDATION_EXIT

    def test_explicit_run_dir_with_familiar_output_flags(self, tmp_path, inputs, scratch):
        fp, bp = inputs
        run = tmp_path / "runs" / "silver-0001"
        argv = ["eddy", "run-silver", "--flux-parquet", str(fp), "--biomet-parquet", str(bp),
                "--run-dir", str(run), "--output-table", str(run / "tables" / "silver.parquet"),
                "--output-run-json", str(run / "meta" / "silver_run.json"), "--scratch-dir", str(scratch)]
        assert cli.main(argv) == 0
        manifest = publish.validate_run(run)
        assert {r["path"] for r in manifest["artifacts"]} == {"tables/silver.parquet", "meta/silver_run.json"}
        assert publish.find_run_dir(run / "tables" / "silver.parquet") == run.resolve()


# ---------------------------------------------------------------------------
# Ownership: reuse, overlap, traversal, unsafe names
# ---------------------------------------------------------------------------


class TestOwnership:
    def test_reuse_is_rejected_prior_run_unchanged_and_fresh_retry_succeeds(self, tmp_path, inputs, scratch):
        fp, bp = inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == 0
        prior = _tree_digest(tmp_path / "srun")
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.VALIDATION_EXIT
        assert _tree_digest(tmp_path / "srun") == prior
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "srun-retry")) == 0
        publish.validate_run(tmp_path / "srun-retry")
        assert _tree_digest(tmp_path / "srun") == prior

    @pytest.mark.parametrize("case", ["table_traversal", "json_outside", "groups_outside", "run_is_input_dir",
                                      "run_inside_input_dir", "scratch_inside_run", "same_artifact",
                                      "uri_run_dir"])
    def test_unsafe_layouts_are_rejected_before_any_write(self, tmp_path, inputs, scratch, case):
        fp, bp = inputs
        run = tmp_path / "run"
        table, run_json = run / "silver.parquet", run / "silver_run.json"
        extra = ["--scratch-dir", str(scratch)]
        if case == "table_traversal":
            table = run / ".." / "escaped.parquet"
            extra += ["--run-dir", str(run)]
        elif case == "json_outside":
            run_json = tmp_path / "elsewhere.json"
        elif case == "groups_outside":
            extra += ["--group-column", "site_id", "--output-groups-dir", str(tmp_path / "groups")]
        elif case == "run_is_input_dir":
            table, run_json = fp.parent / "silver.parquet", fp.parent / "silver_run.json"
        elif case == "run_inside_input_dir":
            import synthetic_eddy

            synthetic_eddy.write_fixture(tmp_path / "csv", days=1, seed=3)
            flux_dir = tmp_path / "csv" / "full_output"
            fp_args = ["--flux-dir", str(flux_dir), "--biomet-dir", str(tmp_path / "csv" / "biomet")]
            table, run_json = flux_dir / "nested" / "silver.parquet", flux_dir / "nested" / "silver_run.json"
        elif case == "scratch_inside_run":
            extra = ["--scratch-dir", str(run / "tmp")]
        elif case == "same_artifact":
            run_json = run / "SILVER.parquet"
        elif case == "uri_run_dir":
            extra += ["--run-dir", "gs://bucket/run"]
        before = _tree_digest(tmp_path)
        sources = fp_args if case == "run_inside_input_dir" else [
            "--flux-parquet", str(fp), "--biomet-parquet", str(bp)]
        argv = ["eddy", "run-silver", *sources,
                "--output-table", str(table), "--output-run-json", str(run_json), *extra]
        assert cli.main(argv) == cli.VALIDATION_EXIT
        assert _tree_digest(tmp_path) == before
        assert not run.exists() and not (tmp_path / "escaped.parquet").exists()

    def test_gold_run_inside_the_silver_run_is_rejected(self, tmp_path, inputs, scratch, fake_engine):
        fp, bp = inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == 0
        prior = _tree_digest(tmp_path / "srun")
        assert cli.main(_gold(tmp_path, tmp_path / "srun" / "silver.parquet", scratch, "srun/gold")) \
            == cli.VALIDATION_EXIT
        assert fake_engine == [] and _tree_digest(tmp_path / "srun") == prior

    @pytest.mark.parametrize("name", ["", "../x", "/abs", "a/../b", "a\\b", "C:/x", "./a", "a//b"])
    def test_unsafe_manifest_names_are_refused(self, name):
        with pytest.raises(publish.RunValidationError):
            publish.safe_relative_name(name)

    def test_case_insensitive_artifact_collision_is_refused(self, tmp_path, scratch):
        pub = publish.RunPublisher(stage="silver", run_dir=tmp_path / "r", outputs={}, inputs=[],
                                   scratch_dir=scratch)
        try:
            pub.staged(tmp_path / "r" / "A.json", role="x")
            with pytest.raises(ValueError, match="collision"):
                pub.staged(tmp_path / "r" / "a.json", role="x")
        finally:
            pub.cleanup_scratch()
        assert list(scratch.iterdir()) == []


# ---------------------------------------------------------------------------
# Fault injection before the publication point
# ---------------------------------------------------------------------------


def _assert_nothing_consumable(tmp_path, run="srun"):
    run_dir = tmp_path / run
    assert not (run_dir / MANIFEST).exists()
    with pytest.raises(publish.RunValidationError):
        publish.validate_run(run_dir)


class TestPrePublicationFaults:
    @pytest.mark.parametrize("target,step,exc", [
        ("silver.parquet", "open", PermissionError(errno.EACCES, "denied")),
        ("silver.parquet", "write", OSError(errno.ENOSPC, "No space left on device")),
        ("silver.parquet", "flush", OSError(errno.EIO, "flush failed")),
        ("silver.parquet", "close", OSError(errno.EIO, "close-time upload failure")),
        ("silver_run.json", "write", OSError(errno.ENOSPC, "No space left on device")),
        ("silver_run.json", "close", PermissionError(errno.EACCES, "denied at close")),
        (MANIFEST, "open", PermissionError(errno.EACCES, "denied")),
    ])
    def test_fault_leaves_no_consumable_run_and_retry_succeeds(self, tmp_path, inputs, scratch, monkeypatch,
                                                                target, step, exc):
        fp, bp = inputs
        before_inputs = _tree_digest(fp.parent)
        restore = _inject(monkeypatch, target, step, exc)
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.RUNTIME_EXIT
        _assert_nothing_consumable(tmp_path)
        assert not (tmp_path / "srun").exists()  # bounded cleanup of invocation-owned files
        assert list(scratch.iterdir()) == []
        assert _tree_digest(fp.parent) == before_inputs
        restore()
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "srun-2")) == 0
        publish.validate_run(tmp_path / "srun-2")

    def test_gold_copy_fault_publishes_nothing_and_silver_is_unchanged(self, tmp_path, inputs, scratch,
                                                                       fake_engine, monkeypatch):
        fp, bp = inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == 0
        prior = _tree_digest(tmp_path / "srun")
        _inject(monkeypatch, "gold_diag.json", "close", OSError(errno.EIO, "close failed"))
        assert cli.main(_gold(tmp_path, tmp_path / "srun" / "silver.parquet", scratch)) == cli.RUNTIME_EXIT
        _assert_nothing_consumable(tmp_path, "grun")
        assert _tree_digest(tmp_path / "srun") == prior and list(scratch.iterdir()) == []

    def test_serialization_fault_never_creates_the_run(self, tmp_path, inputs, scratch, monkeypatch):
        import miaproc.eddy.io as eio

        def boom(path, df, **kw):
            raise OSError(errno.ENOSPC, "No space left on device (scratch)")

        monkeypatch.setattr(eio, "write_parquet_table", boom)
        fp, bp = inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.RUNTIME_EXIT
        assert not (tmp_path / "srun").exists() and list(scratch.iterdir()) == []

    def test_scratch_budget_is_enforced_before_publication(self, tmp_path, inputs, scratch):
        fp, bp = inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "srun", "--scratch-max-bytes", "64")) == cli.RUNTIME_EXIT
        assert not (tmp_path / "srun").exists() and list(scratch.iterdir()) == []

    def test_interruption_before_manifest_cleans_up_and_exposes_nothing(self, tmp_path, inputs, scratch,
                                                                        monkeypatch):
        fp, bp = inputs
        _inject(monkeypatch, "silver_run.json", "write", KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            cli.main(_silver(tmp_path, fp, bp, scratch))
        _assert_nothing_consumable(tmp_path)
        assert list(scratch.iterdir()) == []

    def test_hard_crash_before_manifest_partial_files_are_rejected_by_consumers(
            self, tmp_path, inputs, scratch, monkeypatch, fake_engine, capsys):
        fp, bp = inputs
        # A killed process cannot clean up: disable cleanup, fail mid-copy.
        real_cleanup = publish.RunPublisher.__dict__["_cleanup_owned"]
        monkeypatch.setattr(publish.RunPublisher, "_cleanup_owned", staticmethod(lambda files, dirs: None))
        restore = _inject(monkeypatch, "silver_run.json", "write", OSError(errno.EIO, "killed"))
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.RUNTIME_EXIT
        assert (tmp_path / "srun" / "silver.parquet").is_file()  # partial run left behind
        _assert_nothing_consumable(tmp_path)
        restore()
        monkeypatch.setattr(publish.RunPublisher, "_cleanup_owned", real_cleanup)
        capsys.readouterr()
        assert cli.main(["eddy", "validate-run", "--run-dir", str(tmp_path / "srun")]) == cli.VALIDATION_EXIT
        assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["ok"] is False
        assert cli.main(_gold(tmp_path, tmp_path / "srun" / "silver.parquet", scratch)) == cli.VALIDATION_EXIT
        assert fake_engine == [] and not (tmp_path / "grun").exists()


# ---------------------------------------------------------------------------
# After the publication point: ambiguity is reported, the run is kept
# ---------------------------------------------------------------------------


class TestPostPublication:
    def test_ambiguous_manifest_close_reports_distinct_status_and_keeps_valid_run(
            self, tmp_path, inputs, scratch, monkeypatch, fake_engine):
        fp, bp = inputs
        restore = _inject(monkeypatch, MANIFEST, "close", OSError(errno.EIO, "close acknowledgement lost"))
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.PUBLICATION_AMBIGUOUS_EXIT
        restore()
        publish.validate_run(tmp_path / "srun", stage="silver")  # published and consumable
        assert list(scratch.iterdir()) == []
        prior = _tree_digest(tmp_path / "srun")
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.VALIDATION_EXIT  # no reuse
        assert _tree_digest(tmp_path / "srun") == prior
        assert cli.main(_gold(tmp_path, tmp_path / "srun" / "silver.parquet", scratch)) == 0

    def test_crash_right_after_manifest_leaves_a_valid_run(self, tmp_path, inputs, scratch, monkeypatch,
                                                           fake_engine):
        real_validate = publish.validate_run

        def crash(run_dir, **kw):
            raise KeyboardInterrupt  # process dies right after the publication point

        monkeypatch.setattr(publish, "validate_run", crash)
        fp, bp = inputs
        with pytest.raises(KeyboardInterrupt):
            cli.main(_silver(tmp_path, fp, bp, scratch))
        monkeypatch.setattr(publish, "validate_run", real_validate)
        real_validate(tmp_path / "srun", stage="silver")
        assert list(scratch.iterdir()) == []
        assert cli.main(_gold(tmp_path, tmp_path / "srun" / "silver.parquet", scratch)) == 0

    def test_failed_post_publication_readback_is_ambiguous_not_deleted(self, tmp_path, inputs, scratch,
                                                                       monkeypatch):
        real_validate = publish.validate_run

        def flaky(run_dir, **kw):
            raise OSError(errno.EIO, "transient read error on the mount")

        monkeypatch.setattr(publish, "validate_run", flaky)
        fp, bp = inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.PUBLICATION_AMBIGUOUS_EXIT
        monkeypatch.setattr(publish, "validate_run", real_validate)
        real_validate(tmp_path / "srun")


# ---------------------------------------------------------------------------
# M002-S03 r2 (D019, M002-S03-F5): an unconfirmable manifest is never deleted
# ---------------------------------------------------------------------------


class TestUnconfirmedManifestIsKept:
    @pytest.mark.parametrize("readback", ["transient_read_error", "stale_missing_manifest",
                                          "stale_truncated_manifest"])
    @pytest.mark.parametrize("step", ["write_ack", "flush_ack", "close"])
    def test_acknowledgement_fault_plus_failing_readback_keeps_the_run(
            self, tmp_path, inputs, scratch, monkeypatch, fake_engine, step, readback):
        fp, bp = inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "prior")) == 0
        prior, before_inputs = _tree_digest(tmp_path / "prior"), _tree_digest(fp.parent)
        staged = {}
        real_copy, real_validate = publish.copy_closed, publish.validate_run

        def recording_copy(src, dst, **kw):
            staged[Path(dst).name] = hashlib.sha256(Path(src).read_bytes()).hexdigest()
            real_copy(src, dst, **kw)

        def failing_readback(run_dir, **kw):
            if readback == "transient_read_error":
                raise OSError(errno.EIO, "transient read error on the mount")
            if readback == "stale_missing_manifest":
                raise publish.RunValidationError("no completion manifest (stale mount listing)")
            raise publish.RunValidationError("manifest is truncated or corrupt (stale cached bytes)")

        monkeypatch.setattr(publish, "copy_closed", recording_copy)
        monkeypatch.setattr(publish, "validate_run", failing_readback)
        real_open = builtins.open
        manifest_path = os.path.abspath(tmp_path / "srun" / MANIFEST)

        def stale_open(file, mode="r", *a, **k):
            # Any raw read of the new manifest sees stale, truncated bytes.
            if (readback == "stale_truncated_manifest" and "r" in mode and "+" not in mode
                    and isinstance(file, (str, os.PathLike)) and os.path.abspath(os.fspath(file)) == manifest_path):
                return io.BytesIO(b'{"manifest_kind": "miaproc.run-manif') if "b" in mode else \
                    io.StringIO('{"manifest_kind": "miaproc.run-manif')
            return real_open(file, mode, *a, **k)

        monkeypatch.setattr(builtins, "open", stale_open)
        restore = _inject(monkeypatch, MANIFEST, step, OSError(errno.EIO, f"{step} acknowledgement lost"))
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.PUBLICATION_AMBIGUOUS_EXIT
        monkeypatch.setattr(builtins, "open", real_open)
        at_exit = _tree_digest(tmp_path / "srun")
        assert at_exit == staged and MANIFEST in at_exit  # every possibly published byte is kept
        assert list(scratch.iterdir()) == []
        # Read access recovers: the same bytes validate and are consumable.
        restore()
        monkeypatch.setattr(publish, "validate_run", real_validate)
        monkeypatch.setattr(publish, "copy_closed", real_copy)
        real_validate(tmp_path / "srun", stage="silver")
        assert _tree_digest(tmp_path / "srun") == at_exit
        assert cli.main(_gold(tmp_path, tmp_path / "srun" / "silver.parquet", scratch)) == 0
        # No reuse; a fresh name retries; inputs and the prior run are unchanged.
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.VALIDATION_EXIT
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "srun-retry")) == 0
        real_validate(tmp_path / "srun-retry")
        assert _tree_digest(tmp_path / "prior") == prior and _tree_digest(fp.parent) == before_inputs
        assert _tree_digest(tmp_path / "srun") == at_exit and list(scratch.iterdir()) == []

    @pytest.mark.parametrize("step,exc", [("write", OSError(errno.ENOSPC, "No space left on device")),
                                          ("flush", OSError(errno.EIO, "flush failed"))])
    def test_manifest_write_or_flush_fault_is_ambiguous_kept_and_not_consumable(
            self, tmp_path, inputs, scratch, monkeypatch, fake_engine, step, exc):
        # After the manifest exists a generic write/flush error cannot prove that
        # nothing was published, so nothing is deleted (exit 5). These fixtures
        # store an empty manifest, which consumers refuse.
        fp, bp = inputs
        before_inputs = _tree_digest(fp.parent)
        restore = _inject(monkeypatch, MANIFEST, step, exc)
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == cli.PUBLICATION_AMBIGUOUS_EXIT
        restore()
        kept = _tree_digest(tmp_path / "srun")
        assert {"silver.parquet", "silver_run.json", MANIFEST} <= set(kept)
        with pytest.raises(publish.RunValidationError):
            publish.validate_run(tmp_path / "srun")
        assert cli.main(_gold(tmp_path, tmp_path / "srun" / "silver.parquet", scratch)) == cli.VALIDATION_EXIT
        assert fake_engine == [] and not (tmp_path / "grun").exists()
        assert _tree_digest(tmp_path / "srun") == kept and list(scratch.iterdir()) == []
        assert _tree_digest(fp.parent) == before_inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "srun-retry")) == 0
        publish.validate_run(tmp_path / "srun-retry")


# ---------------------------------------------------------------------------
# M002-S03 r2 (D019, M002-S03-F7): the scratch byte limit holds while staging
# ---------------------------------------------------------------------------


class _PeakProbe:
    """Independent observer of bytes reaching scratch files (max extent per file)."""

    def __init__(self):
        self.extents: dict[str, int] = {}
        self.peak = 0

    def install(self, monkeypatch):
        real_open = publish._open_scratch
        probe = self

        class Recorded:
            def __init__(self, fh, key):
                self._fh, self._key = fh, key

            def write(self, data):
                written = self._fh.write(data)
                probe.extents[self._key] = max(probe.extents.get(self._key, 0), self._fh.tell())
                probe.peak = max(probe.peak, sum(probe.extents.values()))
                return written

            def __getattr__(self, name):
                return getattr(self._fh, name)

        monkeypatch.setattr(publish, "_open_scratch", lambda path: Recorded(real_open(path), str(path)))
        return self


def _frame(rows, kind):
    import pandas as pd

    return pd.DataFrame({"timestamp": pd.date_range("2025-03-01", periods=rows, freq="30min", tz="UTC"),
                         "site_id": [f"{kind}-site"] * rows, "primary_key": range(rows),
                         "co2_flux": [0.25 * i for i in range(rows)]})


def _stage_and_publish(tmp_path, scratch, run, limit, order=("a.parquet", "b.parquet", "c.csv")):
    """Stage Parquet + CSV tables, diagnostics and run metadata JSON through the CLI writers."""
    run_dir = tmp_path / run
    pub = publish.RunPublisher(stage="silver", run_dir=run_dir, inputs=[], scratch_dir=scratch,
                               scratch_max_bytes=limit,
                               outputs={"--output-table": run_dir / "c.csv",
                                        "--output-run-json": run_dir / "run.json"})
    frames = {"a.parquet": _frame(40, "a"), "b.parquet": _frame(40, "b"), "c.csv": _frame(40, "c")}
    try:
        for name in order:
            cli._write_table(pub.staged(run_dir / name, role="table", expected_rows=40), frames[name],
                             publisher=pub)
        cli._write_json(pub.staged(run_dir / "diag.json", role="diagnostics"), {"note": "x" * 64},
                        publisher=pub)
        cli._write_json(pub.staged(run_dir / "run.json", role="run_metadata"), {"stage": "silver"},
                        publisher=pub)
        return pub, pub.publish(run_metadata=run_dir / "run.json", diagnostics=run_dir / "diag.json")
    except publish.ScratchBudgetExceeded:
        on_disk = sum(p.stat().st_size for p in pub.scratch.rglob("*") if p.is_file())
        pub.cleanup_scratch()
        raise publish.ScratchBudgetExceeded(on_disk)
    finally:
        pub.cleanup_scratch()


def _published_bytes(run_dir):
    return {p.name: p.stat().st_size for p in run_dir.rglob("*") if p.is_file()}


class TestScratchByteLimitWhileStaging:
    def test_exact_limit_succeeds_and_one_byte_less_is_refused_before_publication(self, tmp_path, scratch,
                                                                                  monkeypatch):
        _, manifest = _stage_and_publish(tmp_path, scratch, "run-a", 10 ** 7)
        sizes = _published_bytes(tmp_path / "run-a")
        total = sum(sizes.values())  # three tables, two JSON files and the manifest
        assert set(sizes) == {"a.parquet", "b.parquet", "c.csv", "diag.json", "run.json", MANIFEST}
        assert {r["format"] for r in manifest["artifacts"]} == {"parquet", "csv", "json"}

        probe = _PeakProbe().install(monkeypatch)
        pub, _ = _stage_and_publish(tmp_path, scratch, "run-b", total)
        assert probe.peak == total == pub.staged_bytes and _published_bytes(tmp_path / "run-b") == sizes

        probe = _PeakProbe().install(monkeypatch)
        with pytest.raises(publish.ScratchBudgetExceeded) as refused:
            _stage_and_publish(tmp_path, scratch, "run-c", total - 1)
        on_disk = refused.value.args[0]
        assert probe.peak <= total - 1 and on_disk <= total - 1
        assert probe.peak >= total - sizes[MANIFEST]  # only the manifest itself no longer fit
        assert not (tmp_path / "run-c").exists() and list(scratch.iterdir()) == []

    @pytest.mark.parametrize("first", ["a.parquet", "c.csv"])
    def test_single_oversized_artifact_stops_before_the_limit(self, tmp_path, scratch, monkeypatch, first):
        _stage_and_publish(tmp_path, scratch, "run-a", 10 ** 7)
        size = _published_bytes(tmp_path / "run-a")[first]
        limit = size // 2
        probe = _PeakProbe().install(monkeypatch)
        order = (first,) + tuple(n for n in ("a.parquet", "b.parquet", "c.csv") if n != first)
        with pytest.raises(publish.ScratchBudgetExceeded) as refused:
            _stage_and_publish(tmp_path, scratch, "run-b", limit, order=order)
        assert probe.peak <= limit and refused.value.args[0] <= limit
        assert len(probe.extents) <= 1  # refused while writing the first artifact
        assert not (tmp_path / "run-b").exists() and list(scratch.iterdir()) == []

    def test_individually_fitting_artifacts_stop_at_the_aggregate_limit(self, tmp_path, scratch, monkeypatch):
        _stage_and_publish(tmp_path, scratch, "run-a", 10 ** 7)
        sizes = _published_bytes(tmp_path / "run-a")
        limit = sizes["a.parquet"] + sizes["b.parquet"] - 1
        assert max(sizes["a.parquet"], sizes["b.parquet"], sizes["c.csv"]) < limit
        probe = _PeakProbe().install(monkeypatch)
        with pytest.raises(publish.ScratchBudgetExceeded):
            _stage_and_publish(tmp_path, scratch, "run-b", limit)
        assert sizes["a.parquet"] <= probe.peak <= limit and 1 <= len(probe.extents) <= 2
        assert not (tmp_path / "run-b").exists() and list(scratch.iterdir()) == []

    def test_grouped_cli_run_stops_while_accumulating_group_tables(self, tmp_path, inputs2, scratch,
                                                                   monkeypatch, fake_engine):
        fp, bp = inputs2
        before = _tree_digest(fp.parent)
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "ok", "--group-column", "site_id")) == 0
        groups = [r["size"] for r in publish.validate_run(tmp_path / "ok")["artifacts"]
                  if r["role"] == "group_table"]
        limit = max(groups) + min(groups) // 2  # each group table fits alone, not both
        probe = _PeakProbe().install(monkeypatch)
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "srun", "--group-column", "site_id",
                                "--scratch-max-bytes", str(limit))) == cli.RUNTIME_EXIT
        assert min(groups) <= probe.peak <= limit and 1 <= len(probe.extents) <= 2
        _assert_nothing_consumable(tmp_path)
        assert not (tmp_path / "srun").exists() and list(scratch.iterdir()) == []
        assert _tree_digest(fp.parent) == before

    def test_environment_selects_scratch_dir_and_limit_and_flags_override(self, tmp_path, inputs, monkeypatch):
        fp, bp = inputs
        env_scratch, flag_scratch = tmp_path / "env-scratch", tmp_path / "flag-scratch"
        env_scratch.mkdir()
        flag_scratch.mkdir()
        made = []
        real_mkdtemp = publish.tempfile.mkdtemp

        def recording_mkdtemp(*a, **kw):
            made.append(Path(kw["dir"]).resolve())
            return real_mkdtemp(*a, **kw)

        monkeypatch.setattr(publish.tempfile, "mkdtemp", recording_mkdtemp)
        monkeypatch.setenv(publish.SCRATCH_DIR_ENV, str(env_scratch))
        monkeypatch.setenv(publish.SCRATCH_MAX_BYTES_ENV, "64")
        argv = ["eddy", "run-silver", "--flux-parquet", str(fp), "--biomet-parquet", str(bp)]
        out = lambda run: ["--output-table", str(tmp_path / run / "silver.parquet"),  # noqa: E731
                           "--output-run-json", str(tmp_path / run / "silver_run.json")]
        assert cli.main(argv + out("env-limited")) == cli.RUNTIME_EXIT  # env limit applies
        assert made[-1] == env_scratch.resolve() and not (tmp_path / "env-limited").exists()
        assert cli.main(argv + out("flag-limit") + ["--scratch-max-bytes", str(10 ** 7)]) == 0
        run = json.loads((tmp_path / "flag-limit" / "silver_run.json").read_text(encoding="utf-8"))
        assert run["publication"]["scratch_max_bytes"] == 10 ** 7
        assert cli.main(argv + out("flag-dir") + ["--scratch-max-bytes", str(10 ** 7),
                                                  "--scratch-dir", str(flag_scratch)]) == 0
        assert made[-1] == flag_scratch.resolve()
        assert list(env_scratch.iterdir()) == [] and list(flag_scratch.iterdir()) == []

    @pytest.mark.parametrize("source,value", [("flag", "0"), ("flag", "-5"), ("env", "0"), ("env", "-1"),
                                              ("env", "abc"), ("env", "1.5")])
    def test_invalid_limits_are_refused_before_any_work(self, tmp_path, inputs, scratch, monkeypatch,
                                                        source, value):
        fp, bp = inputs
        extra = []
        if source == "flag":
            extra = ["--scratch-max-bytes", value]
        else:
            monkeypatch.setenv(publish.SCRATCH_MAX_BYTES_ENV, value)
        assert cli.main(_silver(tmp_path, fp, bp, scratch, "srun", *extra)) == cli.VALIDATION_EXIT
        assert not (tmp_path / "srun").exists() and list(scratch.iterdir()) == []

    def test_non_integer_limit_flag_is_a_usage_error(self, tmp_path, inputs, scratch):
        fp, bp = inputs
        with pytest.raises(SystemExit) as usage:
            cli.main(_silver(tmp_path, fp, bp, scratch, "srun", "--scratch-max-bytes", "lots"))
        assert usage.value.code == 2 and not (tmp_path / "srun").exists()


# ---------------------------------------------------------------------------
# Consumers reject corrupt, truncated or mismatched runs
# ---------------------------------------------------------------------------


class TestConsumerValidation:
    @pytest.mark.parametrize("damage", ["truncate_table", "flip_table_byte", "delete_run_json",
                                        "truncate_manifest", "manifest_version", "manifest_traversal",
                                        "manifest_missing", "unlisted_table"])
    def test_gold_and_validate_run_reject_damaged_silver(self, tmp_path, inputs, scratch, monkeypatch,
                                                         fake_engine, damage):
        called = []
        monkeypatch.setattr(cli, "_run_preflight_or_exit", lambda root: called.append(root))
        fp, bp = inputs
        assert cli.main(_silver(tmp_path, fp, bp, scratch)) == 0
        srun = tmp_path / "srun"
        table, manifest_path = srun / "silver.parquet", srun / MANIFEST
        silver_arg = table
        if damage == "truncate_table":
            table.write_bytes(table.read_bytes()[:-100])
        elif damage == "flip_table_byte":
            data = bytearray(table.read_bytes())
            data[len(data) // 2] ^= 0xFF
            table.write_bytes(bytes(data))
        elif damage == "delete_run_json":
            (srun / "silver_run.json").unlink()
        elif damage == "truncate_manifest":
            manifest_path.write_bytes(manifest_path.read_bytes()[:40])
        elif damage in ("manifest_version", "manifest_traversal"):
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if damage == "manifest_version":
                manifest["manifest_version"] = 99
            else:
                manifest["artifacts"][0]["path"] = "../in/flux.parquet"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        elif damage == "manifest_missing":
            manifest_path.unlink()
        elif damage == "unlisted_table":
            silver_arg = srun / "silver_run.json"  # listed, but not the silver table
        assert cli.main(["eddy", "validate-run", "--run-dir", str(srun)]) == (
            0 if damage == "unlisted_table" else cli.VALIDATION_EXIT)
        argv = _gold(tmp_path, silver_arg, scratch) + ["--engine", "reddyproc-reference",
                                                       "--repo-root", str(tmp_path)]
        assert cli.main(argv) == cli.VALIDATION_EXIT
        assert called == [] and fake_engine == [] and not (tmp_path / "grun").exists()

    def test_unpublished_csv_silver_needs_the_explicit_legacy_flag(self, tmp_path, scratch, fake_engine):
        import synthetic_eddy

        frame = synthetic_eddy.generate_raw_frames(days=1, seed=4)[0]
        legacy = tmp_path / "legacy" / "silver.csv"
        legacy.parent.mkdir()
        frame.to_csv(legacy, index=False)
        assert cli.main(_gold(tmp_path, legacy, scratch)) == cli.VALIDATION_EXIT
        assert fake_engine == [] and not (tmp_path / "grun").exists()

    def test_unpublished_parquet_silver_is_rejected_even_with_the_legacy_flag(self, tmp_path, inputs, scratch,
                                                                             fake_engine):
        fp, _ = inputs
        argv = _gold(tmp_path, fp, scratch) + ["--allow-legacy-silver-csv"]
        assert cli.main(argv) == cli.VALIDATION_EXIT
        assert fake_engine == []


# ---------------------------------------------------------------------------
# Simulated FUSE constraints (no mount): rename, replace, append, locks
# ---------------------------------------------------------------------------


def test_publication_needs_no_rename_append_lock_or_overwrite(tmp_path, inputs2, scratch, monkeypatch, fake_engine):
    inputs = inputs2
    def unsupported(*a, **k):
        raise OSError(errno.EOPNOTSUPP, "operation not supported on this mount")

    for target, name in ((os, "rename"), (os, "replace"), (Path, "rename"), (Path, "replace")):
        monkeypatch.setattr(target, name, unsupported)
    for modname, attr in (("fcntl", "flock"), ("fcntl", "lockf"), ("msvcrt", "locking")):
        try:
            mod = __import__(modname)
        except ImportError:
            continue
        monkeypatch.setattr(mod, attr, unsupported)
    real_open = builtins.open
    run_root = str((tmp_path / "runs").resolve())

    def guarded_open(file, mode="r", *a, **k):
        if isinstance(file, (str, os.PathLike)) and os.path.abspath(os.fspath(file)).startswith(run_root):
            if "a" in mode or "+" in mode or ("w" in mode and os.path.exists(file)):
                raise OSError(errno.EOPNOTSUPP, f"mode {mode!r} unsupported on the simulated mount")
        return real_open(file, mode, *a, **k)

    monkeypatch.setattr(builtins, "open", guarded_open)
    fp, bp = inputs
    assert cli.main(_silver(tmp_path, fp, bp, scratch, "runs/silver-1", "--group-column", "site_id")) == 0
    assert cli.main(_gold(tmp_path, tmp_path / "runs" / "silver-1" / "silver.parquet", scratch, "runs/gold-1",
                          "--group-column", "site_id")) == 0
    for run in ("silver-1", "gold-1"):
        publish.validate_run(tmp_path / "runs" / run)
    assert list(scratch.iterdir()) == []
