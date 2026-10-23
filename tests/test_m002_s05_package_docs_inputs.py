"""M002-S05: typed multi-site Parquet inputs for the Docker mount user path,
and self-contained package documentation links.

The generator option ``synthetic_eddy.py ROOT --site ... --parquet`` is the
input step of the documented Docker mount user path
(``docker/README.md``); this module proves its files are typed as
documented and are accepted by ``eddy run-silver`` with ``--group-column``.
The link check proves the package Markdown docs only link to files inside
the package (C5: package docs are self-contained).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import miaproc
import synthetic_eddy
from miaproc import cli

# The package root of the miaproc source under test (src/miaproc -> package),
# not of this test file: verifier self-tests run copies of tests/ elsewhere.
PKG_ROOT = Path(miaproc.__file__).resolve().parents[2]
SITES = [("SITE_A", 3, 20261101), ("SITE_B", 2, 20261102)]


class TestTypedParquetSiteInputs:
    def test_cli_writes_typed_parquet_pair(self, tmp_path):
        argv = [str(tmp_path / "in")]
        for site, days, seed in SITES:
            argv += ["--site", f"{site}:{days}:{seed}"]
        assert synthetic_eddy.main(argv + ["--parquet"]) == 0
        assert sorted(p.name for p in (tmp_path / "in").iterdir()) == ["biomet.parquet", "flux.parquet"]
        flux = pq.read_table(tmp_path / "in" / "flux.parquet")
        biomet = pq.read_table(tmp_path / "in" / "biomet.parquet")
        for table in (flux, biomet):
            schema = table.schema
            assert schema.field("site_id").type == pa.string()
            assert schema.field("timestamp").type == pa.timestamp("ns", tz="UTC")
            assert not any(name.startswith("__index_level") for name in schema.names)
            assert sorted(set(table.column("site_id").to_pylist())) == ["SITE_A", "SITE_B"]
        assert flux.schema.field("primary_key").type == pa.int64()
        keys = flux.column("primary_key").to_pylist()
        assert min(keys) == synthetic_eddy.PARQUET_PRIMARY_KEY_BASE == 2**53 + 1
        assert len(set(keys)) == len(keys)
        # Biomet keeps every half hour; flux drops a few whole timestamps.
        assert biomet.num_rows == sum(days * 48 for _site, days, _seed in SITES)
        assert flux.num_rows < biomet.num_rows

    def test_frames_match_csv_generator_values(self):
        flux, biomet = synthetic_eddy.site_parquet_frames([("S1", 2, 7)])
        raw_flux, raw_biomet = synthetic_eddy.generate_raw_frames(days=2, seed=7)
        assert list(flux["timestamp"].dt.tz_convert(None)) == list(raw_flux["DateTime"])
        assert flux["co2_flux"].equals(raw_flux["co2_flux"])
        assert biomet["SWIN_1_1_1"].equals(raw_biomet["SWIN_1_1_1"])

    @pytest.mark.parametrize("argv", [["--parquet"], ["--parquet", "--site", "A:1:1", "--site", "A:1:2"]])
    def test_parquet_needs_unique_sites(self, tmp_path, argv):
        with pytest.raises((SystemExit, ValueError)):
            synthetic_eddy.main([str(tmp_path / "in")] + argv)

    def test_grouped_silver_accepts_the_inputs(self, tmp_path):
        written = synthetic_eddy.write_site_parquet(tmp_path / "in", SITES)
        run = tmp_path / "runs" / "silver"
        code = cli.main([
            "eddy", "run-silver", "--flux-parquet", str(written["flux.parquet"]),
            "--biomet-parquet", str(written["biomet.parquet"]), "--group-column", "site_id",
            "--output-table", str(run / "silver.parquet"), "--output-run-json", str(run / "silver_run.json"),
            "--scratch-dir", str(tmp_path / "scratch"),
        ])
        assert code == 0
        silver = pq.read_table(run / "silver.parquet").to_pandas()
        assert list(silver.columns).count("timestamp") == 1 and "DateTime" not in silver.columns
        assert silver.groupby("site_id").size().to_dict() == {"SITE_A": 3 * 48, "SITE_B": 2 * 48}
        observed = silver["primary_key"].dropna()
        assert observed.min() == 2**53 + 1  # identity carried without float rounding
        meta = json.loads((run / "silver_run.json").read_text(encoding="utf-8"))
        assert meta["exit_code"] == 0 and meta["inputs"]["flux_format"] == "parquet"
        assert cli.main(["eddy", "validate-run", "--run-dir", str(run), "--stage", "silver"]) == 0


# Package Markdown that M002-S05 owns. ``backend_contract.md`` is outside
# that write boundary and still links two development-history files that
# were never imported (reported as a carried item in the M002-S05 notes).
DOC_FILES = (
    "README.md", "testing_strategy.md", "docker/README.md", "examples/README.md",
    "docs/MIGRATING_FROM_BIGQUERY.md", "docs/PARQUET_FILE_SCHEMA.md", "docs/RUN_PUBLICATION.md",
    "docs/REDDYPROC_LOCAL_TIME_POLICY.md", "docs/m5_reddyproc_hesseflux_magnitude_note.md",
    "06_infra/schemas/README.md", "06_infra/cloudrun/README.md", "06_infra/local_setup.md",
    "06_infra/deployment.md", "06_infra/eddy_handoff.md", "06_infra/architecture.md",
    "09_ops/runbooks.md", "09_ops/monitoring.md", "09_ops/scheduled_jobs.md",
)
LINK = re.compile(r"\]\(([^)\s#]+)(?:#[^)]*)?\)")


@pytest.mark.parametrize("rel", DOC_FILES)
def test_package_doc_links_stay_inside_package(rel):
    doc = PKG_ROOT / rel
    if not doc.is_file():
        pytest.fail(f"documented package file missing: {rel}")
    broken = []
    for target in LINK.findall(doc.read_text(encoding="utf-8")):
        if re.match(r"[a-z][a-z0-9+.-]*:", target):  # external URL
            continue
        resolved = (doc.parent / target).resolve()
        if PKG_ROOT.resolve() not in resolved.parents or not resolved.exists():
            broken.append(target)
    assert broken == []
