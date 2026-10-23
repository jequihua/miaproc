"""M32A tests for the source-truth ``timestamp`` contract constants.

The constants-vs-lineage-CSV checks below pin the ``DateTime ->
timestamp`` row that M32 omitted.

M002-S04 removed the BigQuery silver/gold commands these CLI tests used
to drive. Their storage-neutral equivalents (silver/gold Parquet files
carry exactly one ``timestamp`` and no ``DateTime``; gold reconstructs
``DateTime`` from a timestamp-only silver before backend dispatch) live
in ``test_cli_eddy_m32_source_truth.py`` and are named as
replacements in ``retired_test_ids.json``.
"""
from __future__ import annotations

import csv
import pathlib

from miaproc.eddy import (
    FINAL_TO_INTERNAL_RENAME,
    SILVER_BRONZE_TO_FINAL_ALIASES,
    SILVER_INTERNAL_TO_FINAL_RENAME,
)


# Constants vs lineage CSV (including DateTime row)
# ---------------------------------------------------------------------------


class TestConstantsAgainstLineageCSVM32A:
    @staticmethod
    def _csv_path() -> pathlib.Path:
        # The lineage CSV lives under ``06_infra/schemas/`` at the
        # workspace root. The exact distance from this test file
        # depends on the layout: in the artifact-first dev workspace
        # the file is at ``<root>/08_pkg/tests/...`` (root is two
        # parents up), in the standalone front-facing repo it is at
        # ``<root>/tests/...`` (root is one parent up), and inside
        # the runtime Docker image it is at ``/app/tests/...`` (root
        # is one parent up). Walk upward to find the first ancestor
        # that actually contains the file rather than hard-coding the
        # distance, so the test passes in every layout we ship.
        relative = (
            pathlib.Path("06_infra")
            / "schemas"
            / "eddy_bronze_to_stage_column_lineage_contract.csv"
        )
        here = pathlib.Path(__file__).resolve()
        for ancestor in here.parents:
            candidate = ancestor / relative
            if candidate.is_file():
                return candidate
        raise FileNotFoundError(
            f"Could not locate {relative} above {here}. The lineage "
            f"CSV must exist under a 06_infra/schemas/ directory in "
            f"one of this file's ancestor directories."
        )

    def test_silver_internal_to_final_includes_DateTime_to_timestamp(self):
        assert SILVER_INTERNAL_TO_FINAL_RENAME.get("DateTime") == "timestamp"

    def test_final_to_internal_includes_timestamp_to_DateTime(self):
        assert FINAL_TO_INTERNAL_RENAME.get("timestamp") == "DateTime"

    def test_constants_match_csv_row_by_row_including_time(self):
        with self._csv_path().open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        expected_i2f: dict[str, str] = {}
        expected_aliases: dict[str, str] = {}
        for r in rows:
            bronze = r["v_name_bronze"]
            internal = r["v_name_eddyproc"]
            final = r["v_name_final"]
            if internal and final and internal != final:
                expected_i2f[internal] = final
            # Bronze-to-final alias map: only the unit-transformed
            # variables (or any bronze whose name differs from its
            # final and that has an internal alias too) need an
            # explicit entry. The ``timestamp`` bronze row matches
            # its final name exactly so it does not produce an alias.
            if (
                internal
                and bronze
                and final
                and bronze != final
                and bronze != "timestamp"
            ):
                expected_aliases[bronze] = final
        expected_aliases["u."] = "u_star"
        assert SILVER_INTERNAL_TO_FINAL_RENAME == expected_i2f
        assert FINAL_TO_INTERNAL_RENAME == {
            v: k for k, v in expected_i2f.items()
        }
        assert SILVER_BRONZE_TO_FINAL_ALIASES == expected_aliases
        # And ``timestamp`` is intentionally absent from the bronze
        # alias map — its bronze name matches the final name exactly.
        assert "timestamp" not in SILVER_BRONZE_TO_FINAL_ALIASES

