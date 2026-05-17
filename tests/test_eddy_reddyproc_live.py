"""Optional live-R tests for the reddyproc-rpy2 backend.

These tests are marked ``@pytest.mark.reddyproc`` and additionally guard
themselves with ``pytest.importorskip("rpy2")`` plus a probe for the R-side
``REddyProc`` package. If the local environment does not satisfy the
REddyProc + rpy2 requirements, the tests skip cleanly and the default
Python-only suite is unaffected.

Run only these tests:

    pytest -v -m reddyproc

Exclude them (default CI behavior):

    pytest -v -m "not reddyproc"
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest


pytestmark = pytest.mark.reddyproc

# Skip the whole module cleanly if rpy2 is not installed.
rpy2 = pytest.importorskip("rpy2", reason="rpy2 is not installed in this environment")

try:
    from rpy2.robjects.packages import importr as _importr

    _importr("REddyProc")
    _REDDYPROC_AVAILABLE = True
    _REDDYPROC_SKIP_REASON = ""
except Exception as _exc:   # noqa: BLE001
    _REDDYPROC_AVAILABLE = False
    _REDDYPROC_SKIP_REASON = f"R package REddyProc not loadable: {_exc}"

requires_reddyproc = pytest.mark.skipif(
    not _REDDYPROC_AVAILABLE, reason=_REDDYPROC_SKIP_REASON or "REddyProc unavailable"
)


def _synthetic_long_series(days: int = 100) -> pd.DataFrame:
    """Multi-day 30-min grid with reasonable-looking eddy values for a
    smoke run. Not scientifically validated; only shape and schema matter.

    REddyProc's ``fCheckHHTimeSeries`` rejects series shorter than 90 days
    with ``sEddyProc.initialize::: Time series is shorter than 90 days
    (three months) of data``. The default here is set above that floor so
    the live smoke run exercises real dispatch + gap-fill + partitioning
    rather than aborting at input validation.
    """
    n = days * 48
    start = pd.Timestamp("2025-08-01 00:00", tz="UTC")
    dt = pd.date_range(start, periods=n, freq="30min")
    rng = np.random.default_rng(42)
    hour = dt.hour + dt.minute / 60.0
    rg = np.clip(800 * np.sin(np.pi * (hour - 6) / 12), 0, None)
    tair = 20 + 8 * np.sin(np.pi * (hour - 6) / 12)
    vpd = 5 + 10 * np.clip(np.sin(np.pi * (hour - 6) / 12), 0, None)
    nee = rng.normal(0, 2, n) - 0.01 * rg + 0.3 * (tair - 20)
    ustar = rng.uniform(0.05, 0.4, n)
    qc = rng.integers(0, 3, n)
    return pd.DataFrame(
        {
            "DateTime": dt,
            "NEE": nee,
            "USTAR": ustar,
            "QC_NEE": qc,
            "Tair": tair,
            "VPD": vpd,
            "Rg": rg,
            "rH": rng.uniform(40, 90, n),
        }
    )


class TestRpy2ImportSmoke:
    def test_rpy2_loads_successfully(self):
        import rpy2.robjects as ro

        version = ro.r("as.character(R.Version()$version.string)")[0]
        assert isinstance(version, str)
        assert version.startswith("R")

    @requires_reddyproc
    def test_reddyproc_r_package_loads(self):
        from rpy2.robjects.packages import importr

        reddyproc = importr("REddyProc")
        assert reddyproc is not None

    def test_r6_dollar_dispatch_idiom_parses(self):
        """Regression guard for the M5 corrective pass: the R6 method
        dispatch in ``engine_reddyproc._call_method`` must access the
        ``$`` operator through its backtick-quoted identifier form.

        ``ro.r("$")`` is a parse error (``$`` alone is not a valid R
        expression) and used to surface only during a full live
        backend run. Asserting the idiom parses here makes the
        regression signal immediate and unambiguous if anyone
        reverts the fix.
        """
        import rpy2.robjects as ro

        dollar = ro.r("`$`")
        # Must be callable and dispatch a valid member access. A named
        # list is the simplest R6-adjacent object that honors ``$``.
        obj = ro.r('list(a = 1L, b = 2L)')
        value = dollar(obj, "a")
        assert int(value[0]) == 1


@requires_reddyproc
class TestLiveRun:
    """A single end-to-end smoke run. This is intentionally minimal: we check
    that the backend produces the contract schema and attaches diagnostics.
    Scientific validation belongs to Milestone 5 (case study comparison)."""

    def test_smoke_run_produces_contract_schema(self):
        """
        Backend exceptions must surface as hard test failures once R +
        REddyProc + rpy2 are importable (the outer module-level guards have
        already admitted us). The blanket ``pytest.skip`` used in the M3
        initial pass hid real regressions in an R-capable environment; Gate
        M3 review (item P1) called for this to be strict by default.

        Escape hatch: set ``MIAPROC_ALLOW_LIVE_REDDYPROC_SKIP=1`` in the
        environment to convert live backend exceptions back to skips (useful
        when explicitly probing an environment that is known to be
        partially-broken and the reviewer wants to keep other tests
        running).
        """
        from miaproc.eddy import REDDYPROC_OUTPUT_COLUMNS, ReddyProcConfig
        from miaproc.eddy.engine_reddyproc import run_reddyproc_engine

        df = _synthetic_long_series(days=100)
        cfg = ReddyProcConfig(
            site_name="Synthetic",
            latitude=22.25,
            longitude=-105.50,
            timezone_hour=-7,
            local_tz="America/Mazatlan",
            ustar_n_sample=50,   # keep small for the smoke test
        )
        allow_skip = os.environ.get("MIAPROC_ALLOW_LIVE_REDDYPROC_SKIP") == "1"
        try:
            out = run_reddyproc_engine(df, config=cfg)
        except Exception as exc:
            if allow_skip:
                pytest.skip(
                    "Live REddyProc run raised "
                    f"({type(exc).__name__}: {exc}); skip forced by "
                    "MIAPROC_ALLOW_LIVE_REDDYPROC_SKIP=1."
                )
            raise

        assert tuple(out.columns) == REDDYPROC_OUTPUT_COLUMNS
        assert len(out) == len(df)
        diag = out.attrs.get("miaproc_diagnostics")
        assert diag is not None
        assert diag["backend"] == "reddyproc-rpy2"
        assert diag["site"]["local_tz"] == "America/Mazatlan"
        assert diag["ustar"]["scenario"] == "U50"


# ----------------------------------------------------------------------
# Opt-in R environment preflight (Decision 010 / R11)
# ----------------------------------------------------------------------


_R_PREFLIGHT_ENV = "MIAPROC_RUN_R_PREFLIGHT"


@pytest.mark.skipif(
    os.environ.get(_R_PREFLIGHT_ENV) != "1",
    reason=(
        f"Live R preflight opt-in. Set {_R_PREFLIGHT_ENV}=1 to run. This test "
        "binds rpy2 to the host R runtime to discover runtime metadata; it "
        "does NOT run a full REddyProc case-study reference."
    ),
)
class TestLivePreflight:
    """Opt-in live run of ``preflight_reddyproc_r_environment``.

    This test does **not** produce REddyProc reference output, does
    **not** load case-study data, and does **not** call
    ``run_reddyproc_engine`` or ``postproc``. It only records which R
    runtime ``rpy2`` would bind to and what its metadata is.
    """

    def test_live_preflight_reports_runtime_metadata(self):
        from miaproc.eddy.r_preflight import (
            RRuntimePreflightPolicy,
            preflight_reddyproc_r_environment,
            render_r_preflight_report,
        )

        policy = RRuntimePreflightPolicy(allow_global_r=True)
        result = preflight_reddyproc_r_environment(policy=policy)

        # The report is produced for reviewer visibility; the test body
        # asserts only the invariants. Print via pytest's capture so the
        # report is visible when pytest is run with ``-s``.
        report = render_r_preflight_report(result)
        print(report)   # visible under pytest -s

        # Minimal invariants: rpy2 loaded (module-level importorskip
        # already ensures this), preflight did not raise, and the
        # report renders a status string.
        assert result.status in (
            "ok",
            "missing_r",
            "missing_reddyproc",
            "unapproved",
            "error",
        )
        assert isinstance(report, str) and len(report) > 0
