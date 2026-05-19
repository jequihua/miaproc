# Local Setup

## Python-Only Development

The first supported setup is Python-only. It should be enough to run migration
tests and the hesseflux backend.

Expected workflow after package migration:

```powershell
cd 08_pkg
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest
```

The exact command may change after `08_pkg/pyproject.toml` is migrated from the
legacy package.

## Optional REddyProc rpy2 Development

The parity backend requires:

- R installed and available to the Python process.
- REddyProc installed in R.
- `rpy2` installed in the Python environment.

This setup is optional and should be documented by the coding agent when the
backend is implemented.

Optional tests should be run with a marker, for example:

```powershell
.\.venv\Scripts\python.exe -m pytest -m reddyproc
```

## CI Expectation

Default CI should not require R. R-backed tests can be added later as optional jobs
or run manually by developers who have the environment.
