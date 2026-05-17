"""``python -m miaproc.eddy.r_preflight`` entry point.

Kept separate from ``__init__.py`` so that executing the package with
``python -m`` does not re-execute the already-imported ``__init__``
module (which would trigger a runpy ``RuntimeWarning`` about the
package being present in ``sys.modules`` before the module body runs).
"""
from __future__ import annotations

from . import main


if __name__ == "__main__":   # pragma: no cover
    raise SystemExit(main())
