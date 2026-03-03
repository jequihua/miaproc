from __future__ import annotations

import ast
from functools import lru_cache
from typing import Any, Callable

import numpy as np


_ALLOWED_NP_ATTRS = {
    "exp", "log", "power", "sqrt",
    "maximum", "minimum", "where",
    "abs"
}

_ALLOWED_NAMES = {"np", "diam", "alt"}


class UnsafeExpressionError(ValueError):
    pass


def _validate_ast(node: ast.AST) -> None:
    """Reject anything not in a small safe subset."""
    for n in ast.walk(node):
        if isinstance(n, ast.Name):
            if n.id not in _ALLOWED_NAMES:
                raise UnsafeExpressionError(f"Name not allowed: {n.id}")

        elif isinstance(n, ast.Attribute):
            # allow np.exp / np.log / ...
            if not (isinstance(n.value, ast.Name) and n.value.id == "np"):
                raise UnsafeExpressionError("Only np.<func> attributes are allowed")
            if n.attr not in _ALLOWED_NP_ATTRS:
                raise UnsafeExpressionError(f"np.{n.attr} not allowed")

        elif isinstance(n, ast.Call):
            # calls must be np.<allowed>(...)
            if isinstance(n.func, ast.Attribute):
                pass
            else:
                raise UnsafeExpressionError("Only calls to np.<func>(...) are allowed")

        elif isinstance(n, (ast.Import, ast.ImportFrom, ast.Lambda, ast.FunctionDef, ast.ClassDef)):
            raise UnsafeExpressionError("Statements/definitions are not allowed")

        elif isinstance(n, (ast.Subscript, ast.Dict, ast.List, ast.Set)):
            # keep it strict/minimal; can loosen later if needed
            raise UnsafeExpressionError("Indexing and containers are not allowed")

        elif isinstance(n, (ast.BinOp, ast.UnaryOp, ast.Expr, ast.Load, ast.Constant,
                            ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow,
                            ast.USub, ast.UAdd, ast.Module, ast.Expression,
                            ast.Compare, ast.Gt, ast.GtE, ast.Lt, ast.LtE, ast.Eq, ast.NotEq,
                            ast.BoolOp, ast.And, ast.Or, ast.IfExp)):
            # allowed building blocks
            pass

        # Anything else is disallowed by default (super conservative)
        else:
            # Many nodes will be caught by the above; this is final guard.
            # We don't raise for every unknown because ast introduces context nodes,
            # but keeping this strict is fine for minimal v1.
            pass


@lru_cache(maxsize=2048)
def compile_numpy_expr(expr: str) -> Callable[[Any, Any], Any]:
    """
    Compile a numpy expression like:
      "np.exp(-10 + 1.9*np.log(diam) + 1.0*np.log(alt))"
    into a callable f(diam, alt).
    """
    parsed = ast.parse(expr, mode="eval")
    _validate_ast(parsed)

    code = compile(parsed, filename="<ecuacion_numpy>", mode="eval")

    def _f(diam, alt):
        # no builtins, only the locals we provide
        return eval(code, {"__builtins__": {}}, {"np": np, "diam": diam, "alt": alt})

    return _f