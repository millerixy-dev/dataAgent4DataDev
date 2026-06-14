"""Driver orchestration that does NOT depend on pyspark.

The pyspark-dependent execution loop lives in :mod:`pyspark_driver` (the
top-level entry script). Anything testable lands here.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark_driver_pkg.renderer import (
    MissingRequirementError,
    UnresolvedVariableError,
    render_sql,
)
from pyspark_driver_pkg.sql_splitter import split_sql_statements
from pyspark_driver_pkg.variable_catalog import VariableCatalog, load_catalog

DEFAULT_CATALOG_RELATIVE = "contracts/runtime_variables.yaml"


# -- structured logging ------------------------------------------------------

_STRUCTURED_KEYS = (
    "trace_id",
    "instance_id",
    "version_id",
    "task_id",
    "biz_date",
    "biz_hour",
    "timezone",
    "application_id",
)


def _format_structured(prefix: str, fields: dict[str, object]) -> str:
    parts = [prefix]
    for key in _STRUCTURED_KEYS:
        value = fields.get(key)
        if value in (None, ""):
            continue
        parts.append(f"{key}={value}")
    extras = [f"{k}={v}" for k, v in fields.items() if k not in _STRUCTURED_KEYS]
    parts.extend(extras)
    return " ".join(parts)


def log(prefix: str, fields: dict[str, object] | None = None, *, stream=None) -> None:
    """Emit a single structured line to stdout.

    Format::

        <prefix> trace_id=... instance_id=... [extra=...]

    Driver-side logging stays plain text in MVP — JSON rolls out in phase 2.
    """
    line = _format_structured(prefix, fields or {})
    print(line, flush=True, file=stream or sys.stdout)


def log_err(prefix: str, fields: dict[str, object] | None = None) -> None:
    log(prefix, fields, stream=sys.stderr)


# -- CLI ---------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Spark SQL driver for data-dev-platform")

    parser.add_argument(
        "--sql-file",
        required=True,
        help=(
            "SQL filename distributed via spark-submit --files (basename only when running "
            "in YARN cluster mode). For local testing accepts an absolute path."
        ),
    )
    parser.add_argument(
        "--biz-date",
        required=True,
        help="Business date in yyyyMMdd, e.g. 20260613",
    )
    parser.add_argument(
        "--biz-hour",
        default=None,
        help="Business hour in HH 00-23. Required when SQL references ${hr}.",
    )
    parser.add_argument(
        "--timezone",
        default=None,
        help="IANA timezone (e.g. Asia/Shanghai). Reserved for future wall-clock variables.",
    )
    parser.add_argument(
        "--trace-id",
        default=None,
        help="W3C traceparent value, propagated through structured logs.",
    )
    parser.add_argument(
        "--version-id",
        default=None,
        help="Snapshot version id, for log correlation.",
    )
    parser.add_argument(
        "--instance-id",
        default=None,
        help="Task instance id, for log correlation.",
    )
    parser.add_argument(
        "--task-id",
        default=None,
        help="Task id, for log correlation.",
    )
    parser.add_argument(
        "--catalog-path",
        default=None,
        help=(
            f"Path to runtime variable catalog YAML. "
            f"Defaults to {DEFAULT_CATALOG_RELATIVE} relative to the driver."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render SQL and stop; do not call spark.sql().",
    )
    parser.add_argument(
        "--allow-unresolved-vars",
        action="store_true",
        help="Keep unresolved ${...} placeholders verbatim instead of failing.",
    )
    return parser


# -- file resolution ---------------------------------------------------------


def safe_listdir(directory: str) -> list[str]:
    try:
        return os.listdir(directory)
    except Exception as exc:  # pragma: no cover - exercised only at YARN runtime
        return [f"无法读取目录 {directory}:{exc}"]


def _unique_paths(paths):
    result: list[str] = []
    seen: set[str] = set()
    for p in paths:
        if not p:
            continue
        absolute = os.path.abspath(p)
        if absolute in seen:
            continue
        seen.add(absolute)
        result.append(absolute)
    return result


def resolve_sql_path(filename: str, *, spark_files_root_provider=None) -> str:
    """Locate the SQL file under YARN cluster mode (or a local absolute path).

    Lookup order:
      1. ``filename`` itself if absolute and exists (local testing).
      2. cwd / filename
      3. $PWD / filename
      4. SparkFiles root / filename (only if ``spark_files_root_provider`` given)

    ``spark_files_root_provider`` is injected by the YARN entry script
    so this module stays import-safe without pyspark.
    """
    base = os.path.basename(filename) or filename
    if not base:
        raise ValueError("SQL file name is empty")

    # Local absolute path shortcut for development / tests.
    if os.path.isabs(filename) and os.path.isfile(filename):
        return filename

    cwd = os.getcwd()
    pwd = os.environ.get("PWD", cwd)

    candidates: list[str] = []
    candidates.extend([os.path.join(cwd, base), os.path.join(pwd, base)])
    if spark_files_root_provider is not None:
        try:
            candidates.append(os.path.join(spark_files_root_provider(), base))
        except Exception:  # pragma: no cover - YARN-runtime only
            pass

    for path in _unique_paths(candidates):
        if os.path.isfile(path):
            return path

    raise FileNotFoundError(
        f"SQL 文件不存在: filename={filename}, candidates={candidates}, cwd_files={safe_listdir(cwd)}"
    )


# -- catalog ----------------------------------------------------------------


def _default_catalog_path() -> Path:
    """Return the catalog path next to the running driver."""
    return Path(__file__).resolve().parents[1] / DEFAULT_CATALOG_RELATIVE


def _load_catalog_from_args(args: argparse.Namespace) -> VariableCatalog:
    path = Path(args.catalog_path) if args.catalog_path else _default_catalog_path()
    return load_catalog(path)


# -- run_render -------------------------------------------------------------


@dataclass(frozen=True)
class RenderOutcome:
    statements: tuple[str, ...]
    unresolved: tuple[str, ...]
    rendered_sql: str
    dry_run: bool
    args: argparse.Namespace


def _read_sql(sql_path: str) -> str:
    text = Path(sql_path).read_text(encoding="utf-8-sig")
    if not text.strip():
        raise ValueError(f"SQL 文件内容为空: {sql_path}")
    return text


def run_render(argv: list[str] | None = None) -> RenderOutcome:
    """Parse args, load catalog, read SQL, render variables, split statements.

    SystemExit is raised on user-facing errors so callers (driver entry, tests)
    treat the process as failed without spilling tracebacks.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    # Pre-render structured log header — helps when the failure is in render itself.
    log_fields: dict[str, object] = {
        "trace_id": args.trace_id,
        "instance_id": args.instance_id,
        "version_id": args.version_id,
        "task_id": args.task_id,
        "biz_date": args.biz_date,
        "biz_hour": args.biz_hour,
        "timezone": args.timezone,
    }
    log("[driver] start", log_fields)

    try:
        catalog = _load_catalog_from_args(args)
    except (FileNotFoundError, ValueError) as exc:
        log_err("[driver] catalog load failed", {**log_fields, "error": str(exc)})
        raise SystemExit(2) from exc

    try:
        sql_path = resolve_sql_path(args.sql_file)
    except (FileNotFoundError, ValueError) as exc:
        log_err("[driver] sql file not found", {**log_fields, "error": str(exc)})
        raise SystemExit(3) from exc

    sql_text = _read_sql(sql_path)

    try:
        result = render_sql(
            sql_text,
            catalog=catalog,
            biz_date=args.biz_date,
            biz_hour=args.biz_hour,
            strict=not args.allow_unresolved_vars,
            timezone=args.timezone,
        )
    except MissingRequirementError as exc:
        log_err("[driver] missing requirement", {**log_fields, "error": str(exc)})
        raise SystemExit(4) from exc
    except UnresolvedVariableError as exc:
        log_err(
            "[driver] unresolved variables",
            {**log_fields, "error": str(exc), "placeholders": ",".join(exc.placeholders)},
        )
        raise SystemExit(5) from exc
    except ValueError as exc:
        log_err("[driver] render failed", {**log_fields, "error": str(exc)})
        raise SystemExit(6) from exc

    statements = tuple(split_sql_statements(result.text))

    log(
        "[driver] render ok",
        {
            **log_fields,
            "statements": len(statements),
            "unresolved": len(result.unresolved),
        },
    )

    return RenderOutcome(
        statements=statements,
        unresolved=result.unresolved,
        rendered_sql=result.text,
        dry_run=args.dry_run,
        args=args,
    )
