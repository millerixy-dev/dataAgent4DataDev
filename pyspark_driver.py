#!/usr/bin/env python3
"""YARN cluster-mode entry script.

The pyspark-free orchestration logic lives in :mod:`pyspark_driver_pkg.driver`.
This file is the actual ``spark-submit`` target: it re-exports :func:`run_render`
for tests and tools, then exposes :func:`main` which boots SparkSession and runs
the rendered statements via ``spark.sql``.

Adapted from the legacy single-file driver. Capabilities preserved:

* SQL file located on YARN via cwd / $PWD / SparkFiles.get() fallback
* Hive metastore + Hive SerDe via ``enableHiveSupport()``
* SQL splitter that respects quoted strings, backticks, and SQL comments
* UTF-8-sig SQL reading for BOM tolerance
* Strict variable rendering (raises on undeclared placeholders)

Adapted in this revision:

* Variable rendering moved to :mod:`pyspark_driver_pkg.renderer`
* Catalog loaded from ``contracts/runtime_variables.yaml``
* New CLI args: --biz-hour, --timezone, --trace-id, --version-id,
  --instance-id, --task-id, --catalog-path
* Pre-Spark render phase isolated in :func:`run_render` for testability
"""

from __future__ import annotations

import sys
import traceback

from pyspark_driver_pkg.driver import (
    RenderOutcome,
    log,
    log_err,
    resolve_sql_path,
    run_render,
)

__all__ = ["RenderOutcome", "run_render", "main", "log", "log_err", "resolve_sql_path"]


def _execute(spark, outcome: RenderOutcome) -> None:
    """Run rendered statements through ``spark.sql`` in order."""
    statements = outcome.statements
    if not statements:
        raise ValueError("SQL 文件中没有可执行的 SQL 语句")
    total = len(statements)

    base_fields = {
        "trace_id": outcome.args.trace_id,
        "instance_id": outcome.args.instance_id,
        "version_id": outcome.args.version_id,
        "task_id": outcome.args.task_id,
        "biz_date": outcome.args.biz_date,
        "biz_hour": outcome.args.biz_hour,
        "application_id": spark.sparkContext.applicationId,
    }

    for index, statement in enumerate(statements, start=1):
        fields = {**base_fields, "stmt": f"{index}/{total}"}
        log("[driver] sql start", fields)
        if outcome.dry_run:
            log("[driver] sql dry-run skip", fields)
            continue
        spark.sql(statement)
        log("[driver] sql ok", fields)


def _dry_run(outcome: RenderOutcome) -> None:
    """Print rendered statements without touching pyspark.

    Useful for local CLI smoke tests and CI environments without a Spark
    install. Production runs always go through :func:`main`.
    """
    base_fields = {
        "trace_id": outcome.args.trace_id,
        "instance_id": outcome.args.instance_id,
        "version_id": outcome.args.version_id,
        "task_id": outcome.args.task_id,
        "biz_date": outcome.args.biz_date,
        "biz_hour": outcome.args.biz_hour,
    }
    total = len(outcome.statements)
    for index, statement in enumerate(outcome.statements, start=1):
        fields = {**base_fields, "stmt": f"{index}/{total}"}
        log("[driver] sql start", fields)
        log("[driver] sql preview", {**fields, "sql": statement})
        log("[driver] sql dry-run skip", fields)
    log("[driver] completed (dry-run)", base_fields)


def main() -> None:
    outcome = run_render(sys.argv[1:])

    if outcome.dry_run:
        _dry_run(outcome)
        return

    # Late import — keeps the module test-friendly without pyspark on PATH.
    from pyspark import SparkFiles  # noqa: F401  (touched for sanity / used by resolver below)
    from pyspark.sql import SparkSession

    base_fields = {
        "trace_id": outcome.args.trace_id,
        "instance_id": outcome.args.instance_id,
        "version_id": outcome.args.version_id,
        "task_id": outcome.args.task_id,
        "biz_date": outcome.args.biz_date,
        "biz_hour": outcome.args.biz_hour,
    }

    log("[driver] starting SparkSession", base_fields)
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    fields_with_app = {
        **base_fields,
        "application_id": spark.sparkContext.applicationId,
        "app_name": spark.sparkContext.appName,
        "master": spark.sparkContext.master,
    }
    log("[driver] SparkSession ready", fields_with_app)

    try:
        # Re-resolve the SQL path with SparkFiles for safety (resolves YARN's
        # localized path even if cwd lookup already succeeded earlier).
        try:
            resolve_sql_path(
                outcome.args.sql_file,
                spark_files_root_provider=SparkFiles.getRootDirectory,
            )
        except FileNotFoundError:
            # Already resolved during run_render; ignore here.
            pass
        _execute(spark, outcome)
        log("[driver] completed", fields_with_app)
    finally:
        try:
            spark.stop()
        except Exception:  # pragma: no cover
            log_err("[driver] SparkSession stop failed", fields_with_app)
            traceback.print_exc(file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException:
        log_err("[driver] terminated with exception")
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        raise
