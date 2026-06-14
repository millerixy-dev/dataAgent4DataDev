"""End-to-end checkpoint after groups 1+2+3.

Stitches Resolver -> snapshot file -> spark_submit.sh -> driver in a single
Python test process. Group 5 (Snapshot Service) and group 6 (Command Generator)
are stubbed inline — these are exactly the contracts the upcoming groups must
satisfy, so this test doubles as a reference for them.

This test does NOT start a SparkSession; the driver runs in --dry-run mode.
"""

from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from pyspark_driver_pkg.resolver import InMemoryProjectVariableStore, Resolver
from pyspark_driver_pkg.variable_catalog import load_catalog

REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "contracts" / "runtime_variables.yaml"
SHELL = REPO / "spark_submit.sh"


def _make_exec(path: Path, body: str) -> None:
    path.write_text(f"#!/usr/bin/env bash\n{body}\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def test_cli_e2e_publish_then_run(tmp_path):
    """End-to-end:
        Resolver.bake -> snapshot file -> command generator stub
        -> spark_submit.sh eval -> driver --dry-run
    """
    catalog = load_catalog(CONTRACT)
    store = InMemoryProjectVariableStore()
    store.put(
        project_id="p1",
        name="prj.warehouse",
        value="tmp_dc_ep",
        version=3,
        effective_at=datetime.fromisoformat("2026-01-01T00:00:00"),
    )

    # ---- step 1: user-authored SQL (still has placeholders) -----------------
    user_sql = (
        "INSERT OVERWRITE TABLE ${prj.warehouse}.t_demo PARTITION (dt='${dt}', hr='${hr}')\n"
        "SELECT id, '${dt-1}' AS prev_dt FROM staging WHERE dt='${dt}';"
    )

    # ---- step 2: bake (Resolver) -------------------------------------------
    resolver = Resolver(catalog=catalog, store=store)
    baked = resolver.bake(
        user_sql,
        project_id="p1",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert baked.project_var_versions == {"prj.warehouse": 3}
    assert "${prj.warehouse}" not in baked.text
    assert "${dt}" in baked.text  # runtime placeholders preserved

    # ---- step 3: snapshot (file write) -------------------------------------
    # MVP minimum: Snapshot Service writes baked.text into a versioned path.
    # Group 5 will replace this with WebHDFS + sha256, but the contract is the
    # same: produce a single readable file the driver can `--sql-file` against.
    snap_dir = tmp_path / "snapshots" / "dev" / "task-1" / "v17"
    snap_dir.mkdir(parents=True)
    sql_file = snap_dir / "sql.sql"
    sql_file.write_text(baked.text, encoding="utf-8")

    # ---- step 4: build command (Command Generator stub) --------------------
    # In group 6 this becomes: backend whitelist + shlex_quote escaping that
    # lays out a real spark-submit invocation. For the e2e checkpoint we
    # short-circuit: the SPARK_CMD invokes the driver directly via python so
    # we can run it without a real spark-submit on PATH.
    fake_kinit = tmp_path / "bin" / "kinit"
    fake_kinit.parent.mkdir()
    _make_exec(fake_kinit, "exit 0")
    fake_curl = tmp_path / "bin" / "curl"
    _make_exec(fake_curl, "exit 0")

    spark_cmd = " ".join(
        [
            shlex.quote(sys.executable),
            shlex.quote(str(REPO / "pyspark_driver.py")),
            "--sql-file", shlex.quote(str(sql_file)),
            "--biz-date", "${biz_date}",
            "--biz-hour", "03",
            "--catalog-path", shlex.quote(str(CONTRACT)),
            "--trace-id", "trace-deadbeef",
            "--version-id", "v17",
            "--instance-id", "inst-99",
            "--task-id", "task-1",
            "--dry-run",
        ]
    )

    # ---- step 5+6: run spark_submit.sh -------------------------------------
    # fake keytab
    keytab = tmp_path / "x.keytab"
    keytab.write_bytes(b"fake")

    env = {
        **os.environ,
        "PATH": f"{fake_kinit.parent}:{os.environ.get('PATH', '')}",
        "SPARK_CMD": spark_cmd,
        "BIZ_DATE": "20260614",
        "PRINCIPAL": "a_xy_mn",
        "KEYTAB_PATH": str(keytab),
        "INSTANCE_ID": "inst-99",
        "TRACE_ID": "trace-deadbeef",
        "VERSION_ID": "v17",
        "TASK_ID": "task-1",
    }

    proc = subprocess.run(
        ["bash", str(SHELL)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert proc.returncode == 0, (
        f"e2e failed\n--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )

    # ---- assertions on the rendered SQL the driver produced ---------------
    out = proc.stdout + proc.stderr
    # driver dry-run prints the sql start + sql ok lines with field tags
    assert "trace_id=trace-deadbeef" in out
    assert "biz_date=20260614" in out
    assert "instance_id=inst-99" in out
    # The fully rendered SQL must show:
    #   - prj.warehouse  -> tmp_dc_ep   (baked at publish)
    #   - ${dt}          -> 20260614   (driver renders using BIZ_DATE)
    #   - ${dt-1}        -> 20260613
    #   - ${hr}          -> 03
    assert "tmp_dc_ep.t_eci" not in out  # sanity: not the historical SQL
    assert "tmp_dc_ep.t_demo" in out
    assert "dt='20260614'" in out
    assert "hr='03'" in out
    assert "20260613" in out  # ${dt-1}


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
def test_cli_e2e_runs_in_pure_bash():
    # Sanity guard — if this assertion ever fires, the e2e above isn't really
    # exercising the shell.
    assert SHELL.exists()


def test_cli_e2e_with_real_command_generator(tmp_path):
    """End-to-end with the actual Command Generator (group 4).

    The fake spark-submit shim translates the generated command into a
    driver invocation so we can run without a real Spark install. Importantly,
    the SPARK_CMD string is produced by ``command_generator.generate`` exactly
    as the platform backend would emit it.
    """
    from pyspark_driver_pkg.command_generator import TaskSpec, generate

    catalog = load_catalog(CONTRACT)
    store = InMemoryProjectVariableStore()
    store.put(
        project_id="p1",
        name="prj.warehouse",
        value="tmp_dc_ep",
        version=3,
        effective_at=datetime.fromisoformat("2026-01-01T00:00:00"),
    )
    resolver = Resolver(catalog=catalog, store=store)
    baked = resolver.bake(
        "INSERT OVERWRITE TABLE ${prj.warehouse}.t_demo PARTITION (dt='${dt}')\n"
        "SELECT id, '${dt-1}' AS prev FROM staging WHERE dt='${dt}';",
        project_id="p1",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )

    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    sql_file = snap_dir / "sql.sql"
    sql_file.write_text(baked.text, encoding="utf-8")
    keytab = tmp_path / "x.keytab"
    keytab.write_bytes(b"fake")

    spec = TaskSpec(
        task_id="task-1",
        name="demo",
        queue="root.default",
        principal="a_xy_mn",
        keytab_path=str(keytab),
        driver_path=str(REPO / "pyspark_driver.py"),
        snapshot_hdfs_path=f"hdfs:///{sql_file}",  # ignored by the shim below
        sql_basename="sql.sql",
        spark_conf={"spark.sql.shuffle.partitions": "400"},
        timezone="Asia/Shanghai",
        catalog_path=str(CONTRACT),
        version_id="v17",
    )
    cmd = generate(spec)

    # Fake spark-submit shim:
    #   - peels off all spark-submit-only args (--master, --deploy-mode, --queue,
    #     --principal, --keytab, --name, --conf X, --files X)
    #   - re-execs the driver entry script with everything from driver_path onward,
    #     plus --dry-run so we never need a real Spark.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    shim = bin_dir / "spark-submit"
    shim.write_text(
        "#!/usr/bin/env bash\n"
        "set -e\n"
        # walk argv, dropping spark-submit-only flags + their values\n
        'argv=()\n'
        'while [[ $# -gt 0 ]]; do\n'
        '  case "$1" in\n'
        '    --master|--deploy-mode|--queue|--principal|--keytab|--name|--conf|--files)\n'
        '      shift 2 ;;\n'
        '    *)\n'
        '      argv+=("$1"); shift ;;\n'
        '  esac\n'
        'done\n'
        f'exec {shlex.quote(sys.executable)} "${{argv[@]}}" --dry-run\n',
        encoding="utf-8",
    )
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    _make_exec(bin_dir / "kinit", "exit 0")
    _make_exec(bin_dir / "curl", "exit 0")

    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "SPARK_CMD": cmd.text,
        "BIZ_DATE": "20260614",
        "PRINCIPAL": spec.principal,
        "KEYTAB_PATH": str(keytab),
        "INSTANCE_ID": "inst-99",
        "TRACE_ID": "tr-1",
        "VERSION_ID": "v17",
        "TASK_ID": "task-1",
    }

    proc = subprocess.run(
        ["bash", str(SHELL)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(snap_dir),
        check=False,
    )
    assert proc.returncode == 0, (
        f"e2e failed\n--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )

    out = proc.stdout + proc.stderr
    assert "tmp_dc_ep.t_demo" in out
    assert "dt='20260614'" in out
    assert "20260613" in out  # ${dt-1}
    assert "trace_id=tr-1" in out
    assert "instance_id=inst-99" in out
