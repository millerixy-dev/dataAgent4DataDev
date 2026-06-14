# Driver-level integration tests.
#
# 目标:跑"参数 → 读 SQL → 渲染 → 拆分"全链路,不启动 SparkSession。
# main() 中真正需要 Spark 的部分在 execute 阶段,本套测试不覆盖,留给真实 e2e。

from pathlib import Path

import pytest

import pyspark_driver

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts" / "runtime_variables.yaml"


def _write(tmp_path: Path, name: str, body: str) -> Path:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return p


def test_run_render_basic(tmp_path):
    sql = _write(tmp_path, "x.sql", "SELECT '${dt}' AS d, '${dt-1}' AS y;")
    outcome = pyspark_driver.run_render(
        [
            "--sql-file",
            str(sql),
            "--biz-date",
            "20260613",
            "--catalog-path",
            str(CONTRACTS),
        ]
    )
    assert outcome.statements == ("SELECT '20260613' AS d, '20260612' AS y",)


def test_run_render_with_hr(tmp_path):
    sql = _write(tmp_path, "x.sql", "INSERT INTO t PARTITION (hr='${hr}') VALUES (1);")
    outcome = pyspark_driver.run_render(
        [
            "--sql-file",
            str(sql),
            "--biz-date",
            "20260613",
            "--biz-hour",
            "03",
            "--catalog-path",
            str(CONTRACTS),
        ]
    )
    assert outcome.statements == ("INSERT INTO t PARTITION (hr='03') VALUES (1)",)


def test_run_render_hr_without_biz_hour_fails(tmp_path):
    sql = _write(tmp_path, "x.sql", "SELECT '${hr}';")
    with pytest.raises(SystemExit) as exc:
        pyspark_driver.run_render(
            [
                "--sql-file",
                str(sql),
                "--biz-date",
                "20260613",
                "--catalog-path",
                str(CONTRACTS),
            ]
        )
    assert exc.value.code != 0


def test_run_render_undefined_variable_fails(tmp_path):
    sql = _write(tmp_path, "x.sql", "SELECT '${prj.foo}';")
    with pytest.raises(SystemExit):
        pyspark_driver.run_render(
            [
                "--sql-file",
                str(sql),
                "--biz-date",
                "20260613",
                "--catalog-path",
                str(CONTRACTS),
            ]
        )


def test_run_render_allow_unresolved(tmp_path):
    sql = _write(tmp_path, "x.sql", "SELECT '${prj.foo}', '${dt}';")
    outcome = pyspark_driver.run_render(
        [
            "--sql-file",
            str(sql),
            "--biz-date",
            "20260613",
            "--catalog-path",
            str(CONTRACTS),
            "--allow-unresolved-vars",
        ]
    )
    assert outcome.statements == ("SELECT '${prj.foo}', '20260613'",)
    assert outcome.unresolved == ("${prj.foo}",)


def test_run_render_multiple_statements(tmp_path):
    sql = _write(
        tmp_path,
        "x.sql",
        "INSERT INTO a VALUES ('${dt}');\nINSERT INTO b VALUES ('${dt-1}');\n",
    )
    outcome = pyspark_driver.run_render(
        [
            "--sql-file",
            str(sql),
            "--biz-date",
            "20260613",
            "--catalog-path",
            str(CONTRACTS),
        ]
    )
    assert outcome.statements == (
        "INSERT INTO a VALUES ('20260613')",
        "INSERT INTO b VALUES ('20260612')",
    )


def test_run_render_old_command_still_works(tmp_path):
    """向后兼容:仅传 --sql-file --biz-date,SQL 只用老变量集合。"""
    sql = _write(
        tmp_path,
        "x.sql",
        "SELECT * FROM t WHERE dt='${dt}' AND m='${month}';",
    )
    outcome = pyspark_driver.run_render(
        [
            "--sql-file",
            str(sql),
            "--biz-date",
            "20260613",
            "--catalog-path",
            str(CONTRACTS),
        ]
    )
    assert outcome.statements == ("SELECT * FROM t WHERE dt='20260613' AND m='202606'",)


def test_run_render_emits_structured_log_fields(tmp_path, capsys):
    sql = _write(tmp_path, "x.sql", "SELECT '${dt}';")
    pyspark_driver.run_render(
        [
            "--sql-file",
            str(sql),
            "--biz-date",
            "20260613",
            "--biz-hour",
            "03",
            "--catalog-path",
            str(CONTRACTS),
            "--trace-id",
            "trace-abc",
            "--version-id",
            "v17",
            "--instance-id",
            "inst-99",
        ]
    )
    out = capsys.readouterr().out + capsys.readouterr().err
    # 结构化字段以 [k=v] 形式出现至少一次,便于日志聚合按 key 索引
    assert "trace_id=trace-abc" in out
    assert "version_id=v17" in out
    assert "instance_id=inst-99" in out
    assert "biz_date=20260613" in out


def test_run_render_dry_run_does_not_execute(tmp_path):
    sql = _write(tmp_path, "x.sql", "SELECT '${dt}';")
    outcome = pyspark_driver.run_render(
        [
            "--sql-file",
            str(sql),
            "--biz-date",
            "20260613",
            "--catalog-path",
            str(CONTRACTS),
            "--dry-run",
        ]
    )
    assert outcome.dry_run is True
    assert outcome.statements == ("SELECT '20260613'",)
