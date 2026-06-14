# Regression tests with the real production SQL fixture.

from pathlib import Path

import pyspark_driver

REPO = Path(__file__).resolve().parents[1]
CONTRACTS = REPO / "contracts" / "runtime_variables.yaml"
HISTORICAL_SQL = REPO / "t_eci_company_4_dwi.sql"


def test_historical_sql_renders_through_driver(tmp_path):
    """A real production SQL fixture (no placeholders) survives the new pipeline."""
    outcome = pyspark_driver.run_render(
        [
            "--sql-file",
            str(HISTORICAL_SQL),
            "--biz-date",
            "20260613",
            "--catalog-path",
            str(CONTRACTS),
        ]
    )
    # SQL has 1 INSERT OVERWRITE statement spanning ~100 lines via CTE.
    assert len(outcome.statements) == 1
    statement = outcome.statements[0]
    assert "INSERT OVERWRITE TABLE tmp_dc_ep.t_eci_company_4_dwi" in statement
    assert "filtered_base" in statement
    assert "transformed" in statement


def test_historical_sql_via_old_command_only(tmp_path):
    """Old command (only --sql-file --biz-date --catalog-path) must still work."""
    outcome = pyspark_driver.run_render(
        [
            "--sql-file",
            str(HISTORICAL_SQL),
            "--biz-date",
            "20260613",
            "--catalog-path",
            str(CONTRACTS),
        ]
    )
    assert outcome.unresolved == ()
    assert outcome.dry_run is False


def test_synthetic_old_style_sql_with_dt(tmp_path):
    """A synthetic SQL using only the legacy variables {dt,date,month}."""
    sql = tmp_path / "old.sql"
    sql.write_text(
        "INSERT INTO t PARTITION (dt='${dt}', m='${month}')\n"
        "SELECT id, '${date}' AS biz FROM staging WHERE dt='${dt}';",
        encoding="utf-8",
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
        "INSERT INTO t PARTITION (dt='20260613', m='202606')\n"
        "SELECT id, '2026-06-13' AS biz FROM staging WHERE dt='20260613'",
    )
