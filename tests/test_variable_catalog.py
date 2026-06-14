# Tests for the runtime variable catalog loader.
#
# Catalog 读取 contracts/runtime_variables.yaml 并为 driver / Resolver 提供:
#   - version 与默认时区
#   - 每个变量的元信息 (name, kind, syntax, pattern, requires, derive, examples)
#   - 整体扫描正则 scan_pattern

from pathlib import Path

import pytest

from pyspark_driver_pkg.variable_catalog import VariableCatalog, load_catalog

CONTRACT_PATH = (
    Path(__file__).resolve().parents[1] / "contracts" / "runtime_variables.yaml"
)


def test_load_catalog_returns_version_and_default_timezone():
    catalog = load_catalog(CONTRACT_PATH)

    assert catalog.version == 1
    assert catalog.timezone_default == "Asia/Shanghai"


def test_load_catalog_exposes_known_variables():
    catalog = load_catalog(CONTRACT_PATH)

    names = [v.name for v in catalog.variables]
    assert names == ["dt", "date", "month", "dt-N", "date-N", "hr"]


def test_load_catalog_marks_parameterized_variables():
    catalog = load_catalog(CONTRACT_PATH)

    by_name = {v.name: v for v in catalog.variables}
    assert by_name["dt"].syntax == "literal"
    assert by_name["dt-N"].syntax == "parameterized"
    assert by_name["dt-N"].requires == ("biz_date",)
    assert by_name["hr"].requires == ("biz_hour",)


def test_load_catalog_compiles_scan_pattern_for_full_placeholder():
    catalog = load_catalog(CONTRACT_PATH)

    matches = catalog.scan("WHERE dt = '${dt}' AND hr = '${hr}' AND ts = '${dt-1}'")
    assert matches == ["${dt}", "${hr}", "${dt-1}"]


def test_load_catalog_rejects_missing_file(tmp_path):
    missing = tmp_path / "no_such.yaml"
    with pytest.raises(FileNotFoundError):
        load_catalog(missing)


def test_load_catalog_rejects_invalid_schema(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("version: 1\nvariables: not-a-list\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_catalog(bad)


def test_variable_catalog_returns_variable_by_placeholder():
    catalog = load_catalog(CONTRACT_PATH)

    match = catalog.match_placeholder("${dt}")
    assert match is not None
    assert match.variable.name == "dt"
    assert match.captures == {}


def test_variable_catalog_returns_capture_for_parameterized():
    catalog = load_catalog(CONTRACT_PATH)

    match = catalog.match_placeholder("${dt-7}")
    assert match is not None
    assert match.variable.name == "dt-N"
    assert match.captures == {"n": "7"}


def test_variable_catalog_rejects_unknown_placeholder():
    catalog = load_catalog(CONTRACT_PATH)

    assert catalog.match_placeholder("${prj.foo}") is None
    assert catalog.match_placeholder("${dt-01}") is None  # 前导零非法
    assert catalog.match_placeholder("${dt-1abc}") is None


def test_variable_catalog_is_a_VariableCatalog_instance():
    catalog = load_catalog(CONTRACT_PATH)
    assert isinstance(catalog, VariableCatalog)
