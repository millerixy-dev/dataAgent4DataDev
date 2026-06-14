# Edge cases / failure semantics for render_sql.

from pathlib import Path

import pytest

from pyspark_driver_pkg.renderer import (
    MissingRequirementError,
    UnresolvedVariableError,
    render_sql,
)
from pyspark_driver_pkg.variable_catalog import load_catalog

CONTRACT_PATH = (
    Path(__file__).resolve().parents[1] / "contracts" / "runtime_variables.yaml"
)


@pytest.fixture(scope="module")
def catalog():
    return load_catalog(CONTRACT_PATH)


# ---- dt-N / date-N edge cases ----

def test_dt_minus_n_cross_year(catalog):
    result = render_sql("${dt-1}", biz_date="20260101", catalog=catalog)
    assert result.text == "20251231"


def test_dt_minus_n_cross_month(catalog):
    result = render_sql("${dt-1}", biz_date="20260301", catalog=catalog)
    assert result.text == "20260228"


def test_dt_minus_n_zero(catalog):
    result = render_sql("${dt-0}", biz_date="20260613", catalog=catalog)
    assert result.text == "20260613"


def test_dt_minus_n_large(catalog):
    result = render_sql("${dt-365}", biz_date="20260613", catalog=catalog)
    assert result.text == "20250613"


def test_date_minus_n_cross_year(catalog):
    result = render_sql("${date-1}", biz_date="20260101", catalog=catalog)
    assert result.text == "2025-12-31"


def test_dt_with_leading_zero_is_unresolved(catalog):
    """${dt-01} 前导零非法 - 视为未声明变量(防止用户误写)。"""
    with pytest.raises(UnresolvedVariableError) as exc:
        render_sql("${dt-01}", biz_date="20260613", catalog=catalog)
    assert "${dt-01}" in str(exc.value)


def test_dt_with_alpha_suffix_is_unresolved(catalog):
    with pytest.raises(UnresolvedVariableError) as exc:
        render_sql("${dt-1abc}", biz_date="20260613", catalog=catalog)
    assert "${dt-1abc}" in str(exc.value)


# ---- hr ----

def test_hr_renders(catalog):
    result = render_sql("hr=${hr}", biz_hour="03", catalog=catalog)
    assert result.text == "hr=03"


def test_hr_zero(catalog):
    result = render_sql("hr=${hr}", biz_hour="00", catalog=catalog)
    assert result.text == "hr=00"


def test_hr_missing_biz_hour_raises(catalog):
    """${hr} 缺 biz_hour 必须 fail-fast,与 strict 标志无关。"""
    with pytest.raises(MissingRequirementError) as exc:
        render_sql("hr=${hr}", catalog=catalog)
    assert exc.value.requirement == "biz_hour"
    assert exc.value.placeholder == "${hr}"


def test_hr_invalid_value_raises(catalog):
    with pytest.raises(ValueError):
        render_sql("hr=${hr}", biz_hour="24", catalog=catalog)


def test_hr_single_digit_raises(catalog):
    with pytest.raises(ValueError):
        render_sql("hr=${hr}", biz_hour="3", catalog=catalog)


# ---- strict mode + allow_unresolved ----

def test_strict_undeclared_raises(catalog):
    with pytest.raises(UnresolvedVariableError) as exc:
        render_sql("SELECT '${prj.foo}'", biz_date="20260613", catalog=catalog)
    assert "${prj.foo}" in str(exc.value)


def test_strict_lists_all_undeclared(catalog):
    with pytest.raises(UnresolvedVariableError) as exc:
        render_sql(
            "SELECT '${prj.a}', '${prj.b}', '${dt}'",
            biz_date="20260613",
            catalog=catalog,
        )
    msg = str(exc.value)
    assert "${prj.a}" in msg
    assert "${prj.b}" in msg


def test_non_strict_keeps_undeclared_verbatim(catalog):
    result = render_sql(
        "SELECT '${prj.foo}', '${dt}'",
        biz_date="20260613",
        catalog=catalog,
        strict=False,
    )
    assert result.text == "SELECT '${prj.foo}', '20260613'"
    assert result.unresolved == ("${prj.foo}",)


def test_non_strict_dedupes_unresolved(catalog):
    result = render_sql(
        "${prj.x} ${prj.x} ${prj.y}",
        biz_date="20260613",
        catalog=catalog,
        strict=False,
    )
    # 出现两次 ${prj.x},unresolved 只列一次
    assert sorted(result.unresolved) == ["${prj.x}", "${prj.y}"]


def test_dt_n_does_not_get_clobbered_by_dt_replacement(catalog):
    """关键:替换 ${dt} 时不能误碰 ${dt-7}。"""
    result = render_sql(
        "a=${dt} b=${dt-7}",
        biz_date="20260613",
        catalog=catalog,
    )
    assert result.text == "a=20260613 b=20260606"


def test_invalid_biz_date_format(catalog):
    with pytest.raises(ValueError):
        render_sql("${dt}", biz_date="2026-06-13", catalog=catalog)
