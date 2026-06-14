# Tests for the runtime variable renderer.
#
# 渲染契约:
#   render_sql(text, *, biz_date, biz_hour=None, catalog, strict=True, timezone=...)
#     -> RenderResult(text, unresolved)
#
#   - strict=True (默认) + 未定义占位符 -> 抛 UnresolvedVariableError 列出全部
#   - strict=False + 未定义占位符 -> 保留原样,在 result.unresolved 中返回
#   - 任意 requires 字段缺失(如 ${hr} 但未传 biz_hour) -> 抛 MissingRequirementError
#   - 同一占位符出现多次 -> 全部替换,不依赖出现顺序

from pathlib import Path

import pytest

from pyspark_driver_pkg.renderer import (
    RenderResult,
    render_sql,
)
from pyspark_driver_pkg.variable_catalog import load_catalog

CONTRACT_PATH = (
    Path(__file__).resolve().parents[1] / "contracts" / "runtime_variables.yaml"
)


@pytest.fixture(scope="module")
def catalog():
    return load_catalog(CONTRACT_PATH)


def test_render_dt(catalog):
    result = render_sql("SELECT '${dt}'", biz_date="20260613", catalog=catalog)
    assert isinstance(result, RenderResult)
    assert result.text == "SELECT '20260613'"
    assert result.unresolved == ()


def test_render_date(catalog):
    result = render_sql("WHERE d = '${date}'", biz_date="20260613", catalog=catalog)
    assert result.text == "WHERE d = '2026-06-13'"


def test_render_month(catalog):
    result = render_sql("PARTITION (m = '${month}')", biz_date="20260613", catalog=catalog)
    assert result.text == "PARTITION (m = '202606')"


def test_render_replaces_all_occurrences(catalog):
    text = "SELECT '${dt}' AS a, '${dt}' AS b, '${date}' AS c"
    result = render_sql(text, biz_date="20260613", catalog=catalog)
    assert result.text == "SELECT '20260613' AS a, '20260613' AS b, '2026-06-13' AS c"


def test_render_leaves_non_placeholder_text_unchanged(catalog):
    text = "-- comment ${not a var}\nSELECT $name FROM t WHERE dt = '${dt}'"
    result = render_sql(text, biz_date="20260613", catalog=catalog)
    # `${not a var}` 含空格不匹配 scan_pattern,$name 没有大括号也不匹配
    assert result.text == "-- comment ${not a var}\nSELECT $name FROM t WHERE dt = '20260613'"


def test_render_runs_catalog_examples(catalog):
    """Drive a render through every example declared in the catalog.

    Examples are the cross-stack contract — driver、Resolver、Frontend 都吃这份。
    Whatever the catalog declares MUST round-trip through render_sql.
    """
    for variable in catalog.variables:
        if variable.kind != "runtime":
            continue
        for example in variable.examples:
            input_ = dict(example["input"])
            expected = example["output"]

            placeholder = _placeholder_from_example(variable.name, input_)
            kwargs = {k: v for k, v in input_.items() if k in {"biz_date", "biz_hour"}}
            result = render_sql(placeholder, catalog=catalog, **kwargs)
            assert result.text == expected, (
                f"{variable.name} example failed: input={input_}, "
                f"expected={expected!r}, got={result.text!r}"
            )


def _placeholder_from_example(name: str, input_: dict) -> str:
    """Reconstruct the placeholder text for a parameterized variable example."""
    if name == "dt-N":
        return f"${{dt-{input_['n']}}}"
    if name == "date-N":
        return f"${{date-{input_['n']}}}"
    return f"${{{name}}}"
