"""Variable Resolver — publish-time bake + preview-time render.

Tests cover:
  - project variable bake-in (replaces ${prj.X} with the active value)
  - runtime placeholders (${dt}/${hr}/...) are preserved
  - undeclared placeholders fail by default
  - bake snapshots (name, version) of every project variable used
  - preview composes bake + driver renderer, end-to-end
"""

from datetime import datetime
from pathlib import Path

import pytest

from pyspark_driver_pkg.resolver import (
    BakeResult,
    InMemoryProjectVariableStore,
    ProjectVariableValue,
    Resolver,
    UnresolvedPublishError,
)
from pyspark_driver_pkg.variable_catalog import load_catalog

CONTRACT_PATH = (
    Path(__file__).resolve().parents[1] / "contracts" / "runtime_variables.yaml"
)


@pytest.fixture(scope="module")
def catalog():
    return load_catalog(CONTRACT_PATH)


def _store(*entries: tuple[str, str, str, int, str]):
    """entries: (project_id, name, value, version, effective_at_iso)"""
    s = InMemoryProjectVariableStore()
    for project_id, name, value, version, effective_at in entries:
        s.put(
            project_id=project_id,
            name=name,
            value=value,
            version=version,
            effective_at=datetime.fromisoformat(effective_at),
        )
    return s


# ---- bake ------------------------------------------------------------------


def test_bake_replaces_project_variable(catalog):
    store = _store(("p1", "prj.warehouse", "s3://prod/dwh", 1, "2026-01-01T00:00:00"))
    resolver = Resolver(catalog=catalog, store=store)

    result = resolver.bake(
        "INSERT INTO ${prj.warehouse}.t SELECT 1",
        project_id="p1",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert isinstance(result, BakeResult)
    assert result.text == "INSERT INTO s3://prod/dwh.t SELECT 1"
    assert result.project_var_versions == {"prj.warehouse": 1}


def test_bake_preserves_runtime_placeholders(catalog):
    store = _store(("p1", "prj.warehouse", "s3://prod/dwh", 1, "2026-01-01T00:00:00"))
    resolver = Resolver(catalog=catalog, store=store)

    result = resolver.bake(
        "SELECT * FROM ${prj.warehouse}.t WHERE dt='${dt}' AND hr='${hr}'",
        project_id="p1",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert result.text == "SELECT * FROM s3://prod/dwh.t WHERE dt='${dt}' AND hr='${hr}'"
    assert result.project_var_versions == {"prj.warehouse": 1}


def test_bake_fails_on_undeclared_placeholder(catalog):
    store = _store(("p1", "prj.warehouse", "s3://prod/dwh", 1, "2026-01-01T00:00:00"))
    resolver = Resolver(catalog=catalog, store=store)

    with pytest.raises(UnresolvedPublishError) as exc:
        resolver.bake(
            "SELECT ${prj.unknown} FROM ${prj.warehouse}.t",
            project_id="p1",
            at=datetime.fromisoformat("2026-06-14T00:00:00"),
        )
    assert "${prj.unknown}" in str(exc.value)


def test_bake_lists_all_undeclared_placeholders(catalog):
    store = _store(("p1", "prj.warehouse", "s3://prod/dwh", 1, "2026-01-01T00:00:00"))
    resolver = Resolver(catalog=catalog, store=store)

    with pytest.raises(UnresolvedPublishError) as exc:
        resolver.bake(
            "SELECT ${prj.a}, ${prj.b}, ${prj.warehouse}",
            project_id="p1",
            at=datetime.fromisoformat("2026-06-14T00:00:00"),
        )
    msg = str(exc.value)
    assert "${prj.a}" in msg
    assert "${prj.b}" in msg


def test_bake_uses_latest_version_at_publish_time(catalog):
    store = _store(
        ("p1", "prj.x", "v1-value", 1, "2026-01-01T00:00:00"),
        ("p1", "prj.x", "v2-value", 2, "2026-03-01T00:00:00"),
        ("p1", "prj.x", "v3-value", 3, "2026-09-01T00:00:00"),  # future
    )
    resolver = Resolver(catalog=catalog, store=store)

    # 2026-06-14 < v3 effective_at(2026-09), so v2 wins.
    result = resolver.bake(
        "X=${prj.x}",
        project_id="p1",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert result.text == "X=v2-value"
    assert result.project_var_versions == {"prj.x": 2}


def test_bake_does_not_leak_other_projects(catalog):
    store = _store(
        ("p1", "prj.x", "p1-val", 1, "2026-01-01T00:00:00"),
        ("p2", "prj.x", "p2-val", 1, "2026-01-01T00:00:00"),
    )
    resolver = Resolver(catalog=catalog, store=store)

    result = resolver.bake(
        "X=${prj.x}",
        project_id="p2",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert result.text == "X=p2-val"
    assert result.project_var_versions == {"prj.x": 1}


def test_bake_records_each_referenced_variable_version_once(catalog):
    store = _store(
        ("p1", "prj.x", "vx", 4, "2026-01-01T00:00:00"),
        ("p1", "prj.y", "vy", 7, "2026-01-01T00:00:00"),
    )
    resolver = Resolver(catalog=catalog, store=store)

    result = resolver.bake(
        "${prj.x} ${prj.y} ${prj.x}",
        project_id="p1",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert result.text == "vx vy vx"
    assert result.project_var_versions == {"prj.x": 4, "prj.y": 7}


def test_bake_treats_runtime_only_sql_as_no_project_vars(catalog):
    resolver = Resolver(catalog=catalog, store=InMemoryProjectVariableStore())

    result = resolver.bake(
        "SELECT '${dt}', '${hr}'",
        project_id="p1",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert result.text == "SELECT '${dt}', '${hr}'"
    assert result.project_var_versions == {}


# ---- preview ---------------------------------------------------------------


def test_preview_substitutes_project_then_renders_runtime(catalog):
    store = _store(("p1", "prj.warehouse", "s3://prod/dwh", 1, "2026-01-01T00:00:00"))
    resolver = Resolver(catalog=catalog, store=store)

    text = resolver.preview(
        "INSERT INTO ${prj.warehouse}.t WHERE dt='${dt}' AND hr='${hr}'",
        project_id="p1",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
        biz_date="20260614",
        biz_hour="03",
    )
    assert text == "INSERT INTO s3://prod/dwh.t WHERE dt='20260614' AND hr='03'"


def test_preview_fails_when_undeclared_placeholder(catalog):
    resolver = Resolver(catalog=catalog, store=InMemoryProjectVariableStore())
    with pytest.raises(UnresolvedPublishError):
        resolver.preview(
            "SELECT ${prj.unknown}",
            project_id="p1",
            at=datetime.fromisoformat("2026-06-14T00:00:00"),
            biz_date="20260614",
        )


def test_preview_propagates_runtime_render_errors(catalog):
    """Preview SQL with ${hr} but no biz_hour must surface the render error."""
    resolver = Resolver(catalog=catalog, store=InMemoryProjectVariableStore())
    with pytest.raises(Exception):
        resolver.preview(
            "SELECT ${hr}",
            project_id="p1",
            at=datetime.fromisoformat("2026-06-14T00:00:00"),
            biz_date="20260614",
            biz_hour=None,
        )


# ---- store basics ----------------------------------------------------------


def test_store_returns_none_when_no_active_version():
    s = InMemoryProjectVariableStore()
    s.put(
        project_id="p1",
        name="prj.x",
        value="future",
        version=1,
        effective_at=datetime.fromisoformat("2027-01-01T00:00:00"),
    )
    got = s.get_active(
        project_id="p1",
        name="prj.x",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert got is None


def test_store_returns_value_with_version():
    s = InMemoryProjectVariableStore()
    s.put(
        project_id="p1",
        name="prj.x",
        value="hello",
        version=2,
        effective_at=datetime.fromisoformat("2026-01-01T00:00:00"),
    )
    got = s.get_active(
        project_id="p1",
        name="prj.x",
        at=datetime.fromisoformat("2026-06-14T00:00:00"),
    )
    assert isinstance(got, ProjectVariableValue)
    assert got.value == "hello"
    assert got.version == 2


# ---- namespacing edge cases -----------------------------------------------


def test_bake_treats_prj_prefix_with_hyphen_as_unresolved(catalog):
    """${prj-foo} (hyphen, not dot) is not a project variable namespace."""
    resolver = Resolver(catalog=catalog, store=InMemoryProjectVariableStore())
    with pytest.raises(UnresolvedPublishError) as exc:
        resolver.bake(
            "SELECT ${prj-foo}",
            project_id="p1",
            at=datetime.fromisoformat("2026-06-14T00:00:00"),
        )
    assert "${prj-foo}" in str(exc.value)


def test_bake_unknown_project_variable_fails(catalog):
    """Project variable name not in store fails publish (don't silently keep verbatim)."""
    resolver = Resolver(catalog=catalog, store=InMemoryProjectVariableStore())
    with pytest.raises(UnresolvedPublishError) as exc:
        resolver.bake(
            "SELECT ${prj.nope}",
            project_id="p1",
            at=datetime.fromisoformat("2026-06-14T00:00:00"),
        )
    assert "${prj.nope}" in str(exc.value)
