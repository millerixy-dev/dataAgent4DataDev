"""Variable Resolver — publish-time bake + preview-time render.

The Resolver is the platform-side counterpart to :mod:`renderer`:

* :meth:`Resolver.bake` runs at publish time. It substitutes project variables
  using the values that are *active* at the publish moment, captures their
  ``(name, version)`` pair so the snapshot's ``meta.json`` can pin them, and
  preserves runtime placeholders verbatim.
* :meth:`Resolver.preview` runs whenever the editor wants ``${dt}`` style
  substitution to be visible. It chains :meth:`bake` + :func:`renderer.render_sql`
  so the preview text is byte-identical to what the driver will execute on
  the same ``(version_id, biz_date, biz_hour)``.

Any ``${...}`` placeholder that is neither a project variable nor a declared
runtime variable causes ``UnresolvedPublishError`` — publish must fail rather
than create an unrunnable snapshot.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Protocol

from pyspark_driver_pkg.renderer import render_sql
from pyspark_driver_pkg.variable_catalog import VariableCatalog


# -- store -------------------------------------------------------------------


@dataclass(frozen=True)
class ProjectVariableValue:
    value: str
    version: int


class ProjectVariableStore(Protocol):
    """The minimal surface the Resolver needs from project variable storage."""

    def get_active(
        self, *, project_id: str, name: str, at: datetime
    ) -> ProjectVariableValue | None: ...


@dataclass
class _Entry:
    value: str
    version: int
    effective_at: datetime


class InMemoryProjectVariableStore:
    """Pure-memory store. Backend will replace with SQLAlchemy in group 7."""

    def __init__(self) -> None:
        self._by_project: Dict[str, Dict[str, list[_Entry]]] = defaultdict(
            lambda: defaultdict(list)
        )

    def put(
        self,
        *,
        project_id: str,
        name: str,
        value: str,
        version: int,
        effective_at: datetime,
    ) -> None:
        self._by_project[project_id][name].append(
            _Entry(value=value, version=version, effective_at=effective_at)
        )

    def get_active(
        self,
        *,
        project_id: str,
        name: str,
        at: datetime,
    ) -> ProjectVariableValue | None:
        entries = self._by_project.get(project_id, {}).get(name, [])
        eligible = [e for e in entries if e.effective_at <= at]
        if not eligible:
            return None
        latest = max(eligible, key=lambda e: e.effective_at)
        return ProjectVariableValue(value=latest.value, version=latest.version)


# -- bake / preview ---------------------------------------------------------


class UnresolvedPublishError(ValueError):
    """SQL has placeholders neither in project variable dict nor in runtime catalog."""

    def __init__(self, placeholders: tuple[str, ...]) -> None:
        super().__init__(
            "publish blocked - unknown placeholders: " + ", ".join(sorted(set(placeholders)))
        )
        self.placeholders = placeholders


@dataclass(frozen=True)
class BakeResult:
    """Snapshot-bound output of :meth:`Resolver.bake`.

    ``project_var_versions`` is recorded into ``meta.json`` so a future audit
    can determine which project variable values were used.
    """

    text: str
    project_var_versions: Dict[str, int] = field(default_factory=dict)


def _is_project_variable_name(name: str) -> bool:
    """Project variables live under the ``prj.`` namespace.

    Driver runtime variables never start with ``prj.``; this single rule keeps
    namespacing simple and avoids accidental shadowing.
    """
    return name.startswith("prj.")


def _placeholder_name(placeholder: str) -> str:
    """``${prj.foo}`` -> ``prj.foo``. Caller guarantees the outer ``${ }``."""
    return placeholder[2:-1]


class Resolver:
    def __init__(self, *, catalog: VariableCatalog, store: ProjectVariableStore) -> None:
        self.catalog = catalog
        self.store = store

    def bake(
        self,
        sql: str,
        *,
        project_id: str,
        at: datetime,
    ) -> BakeResult:
        placeholders = self.catalog.scan(sql)
        seen: set[str] = set()
        replacements: dict[str, str] = {}
        versions: dict[str, int] = {}
        unresolved: list[str] = []

        for placeholder in placeholders:
            if placeholder in seen:
                continue
            seen.add(placeholder)

            name = _placeholder_name(placeholder)

            if _is_project_variable_name(name):
                value = self.store.get_active(project_id=project_id, name=name, at=at)
                if value is None:
                    unresolved.append(placeholder)
                    continue
                replacements[placeholder] = value.value
                versions[name] = value.version
                continue

            # Not a project variable. Must be a declared runtime variable;
            # leave verbatim. If unknown, fail fast.
            if self.catalog.match_placeholder(placeholder) is None:
                unresolved.append(placeholder)

        if unresolved:
            raise UnresolvedPublishError(tuple(unresolved))

        baked = sql
        for placeholder, value in replacements.items():
            baked = baked.replace(placeholder, value)

        return BakeResult(text=baked, project_var_versions=versions)

    def preview(
        self,
        sql: str,
        *,
        project_id: str,
        at: datetime,
        biz_date: str,
        biz_hour: Optional[str] = None,
    ) -> str:
        baked = self.bake(sql, project_id=project_id, at=at)
        rendered = render_sql(
            baked.text,
            catalog=self.catalog,
            biz_date=biz_date,
            biz_hour=biz_hour,
            strict=True,
        )
        return rendered.text
