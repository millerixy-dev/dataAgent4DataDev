"""SQL runtime variable renderer.

Resolves ``${...}`` placeholders against the shared :mod:`variable_catalog` using
``biz_date`` (mandatory) and ``biz_hour`` (only when ``${hr}`` is referenced).

Strict mode (default): undeclared placeholders raise ``UnresolvedVariableError``.
``requires`` field absence (e.g. ``${hr}`` without ``--biz-hour``) always raises
``MissingRequirementError`` — that's a configuration error, not a render flag.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable, Mapping

from pyspark_driver_pkg.variable_catalog import PlaceholderMatch, VariableCatalog

BIZ_DATE_FORMAT = "%Y%m%d"


class UnresolvedVariableError(ValueError):
    """SQL contains placeholders the catalog does not declare."""

    def __init__(self, placeholders: tuple[str, ...]) -> None:
        super().__init__(
            "SQL contains undeclared variables: " + ", ".join(sorted(set(placeholders)))
        )
        self.placeholders = placeholders


class MissingRequirementError(ValueError):
    """A declared variable needs a CLI argument that was not supplied."""

    def __init__(self, placeholder: str, requirement: str) -> None:
        super().__init__(f"variable {placeholder} requires --{requirement.replace('_', '-')}")
        self.placeholder = placeholder
        self.requirement = requirement


@dataclass(frozen=True)
class RenderResult:
    """Outcome of a render. ``unresolved`` is empty under strict mode."""

    text: str
    unresolved: tuple[str, ...] = ()


def _parse_biz_date(biz_date: str) -> date:
    return datetime.strptime(biz_date, BIZ_DATE_FORMAT).date()


# -- derive functions: pure (str/inputs) -> str. Must round-trip the catalog examples. --


def _identity_yyyymmdd(captures: Mapping[str, str], inputs: Mapping[str, object]) -> str:
    return _parse_biz_date(str(inputs["biz_date"])).strftime("%Y%m%d")


def _identity_yyyy_mm_dd(captures: Mapping[str, str], inputs: Mapping[str, object]) -> str:
    return _parse_biz_date(str(inputs["biz_date"])).strftime("%Y-%m-%d")


def _identity_yyyymm(captures: Mapping[str, str], inputs: Mapping[str, object]) -> str:
    return _parse_biz_date(str(inputs["biz_date"])).strftime("%Y%m")


def _dt_minus_n(captures: Mapping[str, str], inputs: Mapping[str, object]) -> str:
    base = _parse_biz_date(str(inputs["biz_date"]))
    n = int(captures["n"])
    return (base - timedelta(days=n)).strftime("%Y%m%d")


def _date_minus_n(captures: Mapping[str, str], inputs: Mapping[str, object]) -> str:
    base = _parse_biz_date(str(inputs["biz_date"]))
    n = int(captures["n"])
    return (base - timedelta(days=n)).strftime("%Y-%m-%d")


def _identity_hh(captures: Mapping[str, str], inputs: Mapping[str, object]) -> str:
    raw = str(inputs["biz_hour"])
    if not (len(raw) == 2 and raw.isdigit() and 0 <= int(raw) <= 23):
        raise ValueError(f"biz_hour must be HH 00-23, got {raw!r}")
    return raw


_DERIVERS: dict[str, Callable[[Mapping[str, str], Mapping[str, object]], str]] = {
    "identity_yyyymmdd": _identity_yyyymmdd,
    "identity_yyyy_mm_dd": _identity_yyyy_mm_dd,
    "identity_yyyymm": _identity_yyyymm,
    "dt_minus_n": _dt_minus_n,
    "date_minus_n": _date_minus_n,
    "identity_hh": _identity_hh,
}


def _render_match(
    placeholder: str,
    match: PlaceholderMatch,
    inputs: Mapping[str, object],
) -> str:
    for requirement in match.variable.requires:
        if inputs.get(requirement) is None:
            raise MissingRequirementError(placeholder, requirement)
    derive = _DERIVERS.get(match.variable.derive)
    if derive is None:
        raise ValueError(
            f"catalog declares unknown derive function {match.variable.derive!r} "
            f"for variable {match.variable.name}"
        )
    return derive(match.captures, inputs)


def render_sql(
    text: str,
    *,
    catalog: VariableCatalog,
    biz_date: str | None = None,
    biz_hour: str | None = None,
    strict: bool = True,
    timezone: str | None = None,
) -> RenderResult:
    """Render every catalog placeholder in ``text``.

    ``timezone`` is accepted for API symmetry but currently unused — the
    renderer derives variables from absolute ``biz_date``/``biz_hour`` strings,
    independent of any wall-clock time. The argument exists so callers can pin
    a timezone explicitly when extensions (e.g. wall-clock derived variables)
    are added in a later phase.
    """
    inputs: dict[str, object] = {"biz_date": biz_date, "biz_hour": biz_hour}

    placeholders = catalog.scan(text)
    unresolved: list[str] = []
    replacements: dict[str, str] = {}

    for placeholder in placeholders:
        if placeholder in replacements or placeholder in unresolved:
            continue
        match = catalog.match_placeholder(placeholder)
        if match is None:
            unresolved.append(placeholder)
            continue
        replacements[placeholder] = _render_match(placeholder, match, inputs)

    if unresolved and strict:
        raise UnresolvedVariableError(tuple(unresolved))

    rendered = text
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)

    return RenderResult(text=rendered, unresolved=tuple(unresolved))
