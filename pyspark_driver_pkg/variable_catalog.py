"""Runtime variable catalog loader.

Reads ``contracts/runtime_variables.yaml`` (the cross-stack contract shared by
driver, Backend Variable Resolver and Frontend variable panel) and exposes a
typed view used at render time.

Schema validation is deliberately strict — a malformed catalog is a deployment
issue, not a runtime fallback.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import yaml


@dataclass(frozen=True)
class Variable:
    """A single runtime variable as declared in the catalog."""

    name: str
    kind: str
    syntax: str
    pattern: re.Pattern[str]
    requires: tuple[str, ...]
    derive: str
    description: str
    examples: tuple[Mapping[str, object], ...]


@dataclass(frozen=True)
class PlaceholderMatch:
    """Result of matching a literal ``${...}`` placeholder against the catalog."""

    variable: Variable
    captures: Mapping[str, str]


class VariableCatalog:
    """Materialised catalog. Holds ``Variable`` objects and the scan regex."""

    def __init__(
        self,
        version: int,
        timezone_default: str,
        variables: Sequence[Variable],
        scan_pattern: re.Pattern[str],
    ) -> None:
        self.version = version
        self.timezone_default = timezone_default
        self.variables: tuple[Variable, ...] = tuple(variables)
        self._scan_pattern = scan_pattern

    def scan(self, text: str) -> list[str]:
        """Return every ``${...}`` placeholder that appears in ``text``."""

        return self._scan_pattern.findall(text)

    def match_placeholder(self, placeholder: str) -> PlaceholderMatch | None:
        """Return the matching :class:`Variable` for ``placeholder`` (e.g. ``${dt-7}``).

        Returns ``None`` if no declared variable matches.
        """

        for variable in self.variables:
            m = variable.pattern.fullmatch(placeholder)
            if m is not None:
                return PlaceholderMatch(variable=variable, captures=m.groupdict())
        return None


def _expect_mapping(value: object, *, where: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{where}: expected mapping, got {type(value).__name__}")
    return value


def _expect_str(value: object, *, where: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{where}: expected string, got {type(value).__name__}")
    return value


def _build_variable(raw: Mapping[str, object], *, where: str) -> Variable:
    name = _expect_str(raw.get("name"), where=f"{where}.name")
    kind = _expect_str(raw.get("kind"), where=f"{where}.kind")
    syntax = _expect_str(raw.get("syntax"), where=f"{where}.syntax")
    pattern = _expect_str(raw.get("pattern"), where=f"{where}.pattern")
    derive = _expect_str(raw.get("derive"), where=f"{where}.derive")
    description = _expect_str(raw.get("description", ""), where=f"{where}.description")

    requires_raw = raw.get("requires", [])
    if not isinstance(requires_raw, list) or not all(isinstance(x, str) for x in requires_raw):
        raise ValueError(f"{where}.requires: expected list[str]")

    examples_raw = raw.get("examples", [])
    if not isinstance(examples_raw, list):
        raise ValueError(f"{where}.examples: expected list of mappings")

    return Variable(
        name=name,
        kind=kind,
        syntax=syntax,
        pattern=re.compile(pattern),
        requires=tuple(requires_raw),
        derive=derive,
        description=description,
        examples=tuple(_expect_mapping(e, where=f"{where}.examples[*]") for e in examples_raw),
    )


def load_catalog(path: str | Path) -> VariableCatalog:
    """Load the variable catalog from ``path`` and validate its schema.

    Raises :class:`FileNotFoundError` if the file is missing and :class:`ValueError`
    on any schema violation. The returned object is immutable from the caller's
    perspective.
    """

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"variable catalog not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("catalog root must be a mapping")

    version = raw.get("version")
    if not isinstance(version, int):
        raise ValueError("catalog.version must be an integer")

    timezone_default = _expect_str(raw.get("timezone_default", ""), where="timezone_default")

    variables_raw = raw.get("variables")
    if not isinstance(variables_raw, list):
        raise ValueError("catalog.variables must be a list")

    variables = [
        _build_variable(_expect_mapping(item, where=f"variables[{i}]"), where=f"variables[{i}]")
        for i, item in enumerate(variables_raw)
    ]

    scan_pattern_raw = _expect_str(raw.get("scan_pattern", ""), where="scan_pattern")
    if not scan_pattern_raw:
        raise ValueError("catalog.scan_pattern must be a non-empty regex")

    return VariableCatalog(
        version=version,
        timezone_default=timezone_default,
        variables=variables,
        scan_pattern=re.compile(scan_pattern_raw),
    )
