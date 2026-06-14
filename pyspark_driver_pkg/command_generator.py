"""spark-submit command generator.

Single defense line for the platform: whatever the user types in the editor
must round-trip through this module before it touches a shell. The module:

1. Filters every ``spark.*`` config through a key-level white/black-list.
2. Quotes every user value with :func:`shlex.quote` so shell metacharacters
   become literal text.
3. Re-parses the produced command string with :func:`shlex.split` to confirm
   the token shape matches the planned shape (the "self-check").

Instance-level fields — ``${biz_date}``, ``${TRACE_ID}``, ``${INSTANCE_ID}``,
``${TASK_ID}``, ``${VERSION_ID}`` — are left as shell variable references that
``spark_submit.sh`` resolves at run time. These are NOT user-controlled (the
DS task body env vars come from the platform), so they don't need quoting.

Reference: design.md Decision 9.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field
from typing import Mapping


# -- whitelist / blacklist --------------------------------------------------

# Whitelist: a conf key passes only if it is exactly one of these or starts
# with one of the prefixes. Order does not matter.
WHITELIST_PREFIXES: tuple[str, ...] = (
    "spark.sql.",
    "spark.shuffle.",
    "spark.dynamicAllocation.",
    "spark.executor.memory",  # matches memory and memoryOverhead
    "spark.driver.memory",
    "spark.yarn.appMasterEnv.",
    "spark.executorEnv.",
    "spark.hadoop.hive.exec.dynamic.partition",
)

WHITELIST_KEYS: frozenset[str] = frozenset(
    {
        "spark.executor.cores",
        "spark.driver.cores",
    }
)

# Blacklist takes priority over the whitelist.
BLACKLIST_KEYS: frozenset[str] = frozenset(
    {
        "spark.driver.extraJavaOptions",
        "spark.executor.extraJavaOptions",
        "spark.driver.extraClassPath",
        "spark.driver.extraLibraryPath",
        "spark.executor.extraClassPath",
        "spark.executor.extraLibraryPath",
        "spark.yarn.principal",
        "spark.yarn.keytab",
    }
)

BLACKLIST_PREFIXES: tuple[str, ...] = (
    "spark.yarn.dist.",
    "spark.kerberos.",
    "spark.security.credentials.",
)


def _is_blacklisted(key: str) -> bool:
    if key in BLACKLIST_KEYS:
        return True
    return any(key.startswith(p) for p in BLACKLIST_PREFIXES)


def _is_whitelisted(key: str) -> bool:
    if key in WHITELIST_KEYS:
        return True
    return any(key.startswith(p) for p in WHITELIST_PREFIXES)


class ConfRejected(ValueError):
    """One or more user conf entries failed the white/black-list check."""

    def __init__(self, rejected: Mapping[str, str]) -> None:
        msg = "; ".join(f"{k}: {reason}" for k, reason in rejected.items())
        super().__init__(f"conf rejected: {msg}")
        self.rejected: dict[str, str] = dict(rejected)


# -- specs ------------------------------------------------------------------


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    name: str
    queue: str
    principal: str
    keytab_path: str
    driver_path: str
    snapshot_hdfs_path: str
    sql_basename: str
    spark_conf: Mapping[str, str]
    timezone: str
    catalog_path: str
    version_id: str

    def validate(self) -> None:
        if not self.principal:
            raise ValueError("principal must be non-empty")
        if not self.queue:
            raise ValueError("queue must be non-empty")
        if not self.name:
            raise ValueError("name must be non-empty")
        if not self.keytab_path.startswith("/"):
            raise ValueError(f"keytab_path must be absolute, got {self.keytab_path!r}")
        if not (
            self.snapshot_hdfs_path.startswith("hdfs://")
            or self.snapshot_hdfs_path.startswith("hdfs:/")
        ):
            raise ValueError(
                f"snapshot_hdfs_path must be a hdfs:// URI, got {self.snapshot_hdfs_path!r}"
            )
        if "/" in self.sql_basename or "\\" in self.sql_basename or not self.sql_basename:
            raise ValueError(
                f"sql_basename must not contain path separators, got {self.sql_basename!r}"
            )
        if not self.timezone:
            raise ValueError("timezone must be non-empty")
        if not self.catalog_path:
            raise ValueError("catalog_path must be non-empty")
        if not self.version_id:
            raise ValueError("version_id must be non-empty")


@dataclass(frozen=True)
class GeneratedCommand:
    text: str
    audit_tokens: tuple[str, ...] = field(default_factory=tuple)


# -- generation -------------------------------------------------------------


_DEFAULT_CONF: tuple[tuple[str, str], ...] = (
    ("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "python3"),
    ("spark.executorEnv.PYSPARK_PYTHON", "python3"),
    ("spark.yarn.appMasterEnv.PYTHONUNBUFFERED", "1"),
    ("spark.executorEnv.PYTHONUNBUFFERED", "1"),
)


def _filter_conf(spark_conf: Mapping[str, str]) -> dict[str, str]:
    rejected: dict[str, str] = {}
    accepted: dict[str, str] = {}
    for key, value in spark_conf.items():
        if _is_blacklisted(key):
            rejected[key] = "blacklisted"
            continue
        if not _is_whitelisted(key):
            rejected[key] = "not in whitelist"
            continue
        accepted[key] = value
    if rejected:
        raise ConfRejected(rejected)
    return accepted


def _q(value: str) -> str:
    """Shell-quote a user-controlled value so it lands as exactly one token."""
    return shlex.quote(value)


def _self_check(text: str, planned_user_values: list[str]) -> tuple[str, ...]:
    """Round-trip the command via shlex.split and verify the planned values
    show up unchanged in the resulting token list. Raises ValueError on a
    mismatch — that is a generator bug, not a user bug.
    """
    tokens = shlex.split(text, posix=True)
    for value in planned_user_values:
        if value not in tokens and not any(t.endswith(f"={value}") for t in tokens):
            raise ValueError(
                f"self-check failed: planned value {value!r} not found in re-parsed tokens"
            )
    return tuple(tokens)


def generate(spec: TaskSpec) -> GeneratedCommand:
    spec.validate()

    user_conf = _filter_conf(spec.spark_conf)
    merged_conf: list[tuple[str, str]] = []
    seen: set[str] = set()
    for k, v in user_conf.items():
        merged_conf.append((k, v))
        seen.add(k)
    for k, v in _DEFAULT_CONF:
        if k in seen:
            continue
        merged_conf.append((k, v))

    # Build the command piece by piece. We track planned user values so the
    # self-check can verify each survived as a single token.
    parts: list[str] = ["spark-submit"]
    planned_user_values: list[str] = []

    def _add(flag: str, value: str) -> None:
        parts.append(flag)
        parts.append(_q(value))
        planned_user_values.append(value)

    parts.extend(["--master", "yarn"])
    parts.extend(["--deploy-mode", "cluster"])
    _add("--queue", spec.queue)
    _add("--principal", spec.principal)
    _add("--keytab", spec.keytab_path)
    _add("--name", spec.name)

    for k, v in merged_conf:
        kv = f"{k}={v}"
        parts.extend(["--conf", _q(kv)])
        planned_user_values.append(kv)

    _add("--files", spec.snapshot_hdfs_path)
    parts.append(_q(spec.driver_path))
    planned_user_values.append(spec.driver_path)

    # Driver args.
    parts.extend(["--sql-file", _q(spec.sql_basename)])
    planned_user_values.append(spec.sql_basename)

    # ${biz_date} is single-quoted so it survives shlex.split as a literal,
    # then spark_submit.sh substitutes it via shell parameter expansion.
    parts.extend(["--biz-date", _q("${biz_date}")])

    _add("--catalog-path", spec.catalog_path)
    _add("--timezone", spec.timezone)

    # Runtime correlation fields are *shell variable references* — eval will
    # expand them at run time using env vars set by spark_submit.sh.
    # We deliberately use the bare ${VAR} form (no surrounding quotes) so
    # that shlex.split, in posix mode, sees the literal "${TRACE_ID}" as a
    # token. The shell still expands the reference because eval is invoked
    # without preserving any quoting of these tokens.
    parts.extend(["--trace-id", "${TRACE_ID}"])
    parts.extend(["--instance-id", "${INSTANCE_ID}"])
    parts.extend(["--task-id", "${TASK_ID}"])
    _add("--version-id", spec.version_id)

    text = " ".join(parts)
    tokens = _self_check(text, planned_user_values)
    return GeneratedCommand(text=text, audit_tokens=tokens)
