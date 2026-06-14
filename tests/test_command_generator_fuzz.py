"""Property-based fuzz tests for the command generator.

Goal: whatever the user types, the generated command string must:
  - either be rejected by the white/blacklist (ConfRejected / ValueError), OR
  - round-trip through shlex.split with user values preserved as single tokens
    and never split by shell metacharacters.

The test does NOT make claims about whether spark itself accepts the value —
it only checks the platform's transport layer is shell-injection-proof.
"""

from __future__ import annotations

import shlex

from hypothesis import HealthCheck, given, settings, strategies as st

from pyspark_driver_pkg.command_generator import (
    ConfRejected,
    TaskSpec,
    generate,
)

# Whitelisted-ish keys to exercise (do not include blacklisted keys here —
# those are the negative path covered by unit tests).
_KEYS = st.sampled_from(
    [
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.warehouseDir",
        "spark.shuffle.compress",
        "spark.dynamicAllocation.maxExecutors",
        "spark.executor.memory",
        "spark.executor.memoryOverhead",
        "spark.executor.cores",
        "spark.driver.memory",
        "spark.driver.cores",
        "spark.yarn.appMasterEnv.MY_VAR",
        "spark.executorEnv.MY_VAR",
        "spark.hadoop.hive.exec.dynamic.partition",
        "spark.hadoop.hive.exec.dynamic.partition.mode",
    ]
)

# Values include shell metacharacters, control bytes, unicode, and embedded
# substitution attempts. Avoid NUL since shlex disallows it on most platforms.
_VALUES = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters=("\x00",)),
    min_size=0,
    max_size=64,
)

# Names / queues / principals carry similarly hostile chars but stay reasonably
# small; we keep the keytab path absolute (validation requires it).
_USER_STR = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters=("\x00",)),
    min_size=1,
    max_size=32,
)


def _build_spec(name: str, queue: str, principal: str, conf: dict[str, str]) -> TaskSpec:
    return TaskSpec(
        task_id="task-1",
        name=name,
        queue=queue,
        principal=principal,
        keytab_path="/etc/keytabs/x.keytab",
        driver_path="hdfs:///apps/platform/pyspark_driver.py",
        snapshot_hdfs_path="hdfs:///dwh/platform/snapshots/dev/task-1/v17/sql.sql",
        sql_basename="sql.sql",
        spark_conf=conf,
        timezone="Asia/Shanghai",
        catalog_path="hdfs:///apps/platform/contracts/runtime_variables.yaml",
        version_id="v17",
    )


@settings(
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
    max_examples=200,
)
@given(
    name=_USER_STR,
    queue=_USER_STR,
    principal=_USER_STR,
    confs=st.lists(st.tuples(_KEYS, _VALUES), max_size=8),
)
def test_fuzz_user_values_never_break_token_boundaries(name, queue, principal, confs):
    conf = dict(confs)
    try:
        cmd = generate(_build_spec(name, queue, principal, conf))
    except (ConfRejected, ValueError):
        return  # rejected at the gate — expected.

    # Re-parse the command. shlex.split must not raise.
    tokens = shlex.split(cmd.text, posix=True)

    # Every user-provided value lands in exactly one token (either a bare
    # token, e.g. for --name, or as the right-hand side of a key=value conf
    # pair).
    expected_singletons = [name, queue, principal]
    for v in expected_singletons:
        assert v in tokens, f"value {v!r} lost in tokens {tokens!r}"

    for k, v in conf.items():
        kv = f"{k}={v}"
        assert kv in tokens, f"conf {kv!r} lost in tokens {tokens!r}"


@settings(deadline=None, max_examples=100)
@given(value=_VALUES)
def test_fuzz_single_conf_value_never_breaks_token(value):
    """Focused stress: any single conf value must round-trip as one token."""
    spec = _build_spec(
        name="demo", queue="root.default", principal="p",
        conf={"spark.sql.adaptive.enabled": value},
    )
    cmd = generate(spec)
    tokens = shlex.split(cmd.text, posix=True)
    assert f"spark.sql.adaptive.enabled={value}" in tokens
