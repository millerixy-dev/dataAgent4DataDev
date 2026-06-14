"""Command Generator — produces the spark-submit command string.

Threat model: every user-controlled string MUST round-trip through shlex.quote
+ shlex.split unchanged. The generator's twin defense lines:

  1. white/black-list filtering (key-level).
  2. shlex-based single-quote escaping for every value.
  3. shlex.split self-check: parse the generated text back into tokens and
     verify each user value lands in exactly one token.

Instance-level fields (${biz_date}, ${TRACE_ID}, ${INSTANCE_ID}, ...) are
shell variable references that spark_submit.sh expands at run time. They are
NOT user controlled — they come from DS task body env vars set by the platform.
"""

from __future__ import annotations

import shlex

import pytest

from pyspark_driver_pkg.command_generator import (
    BLACKLIST_KEYS,
    BLACKLIST_PREFIXES,
    WHITELIST_PREFIXES,
    ConfRejected,
    GeneratedCommand,
    TaskSpec,
    generate,
)


# ---- helpers ----


def _spec(**overrides) -> TaskSpec:
    base = dict(
        task_id="task-1",
        name="demo",
        queue="root.a_dc_qysjrh",
        principal="a_xy_mn",
        keytab_path="/etc/keytabs/a_xy_mn.keytab",
        driver_path="hdfs:///apps/platform/pyspark_driver.py",
        snapshot_hdfs_path="hdfs:///dwh/platform/snapshots/dev/task-1/v17/sql.sql",
        sql_basename="sql.sql",
        spark_conf={"spark.sql.shuffle.partitions": "400"},
        timezone="Asia/Shanghai",
        catalog_path="hdfs:///apps/platform/contracts/runtime_variables.yaml",
        version_id="v17",
    )
    base.update(overrides)
    return TaskSpec(**base)


def _tokens(cmd: GeneratedCommand) -> list[str]:
    return shlex.split(cmd.text, posix=True)


# ---- happy path -----------------------------------------------------------


def test_generate_returns_command_with_required_args():
    cmd = generate(_spec())
    tokens = _tokens(cmd)

    # spark-submit + structural args
    assert tokens[0] == "spark-submit"
    assert "--master" in tokens and tokens[tokens.index("--master") + 1] == "yarn"
    assert "--deploy-mode" in tokens and tokens[tokens.index("--deploy-mode") + 1] == "cluster"
    assert "--queue" in tokens and tokens[tokens.index("--queue") + 1] == "root.a_dc_qysjrh"
    assert "--principal" in tokens and tokens[tokens.index("--principal") + 1] == "a_xy_mn"
    assert (
        "--keytab" in tokens
        and tokens[tokens.index("--keytab") + 1] == "/etc/keytabs/a_xy_mn.keytab"
    )
    assert "--name" in tokens and tokens[tokens.index("--name") + 1] == "demo"
    assert (
        "--files" in tokens
        and tokens[tokens.index("--files") + 1]
        == "hdfs:///dwh/platform/snapshots/dev/task-1/v17/sql.sql"
    )

    # driver entry + driver args
    assert "hdfs:///apps/platform/pyspark_driver.py" in tokens
    assert "--sql-file" in tokens and tokens[tokens.index("--sql-file") + 1] == "sql.sql"
    assert "--biz-date" in tokens
    assert "--timezone" in tokens and tokens[tokens.index("--timezone") + 1] == "Asia/Shanghai"


def test_generate_emits_user_conf():
    cmd = generate(_spec(spark_conf={"spark.sql.shuffle.partitions": "400"}))
    tokens = _tokens(cmd)
    assert "spark.sql.shuffle.partitions=400" in tokens


def test_generate_emits_default_python_conf():
    """PYSPARK_PYTHON must be wired for both driver and executors."""
    cmd = generate(_spec())
    text = cmd.text
    assert "spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3" in text
    assert "spark.executorEnv.PYSPARK_PYTHON=python3" in text


# ---- biz_date / trace_id / instance_id placeholders -----------------------


def test_biz_date_placeholder_is_left_for_shell_to_substitute():
    """${biz_date} is consumed by spark_submit.sh, not by the generator."""
    cmd = generate(_spec())
    # The placeholder must be present in *single-quoted* form so that the user
    # cannot inject anything else with that name AND so that ``eval`` in the
    # shell still sees ``${biz_date}`` literally for the script's sed-like
    # parameter substitution to work.
    # Our shell does ``${SPARK_CMD//\${biz_date}/$BIZ_DATE}`` BEFORE eval, so
    # the placeholder must survive being a single-quoted token.
    assert "${biz_date}" in cmd.text
    tokens = _tokens(cmd)
    assert "${biz_date}" in tokens


def test_runtime_correlation_fields_use_shell_env():
    """trace_id / instance_id / task_id / version_id come from shell env."""
    cmd = generate(_spec())
    tokens = _tokens(cmd)
    # Values are shell variable expansions — after shlex.split with posix=True
    # they appear as the literal "$TRACE_ID" etc since we emit them
    # double-quoted (so shell expands at eval time but our self-check sees them).
    idx_trace = tokens.index("--trace-id")
    assert tokens[idx_trace + 1] == "${TRACE_ID}"
    idx_instance = tokens.index("--instance-id")
    assert tokens[idx_instance + 1] == "${INSTANCE_ID}"
    idx_task = tokens.index("--task-id")
    assert tokens[idx_task + 1] == "${TASK_ID}"


# ---- whitelist / blacklist ------------------------------------------------


def test_blacklist_extra_java_options_rejected():
    with pytest.raises(ConfRejected) as exc:
        generate(_spec(spark_conf={"spark.driver.extraJavaOptions": "-Dfoo=bar"}))
    assert "spark.driver.extraJavaOptions" in exc.value.rejected


def test_blacklist_executor_class_path_rejected():
    with pytest.raises(ConfRejected) as exc:
        generate(_spec(spark_conf={"spark.executor.extraClassPath": "/tmp/evil.jar"}))
    assert "spark.executor.extraClassPath" in exc.value.rejected


def test_blacklist_yarn_dist_files_rejected():
    """spark.yarn.dist.* is a wildcard ban."""
    with pytest.raises(ConfRejected):
        generate(_spec(spark_conf={"spark.yarn.dist.archives": "/tmp/x.tgz"}))
    with pytest.raises(ConfRejected):
        generate(_spec(spark_conf={"spark.yarn.dist.jars": "/tmp/x.jar"}))


def test_blacklist_kerberos_rejected():
    with pytest.raises(ConfRejected):
        generate(_spec(spark_conf={"spark.kerberos.principal": "evil"}))
    with pytest.raises(ConfRejected):
        generate(_spec(spark_conf={"spark.yarn.principal": "evil"}))
    with pytest.raises(ConfRejected):
        generate(_spec(spark_conf={"spark.yarn.keytab": "/tmp/x.keytab"}))


def test_blacklist_security_credentials_rejected():
    with pytest.raises(ConfRejected):
        generate(_spec(spark_conf={"spark.security.credentials.hive.enabled": "false"}))


def test_unknown_key_outside_whitelist_rejected():
    with pytest.raises(ConfRejected) as exc:
        generate(_spec(spark_conf={"random.unknown.key": "1"}))
    assert "random.unknown.key" in exc.value.rejected


def test_whitelist_keys_pass():
    cmd = generate(
        _spec(
            spark_conf={
                "spark.sql.shuffle.partitions": "400",
                "spark.shuffle.compress": "true",
                "spark.dynamicAllocation.maxExecutors": "30",
                "spark.executor.memory": "6G",
                "spark.executor.memoryOverhead": "2G",
                "spark.executor.cores": "2",
                "spark.driver.memory": "3G",
                "spark.driver.memoryOverhead": "1G",
                "spark.driver.cores": "1",
                "spark.yarn.appMasterEnv.MY_VAR": "x",
                "spark.executorEnv.MY_VAR": "x",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
            }
        )
    )
    text = cmd.text
    for k, v in [
        ("spark.sql.shuffle.partitions", "400"),
        ("spark.executor.memory", "6G"),
        ("spark.driver.cores", "1"),
    ]:
        assert f"{k}={v}" in text


def test_blacklist_overrides_whitelist():
    """spark.driver.extraJavaOptions matches no whitelist anyway, but the test
    documents that even if a future relaxation lists 'spark.driver.*' on the
    whitelist, the explicit blacklist still wins."""
    with pytest.raises(ConfRejected):
        generate(_spec(spark_conf={"spark.driver.extraJavaOptions": "-Dfoo=bar"}))


def test_constants_expose_lists():
    """Make the lists discoverable for runbooks / audits."""
    assert "spark.driver.extraJavaOptions" in BLACKLIST_KEYS
    assert "spark.kerberos." in BLACKLIST_PREFIXES
    assert "spark.sql." in WHITELIST_PREFIXES


# ---- escaping & shlex self-check ------------------------------------------


def test_user_value_with_single_quote_is_escaped():
    cmd = generate(_spec(name="my'app"))
    tokens = _tokens(cmd)
    name_idx = tokens.index("--name")
    assert tokens[name_idx + 1] == "my'app"


def test_user_value_with_shell_metachars_is_inert():
    cmd = generate(
        _spec(
            spark_conf={"spark.sql.warehouseDir": "s3://b;ls /;`whoami`;$(whoami)"},
        )
    )
    tokens = _tokens(cmd)
    # The conf value must round-trip as a single token, not split by ; or expanded.
    found = [t for t in tokens if t.startswith("spark.sql.warehouseDir=")]
    assert found == ["spark.sql.warehouseDir=s3://b;ls /;`whoami`;$(whoami)"]


def test_user_value_with_newline_is_inert():
    cmd = generate(_spec(spark_conf={"spark.sql.adaptive.enabled": "true\nrm -rf /"}))
    tokens = _tokens(cmd)
    found = [t for t in tokens if t.startswith("spark.sql.adaptive.enabled=")]
    assert found == ["spark.sql.adaptive.enabled=true\nrm -rf /"]


def test_command_passes_self_check():
    """generate() runs shlex.split internally and stashes audit_tokens."""
    cmd = generate(_spec(name="fancy app", spark_conf={"spark.sql.x": "hello'world"}))
    assert isinstance(cmd, GeneratedCommand)
    assert cmd.audit_tokens
    # audit_tokens equals the public shlex.split of cmd.text
    assert list(cmd.audit_tokens) == _tokens(cmd)


# ---- structured input validation ------------------------------------------


def test_empty_principal_rejected():
    with pytest.raises(ValueError):
        generate(_spec(principal=""))


def test_keytab_path_must_be_absolute():
    with pytest.raises(ValueError):
        generate(_spec(keytab_path="relative.keytab"))


def test_snapshot_must_be_hdfs_uri():
    with pytest.raises(ValueError):
        generate(_spec(snapshot_hdfs_path="/local/snap.sql"))


def test_sql_basename_no_slashes():
    with pytest.raises(ValueError):
        generate(_spec(sql_basename="dir/x.sql"))
