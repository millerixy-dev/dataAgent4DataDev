"""Integration tests for spark_submit.sh.

We exercise the script as a subprocess and inject mocked binaries via PATH
so the test never actually calls kinit/spark-submit/curl. Each mock records
its invocation in a temp file so the test can assert what the script did.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SHELL = REPO / "spark_submit.sh"


# ---- helpers ----


def _make_mock(bin_dir: Path, name: str, body: str) -> None:
    """Drop an executable shim named ``name`` in ``bin_dir`` containing ``body``."""
    p = bin_dir / name
    p.write_text(f"#!/usr/bin/env bash\n{body}\n", encoding="utf-8")
    p.chmod(p.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _run(
    *,
    env: dict[str, str],
    bin_dir: Path,
    extra_path: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the shell with ``bin_dir`` taking precedence on PATH."""
    base_path = extra_path or os.environ.get("PATH", "")
    full_env = {
        **os.environ,
        **env,
        "PATH": f"{bin_dir}:{base_path}",
    }
    return subprocess.run(
        ["bash", str(SHELL)],
        capture_output=True,
        text=True,
        env=full_env,
        check=False,
    )


def _new_bin(tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    return bin_dir


def _keytab(tmp_path: Path) -> str:
    keytab = tmp_path / "x.keytab"
    keytab.write_bytes(b"fake-keytab-bytes")
    return str(keytab)


def _base_env(tmp_path: Path) -> dict[str, str]:
    """A minimum-valid environment that satisfies the shell preconditions."""
    return {
        "SPARK_CMD": "spark-submit --name demo --conf spark.x=y --files snap.sql driver.py --biz-date '${biz_date}'",
        "BIZ_DATE": "20260613",
        "PRINCIPAL": "a_xy_mn",
        "KEYTAB_PATH": _keytab(tmp_path),
        "INSTANCE_ID": "inst-1",
        "TRACE_ID": "trace-1",
        "VERSION_ID": "v17",
        "TASK_ID": "task-1",
    }


# ---- preconditions ----


def test_missing_spark_cmd_fails(tmp_path):
    bin_dir = _new_bin(tmp_path)
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(bin_dir, "spark-submit", "exit 0")

    env = _base_env(tmp_path)
    env.pop("SPARK_CMD")
    proc = _run(env=env, bin_dir=bin_dir)

    assert proc.returncode != 0
    assert "SPARK_CMD" in proc.stderr or "SPARK_CMD" in proc.stdout


def test_missing_biz_date_fails(tmp_path):
    bin_dir = _new_bin(tmp_path)
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(bin_dir, "spark-submit", "exit 0")

    env = _base_env(tmp_path)
    env.pop("BIZ_DATE")
    proc = _run(env=env, bin_dir=bin_dir)

    assert proc.returncode != 0
    assert "BIZ_DATE" in proc.stderr or "BIZ_DATE" in proc.stdout


def test_missing_keytab_fails(tmp_path):
    bin_dir = _new_bin(tmp_path)
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(bin_dir, "spark-submit", "exit 0")

    env = _base_env(tmp_path)
    env["KEYTAB_PATH"] = str(tmp_path / "no-such-keytab")
    proc = _run(env=env, bin_dir=bin_dir)

    assert proc.returncode != 0
    msg = proc.stderr + proc.stdout
    assert "KEYTAB" in msg or "keytab" in msg


# ---- happy path ----


def test_runs_kinit_and_eval_spark_cmd(tmp_path):
    bin_dir = _new_bin(tmp_path)
    kinit_log = tmp_path / "kinit.log"
    submit_log = tmp_path / "submit.log"
    _make_mock(
        bin_dir,
        "kinit",
        f'echo "kinit $@" >> "{kinit_log}"',
    )
    _make_mock(
        bin_dir,
        "spark-submit",
        f'echo "spark-submit $@" >> "{submit_log}"\n'
        'echo "Submitted application application_1700000000_0042"\n'
        "exit 0",
    )
    _make_mock(bin_dir, "curl", "exit 0")

    env = _base_env(tmp_path)
    proc = _run(env=env, bin_dir=bin_dir)

    assert proc.returncode == 0, proc.stderr
    assert kinit_log.exists()
    kinit_args = kinit_log.read_text(encoding="utf-8").strip()
    assert "-kt" in kinit_args
    assert env["KEYTAB_PATH"] in kinit_args
    assert env["PRINCIPAL"] in kinit_args

    assert submit_log.exists()
    submit_args = submit_log.read_text(encoding="utf-8").strip()
    # ${biz_date} placeholder must be replaced by BIZ_DATE
    assert "20260613" in submit_args
    assert "${biz_date}" not in submit_args


def test_propagates_application_id_via_callback(tmp_path):
    bin_dir = _new_bin(tmp_path)
    curl_log = tmp_path / "curl.log"
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(
        bin_dir,
        "spark-submit",
        'echo "Submitted application application_1700000000_0042"\nexit 0',
    )
    _make_mock(bin_dir, "curl", f'echo "curl $@" >> "{curl_log}"\nexit 0')

    env = _base_env(tmp_path)
    env["PLATFORM_CALLBACK_URL"] = "http://platform.local/api/v1/instance/callback"
    proc = _run(env=env, bin_dir=bin_dir)

    assert proc.returncode == 0, proc.stderr
    assert curl_log.exists()
    curl_args = curl_log.read_text(encoding="utf-8").strip()
    assert "application_1700000000_0042" in curl_args
    assert env["INSTANCE_ID"] in curl_args
    assert "http://platform.local/api/v1/instance/callback" in curl_args


def test_callback_failure_does_not_change_exit_code(tmp_path):
    bin_dir = _new_bin(tmp_path)
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(
        bin_dir,
        "spark-submit",
        'echo "Submitted application application_x_1"\nexit 0',
    )
    _make_mock(bin_dir, "curl", "echo callback-failed >&2\nexit 7")

    env = _base_env(tmp_path)
    env["PLATFORM_CALLBACK_URL"] = "http://platform.local/cb"
    proc = _run(env=env, bin_dir=bin_dir)

    # spark-submit succeeded; callback failure must not flip the script's exit.
    assert proc.returncode == 0, proc.stderr


def test_no_callback_url_skips_curl(tmp_path):
    bin_dir = _new_bin(tmp_path)
    curl_called = tmp_path / "curl-called"
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(
        bin_dir,
        "spark-submit",
        'echo "Submitted application application_x_1"\nexit 0',
    )
    _make_mock(bin_dir, "curl", f'touch "{curl_called}"\nexit 0')

    env = _base_env(tmp_path)
    env.pop("PLATFORM_CALLBACK_URL", None)
    proc = _run(env=env, bin_dir=bin_dir)

    assert proc.returncode == 0
    assert not curl_called.exists()


# ---- exit code propagation ----


def test_propagates_spark_submit_exit_code(tmp_path):
    bin_dir = _new_bin(tmp_path)
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(bin_dir, "spark-submit", 'echo "fake spark-submit failed" >&2\nexit 42')
    _make_mock(bin_dir, "curl", "exit 0")

    env = _base_env(tmp_path)
    env["PLATFORM_CALLBACK_URL"] = "http://platform.local/cb"
    proc = _run(env=env, bin_dir=bin_dir)

    assert proc.returncode == 42


def test_kinit_failure_aborts(tmp_path):
    bin_dir = _new_bin(tmp_path)
    _make_mock(bin_dir, "kinit", "echo kinit-failed >&2\nexit 99")
    submit_log = tmp_path / "submit.log"
    _make_mock(bin_dir, "spark-submit", f'echo "spark-submit $@" >> "{submit_log}"\nexit 0')

    env = _base_env(tmp_path)
    proc = _run(env=env, bin_dir=bin_dir)

    assert proc.returncode != 0
    assert not submit_log.exists()  # spark-submit must NOT run if kinit failed


# ---- structured log header ----


def test_emits_trace_header(tmp_path):
    bin_dir = _new_bin(tmp_path)
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(
        bin_dir,
        "spark-submit",
        'echo "Submitted application application_x_1"\nexit 0',
    )
    _make_mock(bin_dir, "curl", "exit 0")

    env = _base_env(tmp_path)
    proc = _run(env=env, bin_dir=bin_dir)

    out = proc.stdout + proc.stderr
    assert "trace_id=trace-1" in out
    assert "instance_id=inst-1" in out
    assert "version_id=v17" in out


# ---- no biz_date injection ----


def test_eval_does_not_run_arbitrary_code_from_biz_date(tmp_path):
    """BIZ_DATE is replaced via shell parameter expansion, not eval'd a second time.

    If a malicious BIZ_DATE leaked into eval via double-eval, this would create
    /tmp/PWNED. The script must not.
    """
    bin_dir = _new_bin(tmp_path)
    _make_mock(bin_dir, "kinit", "exit 0")
    _make_mock(bin_dir, "spark-submit", 'echo "$@"\nexit 0')
    _make_mock(bin_dir, "curl", "exit 0")

    canary = tmp_path / "PWNED"
    env = _base_env(tmp_path)
    # If the shell were to eval BIZ_DATE itself, $(touch) would fire.
    env["BIZ_DATE"] = "20260613$(touch " + str(canary) + ")"
    proc = _run(env=env, bin_dir=bin_dir)

    # Script can fail or succeed — we only care that the canary did NOT fire.
    assert not canary.exists(), proc.stdout + proc.stderr


# ---- shellcheck (skipped if not installed) ----


def test_shellcheck_passes():
    if shutil.which("shellcheck") is None:
        pytest.skip("shellcheck not installed")
    proc = subprocess.run(
        ["shellcheck", "--severity=warning", str(SHELL)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
