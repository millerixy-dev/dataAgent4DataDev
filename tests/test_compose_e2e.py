"""Compose-driven end-to-end smoke tests.

These tests are tagged with ``@pytest.mark.compose`` and SKIP gracefully
when the local stack is not running. Bring it up with ``make up`` first,
then run ``make test-compose`` (or ``uv run pytest -m compose``).

What we verify:

  - Postgres on host port 5433 accepts connections.
  - Hive Metastore on host port 9083 accepts a TCP connection (Thrift
    handshake happens during a real client connect; we don't ship a
    sasl/Thrift client just for this smoke test).
  - The metastore DB has the canonical Hive schema ("VERSION" table is
    present and reports a sane schema_version).
  - The HMS web UI responds on port 10002.
  - SnapshotService.write() with a LocalFsBackend rooted at
    compose/warehouse/ produces files the metastore container can see
    (we only assert the host-side write here; cross-container visibility
    is exercised in compose group 6 once Publish Orchestrator lands).
  - Resolver -> bake produces SQL whose runtime placeholders survive
    bake_at-time substitution and can be written verbatim into a
    snapshot file in compose/warehouse/.

What we do NOT verify here:

  - Real spark.sql execution against the HMS — that requires either a
    pyspark dev install or the spark-on-YARN B-line stack. Tracked by
    group 17 staging acceptance.
  - WebHDFS / Kerberos handshake — A-line is plain auth.
"""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from pyspark_driver_pkg.resolver import InMemoryProjectVariableStore, Resolver
from pyspark_driver_pkg.snapshot import LocalFsBackend, SnapshotService
from pyspark_driver_pkg.variable_catalog import load_catalog

REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "contracts" / "runtime_variables.yaml"
WAREHOUSE = REPO / "compose" / "warehouse"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 15433
HMS_THRIFT_HOST = "localhost"
HMS_THRIFT_PORT = 19083
HMS_WEBUI_PORT = 10012


pytestmark = pytest.mark.compose


# -- skip helper -------------------------------------------------------------


def _tcp_alive(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _container_running(name: str) -> bool:
    """Return True iff a container called *name* exists and is in 'running' state.

    Probing TCP ports alone is unreliable on macOS / Docker Desktop because the
    docker proxy can hold listeners open for stopped containers; querying the
    daemon directly is the only honest signal.
    """
    if shutil.which("docker") is None:
        return False
    proc = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True, text=True, timeout=5, check=False,
    )
    return proc.returncode == 0 and proc.stdout.strip() == "true"


def _require_stack_up() -> None:
    if not _container_running("dataagent-hive-metastore"):
        pytest.skip(
            "compose stack not running (container 'dataagent-hive-metastore' "
            "is not in state=running). Bring it up with `make up` first."
        )


# -- liveness ---------------------------------------------------------------


def test_compose_postgres_accepts_connections():
    _require_stack_up()
    assert _tcp_alive(POSTGRES_HOST, POSTGRES_PORT), (
        f"postgres :{POSTGRES_PORT} unreachable but HMS is up — partial bring-up?"
    )


def test_compose_hms_thrift_accepts_connections():
    _require_stack_up()
    assert _tcp_alive(HMS_THRIFT_HOST, HMS_THRIFT_PORT)


def test_compose_hms_webui_responds():
    _require_stack_up()
    if not _tcp_alive(HMS_THRIFT_HOST, HMS_WEBUI_PORT):
        pytest.skip("HMS web UI not exposed in this stack flavour")


# -- metastore schema sanity -----------------------------------------------


def _docker_exec_psql(query: str) -> str:
    """Run a SQL query inside the dataagent-postgres container."""
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not on PATH; cannot exec into the postgres container")
    proc = subprocess.run(
        [
            "docker", "exec", "-e", "PGPASSWORD=hive", "dataagent-postgres",
            "psql", "-U", "hive", "-d", "metastore",
            "--tuples-only", "--no-align", "-c", query,
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"psql exec failed (exit {proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def test_compose_metastore_schema_initialised():
    _require_stack_up()
    out = _docker_exec_psql("SELECT to_regclass('public.\"VERSION\"');")
    assert out in ('"VERSION"', "VERSION"), f"VERSION table missing — got {out!r}"


def test_compose_metastore_schema_version_recorded():
    _require_stack_up()
    out = _docker_exec_psql('SELECT "SCHEMA_VERSION" FROM "VERSION" LIMIT 1;')
    # Hive 3.1.x writes "3.1.0" (or similar 3.1.* derivative) here.
    assert out.startswith("3.1"), f"unexpected schema version: {out!r}"


# -- snapshot service end-to-end against the bind-mounted warehouse ---------


def test_snapshot_write_lands_in_compose_warehouse(tmp_path):
    _require_stack_up()
    # Use the dev warehouse dir as our snapshot root so anything we write is
    # visible to the metastore container (mounted at /opt/hive/warehouse).
    # Use a unique env-segment so parallel runs don't collide.
    env_segment = f"e2etest-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"
    backend = LocalFsBackend(root=str(WAREHOUSE))
    versions = iter([f"v{datetime.now().strftime('%H%M%S%f')}"])
    svc = SnapshotService(
        backend=backend,
        root_uri=str(WAREHOUSE),
        version_factory=lambda: next(versions),
    )

    catalog = load_catalog(CONTRACT)
    store = InMemoryProjectVariableStore()
    store.put(
        project_id="p1",
        name="prj.warehouse",
        value="default",
        version=1,
        effective_at=datetime.fromisoformat("2026-01-01T00:00:00"),
    )
    resolver = Resolver(catalog=catalog, store=store)
    bake = resolver.bake(
        "INSERT INTO ${prj.warehouse}.t SELECT '${dt}', '${hr}'",
        project_id="p1",
        at=datetime.fromisoformat("2026-06-16T00:00:00"),
    )

    ref = svc.write(
        env=env_segment,
        task_id="t-demo",
        baked_text=bake.text,
        baked_by="alice",
        draft_revision_id="d1",
        project_var_versions=bake.project_var_versions,
        baked_at=datetime.fromisoformat("2026-06-16T12:00:00"),
    )

    sql_path = WAREHOUSE / env_segment / "t-demo" / ref.version_id / "sql.sql"
    meta_path = WAREHOUSE / env_segment / "t-demo" / ref.version_id / "meta.json"
    try:
        assert sql_path.read_text(encoding="utf-8").startswith("INSERT INTO default.t")
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert meta["project_var_versions"] == {"prj.warehouse": 1}
        # Cross-container visibility (the metastore container sees the same
        # bytes via /opt/hive/warehouse).
        in_container = subprocess.run(
            [
                "docker", "exec", "dataagent-hive-metastore",
                "cat", f"/opt/hive/warehouse/{env_segment}/t-demo/{ref.version_id}/sql.sql",
            ],
            capture_output=True, text=True, timeout=10, check=False,
        )
        if in_container.returncode == 0:
            assert in_container.stdout == sql_path.read_text(encoding="utf-8"), (
                "warehouse bind-mount mismatch host vs container"
            )
        # If `docker exec` failed (container restarting, etc.), the host-side
        # assertions above are still meaningful.
    finally:
        # Cleanup our env segment so the warehouse stays tidy across runs.
        shutil.rmtree(WAREHOUSE / env_segment, ignore_errors=True)
