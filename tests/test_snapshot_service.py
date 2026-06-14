"""Snapshot Service tests.

Two backends in scope:
  - LocalFsBackend: MVP-grade, used by unit tests
  - WebHDFSBackend: contract-tested separately with a stub HTTP layer

The service guarantees:
  - exclusive create (O_CREAT|O_EXCL) per path
  - sha256 read-back verification after every write
  - immutable address: (env, task_id, version_id) maps to one byte string
  - meta.json shipped alongside sql.sql with bake-time provenance
  - on failure, the failed path gets a `.failed` suffix and is left intact;
    the (success) path never appears, so publish rollbacks are clean
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pytest

from pyspark_driver_pkg.snapshot import (
    LocalFsBackend,
    SnapshotConflict,
    SnapshotMeta,
    SnapshotRef,
    SnapshotService,
    SnapshotWriteError,
)


# -- helpers ----------------------------------------------------------------


def _service(tmp_path: Path, version_iter: Iterator[str] | None = None):
    """Build a SnapshotService rooted at tmp_path with a deterministic version factory."""
    backend = LocalFsBackend(root=str(tmp_path))
    if version_iter is not None:
        factory = lambda: next(version_iter)  # noqa: E731
    else:
        # Use ULID-style monotonic ids; tests that need predictability override.
        from pyspark_driver_pkg.snapshot import default_version_factory
        factory = default_version_factory
    return backend, SnapshotService(
        backend=backend,
        root_uri=str(tmp_path),
        version_factory=factory,
    )


def _baked_at() -> datetime:
    return datetime.fromisoformat("2026-06-14T12:00:00")


# -- happy path -------------------------------------------------------------


def test_write_emits_sql_and_meta(tmp_path):
    versions = iter(["v17"])
    backend, svc = _service(tmp_path, versions)
    ref = svc.write(
        env="dev",
        task_id="task-1",
        baked_text="SELECT 1",
        baked_by="alice",
        draft_revision_id="draft-abc",
        project_var_versions={"prj.warehouse": 3},
        baked_at=_baked_at(),
    )

    assert isinstance(ref, SnapshotRef)
    assert ref.version_id == "v17"
    assert ref.sha256 == hashlib.sha256(b"SELECT 1").hexdigest()
    assert ref.path == f"{tmp_path}/dev/task-1/v17/sql.sql"

    sql_path = tmp_path / "dev" / "task-1" / "v17" / "sql.sql"
    meta_path = tmp_path / "dev" / "task-1" / "v17" / "meta.json"
    assert sql_path.read_bytes() == b"SELECT 1"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["task_id"] == "task-1"
    assert meta["version_id"] == "v17"
    assert meta["env"] == "dev"
    assert meta["draft_revision_id"] == "draft-abc"
    assert meta["baked_by"] == "alice"
    assert meta["baked_at"] == "2026-06-14T12:00:00"
    assert meta["project_var_versions"] == {"prj.warehouse": 3}
    assert meta["sha256"] == ref.sha256
    assert meta["byte_size"] == 8


def test_read_round_trips_bytes_and_meta(tmp_path):
    versions = iter(["v1", "v2"])
    backend, svc = _service(tmp_path, versions)

    svc.write(
        env="prod",
        task_id="t1",
        baked_text="SELECT 'a'",
        baked_by="alice",
        draft_revision_id="d1",
        project_var_versions={},
        baked_at=_baked_at(),
    )
    svc.write(
        env="prod",
        task_id="t1",
        baked_text="SELECT 'b'",
        baked_by="bob",
        draft_revision_id="d2",
        project_var_versions={"prj.x": 5},
        baked_at=_baked_at(),
    )

    text1, meta1 = svc.read(env="prod", task_id="t1", version_id="v1")
    text2, meta2 = svc.read(env="prod", task_id="t1", version_id="v2")
    assert text1 == "SELECT 'a'"
    assert text2 == "SELECT 'b'"
    assert meta1.draft_revision_id == "d1"
    assert meta2.project_var_versions == {"prj.x": 5}
    assert isinstance(meta1, SnapshotMeta)


def test_unicode_text_round_trip(tmp_path):
    versions = iter(["v1"])
    _, svc = _service(tmp_path, versions)
    text = "INSERT INTO t VALUES ('中文', 'éñ', '🚀')"
    svc.write(
        env="dev",
        task_id="t1",
        baked_text=text,
        baked_by="alice",
        draft_revision_id="d1",
        project_var_versions={},
        baked_at=_baked_at(),
    )
    got, meta = svc.read(env="dev", task_id="t1", version_id="v1")
    assert got == text
    assert meta.byte_size == len(text.encode("utf-8"))


# -- exclusivity / immutability --------------------------------------------


def test_write_to_existing_path_is_rejected(tmp_path):
    """Re-using a version_id is a contract violation, not a no-op."""
    versions = iter(["v1", "v1"])
    _, svc = _service(tmp_path, versions)

    svc.write(
        env="dev", task_id="t1", baked_text="A",
        baked_by="alice", draft_revision_id="d1",
        project_var_versions={}, baked_at=_baked_at(),
    )
    with pytest.raises(SnapshotConflict):
        svc.write(
            env="dev", task_id="t1", baked_text="B",
            baked_by="alice", draft_revision_id="d2",
            project_var_versions={}, baked_at=_baked_at(),
        )

    # Original content untouched.
    text, _ = svc.read(env="dev", task_id="t1", version_id="v1")
    assert text == "A"


def test_default_factory_yields_unique_versions(tmp_path):
    """Two writes back-to-back must land on different version_ids."""
    backend = LocalFsBackend(root=str(tmp_path))
    from pyspark_driver_pkg.snapshot import default_version_factory
    svc = SnapshotService(
        backend=backend, root_uri=str(tmp_path),
        version_factory=default_version_factory,
    )
    seen = set()
    for _ in range(8):
        ref = svc.write(
            env="dev", task_id="t1", baked_text="x",
            baked_by="alice", draft_revision_id="d",
            project_var_versions={}, baked_at=_baked_at(),
        )
        assert ref.version_id not in seen
        seen.add(ref.version_id)


# -- failure: corrupted write -----------------------------------------------


def test_sha_mismatch_marks_failed_path_and_raises(tmp_path, monkeypatch):
    """Force the backend's read-back to return tampered bytes — the service
    must catch the sha mismatch, mark the path .failed, and raise."""
    versions = iter(["v1"])
    backend, svc = _service(tmp_path, versions)

    real_read = backend.read

    def tamper(path):
        if path.endswith("/sql.sql"):
            return b"TAMPERED"
        return real_read(path)

    monkeypatch.setattr(backend, "read", tamper)

    with pytest.raises(SnapshotWriteError):
        svc.write(
            env="dev", task_id="t1", baked_text="SELECT 1",
            baked_by="alice", draft_revision_id="d1",
            project_var_versions={}, baked_at=_baked_at(),
        )

    failed_path = tmp_path / "dev" / "t1" / "v1" / "sql.sql.failed"
    assert failed_path.exists(), "evidence file must remain on disk"
    success_path = tmp_path / "dev" / "t1" / "v1" / "sql.sql"
    assert not success_path.exists(), "no committed snapshot at the success path"


def test_failed_write_does_not_block_future_versions(tmp_path, monkeypatch):
    """A failed write must NOT poison the (env, task) directory."""
    versions = iter(["v1", "v2"])
    backend, svc = _service(tmp_path, versions)
    real_read = backend.read

    # First write fails
    monkeypatch.setattr(
        backend, "read",
        lambda p: b"TAMPERED" if p.endswith("/sql.sql") else real_read(p),
    )
    with pytest.raises(SnapshotWriteError):
        svc.write(
            env="dev", task_id="t1", baked_text="A",
            baked_by="alice", draft_revision_id="d1",
            project_var_versions={}, baked_at=_baked_at(),
        )

    # Second write succeeds
    monkeypatch.setattr(backend, "read", real_read)
    ref = svc.write(
        env="dev", task_id="t1", baked_text="B",
        baked_by="alice", draft_revision_id="d2",
        project_var_versions={}, baked_at=_baked_at(),
    )
    assert ref.version_id == "v2"
    text, _ = svc.read(env="dev", task_id="t1", version_id="v2")
    assert text == "B"


# -- read-side ---------------------------------------------------------------


def test_read_missing_version_raises(tmp_path):
    backend = LocalFsBackend(root=str(tmp_path))
    svc = SnapshotService(
        backend=backend, root_uri=str(tmp_path),
        version_factory=lambda: "irrelevant",
    )
    with pytest.raises(FileNotFoundError):
        svc.read(env="dev", task_id="nope", version_id="v999")


def test_read_corrupted_meta_raises(tmp_path):
    versions = iter(["v1"])
    _, svc = _service(tmp_path, versions)
    svc.write(
        env="dev", task_id="t1", baked_text="A",
        baked_by="alice", draft_revision_id="d1",
        project_var_versions={}, baked_at=_baked_at(),
    )
    meta_path = tmp_path / "dev" / "t1" / "v1" / "meta.json"
    meta_path.write_text("not json", encoding="utf-8")
    with pytest.raises(ValueError):
        svc.read(env="dev", task_id="t1", version_id="v1")


# -- path / uri sanitization ------------------------------------------------


@pytest.mark.parametrize("bad", ["..", "../etc", "a/b", ".", ""])
def test_write_rejects_path_traversal(tmp_path, bad):
    versions = iter(["v1"])
    _, svc = _service(tmp_path, versions)
    with pytest.raises(ValueError):
        svc.write(
            env=bad, task_id="t1", baked_text="A",
            baked_by="alice", draft_revision_id="d1",
            project_var_versions={}, baked_at=_baked_at(),
        )


@pytest.mark.parametrize("bad", ["..", "a/b", "x\x00y"])
def test_write_rejects_bad_task_id(tmp_path, bad):
    versions = iter(["v1"])
    _, svc = _service(tmp_path, versions)
    with pytest.raises(ValueError):
        svc.write(
            env="dev", task_id=bad, baked_text="A",
            baked_by="alice", draft_revision_id="d1",
            project_var_versions={}, baked_at=_baked_at(),
        )


# -- bridge to BakeResult ---------------------------------------------------


def test_write_accepts_bake_result_shape(tmp_path):
    """Resolver.bake() returns BakeResult(text, project_var_versions). The
    service must accept those fields without further glue."""
    from pyspark_driver_pkg.resolver import BakeResult
    versions = iter(["v1"])
    _, svc = _service(tmp_path, versions)
    bake = BakeResult(text="SELECT 1", project_var_versions={"prj.x": 7})

    ref = svc.write(
        env="dev",
        task_id="t1",
        baked_text=bake.text,
        baked_by="alice",
        draft_revision_id="d1",
        project_var_versions=bake.project_var_versions,
        baked_at=_baked_at(),
    )
    _, meta = svc.read(env="dev", task_id="t1", version_id=ref.version_id)
    assert meta.project_var_versions == {"prj.x": 7}
