"""Snapshot Service — write immutable SQL artefacts and their meta.json.

Path protocol::

    {root_uri}/{env}/{task_id}/{version_id}/sql.sql
    {root_uri}/{env}/{task_id}/{version_id}/meta.json

Guarantees:

* Exclusive create — re-using a (env, task_id, version_id) raises
  :class:`SnapshotConflict`. The service never silently overwrites.
* Read-back sha256 verification — the bytes the backend gives back must hash
  to the same digest computed from the input. Mismatch leaves the path with a
  ``.failed`` suffix as evidence and raises :class:`SnapshotWriteError`.
* Backend is pluggable. :class:`LocalFsBackend` is the MVP/test backend;
  ``WebHDFSBackend`` (group 5 follow-up) implements the same interface against
  WebHDFS REST + SPNEGO Kerberos.

Reference: design.md Decision 2; specs/sql-snapshot.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Mapping, Protocol


SQL_FILENAME = "sql.sql"
META_FILENAME = "meta.json"

_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9._-]+$")


# -- backend protocol -------------------------------------------------------


class SnapshotBackend(Protocol):
    """Filesystem-flavoured interface the service depends on."""

    def write_exclusive(self, path: str, data: bytes) -> None: ...
    def read(self, path: str) -> bytes: ...
    def exists(self, path: str) -> bool: ...
    def rename(self, src: str, dst: str) -> None: ...


class LocalFsBackend:
    """File-system backend rooted at a local directory.

    Used in tests; will live alongside a future WebHDFSBackend that conforms
    to the same surface.
    """

    def __init__(self, *, root: str) -> None:
        self.root = root
        os.makedirs(self.root, exist_ok=True)

    def _abs(self, path: str) -> str:
        return path if os.path.isabs(path) else os.path.join(self.root, path)

    def write_exclusive(self, path: str, data: bytes) -> None:
        absolute = self._abs(path)
        os.makedirs(os.path.dirname(absolute), exist_ok=True)
        # O_CREAT|O_EXCL so two concurrent writers never overwrite each other.
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            fd = os.open(absolute, flags, 0o644)
        except FileExistsError as exc:
            raise SnapshotConflict(f"path already exists: {absolute}") from exc
        try:
            os.write(fd, data)
        finally:
            os.close(fd)

    def read(self, path: str) -> bytes:
        with open(self._abs(path), "rb") as f:
            return f.read()

    def exists(self, path: str) -> bool:
        return os.path.exists(self._abs(path))

    def rename(self, src: str, dst: str) -> None:
        os.rename(self._abs(src), self._abs(dst))


# -- WebHDFS backend --------------------------------------------------------


class WebHDFSBackend:
    """WebHDFS REST + (optional) SPNEGO Kerberos backend.

    Path conventions: ``/snapshots/dev/task-1/v17/sql.sql`` (the leading slash
    is the HDFS root). The backend itself does NOT prepend any prefix; the
    caller-supplied path is appended verbatim under ``/webhdfs/v1``.

    The HTTP client is injectable so tests can mount an in-process transport.
    """

    _PREFIX = "/webhdfs/v1"

    def __init__(self, *, base_url: str, client: Any | None = None, auth: Any | None = None) -> None:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("WebHDFSBackend requires httpx") from exc
        self.base_url = base_url.rstrip("/")
        self._httpx = httpx
        self._client = client or httpx.Client(base_url=self.base_url, auth=auth, timeout=30.0)
        self._auth = auth

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{self._PREFIX}{path}"

    @staticmethod
    def _remote_exception(response) -> str | None:
        try:
            payload = response.json()
        except Exception:
            return None
        ex = payload.get("RemoteException") if isinstance(payload, dict) else None
        return ex.get("exception") if isinstance(ex, dict) else None

    def _check(self, response, *, allow_404: bool = False) -> None:
        status = response.status_code
        if status == 401:
            raise RuntimeError(f"WebHDFS 401: auth required ({response.text[:200]})")
        if status == 404 and not allow_404:
            ex = self._remote_exception(response)
            if ex == "FileNotFoundException":
                raise FileNotFoundError(response.url.path)
            raise RuntimeError(f"WebHDFS 404: {response.text[:200]}")
        if status == 403:
            ex = self._remote_exception(response)
            if ex == "FileAlreadyExistsException":
                raise SnapshotConflict(f"WebHDFS path conflict: {response.url.path}")
            raise RuntimeError(f"WebHDFS 403: {response.text[:200]}")
        if status >= 400:
            raise RuntimeError(f"WebHDFS {status}: {response.text[:200]}")

    def write_exclusive(self, path: str, data: bytes) -> None:
        # Step 1: NameNode CREATE — expect 307 redirect to a DataNode.
        nn_resp = self._client.put(
            self._url(path),
            params={"op": "CREATE", "overwrite": "false"},
            follow_redirects=False,
        )
        if nn_resp.status_code in (307, 308):
            location = nn_resp.headers.get("Location")
            if not location:
                raise RuntimeError("WebHDFS CREATE: missing Location header")
            dn_resp = self._client.put(location, content=data, follow_redirects=False)
            self._check(dn_resp)
            return
        self._check(nn_resp)

    def read(self, path: str) -> bytes:
        nn_resp = self._client.get(
            self._url(path), params={"op": "OPEN"}, follow_redirects=False
        )
        if nn_resp.status_code in (307, 308):
            location = nn_resp.headers.get("Location")
            if not location:
                raise RuntimeError("WebHDFS OPEN: missing Location header")
            dn_resp = self._client.get(location, follow_redirects=False)
            self._check(dn_resp)
            return dn_resp.content
        self._check(nn_resp)
        return nn_resp.content

    def exists(self, path: str) -> bool:
        resp = self._client.get(
            self._url(path), params={"op": "GETFILESTATUS"}, follow_redirects=False
        )
        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            return False
        self._check(resp)
        return False

    def rename(self, src: str, dst: str) -> None:
        resp = self._client.put(
            self._url(src),
            params={"op": "RENAME", "destination": dst},
            follow_redirects=False,
        )
        self._check(resp)


# -- exceptions -------------------------------------------------------------


class SnapshotConflict(Exception):
    """Tried to write to an already-existing snapshot path."""


class SnapshotWriteError(Exception):
    """Write succeeded but read-back verification failed."""


# -- value objects ----------------------------------------------------------


@dataclass(frozen=True)
class SnapshotMeta:
    task_id: str
    version_id: str
    env: str
    draft_revision_id: str
    baked_at: str          # ISO-8601 string for direct JSON round-trip
    baked_by: str
    project_var_versions: Mapping[str, int] = field(default_factory=dict)
    sha256: str = ""
    byte_size: int = 0

    def to_json_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "version_id": self.version_id,
            "env": self.env,
            "draft_revision_id": self.draft_revision_id,
            "baked_at": self.baked_at,
            "baked_by": self.baked_by,
            "project_var_versions": dict(self.project_var_versions),
            "sha256": self.sha256,
            "byte_size": self.byte_size,
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> "SnapshotMeta":
        return cls(
            task_id=data["task_id"],
            version_id=data["version_id"],
            env=data["env"],
            draft_revision_id=data["draft_revision_id"],
            baked_at=data["baked_at"],
            baked_by=data["baked_by"],
            project_var_versions=dict(data.get("project_var_versions", {})),
            sha256=data.get("sha256", ""),
            byte_size=int(data.get("byte_size", 0)),
        )


@dataclass(frozen=True)
class SnapshotRef:
    path: str
    version_id: str
    sha256: str
    meta: SnapshotMeta


# -- version factory --------------------------------------------------------


_ULID_BASE32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_last_ulid_ms: int = 0
_last_ulid_seq: int = 0


def default_version_factory() -> str:
    """Generate a 26-char Crockford-base32 ULID-style id, monotonic per process."""
    global _last_ulid_ms, _last_ulid_seq
    ms = int(time.time() * 1000)
    if ms <= _last_ulid_ms:
        ms = _last_ulid_ms
        _last_ulid_seq += 1
    else:
        _last_ulid_ms = ms
        _last_ulid_seq = 0

    # 48 bits of timestamp + 80 bits derived from os.urandom + sequence to
    # break ties within the same millisecond.
    rand_bytes = os.urandom(10)
    rand_int = int.from_bytes(rand_bytes, "big") ^ _last_ulid_seq
    value = (ms << 80) | rand_int

    chars = []
    for _ in range(26):
        chars.append(_ULID_BASE32[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


# -- service ----------------------------------------------------------------


def _validate_segment(name: str, *, label: str) -> None:
    if not name or not _SAFE_SEGMENT.fullmatch(name):
        raise ValueError(f"{label} must match {_SAFE_SEGMENT.pattern}, got {name!r}")
    if name in (".", "..") or set(name) <= {"."}:
        raise ValueError(f"{label} must not be a relative path component, got {name!r}")


class SnapshotService:
    def __init__(
        self,
        *,
        backend: SnapshotBackend,
        root_uri: str,
        version_factory: Callable[[], str] = default_version_factory,
    ) -> None:
        self.backend = backend
        self.root_uri = root_uri.rstrip("/")
        self.version_factory = version_factory

    def _dir(self, env: str, task_id: str, version_id: str) -> str:
        _validate_segment(env, label="env")
        _validate_segment(task_id, label="task_id")
        _validate_segment(version_id, label="version_id")
        return f"{self.root_uri}/{env}/{task_id}/{version_id}"

    def write(
        self,
        *,
        env: str,
        task_id: str,
        baked_text: str,
        baked_by: str,
        draft_revision_id: str,
        project_var_versions: Mapping[str, int],
        baked_at: datetime,
    ) -> SnapshotRef:
        _validate_segment(env, label="env")
        _validate_segment(task_id, label="task_id")

        version_id = self.version_factory()
        _validate_segment(version_id, label="version_id")

        directory = f"{self.root_uri}/{env}/{task_id}/{version_id}"
        sql_path = f"{directory}/{SQL_FILENAME}"
        meta_path = f"{directory}/{META_FILENAME}"

        data = baked_text.encode("utf-8")
        sha = hashlib.sha256(data).hexdigest()

        # Step 1: write the SQL exclusively.
        self.backend.write_exclusive(sql_path, data)

        # Step 2: read back and verify integrity. Any mismatch means we don't
        # trust the storage, so the success path must NOT remain populated.
        try:
            actual = self.backend.read(sql_path)
            if hashlib.sha256(actual).hexdigest() != sha:
                raise SnapshotWriteError(
                    f"sha256 mismatch on read-back at {sql_path}"
                )
        except SnapshotWriteError:
            self.backend.rename(sql_path, sql_path + ".failed")
            raise
        except Exception as exc:
            self.backend.rename(sql_path, sql_path + ".failed")
            raise SnapshotWriteError(f"read-back failed: {exc}") from exc

        meta = SnapshotMeta(
            task_id=task_id,
            version_id=version_id,
            env=env,
            draft_revision_id=draft_revision_id,
            baked_at=baked_at.isoformat(),
            baked_by=baked_by,
            project_var_versions=dict(project_var_versions),
            sha256=sha,
            byte_size=len(data),
        )
        meta_bytes = (json.dumps(meta.to_json_dict(), ensure_ascii=False, indent=2)
                      + "\n").encode("utf-8")
        # meta is exclusive too — no reason for it to ever pre-exist
        self.backend.write_exclusive(meta_path, meta_bytes)

        return SnapshotRef(path=sql_path, version_id=version_id, sha256=sha, meta=meta)

    def read(
        self, *, env: str, task_id: str, version_id: str
    ) -> tuple[str, SnapshotMeta]:
        directory = self._dir(env, task_id, version_id)
        sql_path = f"{directory}/{SQL_FILENAME}"
        meta_path = f"{directory}/{META_FILENAME}"
        if not self.backend.exists(sql_path):
            raise FileNotFoundError(sql_path)
        text = self.backend.read(sql_path).decode("utf-8")
        meta_bytes = self.backend.read(meta_path)
        try:
            data = json.loads(meta_bytes.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"corrupt meta.json at {meta_path}: {exc}") from exc
        return text, SnapshotMeta.from_json_dict(data)
