"""WebHDFS backend contract tests.

We don't talk to a real cluster here — those run in group 17 against staging.
Instead we mount a stub HTTP transport into httpx so we can assert the
*shape* of the requests the backend issues:

  - CREATE  uses ?op=CREATE&overwrite=false to enforce O_CREAT|O_EXCL.
    On real WebHDFS that returns 307 -> data node URL. The backend must
    follow the redirect and PUT the body there.
  - OPEN    uses ?op=OPEN  GET, body is the file content.
  - GETFILESTATUS  ?op=GETFILESTATUS used by exists().
  - RENAME  ?op=RENAME&destination=...

Auth: SPNEGO Kerberos. We cannot test the real handshake here, but we can
assert the backend wires `auth=` and surfaces 401s.
"""

from __future__ import annotations

import httpx
import pytest

from pyspark_driver_pkg.snapshot import WebHDFSBackend


def _stub(handler):
    """Build an httpx.Client backed by an in-process MockTransport."""
    return httpx.Client(transport=httpx.MockTransport(handler))


# -- write_exclusive --------------------------------------------------------


def test_write_exclusive_two_step_create_redirect_then_put():
    """WebHDFS CREATE returns 307; backend must follow with a PUT to the data node."""
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, dict(request.url.params)))
        if request.method == "PUT" and request.url.path.startswith("/webhdfs/v1"):
            # NameNode step.
            return httpx.Response(
                307,
                headers={
                    "Location": "http://datanode.example/webhdfs/v1/snap/x?op=CREATE&namenoderpcaddress=nn:8020",
                },
            )
        if request.method == "PUT" and request.url.host == "datanode.example":
            assert request.content == b"hello"
            return httpx.Response(201)
        raise AssertionError(f"unexpected request {request.method} {request.url}")

    backend = WebHDFSBackend(
        base_url="http://nn.example:50070",
        client=_stub(handler),
    )
    backend.write_exclusive("/snap/x", b"hello")

    methods_paths = [(m, p, params) for m, p, params in seen]
    nn_step = methods_paths[0]
    assert nn_step[0] == "PUT"
    assert nn_step[1] == "/webhdfs/v1/snap/x"
    assert nn_step[2].get("op") == "CREATE"
    assert nn_step[2].get("overwrite") == "false"


def test_write_exclusive_conflict_on_already_exists():
    """WebHDFS returns 403 with FileAlreadyExistsException JSON when overwrite=false collides."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={
                "RemoteException": {
                    "exception": "FileAlreadyExistsException",
                    "message": "/snap/x already exists",
                }
            },
        )

    backend = WebHDFSBackend(
        base_url="http://nn.example:50070",
        client=_stub(handler),
    )
    from pyspark_driver_pkg.snapshot import SnapshotConflict
    with pytest.raises(SnapshotConflict):
        backend.write_exclusive("/snap/x", b"hello")


# -- read -------------------------------------------------------------------


def test_read_uses_open_op():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "datanode.example":
            return httpx.Response(200, content=b"file-bytes")
        if request.method == "GET" and request.url.path == "/webhdfs/v1/snap/x":
            assert request.url.params.get("op") == "OPEN"
            return httpx.Response(
                307,
                headers={
                    "Location": "http://datanode.example/webhdfs/v1/snap/x?op=OPEN",
                },
            )
        raise AssertionError(f"unexpected {request.method} {request.url}")

    backend = WebHDFSBackend(
        base_url="http://nn.example:50070",
        client=_stub(handler),
    )
    assert backend.read("/snap/x") == b"file-bytes"


def test_read_missing_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            json={
                "RemoteException": {
                    "exception": "FileNotFoundException",
                    "message": "/snap/x not found",
                }
            },
        )

    backend = WebHDFSBackend(
        base_url="http://nn.example:50070",
        client=_stub(handler),
    )
    with pytest.raises(FileNotFoundError):
        backend.read("/snap/x")


# -- exists -----------------------------------------------------------------


def test_exists_true_for_getfilestatus_200():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get("op") == "GETFILESTATUS"
        return httpx.Response(200, json={"FileStatus": {"length": 7}})

    backend = WebHDFSBackend(
        base_url="http://nn.example:50070",
        client=_stub(handler),
    )
    assert backend.exists("/snap/x") is True


def test_exists_false_for_404():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            json={"RemoteException": {"exception": "FileNotFoundException"}},
        )

    backend = WebHDFSBackend(
        base_url="http://nn.example:50070",
        client=_stub(handler),
    )
    assert backend.exists("/snap/x") is False


# -- rename -----------------------------------------------------------------


def test_rename_uses_rename_op_with_destination():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["params"] = dict(request.url.params)
        seen["method"] = request.method
        seen["path"] = request.url.path
        return httpx.Response(200, json={"boolean": True})

    backend = WebHDFSBackend(
        base_url="http://nn.example:50070",
        client=_stub(handler),
    )
    backend.rename("/snap/x", "/snap/x.failed")
    assert seen["method"] == "PUT"
    assert seen["path"] == "/webhdfs/v1/snap/x"
    assert seen["params"]["op"] == "RENAME"
    assert seen["params"]["destination"] == "/snap/x.failed"


# -- auth wiring (smoke) ----------------------------------------------------


def test_propagates_401_as_runtime_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, content=b"GSSAPI auth required")

    backend = WebHDFSBackend(
        base_url="http://nn.example:50070",
        client=_stub(handler),
    )
    with pytest.raises(RuntimeError, match=r"401|auth"):
        backend.read("/snap/x")
