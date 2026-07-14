"""Tests for ProgressServer and RemoteTaskProgress."""
import threading
import time

import pytest

from depio.progress import ProgressServer, RemoteTaskProgress, TaskProgress, current_progress, _register_progress, _unregister_progress


# ---------------------------------------------------------------------------
# ProgressServer
# ---------------------------------------------------------------------------

def test_server_starts_and_has_address():
    server = ProgressServer()
    host, port = server.address
    assert isinstance(host, str) and host
    assert 1 <= port <= 65535
    server.stop()


def test_server_register_unregister():
    server = ProgressServer()
    prog = TaskProgress()
    server.register("t1", prog)
    server.unregister("t1")
    server.stop()


def test_server_receives_update():
    server = ProgressServer()
    prog = TaskProgress()
    server.register("task-42", prog)

    host, port = server.address
    client = RemoteTaskProgress(host, port, "task-42")
    client.update(total=100, current=10, phase="train", message="ok")

    # Give the server thread time to process the message.
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if prog.total == 100:
            break
        time.sleep(0.01)

    assert prog.total == 100
    assert prog.current == 10
    assert prog.phase == "train"
    assert prog.message == "ok"
    server.stop()


def test_server_receives_advance():
    server = ProgressServer()
    prog = TaskProgress()
    server.register("task-adv", prog)

    host, port = server.address
    client = RemoteTaskProgress(host, port, "task-adv")
    client.advance(3)
    client.advance(2)

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if prog.current >= 5:
            break
        time.sleep(0.01)

    assert prog.current == 5
    server.stop()


def test_server_ignores_unknown_task_id():
    server = ProgressServer()
    host, port = server.address
    client = RemoteTaskProgress(host, port, "nonexistent")
    client.update(current=99)   # should not raise
    time.sleep(0.05)
    server.stop()


# ---------------------------------------------------------------------------
# RemoteTaskProgress local state
# ---------------------------------------------------------------------------

def test_remote_progress_snapshot_local():
    server = ProgressServer()
    host, port = server.address
    server.register("snap", TaskProgress())
    client = RemoteTaskProgress(host, port, "snap")
    client.update(current=3, total=10, message="hi", phase="p1")
    snap = client.snapshot()
    assert snap == {"current": 3, "total": 10, "message": "hi", "phase": "p1"}
    server.stop()


def test_remote_progress_fraction():
    server = ProgressServer()
    host, port = server.address
    server.register("frac", TaskProgress())
    client = RemoteTaskProgress(host, port, "frac")
    assert client.fraction is None
    client.update(total=10, current=5)
    assert client.fraction == pytest.approx(0.5)
    server.stop()


# ---------------------------------------------------------------------------
# current_progress() with RemoteTaskProgress
# ---------------------------------------------------------------------------

def test_current_progress_returns_remote_progress():
    server = ProgressServer()
    host, port = server.address
    server.register("cp", TaskProgress())
    client = RemoteTaskProgress(host, port, "cp")

    _register_progress(client)
    try:
        assert current_progress() is client
    finally:
        _unregister_progress()
    server.stop()
