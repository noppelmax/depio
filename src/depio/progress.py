"""Per-task structured progress, accessible via current_progress()."""

import json
import socket
import threading
from typing import Optional, Tuple


_progress_registry: dict[int, "TaskProgress"] = {}
_progress_lock = threading.Lock()


class TaskProgress:
    """Structured progress state a task can mutate during execution.

    Usage inside a task::

        import depio
        prog = depio.current_progress()
        prog.update(total=n_gen, phase="GA")
        for i in range(n_gen):
            ...
            prog.update(current=i + 1, message=f"best AUC {auc:.4f}")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.current: int = 0
        self.total: Optional[int] = None
        self.message: str = ""
        self.phase: str = ""

    def update(
        self,
        current: Optional[int] = None,
        total: Optional[int] = None,
        message: Optional[str] = None,
        phase: Optional[str] = None,
    ) -> None:
        """Atomically update any subset of fields."""
        with self._lock:
            if current is not None:
                self.current = current
            if total is not None:
                self.total = total
            if message is not None:
                self.message = message
            if phase is not None:
                self.phase = phase

    def advance(self, n: int = 1) -> None:
        """Increment current by n."""
        with self._lock:
            self.current += n

    def snapshot(self) -> dict:
        """Return a consistent copy of all fields (for rendering)."""
        with self._lock:
            return {
                "current": self.current,
                "total":   self.total,
                "message": self.message,
                "phase":   self.phase,
            }

    @property
    def fraction(self) -> Optional[float]:
        """0.0–1.0 if total is set, else None."""
        with self._lock:
            if self.total and self.total > 0:
                return min(1.0, self.current / self.total)
        return None


class RemoteTaskProgress:
    """Client-side progress stub that forwards updates to a ProgressServer over TCP.

    Runs on the SLURM worker.  Maintains a local copy of state for snapshot()/fraction
    so that reads never need a round-trip.
    """

    def __init__(self, host: str, port: int, task_id: str) -> None:
        self._host = host
        self._port = port
        self._task_id = task_id
        self._state_lock = threading.Lock()
        self._sock_lock = threading.Lock()
        self._sock: Optional[socket.socket] = None
        self.current: int = 0
        self.total: Optional[int] = None
        self.message: str = ""
        self.phase: str = ""

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _send(self, payload: dict) -> None:
        payload["task_id"] = self._task_id
        data = (json.dumps(payload) + "\n").encode()
        with self._sock_lock:
            for attempt in range(3):
                try:
                    if self._sock is None:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5.0)
                        sock.connect((self._host, self._port))
                        self._sock = sock
                    self._sock.sendall(data)
                    return
                except OSError:
                    if self._sock is not None:
                        try:
                            self._sock.close()
                        except OSError:
                            pass
                        self._sock = None

    # ------------------------------------------------------------------
    # Public API  (mirrors TaskProgress)
    # ------------------------------------------------------------------

    def update(
        self,
        current: Optional[int] = None,
        total: Optional[int] = None,
        message: Optional[str] = None,
        phase: Optional[str] = None,
    ) -> None:
        payload: dict = {"action": "update"}
        with self._state_lock:
            if current is not None:
                self.current = current
                payload["current"] = current
            if total is not None:
                self.total = total
                payload["total"] = total
            if message is not None:
                self.message = message
                payload["message"] = message
            if phase is not None:
                self.phase = phase
                payload["phase"] = phase
        if len(payload) > 1:
            self._send(payload)

    def advance(self, n: int = 1) -> None:
        with self._state_lock:
            self.current += n
            current = self.current
        self._send({"action": "update", "current": current})

    def snapshot(self) -> dict:
        with self._state_lock:
            return {
                "current": self.current,
                "total":   self.total,
                "message": self.message,
                "phase":   self.phase,
            }

    @property
    def fraction(self) -> Optional[float]:
        with self._state_lock:
            if self.total and self.total > 0:
                return min(1.0, self.current / self.total)
        return None


class ProgressServer:
    """TCP server that receives progress updates from remote SLURM workers.

    Start once per executor; register each task before submitting it.
    The server runs on a background daemon thread and updates the
    TaskProgress objects that the TUI already reads from task.progress.

    Usage::

        server = ProgressServer()
        host, port = server.address
        server.register(task_id, task.progress)
        # ... submit job with _progress_addr = (host, port) ...
        server.unregister(task_id)   # optional; called automatically on stop()
    """

    def __init__(self) -> None:
        self._registry: dict[str, TaskProgress] = {}
        self._registry_lock = threading.Lock()

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind(("", 0))
        self._server_sock.listen(128)
        _, port = self._server_sock.getsockname()
        self._address: Tuple[str, int] = (socket.getfqdn(), port)

        self._thread = threading.Thread(target=self._serve, daemon=True, name="depio-progress-server")
        self._thread.start()

    @property
    def address(self) -> Tuple[str, int]:
        """(host, port) that workers should connect to."""
        return self._address

    def register(self, task_id: str, progress: TaskProgress) -> None:
        with self._registry_lock:
            self._registry[task_id] = progress

    def unregister(self, task_id: str) -> None:
        with self._registry_lock:
            self._registry.pop(task_id, None)

    def stop(self) -> None:
        try:
            self._server_sock.close()
        except OSError:
            pass

    # ------------------------------------------------------------------
    # Internal server loop
    # ------------------------------------------------------------------

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self._server_sock.accept()
            except OSError:
                break
            threading.Thread(
                target=self._handle_connection,
                args=(conn,),
                daemon=True,
                name="depio-progress-conn",
            ).start()

    def _handle_connection(self, conn: socket.socket) -> None:
        buf = ""
        with conn:
            while True:
                try:
                    chunk = conn.recv(4096).decode(errors="replace")
                except OSError:
                    break
                if not chunk:
                    break
                buf += chunk
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if line:
                        self._dispatch(line)

    def _dispatch(self, line: str) -> None:
        try:
            msg = json.loads(line)
            task_id: str = msg["task_id"]
        except (json.JSONDecodeError, KeyError):
            return
        with self._registry_lock:
            prog = self._registry.get(task_id)
        if prog is None:
            return
        if msg.get("action") == "update":
            prog.update(
                current=msg.get("current"),
                total=msg.get("total"),
                message=msg.get("message"),
                phase=msg.get("phase"),
            )


def _register_progress(prog: "TaskProgress") -> None:
    ident = threading.current_thread().ident
    with _progress_lock:
        _progress_registry[ident] = prog


def _unregister_progress() -> None:
    ident = threading.current_thread().ident
    with _progress_lock:
        _progress_registry.pop(ident, None)


def current_progress() -> Optional["TaskProgress"]:
    """Return the TaskProgress bound to the calling thread, or None."""
    ident = threading.current_thread().ident
    with _progress_lock:
        return _progress_registry.get(ident)


__all__ = ["TaskProgress", "RemoteTaskProgress", "ProgressServer", "current_progress"]
