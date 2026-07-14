# from https://gitlab.com/yquemener/stdout-redirects with some adaptions.
# copied from https://stackoverflow.com/a/43667367/1193986
#
# (c) umichscoots 2017
# License unsepcified. Assumed to be CC-by-sa as is StackOverflow's policy
#
# The class LocalProxy is taken from the werkzeug project
# https://raw.githubusercontent.com/pallets/werkzeug/ef545f0d0bf28cbad02066b4cb7471bea50a93ee/src/werkzeug/local.py
# It is licensed under the BSD-3-Clause License
#
# I guess that means the result is CC-by-SA


import re
import threading
import sys
from io import StringIO
import copy

from typing import Any
from typing import Optional
from typing import Union

# Save all of the objects for use later.
orig___stdout__ = sys.__stdout__
orig___stderr__ = sys.__stderr__
orig_stdout = sys.stdout
orig_stderr = sys.stderr
thread_proxies = {}
_thread_proxies_lock = threading.Lock()


class LocalProxy:
    """Acts as a proxy for a werkzeug local.  Forwards all operations to
    a proxied object.  The only operations not supported for forwarding
    are right handed operands and any kind of assignment.
    Example usage::
        from werkzeug.local import Local
        l = Local()
        # these are proxies
        request = l('request')
        user = l('user')
        from werkzeug.local import LocalStack
        _response_local = LocalStack()
        # this is a proxy
        response = _response_local()
    Whenever something is bound to l.user / l.request the proxy objects
    will forward all operations.  If no object is bound a :exc:`RuntimeError`
    will be raised.
    To create proxies to :class:`Local` or :class:`LocalStack` objects,
    call the object as shown above.  If you want to have a proxy to an
    object looked up by a function, you can (as of Werkzeug 0.6.1) pass
    a function to the :class:`LocalProxy` constructor::
        session = LocalProxy(lambda: get_current_request().session)
    .. versionchanged:: 0.6.1
       The class can be instantiated with a callable as well now.
    """

    __slots__ = ("__local", "__dict__", "__name__", "__wrapped__")

    def __init__(
            self, local: Union[Any, "LocalProxy"], name: Optional[str] = None,
    ) -> None:
        object.__setattr__(self, "_LocalProxy__local", local)
        object.__setattr__(self, "__name__", name)
        if callable(local) and not hasattr(local, "__release_local__"):
            # "local" is a callable that is not an instance of Local or
            # LocalManager: mark it as a wrapped function.
            object.__setattr__(self, "__wrapped__", local)

    def _get_current_object(self, ) -> object:
        """Return the current object.  This is useful if you want the real
        object behind the proxy at a time for performance reasons or because
        you want to pass the object into a different context.
        """
        if not hasattr(self.__local, "__release_local__"):
            return self.__local()
        try:
            return getattr(self.__local, self.__name__)
        except AttributeError:
            raise RuntimeError(f"no object bound to {self.__name__}")

    @property
    def __dict__(self):
        try:
            return self._get_current_object().__dict__
        except RuntimeError:
            raise AttributeError("__dict__")

    def __repr__(self) -> str:
        try:
            obj = self._get_current_object()
        except RuntimeError:
            return f"<{type(self).__name__} unbound>"
        return repr(obj)

    def __bool__(self) -> bool:
        try:
            return bool(self._get_current_object())
        except RuntimeError:
            return False

    def __dir__(self):
        try:
            return dir(self._get_current_object())
        except RuntimeError:
            return []

    def __getattr__(self, name: str) -> Any:
        if name == "__members__":
            return dir(self._get_current_object())
        return getattr(self._get_current_object(), name)

    def __setitem__(self, key: Any, value: Any) -> None:
        self._get_current_object()[key] = value  # type: ignore

    def __delitem__(self, key):
        del self._get_current_object()[key]

    __setattr__ = lambda x, n, v: setattr(x._get_current_object(), n, v)  # type: ignore
    __delattr__ = lambda x, n: delattr(x._get_current_object(), n)  # type: ignore
    __str__ = lambda x: str(x._get_current_object())  # type: ignore
    __lt__ = lambda x, o: x._get_current_object() < o
    __le__ = lambda x, o: x._get_current_object() <= o
    __eq__ = lambda x, o: x._get_current_object() == o  # type: ignore
    __ne__ = lambda x, o: x._get_current_object() != o  # type: ignore
    __gt__ = lambda x, o: x._get_current_object() > o
    __ge__ = lambda x, o: x._get_current_object() >= o
    __hash__ = lambda x: hash(x._get_current_object())  # type: ignore
    __call__ = lambda x, *a, **kw: x._get_current_object()(*a, **kw)
    __len__ = lambda x: len(x._get_current_object())
    __getitem__ = lambda x, i: x._get_current_object()[i]
    __iter__ = lambda x: iter(x._get_current_object())
    __contains__ = lambda x, i: i in x._get_current_object()
    __add__ = lambda x, o: x._get_current_object() + o
    __sub__ = lambda x, o: x._get_current_object() - o
    __mul__ = lambda x, o: x._get_current_object() * o
    __floordiv__ = lambda x, o: x._get_current_object() // o
    __mod__ = lambda x, o: x._get_current_object() % o
    __divmod__ = lambda x, o: x._get_current_object().__divmod__(o)
    __pow__ = lambda x, o: x._get_current_object() ** o
    __lshift__ = lambda x, o: x._get_current_object() << o
    __rshift__ = lambda x, o: x._get_current_object() >> o
    __and__ = lambda x, o: x._get_current_object() & o
    __xor__ = lambda x, o: x._get_current_object() ^ o
    __or__ = lambda x, o: x._get_current_object() | o
    __div__ = lambda x, o: x._get_current_object().__div__(o)
    __truediv__ = lambda x, o: x._get_current_object().__truediv__(o)
    __neg__ = lambda x: -(x._get_current_object())
    __pos__ = lambda x: +(x._get_current_object())
    __abs__ = lambda x: abs(x._get_current_object())
    __invert__ = lambda x: ~(x._get_current_object())
    __complex__ = lambda x: complex(x._get_current_object())
    __int__ = lambda x: int(x._get_current_object())
    __float__ = lambda x: float(x._get_current_object())
    __oct__ = lambda x: oct(x._get_current_object())
    __hex__ = lambda x: hex(x._get_current_object())
    __index__ = lambda x: x._get_current_object().__index__()
    __coerce__ = lambda x, o: x._get_current_object().__coerce__(x, o)
    __enter__ = lambda x: x._get_current_object().__enter__()
    __exit__ = lambda x, *a, **kw: x._get_current_object().__exit__(*a, **kw)
    __radd__ = lambda x, o: o + x._get_current_object()
    __rsub__ = lambda x, o: o - x._get_current_object()
    __rmul__ = lambda x, o: o * x._get_current_object()
    __rdiv__ = lambda x, o: o / x._get_current_object()
    __rtruediv__ = __rdiv__
    __rfloordiv__ = lambda x, o: o // x._get_current_object()
    __rmod__ = lambda x, o: o % x._get_current_object()
    __rdivmod__ = lambda x, o: x._get_current_object().__rdivmod__(o)
    __copy__ = lambda x: copy.copy(x._get_current_object())
    __deepcopy__ = lambda x, memo: copy.deepcopy(x._get_current_object(), memo)


_ANSI_CSI_RE = re.compile(r"\x1b\[([0-9;]*)([A-Za-z])")


class TaskOutputBuffer:
    """Write buffer that emulates basic terminal control sequences.

    Handles ``\\r`` (carriage return), ``\\n`` (newline), and common ANSI
    CSI escape sequences so that output from tqdm, Rich progress bars,
    and similar libraries is collapsed correctly instead of ballooning
    the buffer.

    Supported ANSI sequences:

    * ``\\x1b[<n>A`` — cursor up *n* lines (default 1)
    * ``\\x1b[K`` / ``\\x1b[0K`` — erase to end of line
    * ``\\x1b[2K`` — erase entire line
    * All other CSI sequences are silently stripped.

    The buffer caps retained lines to *maxlines* (default 5 000).
    """

    def __init__(self, maxlines: int = 5_000):
        self._lines: list[str] = [""]
        self._cursor: int = 0  # index into self._lines for current line
        self._maxlines = maxlines
        self._lock = threading.Lock()
        self._truncated: int = 0  # how many lines were dropped

    # -- file-like interface (write / flush / getvalue) -----------------------

    def write(self, text: str) -> int:
        if not text:
            return 0
        with self._lock:
            self._write_locked(text)
        return len(text)

    def _write_locked(self, text: str) -> None:
        # Strip ANSI CSI sequences, but handle cursor-up and erase-line
        # by processing them as control operations.
        pos = 0
        for m in _ANSI_CSI_RE.finditer(text):
            # Write any text before this escape
            chunk = text[pos:m.start()]
            if chunk:
                self._write_plain(chunk)
            pos = m.end()

            param_str, cmd = m.group(1), m.group(2)
            n = int(param_str) if param_str else 1

            if cmd == "A":
                # Cursor up
                self._cursor = max(0, self._cursor - n)
            elif cmd == "K":
                # Erase in line: 0/default = to end, 2 = whole line
                if param_str in ("", "0"):
                    pass  # we don't track column position; no-op is safe
                elif param_str == "2":
                    self._lines[self._cursor] = ""
            # All other CSI sequences are silently dropped.

        # Write remaining text after last escape
        tail = text[pos:]
        if tail:
            self._write_plain(tail)

        # Enforce cap
        excess = len(self._lines) - self._maxlines
        if excess > 0:
            del self._lines[:excess]
            self._cursor -= excess
            if self._cursor < 0:
                self._cursor = 0
            self._truncated += excess

    def _write_plain(self, text: str) -> None:
        """Process plain text (no ANSI escapes) with \\n and \\r handling."""
        for ch in text:
            if ch == "\n":
                # Move cursor to next line; add a new line if at the end.
                self._cursor += 1
                if self._cursor >= len(self._lines):
                    self._lines.append("")
            elif ch == "\r":
                self._lines[self._cursor] = ""
            else:
                self._lines[self._cursor] += ch

    def flush(self) -> None:
        pass  # no-op; kept for file-like interface

    @property
    def encoding(self) -> str:
        return "utf-8"

    def isatty(self) -> bool:
        return False

    # -- reading interface ----------------------------------------------------

    def getvalue(self) -> str:
        """Return the full buffered output as a single string."""
        with self._lock:
            return "\n".join(self._lines)

    def get_tail(self, n: int) -> str:
        """Return the last *n* lines, joined by newlines."""
        with self._lock:
            return "\n".join(self._lines[-n:])

    @property
    def line_count(self) -> int:
        with self._lock:
            return len(self._lines)

    @property
    def truncated_lines(self) -> int:
        return self._truncated


def redirect(stringio: "StringIO | TaskOutputBuffer") -> None:
    """
    Redirects the current thread's stdout/stderr to the given buffer.
    """
    ident = threading.current_thread().ident
    with _thread_proxies_lock:
        thread_proxies[ident] = stringio


def stop_redirect() -> None:
    """
    Stops redirecting the current thread's stdout/stderr.
    """
    ident = threading.current_thread().ident
    with _thread_proxies_lock:
        thread_proxies.pop(ident, None)


def _get_stream(original):
    """
    Returns the inner function for use in the LocalProxy object.

    :param original: The stream to be returned if thread is not proxied.
    :type original: ``file``
    :return: The inner function for use in the LocalProxy object.
    :rtype: ``function``
    """

    def proxy():
        ident = threading.current_thread().ident
        return thread_proxies.get(ident, original)

    return proxy


def enable_proxy():
    """
    Overwrites __stdout__, __stderr__, stdout, and stderr with the proxied
    objects.
    """
    sys.__stdout__ = LocalProxy(_get_stream(sys.__stdout__))
    sys.__stderr__ = LocalProxy(_get_stream(sys.__stderr__))
    sys.stdout = LocalProxy(_get_stream(sys.stdout))
    sys.stderr = LocalProxy(_get_stream(sys.stderr))


def disable_proxy():
    """
    Overwrites __stdout__, __stderr__, stdout, and stderr with the original
    objects.
    """
    sys.__stdout__ = orig___stdout__
    sys.__stderr__ = orig___stderr__
    sys.stdout = orig_stdout
    sys.stderr = orig_stderr


__all__ = [redirect, stop_redirect, enable_proxy, disable_proxy, TaskOutputBuffer]
