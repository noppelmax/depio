from __future__ import annotations

import enum
import io
from os.path import getmtime
from typing import List, Callable, get_origin, Annotated, get_args
import sys
from termcolor import colored

from .stdio_helpers import redirect, stop_redirect
from .exceptions import ProductNotProducedException, TaskRaisedException, UnknownStatusException, ProductNotUpdatedException


class Product():
    pass


class Dependency():
    pass


def python_version_is_greater_equal_3_10():
    return sys.version_info.major > 3 and sys.version_info.minor >= 10


# from https://stackoverflow.com/questions/218616/how-to-get-method-parameter-names
def _get_args_dict(fn, args, kwargs):
    args_names = fn.__code__.co_varnames[:fn.__code__.co_argcount]
    return {**dict(zip(args_names, args)), **kwargs}


def _parse_annotation_for_metaclass(func, metaclass):
    if python_version_is_greater_equal_3_10():
        # For python 3.10 and newer
        # annotations = inspect.get_annotations(func)

        # According to https://docs.python.org/3/howto/annotations.html this is best practice now.
        annotations = getattr(func, '__annotations__', None)
    else:
        # For python 3.9 and older
        if isinstance(func, type):
            annotations = func.__dict__.get('__annotations__', None)
        else:
            annotations = getattr(func, '__annotations__', None)

    results = []

    for name, annotation in annotations.items():
        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            if len(args) <= 1:
                continue

            metadata = args[1:]
            if any(meta is metaclass for meta in metadata):
                results.append(name)

    return results


class TaskStatus(enum.Enum):
    PENDING = enum.auto()
    WAITING = enum.auto()
    RUNNING = enum.auto()
    FINISHED = enum.auto()
    CANCELED = enum.auto()
    FAILED = enum.auto()
    SKIPPED = enum.auto()
    DEPFAILED = enum.auto()
    HOLD = enum.auto()
    UNKNOWN = enum.auto()

TERMINAL_STATES = [
            TaskStatus.FINISHED,
            TaskStatus.FAILED,
            TaskStatus.SKIPPED,
            TaskStatus.DEPFAILED,
            TaskStatus.CANCELED]

SUCCESSFUL_TERMINAL_STATES = [
            TaskStatus.FINISHED,
            TaskStatus.SKIPPED]

FAILED_TERMINAL_STATES = [
            TaskStatus.FAILED,
            TaskStatus.DEPFAILED,
            TaskStatus.CANCELED]





class Task:
    def __init__(self, name: str, func: Callable, dependencies_hard: List[Task] = None, func_args: List = None, func_kwargs: List = None, ):
        self.name = name
        self.queue_id = None
        self.slurmjob = None
        self._status: TaskStatus = TaskStatus.WAITING
        self.func = func
        self.func_args = func_args or []
        self.func_kwargs = func_kwargs or {}
        self.dependencies_hard = dependencies_hard or []

        self.products_args = _parse_annotation_for_metaclass(func, Product)
        self.dependencies_args = _parse_annotation_for_metaclass(func, Dependency)

        args_dict = _get_args_dict(func, self.func_args, self.func_kwargs)

        self.products = [args_dict[argname] for argname in self.products_args]
        self.dependencies = [args_dict[argname] for argname in self.dependencies_args]

    def run(self):
        d = [str(dependency) for dependency in self.dependencies if dependency.exists()]
        if any(not b for b in d):
            self._status = TaskStatus.FAILED
            raise ProductNotProducedException(f"Task {self.name}: Dependency/ies {d} not met.")

        # Store the last-modification timestamp of the already existing products.
        pt_before = [(str(product), getmtime(product)) for product in self.products if product.exists()]

        # Call the actual function
        self._status = TaskStatus.RUNNING

        try:
            self.stdout = redirect()
            self.func(*self.func_args, **self.func_kwargs)
            stop_redirect()
        except Exception as e:
            self._status = TaskStatus.FAILED
            raise TaskRaisedException(e)

        # Check if any product does not exist.
        p = [str(product) for product in self.products if not product.exists()]
        if len(p) > 0:
            self._status = TaskStatus.FAILED
            raise ProductNotProducedException(f"Task {self.name}: Product/s {p} not produced.")

        # Check if any product has not been updated.
        pt_after = [(str(product), getmtime(product)) for product in self.products]
        not_updated_products = []
        for before, after in zip(pt_before, pt_after):
            if before[0] == after[0] and before[1] == after[1]:
                not_updated_products.append(before[0])

        if len(not_updated_products) > 0:
            self._status = TaskStatus.FAILED
            raise ProductNotUpdatedException(f"Task {self.name}: Product/s {not_updated_products} not updated.")

        self._status = TaskStatus.FINISHED

    def __str__(self):
        return f"Task:{self.name}"

    def _update_by_slurmjob(self):
        assert self.slurmjob is not None

        self.slurmjob.watcher.update()
        info = self.slurmjob.get_info()

        self._slurmid = f"{int(self.slurmjob.job_id):6d}-{int(self.slurmjob.task_id):3d}"

        if self.slurmjob.state in ['RUNNING', 'CONFIGURING', 'COMPLETING', 'STAGE_OUT']:
            self._status = TaskStatus.RUNNING
        elif self.slurmjob.state in ['FAILED', 'BOOT_FAIL', 'DEADLINE', 'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'SPECIAL_EXIT', 'STOPPED',
                                     'SUSPENDED', 'TIMEOUT']:
            self._status = TaskStatus.FAILED
        elif self.slurmjob.state in ['READY', 'PENDING', 'REQUEUE_FED', 'REQUEUED']:
            self._status = TaskStatus.PENDING
        elif self.slurmjob.state == 'CANCELED':
            self._status = TaskStatus.CANCELED
        elif self.slurmjob.state in ['COMPLETED']:
            self._status = TaskStatus.FINISHED
        elif self.slurmjob.state in ['RESV_DEL_HOLD', 'REQUEUE_HOLD', 'RESIZING', 'REVOKED', 'SIGNALING']:
            self._status = TaskStatus.HOLD
        elif self.slurmjob.state == 'UNKNOWN':
            self._status = TaskStatus.UNKNOWN
        else:
            raise Exception(f"Unknown slurmjob status! slurmjob.done {self.slurmjob.done}, slurmjob.state {self.slurmjob.state} ")

    @property
    def slurmjob_status(self):
        if not self.slurmjob is None:
            return self.slurmjob.state
        else:
            return ""
    def statuscolor(self,s:TaskStatus=None) -> str:
        if s is None: s = self._status
        status_colors = {
            TaskStatus.WAITING: 'blue',
            TaskStatus.DEPFAILED: 'red',
            TaskStatus.PENDING: 'blue',
            TaskStatus.RUNNING: 'yellow',
            TaskStatus.FINISHED: 'green',
            TaskStatus.SKIPPED: 'green',
            TaskStatus.HOLD: 'white',
            TaskStatus.FAILED: 'red',
            TaskStatus.CANCELED: 'white',
            TaskStatus.UNKNOWN: 'white'
        }
        if s in status_colors:
            return status_colors[s]
        else:
            raise UnknownStatusException("Status {} is unknown.".format(s))

    def statustext(self,s:TaskStatus=None) -> str:
        if s is None: s = self._status
        status_messages = {
            TaskStatus.WAITING: lambda: 'waiting' + (f" for {[d.queue_id for d in self.task_dependencies if not d.is_in_terminal_state]}" if len(
                [d for d in self.task_dependencies if not d.is_in_terminal_state]) > 1 else ""),
            TaskStatus.DEPFAILED: lambda: 'dep. failed' + (
                f" at {[d.queue_id for d in self.task_dependencies if d.is_in_failed_terminal_state]}" if len(
                    [d for d in self.task_dependencies if d.is_in_failed_terminal_state]) > 1 else ""),
            TaskStatus.PENDING: lambda: 'pending',
            TaskStatus.RUNNING: lambda: 'running',
            TaskStatus.FINISHED: lambda: 'finished',
            TaskStatus.SKIPPED: lambda: 'skipped',
            TaskStatus.HOLD: lambda: 'hold',
            TaskStatus.FAILED: lambda: 'failed',
            TaskStatus.CANCELED: lambda: 'cancelled',
            TaskStatus.UNKNOWN: lambda: 'unknown'
        }
        try:
            return status_messages[s]()
        except KeyError:
            raise UnknownStatusException(f"Status {s} is unknown.")

    @property
    def status(self):
        s = self._status
        return s, self.statustext(s), self.statuscolor(s)


    @property
    def is_in_terminal_state(self) -> bool:
        return self.status[0] in TERMINAL_STATES

    @property
    def is_in_successful_terminal_state(self) -> bool:
        return self.status[0] in SUCCESSFUL_TERMINAL_STATES

    @property
    def is_in_failed_terminal_state(self) -> bool:
        return self.status[0] in FAILED_TERMINAL_STATES

    def set_to_depfailed(self) -> None:
        self.status[0] = TaskStatus.DEPFAILED

    @property
    def id(self) -> str:
        return f"{self.queue_id: 4d}"

    @property
    def slurmid(self) -> str:
        if not self.slurmjob is None:
            self._update_by_slurmjob()
            return f"{self._slurmid}"
        else:
            return ""

    def stdout(self) -> str:
        if self.slurmjob:
            return self.slurmjob.stdout()
        else:
            return self.stdout.getvalue()

    def stderr(self) -> str:
        if self.slurmjob:
            return self.slurmjob.stderr()
        else:
            return self.stderr.getvalue()


__all__ = [Task, Product, Dependency]
