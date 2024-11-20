from __future__ import annotations

import pathlib
import typing
from io import StringIO
from os.path import getmtime
from typing import List, Dict, Callable, get_origin, Annotated, get_args, Union
import sys

from .TaskStatus import TaskStatus, TERMINAL_STATES, SUCCESSFUL_TERMINAL_STATES, FAILED_TERMINAL_STATES
from .stdio_helpers import redirect, stop_redirect
from .exceptions import ProductNotProducedException, TaskRaisedExceptionException, UnknownStatusException, ProductNotUpdatedException, \
    DependencyNotMetException


class Product():
    pass


class Dependency():
    pass


class IgnoredForEq():
    pass


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


def python_version_is_greater_or_equal_to_3_10():
    return sys.version_info.major > 3 and sys.version_info.minor >= 10


# from https://stackoverflow.com/questions/218616/how-to-get-method-parameter-names
def _get_args_dict(fn, args, kwargs) -> Dict[str, typing.Any]:
    args_names = fn.__code__.co_varnames[:fn.__code__.co_argcount]
    return {**dict(zip(args_names, args)), **kwargs}


def _parse_annotation_for_metaclass(func, metaclass) -> List[str]:
    if python_version_is_greater_or_equal_to_3_10():
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

    results: List[str] = []

    for name, annotation in annotations.items():
        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            if len(args) <= 1:
                continue

            metadata = args[1:]
            if any(meta is metaclass for meta in metadata):
                results.append(name)

    return results


def _get_not_updated_products(product_timestamps_after_running: typing.Dict, product_timestamps_before_running: typing.Dict) -> typing.List[str]:
    # Calculate the not updated products
    not_updated_products = []
    for product, before_timestamp in product_timestamps_before_running.items():
        after_timestamp = product_timestamps_after_running.get(product)
        if before_timestamp == after_timestamp:
            not_updated_products.append(product)

    return not_updated_products


class Task:
    def __init__(self, name: str, func: Callable, func_args: List = None, func_kwargs: List = None,
                 produces: List[pathlib.Path] = None, depends_on: List[Union[pathlib.Path, Task]] = None):
        produces = produces if produces is not None else []
        depends_on = depends_on if depends_on is not None else []

        self._status: TaskStatus = TaskStatus.WAITING
        self.name: str = name
        self.queue_id: int = None
        self.slurmjob = None
        self.func = func
        self.func_args: List = func_args or []
        self.func_kwargs: Dict = func_kwargs or {}

        # Parse dependencies and products from the annotations and merge with args
        self.products_args: List[str] = _parse_annotation_for_metaclass(func, Product)
        self.dependencies_args: List[str] = _parse_annotation_for_metaclass(func, Dependency)
        self.ignored_for_eq_args: List[str] = _parse_annotation_for_metaclass(func, IgnoredForEq)
        args_dict: Dict[str,typing.Any] = _get_args_dict(func, self.func_args, self.func_kwargs)
        self.products: List[pathlib.Path] = [args_dict[argname] for argname in self.products_args if argname in args_dict] + produces
        self.dependencies: List[Union[Task, pathlib.Path]] = [args_dict[argname] for argname in self.dependencies_args if
                                                              argname in args_dict] + depends_on
        self.cleaned_args: Dict[str,typing.Any] = { k:v for k,v in args_dict.items() if k not in self.ignored_for_eq_args }
        self.stdout = StringIO()
        self.slurmjob = None
        self._slurmid = None
        self._slurmstate = ""

    def __str__(self):
        return f"Task:{self.name}"

    def run(self):

        redirect(self.stdout)

        # Check if all path dependencies are met
        not_existing_path_dependencies: List[str] = [str(dependency) for dependency in self.path_dependencies if not dependency.exists()]
        if len(not_existing_path_dependencies) > 0:
            self._status = TaskStatus.FAILED
            raise DependencyNotMetException(f"Task {self.name}: Dependency/ies {not_existing_path_dependencies} not met.")

        # Store the last-modification timestamp of the already existing products.
        product_timestamps_before_running: Dict[str, float] = {str(product): getmtime(product) for product in self.products if product.exists()}

        # Call the actual function
        self._status = TaskStatus.RUNNING

        try:
            self.func(*self.func_args, **self.func_kwargs)
        except Exception as e:
            self._status = TaskStatus.FAILED
            raise TaskRaisedExceptionException(e)
        finally:
            stop_redirect()

        # Check if any product does not exist.
        not_existing_products: List[str] = [str(product) for product in self.products if not product.exists()]
        if len(not_existing_products) > 0:
            self._status = TaskStatus.FAILED
            raise ProductNotProducedException(f"Task {self.name}: Product/s {not_existing_products} not produced.")

        # Check if any product has not been updated.
        product_timestamps_after_running: Dict = {str(product): getmtime(product) for product in self.products if product.exists()}

        not_updated_products = _get_not_updated_products(product_timestamps_after_running, product_timestamps_before_running)

        if len(not_updated_products) > 0:
            self._status = TaskStatus.FAILED
            raise ProductNotUpdatedException(f"Task {self.name}: Product/s {not_updated_products} not updated.")

        self._status = TaskStatus.FINISHED

    def _update_by_slurmjob(self):
        assert self.slurmjob is not None
        self.slurmjob.watcher.update()
        self._slurmstate = self.slurmjob.state
        self._slurmid = f"{int(self.slurmjob.job_id):d}-{int(self.slurmjob.task_id):d}"

        if self._slurmstate in ['RUNNING', 'CONFIGURING', 'COMPLETING', 'STAGE_OUT']:
            self._status = TaskStatus.RUNNING
        elif self._slurmstate in ['FAILED', 'BOOT_FAIL', 'DEADLINE', 'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'SPECIAL_EXIT', 'STOPPED',
                                  'SUSPENDED', 'TIMEOUT']:
            self._status = TaskStatus.FAILED
        elif self._slurmstate in ['READY', 'PENDING', 'REQUEUE_FED', 'REQUEUED']:
            self._status = TaskStatus.PENDING
        elif self._slurmstate == 'CANCELED':
            self._status = TaskStatus.CANCELED
        elif self._slurmstate in ['COMPLETED']:
            self._status = TaskStatus.FINISHED
        elif self._slurmstate in ['RESV_DEL_HOLD', 'REQUEUE_HOLD', 'RESIZING', 'REVOKED', 'SIGNALING']:
            self._status = TaskStatus.HOLD
        elif self._slurmstate == 'UNKNOWN':
            self._status = TaskStatus.UNKNOWN
        else:
            raise Exception(f"Unknown slurmjob status! slurmjob.done {self.slurmjob.done}, slurmjob.state {self.slurmjob.state} ")

    @property
    def slurmjob_status(self):
        if not self.slurmjob is None:
            if self._slurmstate is None: self._update_by_slurmjob()
            return self._slurmstate
        else:
            return ""

    def statuscolor(self, s: TaskStatus = None) -> str:
        if s is None: s = self._status
        if s in status_colors:
            return status_colors[s]
        else:
            raise UnknownStatusException("Status {} is unknown.".format(s))

    def statustext(self, s: TaskStatus = None) -> str:
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
        s = self._status  # Fix status as temporary to return a consistent tuple
        return s, self.statustext(s), self.statuscolor(s), self._slurmstate

    @property
    def is_in_terminal_state(self) -> bool:
        return self._status in TERMINAL_STATES

    @property
    def is_in_successful_terminal_state(self) -> bool:
        return self._status in SUCCESSFUL_TERMINAL_STATES

    @property
    def is_in_failed_terminal_state(self) -> bool:
        return self._status in FAILED_TERMINAL_STATES

    def set_to_depfailed(self) -> None:
        self._status = TaskStatus.DEPFAILED

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

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            b = (self.func == other.func
                 and self.cleaned_args == other.cleaned_args
                 and self.name == other.name)
            return b
        else:
            return False


__all__ = [Task, Product, Dependency, _get_not_updated_products]
