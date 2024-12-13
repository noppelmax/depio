from __future__ import annotations

import pathlib
import typing
from io import StringIO
from operator import truediv
from os.path import getmtime
from typing import List, Dict, Callable, get_origin, Annotated, get_args, Union
import sys

from .BuildMode import BuildMode
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


_status_colors = {
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

_status_texts = {
    TaskStatus.PENDING: 'pending',
    TaskStatus.RUNNING: 'running',
    TaskStatus.FINISHED: 'finished',
    TaskStatus.SKIPPED: 'skipped',
    TaskStatus.HOLD: 'hold',
    TaskStatus.FAILED: 'failed',
    TaskStatus.CANCELED: 'cancelled',
    TaskStatus.UNKNOWN: 'unknown'
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
                 produces: List[pathlib.Path] = None, depends_on: List[Union[pathlib.Path, Task]] = None,
                 buildmode : BuildMode = BuildMode.IF_MISSING ):


        produces : List[pathlib.Path] = produces or []
        depends_on : List[Union[pathlib.Path, Task]] = depends_on or []

        self._status: TaskStatus = TaskStatus.WAITING
        self.name: str = name
        self._queue_id: int|None = None
        self.slurmjob = None
        self.func : Callable = func
        self.func_args: List = func_args or []
        self.func_kwargs: Dict = func_kwargs or {}
        self.buildmode : BuildMode = buildmode

        self.stdout : StringIO = StringIO()
        self.stderr: StringIO = StringIO()
        self.slurmjob = None
        self._slurmid = None
        self._slurmstate : str = ""

        # Parse dependencies and products from the annotations and merge with args
        products_args: List[str] = _parse_annotation_for_metaclass(func, Product)
        dependencies_args: List[str] = _parse_annotation_for_metaclass(func, Dependency)
        ignored_for_eq_args: List[str] = _parse_annotation_for_metaclass(func, IgnoredForEq)

        args_dict: Dict[str,typing.Any] = _get_args_dict(func, self.func_args, self.func_kwargs)
        self.cleaned_args: Dict[str, typing.Any] = {k: v for k, v in args_dict.items() if k not in ignored_for_eq_args}

        self.products: List[pathlib.Path] = \
            ([args_dict[argname] for argname in products_args if argname in args_dict] + produces)
        self.dependencies: List[Union[Task, pathlib.Path]] = \
            ([args_dict[argname] for argname in dependencies_args if argname in args_dict] + depends_on)

        # Gets filled by Pipeline
        self.path_dependencies = None
        self.task_dependencies = None

    def __str__(self):
        return f"Task:{self.name}"

    def should_run(self, missing_products : List[pathlib.Path]) -> bool:
        if self.buildmode == BuildMode.ALWAYS:
            return True
        elif self.buildmode == BuildMode.IF_MISSING:
            return len(missing_products) > 0
        elif self.buildmode == BuildMode.NEVER:
            return False
        else:
            raise Exception(f"Unkown skipmode: {self.skipmode}")

    def _check_path_dependencies(self):
        not_existing_path_dependencies: List[str] = \
            [str(dependency) for dependency in self.path_dependencies if not dependency.exists()]

        if len(not_existing_path_dependencies) > 0:
            self._status = TaskStatus.FAILED
            raise DependencyNotMetException(
                f"Task {self.name}: Dependency/ies {not_existing_path_dependencies} not met.")

    def _check_existence_of_products(self):
        not_existing_products: List[str] = [str(product) for product in self.products if not product.exists()]
        if len(not_existing_products) > 0:
            self._status = TaskStatus.FAILED
            raise ProductNotProducedException(f"Task {self.name}: Product/s {not_existing_products} not produced.")


    def _get_timestamp_of_products(self) -> Dict[str, float]:
        return {str(product): getmtime(product) for product in self.products if product.exists()}

    def run(self):

        redirect(self.stdout)

        # Check if all path dependencies are met
        self._check_path_dependencies()

        # Store the last-modification timestamp of the already existing products.
        product_timestamps_before_running: Dict[str, float] = self._get_timestamp_of_products()

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
        self._check_existence_of_products()

        # Check if any product has not been updated.
        product_timestamps_after_running: Dict[str, float] = self._get_timestamp_of_products()
        not_updated_products = _get_not_updated_products(product_timestamps_after_running, product_timestamps_before_running)
        if len(not_updated_products) > 0:
            self._status = TaskStatus.FAILED
            raise ProductNotUpdatedException(f"Task {self.name}: Product/s {not_updated_products} not updated.")

        self._status = TaskStatus.FINISHED

    def _set_status_by_slurmstate(self, slurmstate):

        if slurmstate in ['RUNNING', 'CONFIGURING', 'COMPLETING', 'STAGE_OUT']:
            _status = TaskStatus.RUNNING
        elif slurmstate in ['FAILED', 'BOOT_FAIL', 'DEADLINE', 'NODE_FAIL', 'OUT_OF_MEMORY',
                            'PREEMPTED', 'SPECIAL_EXIT', 'STOPPED', 'SUSPENDED', 'TIMEOUT']:
            _status = TaskStatus.FAILED
        elif slurmstate in ['READY', 'PENDING', 'REQUEUE_FED', 'REQUEUED']:
            _status = TaskStatus.PENDING
        elif slurmstate == 'CANCELED':
            _status = TaskStatus.CANCELED
        elif slurmstate in ['COMPLETED']:
            _status = TaskStatus.FINISHED
        elif slurmstate in ['RESV_DEL_HOLD', 'REQUEUE_HOLD', 'RESIZING', 'REVOKED', 'SIGNALING']:
            _status = TaskStatus.HOLD
        elif slurmstate in ['UNKNOWN']:
            _status = TaskStatus.UNKNOWN
        else:
            raise Exception(f"Unknown slurmjob status! -> {slurmstate} ")

        self._status = _status
        return _status

    def _update_by_slurmjob(self):
        assert self.slurmjob is not None

        self.slurmjob.watcher.update()

        self._slurmstate = self.slurmjob.state
        self._set_status_by_slurmstate(self._slurmstate)

        self._slurmid = f"{int(self.slurmjob.job_id):d}-{int(self.slurmjob.task_id):d}"

    @property
    def slurmjob_status(self):
        if self.slurmjob is None: return ""

        if self._slurmstate is None: self._update_by_slurmjob()
        return self._slurmstate

    def statuscolor(self, s: TaskStatus = None) -> str:
        if s is None: s = self._status
        if s in _status_colors:
            return _status_colors[s]
        else:
            raise UnknownStatusException(f"Status {s} is unknown.")

    def statustext(self, s: TaskStatus = None) -> str:
        if s is None: s = self._status
        if s in _status_texts:
            return _status_texts[s]

        status_messages = {
            TaskStatus.WAITING: lambda: 'waiting' + (f" for {[d._queue_id for d in self.task_dependencies if not d.is_in_terminal_state]}" if len(
                [d for d in self.task_dependencies if not d.is_in_terminal_state]) > 1 else ""),
            TaskStatus.DEPFAILED: lambda: 'dep. failed' + (
                f" at {[d._queue_id for d in self.task_dependencies if d.is_in_failed_terminal_state]}" if len(
                    [d for d in self.task_dependencies if d.is_in_failed_terminal_state]) > 1 else "")
        }
        try:
            return status_messages[s]()
        except KeyError:
            raise UnknownStatusException(f"Status {s} is unknown.")

    @property
    def status(self):
        if self.slurmjob is None:
            s = self._status
            slurmstate = ""
        else:
            if self._slurmstate is None: self._update_by_slurmjob()
            slurmstate = self._slurmstate
            s = self._set_status_by_slurmstate(slurmstate)
        return s, self.statustext(s), self.statuscolor(s), slurmstate

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

    def set_to_skipped(self) -> None:
        self._status = TaskStatus.SKIPPED

    @property
    def id(self) -> str:
        if self._queue_id:
            return f"{self._queue_id: 4d}"
        else:
            return "None"

    @property
    def slurmid(self) -> str:
        if self.slurmjob is None:
            return ""

        self._update_by_slurmjob()
        return f"{self._slurmid}"

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.func == other.func
                    and self.cleaned_args == other.cleaned_args
                    and self.name == other.name)
        else:
            return False

    def get_stderr(self):
        if self.slurmjob is None:
            self.stderr.getvalue()
        return self.slurmjob.stderr()

    def get_stdout(self):
        if self.slurmjob is None:
            self.stdout.getvalue()
        return self.slurmjob.stdout()


__all__ = [Task, Product, Dependency, _get_not_updated_products]
