from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from pathlib import Path
import typing
import time
from io import StringIO
from os.path import getmtime
from typing import List, Dict, Callable, get_origin, Annotated, get_args, Union, Optional
import sys

from .BuildMode import BuildMode
from .config import get_config
from .TaskStatus import TaskStatus, TERMINAL_STATES, SUCCESSFUL_TERMINAL_STATES, FAILED_TERMINAL_STATES
from .stdio_helpers import redirect, stop_redirect, TaskOutputBuffer
from .progress import TaskProgress, _register_progress, _unregister_progress
from .code_hash import has_code_changed, record_hash
from .exceptions import ProductNotProducedException, TaskRaisedException, UnknownStatusException, \
    ProductNotUpdatedException, \
    DependencyNotMetException


@dataclass
class TaskOptions:
    """Grouping of optional Task configuration, passed as a single object.

    All fields have defaults so ``TaskOptions()`` is always valid.
    The ``@task`` decorator builds one of these from its keyword args.
    Plain ``Task(...)`` calls may still pass these as flat kwargs for
    backward compatibility — they are absorbed into a ``TaskOptions``
    automatically.
    """
    buildmode: BuildMode = BuildMode.IF_MISSING
    max_age: Optional[float] = None
    track_code: bool = False
    slurm_parameters: dict = field(default_factory=dict)
    arg_resolver: Optional[Callable] = None
    description: str = ""
    on_finished: Optional[Callable] = None
    on_task_failed: Optional[Callable] = None


_OPTION_FIELDS = frozenset(TaskOptions.__dataclass_fields__)


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
    TaskStatus.UNKNOWN: 'unknown',
    TaskStatus.WAITING: 'waiting',
    TaskStatus.DEPFAILED: 'dep. failed'
}


def python_version_is_greater_or_equal_to_3_10():
    return sys.version_info.major > 3 or (sys.version_info.major == 3 and sys.version_info.minor >= 10)


# from https://stackoverflow.com/questions/218616/how-to-get-method-parameter-names
def _get_args_dict(fn, args, kwargs) -> Dict[str, typing.Any]:
    args_names = fn.__code__.co_varnames[:fn.__code__.co_argcount]
    return {**dict(zip(args_names, args)), **kwargs}

def _get_args_dict_nested(fn, args, kwargs) -> Dict[str, typing.Any]:
    args_names = fn.__code__.co_varnames[:fn.__code__.co_argcount]
    base = {**dict(zip(args_names, args)), **kwargs}

    expanded = dict(base)   # copy

    for name, value in base.items():
        if isinstance(value, list):
            # Expand into fake keys: name[0], name[1], ...
            for i, element in enumerate(value):
                expanded[f"{name}_{i}"] = element

    return expanded



def _parse_annotation_for_metaclass(func, args_dict, metaclass) -> List[str]:
    if python_version_is_greater_or_equal_to_3_10():
        annotations = getattr(func, "__annotations__", None)
    else:
        if isinstance(func, type):
            annotations = func.__dict__.get("__annotations__", None)
        else:
            annotations = getattr(func, "__annotations__", None)

    results: List[str] = []

    def expand_list(name: str, value:List):
        """Return ['name[0]', 'name[1]', ...] based on default list size,
        or ['name[]'] if no runtime size is available."""
        if isinstance(value, list):
            return [f"{name}_{i}" for i in range(len(value))]
        return [f"{name}"]

    for name, annotation in annotations.items():

        # Annotated[T, metadata...]
        if get_origin(annotation) is Annotated:
            if len(get_args(annotation)) != 2:
                raise ValueError(f"Malformed annotation. Expected Annotated[T, meta], but got {annotation}")
            
            T, *metadata = get_args(annotation)

            if any(meta is metaclass for meta in metadata):
                # Annotated[List[T], Meta]
                if get_origin(T) in (list, List):
                    val = args_dict[name]
                    results.extend(expand_list(name, val))
                    
                # Annotated[T, Meta]
                else:
                    results.append(name)
            
    return results



def _get_not_updated_products(product_timestamps_after_running: typing.Dict,
                              product_timestamps_before_running: typing.Dict) -> typing.List[str]:
    # Calculate the not updated products
    not_updated_products = []
    for product, before_timestamp in product_timestamps_before_running.items():
        after_timestamp = product_timestamps_after_running.get(product)
        if before_timestamp == after_timestamp:
            not_updated_products.append(product)

    return not_updated_products


class Task:
    def __init__(
        self,
        name: str,
        func: Callable,
        func_args: List = None,
        func_kwargs: Dict = None,
        produces: List[Path] = None,
        depends_on: List[Union[Path, "Task"]] = None,
        options: Optional[TaskOptions] = None,
        **legacy_kwargs,
    ):
        # Absorb old-style flat kwargs (buildmode=, description=, …) into a TaskOptions
        # so callers that pre-date TaskOptions continue to work unchanged.
        if options is None:
            options = TaskOptions()
        for k in list(legacy_kwargs):
            if k in _OPTION_FIELDS:
                setattr(options, k, legacy_kwargs.pop(k))
        if legacy_kwargs:
            raise TypeError(f"Task() got unexpected keyword arguments: {list(legacy_kwargs)}")

        # Apply IF_OLD default max_age from config when caller didn't supply one.
        if options.max_age is None and options.buildmode == BuildMode.IF_OLD:
            options.max_age = get_config()["task"]["max_age_seconds"]

        self.end_time = None
        self.start_time = None
        produces = produces or []
        depends_on = depends_on or []

        self._status: TaskStatus = TaskStatus.WAITING
        self.name: str = name
        self._queue_id: Optional[int] = None
        self.slurmjob = None
        self.func: Callable = func
        self.func_args: List = func_args or []
        self.func_kwargs: Dict = func_kwargs or {}

        # Unpack TaskOptions to direct attributes so external code that reads
        # task.buildmode, task.description etc. continues to work.
        self.buildmode: BuildMode = options.buildmode
        self.max_age: Optional[float] = options.max_age
        self.track_code: bool = options.track_code
        self.slurm_parameters: Dict = options.slurm_parameters
        self.description: str = options.description
        self.on_finished: Optional[Callable] = options.on_finished
        self.on_task_failed: Optional[Callable] = options.on_task_failed

        self._code_hash_key: str = f"{func.__module__}.{func.__qualname__}"
        self._decided_to_run: bool = False

        self.stdout: TaskOutputBuffer = TaskOutputBuffer()
        self.stderr: TaskOutputBuffer = TaskOutputBuffer()
        self.progress: Optional[TaskProgress] = None
        self._slurmid = None
        self._slurmstate: str = ""

        if options.arg_resolver is not None:
            self.func_args, self.func_kwargs = options.arg_resolver(self.func, self.func_args, self.func_kwargs)

        args_dict_flat: Dict[str, typing.Any] = _get_args_dict(func, self.func_args, self.func_kwargs)

        # Parse dependencies and products from the annotations and merge with args
        products_args: List[str] = _parse_annotation_for_metaclass(func, args_dict_flat, Product)
        dependencies_args: List[str] = _parse_annotation_for_metaclass(func, args_dict_flat, Dependency)
        ignored_for_eq_args: List[str] = _parse_annotation_for_metaclass(func, args_dict_flat, IgnoredForEq)

        args_dict: Dict[str, typing.Any] = _get_args_dict_nested(func, self.func_args, self.func_kwargs)
        self.cleaned_args: Dict[str, typing.Any] = {k: v for k, v in args_dict.items() if k not in ignored_for_eq_args}

        self.products: List[Path] = \
            ([args_dict[argname] for argname in products_args if argname in args_dict and args_dict[argname] is not None] + produces)
        self.dependencies: List[Union[Task, Path]] = \
            ([args_dict[argname] for argname in dependencies_args if argname in args_dict and args_dict[argname] is not None] + depends_on)

        # Gets filled by Pipeline
        self.path_dependencies = None
        self.task_dependencies = None

        self.dependent_tasks = []

    def is_ready_for_execution(self) -> bool:
        if not self.should_run():
            self.set_to_skipped()
            return False

        if not self.all_path_dependencies_exist() and not self.is_in_terminal_state:
            self.set_to_depfailed()
            return False

        if self.is_in_terminal_state:
            return False

        return self.all_path_dependencies_exist() and self.all_task_dependencies_terminated_successfully()


    def all_path_dependencies_exist(self) -> bool:
        if self.path_dependencies is None:
            raise RuntimeError(
                f"Task '{self.name}': path_dependencies not initialised. "
                "Call Pipeline._solve_order() before executing tasks."
            )
        return all(p_dep.exists() for p_dep in self.path_dependencies)

    def all_task_dependencies_terminated_successfully(self) -> bool:
        if self.task_dependencies is None:
            raise RuntimeError(
                f"Task '{self.name}': task_dependencies not initialised. "
                "Call Pipeline._solve_order() before executing tasks."
            )
        return all(t_dep.is_in_successful_terminal_state for t_dep in self.task_dependencies)

    def add_dependent_task(self, task):
        self.dependent_tasks.append(task)

    def __str__(self):
        return f"Task:{self.name}"

    def should_run(self) -> bool:
        missing_products: List[Path] = [p for p in self.products if not p.exists()]

        if self.buildmode == BuildMode.ALWAYS:
            result = True
        elif self.buildmode == BuildMode.IF_MISSING:
            result = len(missing_products) > 0
        elif self.buildmode == BuildMode.IF_NEW:
            result = any(t.should_run() for t in self.task_dependencies) or len(missing_products) > 0
        elif self.buildmode == BuildMode.IF_OLDER:
            if missing_products:
                result = True
            else:
                path_deps = [d for d in (self.path_dependencies or []) if d.exists()]
                if not path_deps:
                    result = False
                else:
                    oldest_output = min(p.stat().st_mtime for p in self.products)
                    newest_input  = max(d.stat().st_mtime for d in path_deps)
                    result = oldest_output < newest_input
        elif self.buildmode == BuildMode.IF_OLD:
            if missing_products:
                result = True
            else:
                max_age = self.max_age
                now = time.time()
                result = any(now - p.stat().st_mtime > max_age for p in self.products)
        elif self.buildmode == BuildMode.IF_CODE_CHANGED:
            result = bool(missing_products) or has_code_changed(self._code_hash_key, self.func)
        elif self.buildmode == BuildMode.NEVER:
            result = False
        else:
            raise ValueError(f"Unknown buildmode: {self.buildmode}")

        if self.track_code and not result:
            result = has_code_changed(self._code_hash_key, self.func)

        # Universal invariant: if any upstream task decided to run this session,
        # this task must also run regardless of its own build mode, so that
        # downstream outputs never silently go stale.
        if not result and self.task_dependencies:
            result = any(t._decided_to_run for t in self.task_dependencies)

        if result:
            self._decided_to_run = True

        return result

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

    def get_duration(self) -> int:
        if self.start_time is None:
            return 0 # secs
        if self.end_time is None:
            return int(time.time() - self.start_time)
        return int(self.end_time - self.start_time)

    def run(self):
        self.start_time = time.time()
        _register_progress(self.progress)
        redirect(self.stdout)

        try:
            # Check if all path dependencies are met
            self._check_path_dependencies()

            # Store the last-modification timestamp of the already existing products.
            product_timestamps_before_running: Dict[str, float] = self._get_timestamp_of_products()

            # Call the actual function
            self._status = TaskStatus.RUNNING

            try:
                self.func(*self.func_args, **self.func_kwargs)
            except Exception as e:
                self.set_to_failed()
                raise TaskRaisedException(e) from e
        finally:
            stop_redirect()
            _unregister_progress()

        # Check if any product does not exist.
        self._check_existence_of_products()

        # Check if any product has not been updated.
        product_timestamps_after_running: Dict[str, float] = self._get_timestamp_of_products()
        not_updated_products = _get_not_updated_products(product_timestamps_after_running,
                                                         product_timestamps_before_running)
        if len(not_updated_products) > 0:
            self._status = TaskStatus.FAILED
            raise ProductNotUpdatedException(f"Task {self.name}: Product/s {not_updated_products} not updated.")

        self._status = TaskStatus.FINISHED
        self.end_time = time.time()

        if self.track_code or self.buildmode == BuildMode.IF_CODE_CHANGED:
            record_hash(self._code_hash_key, self.func)

    def barerun(self):
        self.func(*self.func_args, **self.func_kwargs)

    def _set_status_by_slurmstate(self, slurmstate):

        if slurmstate in ['RUNNING', 'CONFIGURING', 'COMPLETING', 'STAGE_OUT']:
            _status = TaskStatus.RUNNING
        elif slurmstate in ['FAILED', 'BOOT_FAIL', 'DEADLINE', 'NODE_FAIL', 'OUT_OF_MEMORY',
                            'PREEMPTED', 'SPECIAL_EXIT', 'STOPPED', 'SUSPENDED', 'TIMEOUT']:
            _status = TaskStatus.FAILED
            self.set_to_failed()
        elif slurmstate in ['READY', 'PENDING', 'REQUEUE_FED', 'REQUEUED']:
            _status = TaskStatus.PENDING
        elif slurmstate == 'CANCELLED':
            _status = TaskStatus.CANCELED
        elif 'CANCELLED' in slurmstate: # Slurm set 'CANCELLED by <number>' sometimes...
            _status = TaskStatus.CANCELED
        elif slurmstate in ['COMPLETED']:
            _status = TaskStatus.FINISHED
        elif slurmstate in ['RESV_DEL_HOLD', 'REQUEUE_HOLD', 'RESIZING', 'REVOKED', 'SIGNALING']:
            _status = TaskStatus.HOLD
        elif slurmstate in ['UNKNOWN']:
            _status = TaskStatus.UNKNOWN
        else:
            #print(f"Unknown slurmjob status! -> {slurmstate} ")
            _status = TaskStatus.UNKNOWN

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
        self._update_by_slurmjob()
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

        raise UnknownStatusException(f"Status {s} is unknown.")

    def statustext_long(self, s: TaskStatus = None) -> str:
        if s is None: s = self._status
        if s == TaskStatus.WAITING:
            pending = [d._queue_id for d in self.task_dependencies if not d.is_in_terminal_state]
            return 'waiting' + (f" for {pending}" if len(pending) > 1 else "")
        if s == TaskStatus.DEPFAILED:
            failed = [d._queue_id for d in self.task_dependencies if d.is_in_failed_terminal_state]
            return 'dep. failed' + (f" at {failed}" if len(failed) > 1 else "")
        if s in _status_texts:
            return _status_texts[s]
        raise UnknownStatusException(f"Status {s} is unknown.")

    @property
    def status(self):
        if self.slurmjob is None:
            s = self._status
            slurmstate = ""
        else:
            self._update_by_slurmjob()
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

    def set_dependent_task_to_depfailed(self):
        for task in self.dependent_tasks:
            task.set_to_depfailed()

    def set_to_failed(self):
        self._status = TaskStatus.FAILED
        if self.slurmjob is not None:
            self.slurmjob.cancel()
        self.set_dependent_task_to_depfailed()

    def set_to_depfailed(self) -> None:
        self._status = TaskStatus.DEPFAILED
        if self.slurmjob is not None:
            self.slurmjob.cancel()
        self.set_dependent_task_to_depfailed()


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
        if not isinstance(other, self.__class__):
            return False
        return self.func == other.func and self.cleaned_args == other.cleaned_args

    def get_stderr(self):
        if self.slurmjob is None:
            return self.stderr.getvalue()
        else:
            return self.slurmjob.stderr()

    def get_stdout(self):
        if self.slurmjob is None:
            return self.stdout.getvalue()
        else:
            return self.slurmjob.stdout()
        
    def __hash__(self):
        # Hash based on function and cleaned_args.
        # List values are converted to tuples so they are hashable.
        def _hashable(v):
            return tuple(v) if isinstance(v, list) else v
        return hash((id(self.func), tuple(sorted(
            (k, _hashable(v)) for k, v in self.cleaned_args.items()
        ))))
    


__all__ = ["Task", "TaskOptions", "Product", "Dependency", "IgnoredForEq", "_get_not_updated_products"]
