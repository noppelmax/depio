from abc import ABC, abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor
import gc
from typing import List, Optional
from pathlib import Path

from .Task import Task
from .config import get_config
from .progress import TaskProgress, RemoteTaskProgress, ProgressServer

import submitit
import queue
import threading


class AbstractTaskExecutor(ABC):

    def __init__(self, max_jobs_pending: int = None, max_jobs_queued: int = None):
        self.max_jobs_pending = max_jobs_pending
        self.max_jobs_queued = max_jobs_queued
        self._progress_server = ProgressServer()

    def _prepare_progress(self, task: Task) -> TaskProgress:
        """Register a TaskProgress with the server and point task.progress at a
        RemoteTaskProgress that sends updates back to it.

        Returns the TaskProgress so SLURM executors can restore task.progress
        to it after pickling (the TUI then reads the server-updated object).
        Local executors can ignore the return value — the TUI reads
        task.progress directly, and RemoteTaskProgress maintains local state.
        """
        main_progress = TaskProgress()
        self._progress_server.register(str(task.id), main_progress)
        task.progress = RemoteTaskProgress(
            self._progress_server.address[0],
            self._progress_server.address[1],
            str(task.id),
        )
        return main_progress

    @abstractmethod
    def submit(self, task, task_dependencies: Optional[List[Task]] = None):
        ...

    @abstractmethod
    def wait_for_all(self):
        ...

    @abstractmethod
    def cancel_all_jobs(self):
        ...

    @abstractmethod
    def handles_dependencies(self):
        ...

    @property
    def has_jobs_queued_limit(self):
        return self.max_jobs_queued is not None

    @property
    def has_jobs_pending_limit(self):
        return self.max_jobs_pending is not None


class SequentialExecutor(AbstractTaskExecutor):
    """Executes tasks sequentially on the calling thread."""

    def __init__(self, max_jobs_pending: int = None, max_jobs_queued: int = None):
        super().__init__(max_jobs_pending=max_jobs_pending, max_jobs_queued=max_jobs_queued)

    def submit(self, task, task_dependencies=None):
        self._prepare_progress(task)
        task.run()

    def wait_for_all(self):
        pass

    def cancel_all_jobs(self):
        pass

    def handles_dependencies(self):
        return False


class MultiGPUExecutor(AbstractTaskExecutor):
    """Distributes tasks across multiple GPUs.

    Maintains one worker thread per GPU.  When a task is submitted it is
    queued on the thread pool; the worker that picks it up claims a free
    GPU from the device pool, overrides the task's ``device`` kwarg, runs
    the task, then returns the GPU to the pool.

    At most ``len(devices)`` tasks run simultaneously — one per GPU.
    Tasks that have no ``device`` kwarg are run without modification on
    whichever GPU worker picks them up.

    Parameters
    ----------
    devices:
        Ordered list of device strings, e.g. ``['cuda:0', 'cuda:1']``.
    """

    def __init__(self, devices: list[str]):
        super().__init__()
        self._devices = list(devices)
        self._executor = ThreadPoolExecutor(max_workers=len(devices))
        self._device_pool: queue.Queue = queue.Queue()
        for d in self._devices:
            self._device_pool.put(d)
        self._futures: list = []

    def submit(self, task, task_dependencies=None):
        self._prepare_progress(task)

        def run():
            device = self._device_pool.get()
            try:
                if "device" in task.func_kwargs:
                    task.func_kwargs["device"] = device
                task.run()
            finally:
                gc.collect()
                try:
                    import torch
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                except ImportError:
                    pass
                self._device_pool.put(device)

        self._futures.append(self._executor.submit(run))

    def wait_for_all(self):
        for f in self._futures:
            f.result()
        self._futures.clear()

    def cancel_all_jobs(self):
        self._executor.shutdown(wait=False)

    def handles_dependencies(self):
        return False


class ParallelExecutor(AbstractTaskExecutor):

    def __init__(self, internal_executor: Optional[Executor] = None, max_jobs_pending: int = None, max_jobs_queued: int = None, **kwargs):
        super().__init__(max_jobs_pending=max_jobs_pending, max_jobs_queued=max_jobs_queued)
        self.internal_executor = internal_executor if internal_executor is not None else ThreadPoolExecutor()
        self.running_jobs = []
        self.running_tasks = []

    def submit(self, task, task_dependencies=None):
        self._prepare_progress(task)

        def run():
            try:
                task.run()
            finally:
                gc.collect()

        job = self.internal_executor.submit(run)
        self.running_jobs.append(job)
        self.running_tasks.append(task)

    def wait_for_all(self):
        for job in self.running_jobs:
            job.result()
        self.running_jobs.clear()
        self.running_tasks.clear()

    def cancel_all_jobs(self):
        pass

    def handles_dependencies(self):
        return False


class _SubmitItBase(AbstractTaskExecutor):
    """Shared config loading, parameter resolution, dependency string building,
    and job management for SubmitIt-based executors."""

    def __init__(
        self,
        max_jobs_pending: Optional[int] = None,
        max_jobs_queued: Optional[int] = None,
        extra_defaults: Optional[dict] = None,
    ):
        cfg = get_config()["executor"]["slurm"]
        super().__init__(
            max_jobs_pending=max_jobs_pending if max_jobs_pending is not None else cfg["max_jobs_pending"],
            max_jobs_queued=max_jobs_queued if max_jobs_queued is not None else cfg["max_jobs_queued"],
        )
        self._cfg = cfg
        self._default_params: dict = {
            "slurm_time":      cfg["time_minutes"],
            "slurm_partition": cfg["partition"],
            "slurm_mem":       cfg["mem_gb"],
            "gpus_per_node":   cfg["gpus_per_node"],
        }
        if extra_defaults:
            self._default_params.update(extra_defaults)
        self._jobs: list = []

    @staticmethod
    def _build_dependency_string(task_dependencies: Optional[List[Task]]) -> Optional[str]:
        if not task_dependencies:
            return None
        ids = [
            str(t.slurmjob.job_id)
            for t in task_dependencies
            if getattr(t, "slurmjob", None) is not None
        ]
        return f"afterok:{':'.join(ids)}" if ids else None

    def _resolve_params(self, task: Task) -> dict:
        return task.slurm_parameters if task.slurm_parameters else self._default_params

    def cancel_all_jobs(self):
        for job in self._jobs:
            job.cancel()

    def handles_dependencies(self) -> bool:
        return True


class SubmitItExecutor(_SubmitItBase):

    def __init__(self, folder: Path = None, internal_executor=None, parameters=None,
                 max_jobs_pending: int = None, max_jobs_queued: int = None):
        super().__init__(max_jobs_pending=max_jobs_pending, max_jobs_queued=max_jobs_queued)
        if parameters is not None:
            self._default_params = parameters

        if internal_executor is None:
            internal_executor = submitit.AutoExecutor(folder=folder)
            internal_executor.update_parameters(**self._default_params)

        self.internal_executor = internal_executor
        self.internal_executor.update_parameters(**self._default_params)

    @property
    def slurmjobs(self):
        return self._jobs

    @slurmjobs.setter
    def slurmjobs(self, value):
        self._jobs = value

    def submit(self, task: Task, task_dependencies: Optional[List[Task]] = None) -> None:
        dep_str = self._build_dependency_string(task_dependencies)
        slurm_additional_parameters = {"dependency": dep_str} if dep_str else {}

        params = self._resolve_params(task)
        self.internal_executor.update_parameters(
            **params, slurm_additional_parameters=slurm_additional_parameters
        )

        # task.progress is set to a RemoteTaskProgress (for the worker).
        # After pickling, restore the main-side TaskProgress so the TUI reads
        # the server-updated object rather than the RemoteTaskProgress stub.
        main_progress = self._prepare_progress(task)
        slurmjob = self.internal_executor.submit(task.run)
        task.progress = main_progress

        task.slurmjob = slurmjob
        self._jobs.append(slurmjob)

    def wait_for_all(self):
        for job in self._jobs:
            job.result()


class SubmitItProcessExecutor(_SubmitItBase):
    """SubmitIt-based executor that runs each task in a separate process (SLURM job).
    Provides memory isolation and avoids Python interpreter memory sharing.
    """

    def __init__(
        self,
        folder: Path,
        parameters: Optional[dict] = None,
        max_jobs_pending: Optional[int] = None,
        max_jobs_queued: Optional[int] = None,
    ):
        cfg_cpus = get_config()["executor"]["slurm"].get("cpus_per_task", 4)
        super().__init__(
            max_jobs_pending=max_jobs_pending,
            max_jobs_queued=max_jobs_queued,
            extra_defaults={"cpus_per_task": cfg_cpus},
        )
        if parameters is not None:
            self._default_params = parameters

        self.executor = submitit.AutoExecutor(folder=str(folder))
        self.executor.update_parameters(**self._default_params)

    def submit(self, task: Task, task_dependencies: Optional[List[Task]] = None):
        dep_str = self._build_dependency_string(task_dependencies)
        slurm_additional_parameters = {"dependency": dep_str} if dep_str else {}

        params = self._resolve_params(task)
        self.executor.update_parameters(
            **params,
            slurm_additional_parameters=slurm_additional_parameters,
        )

        job = self.executor.submit(_run_task_wrapper, task)
        task.slurmjob = job
        self._jobs.append(job)

    def wait_for_all(self):
        for job in self._jobs:
            job.result()


def _run_task_wrapper(task: Task):
    """Wrapper executed inside the SLURM process."""
    try:
        task.run()
    except Exception:
        import traceback
        print("Task failed:", task.name)
        traceback.print_exc()
        raise


__all__ = ["AbstractTaskExecutor", "MultiGPUExecutor", "ParallelExecutor", "SequentialExecutor",
           "SubmitItExecutor", "SubmitItProcessExecutor"]
