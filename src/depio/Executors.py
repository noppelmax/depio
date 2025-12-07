import concurrent.futures
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List
import submitit
from attrs import frozen
from pathlib import Path

from .Task import Task


class AbstractTaskExecutor(ABC):

    def __init__(self, max_jobs_pending: int = None, max_jobs_queued: int = None):
        """
        Abstract class for task executors.
        :param max_jobs_pending: Maximum number of jobs that can be pending at the same time. If None, no limit is applied.
        :param max_jobs_queued: Maximum number of jobs that can be queued at the same time. If None, no limit is applied.
        """

        # Set the maximum number of jobs to run concurrently, None means no limit.
        self.max_jobs_pending = max_jobs_pending
        self.max_jobs_queued = max_jobs_queued

    @abstractmethod
    def submit(self, task, task_dependencies: List[Task] = None):
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
    """
    A very dumb executor, that is just executing a given task right away.
    It can be used for debugging purposes.
    """

    def __init__(self, max_jobs_pending: int = None, max_jobs_queued: int = None):
        super().__init__(max_jobs_pending=max_jobs_pending, max_jobs_queued=max_jobs_queued)
        print("depio-SequentialExecutor initialized")

    def submit(self, task, task_dependencies: List[Task] = None):
        print("==========================================================")
        print(f"I'm going to run task <{task.name}> now")
        print("==========================================================")
        task.run()

    def wait_for_all(self):
        pass

    def cancel_all_jobs(self):
        pass

    def handles_dependencies(self):
        return False


class ParallelExecutor(AbstractTaskExecutor):

    def __init__(self, internal_executor: concurrent.futures.Executor = None, max_jobs_pending: int = None, max_jobs_queued: int = None, **kwargs):
        super().__init__(max_jobs_pending=max_jobs_pending, max_jobs_queued=max_jobs_queued)
        self.internal_executor = internal_executor if internal_executor is not None else ThreadPoolExecutor()
        self.running_jobs = []
        self.running_tasks = []
        print("depio-ParallelExecutor initialized")

    def submit(self, task, task_dependencies: List[Task] = None):
        job = self.internal_executor.submit(task.run)
        self.running_jobs.append(job)
        self.running_tasks.append(task)
        return

    def wait_for_all(self):
        for job in self.running_jobs:
            job.result()

    def cancel_all_jobs(self):
        pass

    def handles_dependencies(self):
        return False


TWO_DAYS_IN_MINUTES = 60 * 48  # 48 hours in minutes
DEFAULT_PARAMS = {
    "slurm_time": TWO_DAYS_IN_MINUTES,
    "slurm_partition": "gpu",
    "slurm_mem": 32,
    "gpus_per_node": 0
}


class SubmitItExecutor(AbstractTaskExecutor):

    def __init__(self, folder: Path = None, internal_executor=None, parameters=None, max_jobs_pending: int = 45, max_jobs_queued: int = 20):
        super().__init__(max_jobs_pending=max_jobs_pending, max_jobs_queued=max_jobs_queued)

        # Overwrite with a default executor.
        if internal_executor is None:
            internal_executor = submitit.AutoExecutor(folder=folder)
            internal_executor.update_parameters(**parameters)

        self.internal_executor = internal_executor
        self.default_parameters = parameters if parameters is not None else DEFAULT_PARAMS
        self.internal_executor.update_parameters(**self.default_parameters)

        self.slurmjobs = []
        print("depio-SubmitItExecutor initialized")

    def submit(self, task, task_dependencies: List[Task] = None):
        slurm_additional_parameters = {}

        tasks_with_slurmjob = [t for t in task_dependencies if t.slurmjob is not None]
        afterok: List[str] = [f"{t.slurmjob.job_id}" for t in tasks_with_slurmjob]

        if len(afterok) > 0:
            slurm_additional_parameters["dependency"] = f"afterok:{':'.join(afterok)}"

        if task.slurm_parameters is not None:
            params = task.slurm_parameters
        else:
            params = self.default_parameters
        self.internal_executor.update_parameters(**params, slurm_additional_parameters=slurm_additional_parameters)

        slurmjob = self.internal_executor.submit(task.run)
        task.slurmjob = slurmjob
        self.slurmjobs.append(slurmjob)
        return

    def wait_for_all(self):
        for job in self.slurmjobs:
            job.result()

    def cancel_all_jobs(self):
        for job in self.slurmjobs:
            job.cancel()

    def handles_dependencies(self):
        return True


__all__ = [AbstractTaskExecutor, ParallelExecutor, SequentialExecutor, SubmitItExecutor]
