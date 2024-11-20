import concurrent.futures
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List
import submitit
from attrs import frozen
from pathlib import Path

from .Task import Task


class AbstractTaskExecutor(ABC):
    @abstractmethod
    def submit(self, task, task_dependencies:List[Task]=None):
        ...

    @abstractmethod
    def wait_for_all(self):
        ...

    def handles_dependencies(self):
        ...

@frozen
class DemoTaskExecutor(AbstractTaskExecutor):
    def submit(self, task, task_dependencies:List[Task]=None):
        task.run()

    def wait_for_all(self):
        pass

    def handles_dependencies(self):
        return False


class ParallelExecutor(AbstractTaskExecutor):

    def __init__(self, internal_executor:concurrent.futures.Executor=None, **kwargs):
        self.internal_executor = internal_executor if internal_executor is not None else ThreadPoolExecutor()
        self.running_jobs = []
        self.running_tasks = []
        print("depio-ParallelExecutor initialized")


    def submit(self, task, task_dependencies:List[Task]=None):
        job = self.internal_executor.submit(task.run)
        self.running_jobs.append(job)
        self.running_tasks.append(task)
        return

    def wait_for_all(self):
        for job in self.running_jobs:
            job.result()

    def handles_dependencies(self):
        return False


TIME_IN_MINUTES = 60 * 48  # 48 hours in minutes
DEFAULT_PARAMS = {
    "slurm_time": TIME_IN_MINUTES,
    "slurm_partition": "gpu",
    "slurm_mem": 32,
    "gpus_per_node": 0
}

class SubmitItExecutor(AbstractTaskExecutor):

    def __init__(self, folder:Path, internal_executor=None, **kwargs):

        self.internal_executor = internal_executor if internal_executor is not None else submitit.AutoExecutor(folder=folder)
        self.internal_executor.update_parameters(**DEFAULT_PARAMS)
        self.running_jobs = []
        self.running_tasks = []
        print("depio-SubmitItExecutor initialized")


    def submit(self, task, task_dependencies:List[Task]=None):
        slurm_additional_parameters = {}
        afterok:List[str] = [f"{t.slurmjob.job_id}" for t in task_dependencies]
        if len(afterok) > 0:
            slurm_additional_parameters["dependency"] = f"afterok:{':'.join(afterok)}"

        self.internal_executor.update_parameters(**DEFAULT_PARAMS, slurm_additional_parameters=slurm_additional_parameters)
        job = self.internal_executor.submit(task.run)
        task.slurmjob = job
        self.running_jobs.append(job)
        self.running_tasks.append(task)
        return

    def wait_for_all(self):
        for job in self.running_jobs:
            job.result()

    def handles_dependencies(self):
        return True


__all__ = [AbstractTaskExecutor, ParallelExecutor, DemoTaskExecutor, SubmitItExecutor]
