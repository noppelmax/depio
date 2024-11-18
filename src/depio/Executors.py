from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

import submitit
from attrs import frozen
from submitit.conftest import executor

from .Task import Task


class AbstractTaskExecutor(ABC):
    @abstractmethod
    def submit(self, task: Task):
        pass

    @abstractmethod
    def wait_for_all(self):
        pass

@frozen
class DemoTaskExecutor(AbstractTaskExecutor):
    def submit(self, task: Task):
        print(f"DemoTaskExecutor: Executing task {task.name}")
        task.run()

    def wait_for_all(self):
        pass


class ParallelExecutor(AbstractTaskExecutor):

    def __init__(self, executor=ThreadPoolExecutor, **kwargs):
        self.executor = executor
        self.running_jobs = []
        self.running_tasks = []
        print("depio-ParallelExecutor initialized")


    def submit(self, task):
        job = self.executor.submit(task.run)
        print(f"Parallel Job started: {job.job_id}")
        self.running_jobs.append(job)
        self.running_tasks.append(task)
        return

    def wait_for_all(self):
        for job in self.running_jobs:
            job.result()


class SubmitItExecutor(AbstractTaskExecutor):

    def __init__(self, executor=submitit.AutoExecutor, **kwargs):
        self.executor = executor
        executor.update_parameters(
            timeout_min=480,
            partition="gpu"
        )
        executor.update_parameters(**kwargs)
        self.running_jobs = []
        self.running_tasks = []
        print("depio-SubmitItExecutor initialized")


    def submit(self, task):
        job = self.executor.submit(task.run)
        print(f"Slurm Job started: {job.job_id}")
        self.running_jobs.append(job)
        self.running_tasks.append(task)
        return

    def wait_for_all(self):
        for job in self.running_jobs:
            job.result()


__all__ = [AbstractTaskExecutor, ParallelExecutor, DemoTaskExecutor, SubmitItExecutor]
