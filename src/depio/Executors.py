import concurrent.futures
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor

import submitit
from attrs import frozen

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
        task.run()

    def wait_for_all(self):
        pass


class ParallelExecutor(AbstractTaskExecutor):

    def __init__(self, internal_executor:concurrent.futures.Executor=None, **kwargs):
        self.internal_executor = internal_executor if internal_executor is not None else ThreadPoolExecutor()
        self.running_jobs = []
        self.running_tasks = []
        print("depio-ParallelExecutor initialized")


    def submit(self, task):
        job = self.internal_executor.submit(task.run)
        self.running_jobs.append(job)
        self.running_tasks.append(task)
        return


    def get_status_of_all_jobs(self):
        done = 0
        running = 0
        cancelled = 0

        for job in self.running_jobs:
            if job.done():
                done += 1
            elif job.running():
                running += 1
            elif job.cancelled():
                cancelled += 1

        return done, running, cancelled

    def wait_for_all(self):
        for job in self.running_jobs:
            job.result()


class SubmitItExecutor(AbstractTaskExecutor):

    def __init__(self, internal_executor=None, **kwargs):

        self.internal_executor = internal_executor if internal_executor is not None else submitit.AutoExecutor()
        self.internal_executor.update_parameters(
            timeout_min=480,
            partition="gpu"
        )
        self.internal_executor.update_parameters(**kwargs)
        self.running_jobs = []
        self.running_tasks = []
        print("depio-SubmitItExecutor initialized")


    def submit(self, task):
        job = self.internal_executor.submit(task.run)
        self.running_jobs.append(job)
        self.running_tasks.append(task)
        return

    def wait_for_all(self):
        for job in self.running_jobs:
            job.result()


__all__ = [AbstractTaskExecutor, ParallelExecutor, DemoTaskExecutor, SubmitItExecutor]
