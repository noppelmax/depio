from abc import ABC, abstractmethod
import submitit
from attrs import frozen

from .Task import Task


class TaskExecutor(ABC):
    @abstractmethod
    def submit(self, task: Task):
        pass

@frozen
class DemoTaskExecutor(TaskExecutor):
    def submit(self, task: Task):
        print(f"DemoTaskExecutor: Executing task {task.name}")
        task.run()

@frozen
class SubmitItExecutor(TaskExecutor):
    executor: submitit.AutoExecutor

    def submit(self, task):
        job = self.executor.submit(task.run)
        result = job.result()
        print(f"Task {task.name} completed with result: {result}")
        return result


__all__ = [TaskExecutor, DemoTaskExecutor, SubmitItExecutor]
