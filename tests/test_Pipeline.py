import pytest
from src.depio.Pipeline import Pipeline, ProductAlreadyRegisteredException, TaskNotInQueueException
from src.depio.Task import Task


class TestPipeline:

    def test_add_task_new_task(self):
        pipeline = Pipeline(depioExecutor=None)
        task1 = Task("task1")
        pipeline.add_task(task1)
        assert task1 in pipeline.tasks
        
    def test_add_task_duplicated_task(self):
        pipeline = Pipeline(depioExecutor=None)
        task1 = Task("task1")
        pipeline.add_task(task1)
        pipeline.add_task(task1)
        assert task1 in pipeline.tasks

    def test_add_task_duplicate_producing_task(self):
        pipeline = Pipeline(depioExecutor=None)
        producing_task = Task("producing_task", produces=[pathlib.Path("test.txt")])
        pipeline.add_task(producing_task)
        assert producing_task in pipeline.tasks
        with pytest.raises(ProductAlreadyRegisteredException):
            pipeline.add_task(producing_task)

    def test_add_task_unregistered_dependency(self):
        pipeline = Pipeline(depioExecutor=None)
        task1 = Task("task1")
        task2 = Task("task2", depends_on=[task1])
        with pytest.raises(TaskNotInQueueException):
            pipeline.add_task(task2)

    def test_add_task_registered_dependency(self):
        pipeline = Pipeline(depioExecutor=None)
        task1 = Task("task1")
        pipeline.add_task(task1)
        assert self.task1.queue_id == 1
        task2 = Task("task2", depends_on=[task1])
        pipeline.add_task(task2)
        assert self.task2.queue_id == 2
        assert self.task2 in self.pipeline.tasks
