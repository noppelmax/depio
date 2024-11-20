import pytest
import pathlib
from depio.Pipeline import Pipeline, ProductAlreadyRegisteredException, TaskNotInQueueException
from depio.Task import Task


@pytest.fixture
def pipeline():
    return Pipeline(None, False, quiet=True)

def dummyfunc(self):
    pass

def test_add_task_new_task(pipeline):
    task1 = Task("task1", dummyfunc)
    pipeline.add_task(task1)
    assert task1 in pipeline.tasks

def test_add_task_duplicated_task(pipeline):
    task1 = Task("task1", dummyfunc)
    pipeline.add_task(task1)
    pipeline.add_task(task1)
    assert task1 in pipeline.tasks

def test_add_task_duplicate_producing_task(pipeline):
    producing_task = Task("producing_task", dummyfunc, produces=[pathlib.Path("test.txt")])
    producing_task2 = Task("producing_task2", dummyfunc, produces=[pathlib.Path("test.txt")])
    pipeline.add_task(producing_task)
    assert producing_task in pipeline.tasks
    with pytest.raises(ProductAlreadyRegisteredException):
        pipeline.add_task(producing_task2)

def test_add_task_unregistered_dependency(pipeline):
    task1 = Task("task1", dummyfunc)
    task2 = Task("task2", dummyfunc, depends_on=[task1])
    with pytest.raises(TaskNotInQueueException):
        pipeline.add_task(task2)

def test_add_task_registered_dependency(pipeline):
    task1 = Task("task1", dummyfunc)
    pipeline.add_task(task1)
    assert task1.queue_id == 1
    task2 = Task("task2", dummyfunc, depends_on=[task1])
    pipeline.add_task(task2)
    assert task2.queue_id == 2
    assert task2 in pipeline.tasks