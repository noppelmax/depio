# test_Pipeline2.py
import pathlib

import pytest
from depio.Pipeline import Pipeline, Task, DependencyNotAvailableException


class TaskMock(Task):
    def __init__(self, func, dependencies=[], products=[]):
        self.func = func
        self.name = str(func)
        self.func_args = []
        self.func_kwargs = {}
        self.dependencies = dependencies
        self.products = products
        self.task_dependencies = []

class PathMock():
    def __init__(self, name, exists=True):
        self.name : str = name
        self._exists : bool = exists

    def exists(self) -> bool:
        return self._exists

    def __str__(self) -> str:
        return self.name

@pytest.fixture
def pipeline():
    return Pipeline(None, False, quiet=True)


def test_solve_order_no_dependency(pipeline):
    product_A = PathMock('product_A', exists=True)
    product_B = PathMock("product_B", exists=True)

    task_A = TaskMock("task_A", [], [product_A])
    pipeline.add_task(task_A)
    pipeline._solve_order()

    assert frozenset(task_A.task_dependencies) == frozenset()


def test_solve_order_single_dependency(pipeline):
    product_A = PathMock('product_A', exists=True)
    product_B = PathMock("product_B", exists=True)

    task_A = TaskMock("task_A", [], [product_A])
    task_B = TaskMock("task_B", [product_A], [product_B])
    pipeline.add_task(task_A)
    pipeline.add_task(task_B)
    pipeline._solve_order()

    assert frozenset(task_B.task_dependencies) == frozenset([task_A])


def test_solve_order_multiple_dependencies(pipeline):
    product_A = PathMock("product_A", exists=True)
    product_B = PathMock("product_B", exists=True)
    product_C = PathMock("product_C", exists=True)

    task_A = TaskMock("task_A", [], [product_A])
    task_B = TaskMock("task_B", [], [product_B])
    task_C = TaskMock("task_C", [product_A, product_B], [product_C])
    task_A = pipeline.add_task(task_A)
    task_B = pipeline.add_task(task_B)
    task_C = pipeline.add_task(task_C)
    pipeline._solve_order()

    assert sorted(task_C.task_dependencies, key=id) == sorted([task_A, task_B], key=id)


def test_solve_order_dependency_not_available(pipeline):
    product_A = PathMock("product_A", exists=True)
    product_B = PathMock("product_B", exists=False)

    task_A = TaskMock("task_A", [product_B], [product_A])

    pipeline.add_task(task_A)

    with pytest.raises(DependencyNotAvailableException):
        pipeline._solve_order()
