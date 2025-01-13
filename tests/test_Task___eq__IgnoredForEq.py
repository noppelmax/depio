from typing import Annotated

from depio.Task import IgnoredForEq
from depio.Task import Task


def func1(x: Annotated[int, IgnoredForEq], y: int):
    pass


def func2(x: Annotated[int, IgnoredForEq], y: int):
    pass


def test_task_eq_same_funcs():
    task1 = Task("task", func1)
    task2 = Task("task", func1)
    assert task1 == task2


def test_task_eq_diff_funcs():
    task1 = Task("task", func1)
    task2 = Task("task", func2)
    assert task1 != task2


def test_task_eq_same_args():
    task1 = Task("task", func1, [1, 2])
    task2 = Task("task", func1, [1, 2])
    assert task1 == task2


def test_task_eq_diff_args():
    task1 = Task("task", func1, [1, 2])
    task2 = Task("task", func1, [1, 3])
    assert task1 != task2


def test_task_eq_same_args_with_none():
    task1 = Task("task", func1, [None, 2])
    task2 = Task("task", func1, [None, 2])
    assert task1 == task2


def test_task_eq_same_args_with_diff_x():
    task1 = Task("task", func1, [1, 2])
    task2 = Task("task", func1, [2, 2])
    assert task1 == task2
