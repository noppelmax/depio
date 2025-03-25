from depio.Task import Task


def func1(a: int, b: int, c: int):
    pass


def func2(a: int, b: int, c: int):
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
    task1 = Task("task", func1, [1, 2, 3])
    task2 = Task("task", func1, [1, 2, 3])
    assert task1 == task2


def test_task_eq_diff_args():
    task1 = Task("task", func1, [1, 2, 3])
    task2 = Task("task", func1, [1, 2, 4])
    assert task1 != task2


def test_task_eq_same_args_with_none():
    task1 = Task("task", func1, [None, 2, 3])
    task2 = Task("task", func1, [None, 2, 3])
    assert task1 == task2


def test_task_eq_diff_args_with_none():
    task1 = Task("task", func1, [None, 2, 3])
    task2 = Task("task", func1, [None, 2, 4])
    assert task1 != task2


def test_task_eq_same_kwargs():
    task1 = Task("task", func1, None, {'a': 1, 'b': 2})
    task2 = Task("task", func1, None, {'a': 1, 'b': 2})
    assert task1 == task2


def test_task_eq_diff_kwargs():
    task1 = Task("task", func1, None, {'a': 1, 'b': 2})
    task2 = Task("task", func1, None, {'a': 1, 'b': 3})
    assert task1 != task2


def test_task_eq_same_name():
    task1 = Task("task1", func1)
    task2 = Task("task1", func1)
    assert task1 == task2


