# Python
import pytest
from depio.Task import Task, _status_colors, UnknownStatusException


def test_statuscolor():
    test_task = Task("test_task", lambda: print("Hello World"))  # Creating a dummy Task object

    for key in _status_colors.keys():
        assert test_task.statuscolor(key) == _status_colors[key], f"Expected task status color for key '{key}' to be '{_status_colors[key]}'"

    with pytest.raises(UnknownStatusException):
        test_task.statuscolor('unrecognized_status')  # Will throw an exception as the status is not recognized.
