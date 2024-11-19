from .Task import Task
from .Pipeline import Pipeline




def task(name : str, *dec_args, **dec_kwargs):
    def wrapper(func):
        def decorator(*func_args, **func_kwargs):
            # Create and add the task
            t = Task(name, func=func, func_args=func_args, func_kwargs=func_kwargs)

            # Not call the function

            return t
        return decorator
    return wrapper

__all__ = [task]