from .Task import Task
from .TaskHandler import TaskHandler




def task(name : str, taskhandler : TaskHandler, *dec_args, **dec_kwargs):
    def wrapper(func):
        def decorator(*func_args, **func_kwargs):
            # Create and add the task
            t = Task(name, func=func, func_args=func_args, func_kwargs=func_kwargs)
            taskhandler.add_task(t)
            # Call the function
            return None
        return decorator
    return wrapper

__all__ = [task]