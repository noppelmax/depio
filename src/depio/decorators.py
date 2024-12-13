from .Pipeline import Pipeline
from .SkipMode import SkipMode
from .Task import Task

def task(name : str|None, pipeline : Pipeline|None = None, skipmode : SkipMode = SkipMode.IF_MISSING, *dec_args, **dec_kwargs):
    def wrapper(func):
        def decorator(*func_args, **func_kwargs):
            # Create and add the task
            t = Task(name, func=func, skipmode=skipmode, func_args=func_args, func_kwargs=func_kwargs)

            # Not call the function!

            if pipeline:
                pipeline.add_task(t)

            return t
        return decorator
    return wrapper

__all__ = [task]