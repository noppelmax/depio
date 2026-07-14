from typing import Optional

from .BuildMode import BuildMode
from .Pipeline import Pipeline
from .Task import Task, TaskOptions


def task(
    name: Optional[str],
    pipeline: Optional[Pipeline] = None,
    buildmode: BuildMode = BuildMode.IF_MISSING,
    max_age: float = None,
    track_code: bool = False,
    description: Optional[str] = None,
):
    def wrapper(func):
        def decorator(*func_args, **func_kwargs):
            t = Task(
                name,
                func=func,
                func_args=func_args,
                func_kwargs=func_kwargs,
                options=TaskOptions(
                    buildmode=buildmode,
                    max_age=max_age,
                    track_code=track_code,
                    description=description or "",
                ),
            )
            if pipeline:
                pipeline.add_task(t)
            return t
        return decorator
    return wrapper


__all__ = ["task"]
