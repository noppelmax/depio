import dataclasses
from pathlib import Path
from typing import Callable, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .TaskStatus import TaskStatus


@dataclasses.dataclass(frozen=True)
class TaskResult:
    """Snapshot of a finished task, passed to ``on_task_finished`` hooks."""
    name:     str
    status:   "TaskStatus"   # final status (FINISHED, FAILED, SKIPPED, …)
    stdout:   str            # captured stdout (empty string if none)
    stderr:   str            # captured stderr (empty string if none)
    duration: float          # wall-clock seconds (0.0 if not measured)
    outputs:  List[Path]     # files declared as products


@dataclasses.dataclass(frozen=True)
class PipelineResult:
    """Summary of a completed pipeline, passed to ``on_pipeline_finished`` hooks."""
    name:         str
    success:      bool            # True when no task is in a failed terminal state
    task_results: List[TaskResult]


def make_save_hook(output_dir: Path) -> Callable[[TaskResult], None]:
    """Return an ``on_task_finished`` hook that writes each task's output to disk.

    Layout::

        <output_dir>/<task-name>/stdout.txt
        <output_dir>/<task-name>/stderr.txt   # only written when non-empty

    Example::

        pipeline = Pipeline(
            depioExecutor=executor,
            on_task_finished=make_save_hook(Path("outputs/")),
        )
    """
    output_dir = Path(output_dir)

    def hook(result: TaskResult) -> None:
        if result.stdout or result.stderr:
            task_dir = output_dir / result.name
            task_dir.mkdir(parents=True, exist_ok=True)
            if result.stdout:
                (task_dir / "stdout.txt").write_text(result.stdout)
            if result.stderr:
                (task_dir / "stderr.txt").write_text(result.stderr)

    return hook


__all__ = ["TaskResult", "PipelineResult", "make_save_hook"]
