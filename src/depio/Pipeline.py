from dataclasses import dataclass, field
from typing import Set, Dict, List, Optional, Callable, TYPE_CHECKING
from pathlib import Path
import queue
import re
import threading
import time
import sys

from rich.console import Console
from rich.live import Live

if TYPE_CHECKING:
    import graphviz

from .hooks import TaskResult, PipelineResult, make_save_hook as _make_save_hook
from .config import get_config as _get_config
from ._tui import render_task_list, render_task_detail
from ._input import check_for_keypress
from .stdio_helpers import enable_proxy
from .Task import Task
from .TaskStatus import TaskStatus
from .Executors import AbstractTaskExecutor
from .exceptions import (
    ProductAlreadyRegisteredException,
    TaskNotInQueueException,
    DependencyNotAvailableException,
)


@dataclass
class PipelineTuiState:
    """All mutable display/interaction state for the Pipeline TUI.

    Separated from Pipeline's core state (task list, executor, hooks) so the
    two concerns don't bleed into each other.  Both ``_tui.py`` and
    ``_input.py`` read/write this object via ``pipeline.tui``.
    """
    paused: bool = False
    last_command_message: str = ""
    last_key_press_time: float = 0.0
    key_sequence: List[str] = field(default_factory=list)
    selected_task_idx: Optional[int] = None
    detail_mode: bool = False
    scroll_offset: int = 0
    pipeline_done: bool = False
    pipeline_failed: bool = False
    quit_confirmation_pending: bool = False
    quit_requested: bool = False


class Pipeline:
    def __init__(self, depioExecutor: AbstractTaskExecutor, name: str = "NONAME",
                 clear_screen: bool = True,
                 hide_successful_terminated_tasks: bool = False,
                 submit_only_if_runnable: bool = False,
                 quiet: bool = False,
                 refreshrate: float = None,
                 exit_when_done: bool = False,
                 on_task_finished: Optional[Callable[[TaskResult], None]] = None,
                 on_task_failed: Optional[Callable[[TaskResult], None]] = None,
                 on_pipeline_finished: Optional[Callable[[PipelineResult], None]] = None):

        # Flags
        _cfg = _get_config()
        self.CLEAR_SCREEN: bool = clear_screen
        self.QUIET: bool = quiet
        self.REFRESHRATE: float = refreshrate if refreshrate is not None else _cfg["pipeline"]["refreshrate"]
        self.HIDE_SUCCESSFUL_TERMINATED_TASKS: bool = hide_successful_terminated_tasks
        self.SUBMIT_ONLY_IF_RUNNABLE: bool = submit_only_if_runnable
        self.EXIT_WHEN_DONE: bool = exit_when_done
        self.on_task_finished: Optional[Callable[[TaskResult], None]] = on_task_finished
        self.on_task_failed: Optional[Callable[[TaskResult], None]] = on_task_failed
        self.on_pipeline_finished: Optional[Callable[[PipelineResult], None]] = on_pipeline_finished

        self.name: str = name
        self.handled_tasks: Optional[List[Task]] = None
        self.tasks: List[Task] = []
        self._task_set: set = set()
        self.depioExecutor: AbstractTaskExecutor = depioExecutor
        self.registered_products: Set[Path] = set()
        self._registered_product_strs: Set[str] = set()

        self.tui = PipelineTuiState()
        self._live: Optional["Live"] = None
        self._hook_fired_tasks: set = set()

    # ── Task registration ──────────────────────────────────────────────────────

    def add_tasks(self, tasks: List[Task]) -> None:
        for task in tasks:
            self.add_task(task)

    def add_task(self, task: Task) -> None:
        # Already registered — return the existing instance
        if task in self._task_set:
            return self.tasks[self.tasks.index(task)]

        # Reject duplicate products
        products_already_registered: List[str] = [
            str(p) for p in task.products if str(p) in self._registered_product_strs
        ]
        if products_already_registered:
            print(task.cleaned_args)
            for p in products_already_registered:
                t = next(t for t in self.tasks if str(p) in {str(pr) for pr in t.products})
                print(f"Product {p} is already registered by task {t.name}. "
                      f"Now again registered by task {task.name}.")
            raise ProductAlreadyRegisteredException(
                f"The product/s {products_already_registered} is/are already registered. "
                f"Each output can only be registered from one task.")

        # Reject out-of-order task dependencies
        missing_tasks: List[Task] = [
            t for t in task.dependencies if isinstance(t, Task) and t not in self._task_set
        ]
        if missing_tasks:
            raise TaskNotInQueueException(
                f"Add the tasks into the queue in the correct order. "
                f"The following task/s is/are missing: {missing_tasks}.")

        self.registered_products.update(task.products)
        self._registered_product_strs.update(str(p) for p in task.products)
        self.tasks.append(task)
        self._task_set.add(task)
        task._queue_id = len(self.tasks)  # TODO Fix this!
        return task

    # ── DAG resolution ─────────────────────────────────────────────────────────

    def _solve_order(self) -> None:
        product_to_task: Dict[Path, Task] = {}
        for task in self.tasks:
            for product in task.products:
                product_to_task[product] = task

        unavailable_dependencies = []

        for task in self.tasks:
            seen_ids = set()
            task.task_dependencies = []
            task.path_dependencies = []

            for d in task.dependencies:
                if isinstance(d, Task):
                    t_id = id(d)
                    if t_id not in seen_ids:
                        seen_ids.add(t_id)
                        task.task_dependencies.append(d)
                        d.add_dependent_task(task)
                else:
                    producing_task = product_to_task.get(d)
                    if producing_task is not None:
                        t_id = id(producing_task)
                        if t_id not in seen_ids:
                            seen_ids.add(t_id)
                            task.task_dependencies.append(producing_task)
                            producing_task.add_dependent_task(task)
                    else:
                        task.path_dependencies.append(d)
                        if not d.exists():
                            unavailable_dependencies.append(d)

        if unavailable_dependencies:
            dep_list = ', '.join(str(d) for d in unavailable_dependencies)
            raise DependencyNotAvailableException(
                f"The following dependencies do not exist and cannot be produced: {dep_list}")

    def _get_non_terminal_tasks(self) -> List[Task]:
        return [task for task in self.tasks if not task.is_in_terminal_state]

    def _get_pending_tasks(self) -> List[Task]:
        return [task for task in self.tasks
                if task.status[0] in [TaskStatus.PENDING, TaskStatus.UNKNOWN]]

    # ── Execution loop ─────────────────────────────────────────────────────────

    def _setup_keyboard(self) -> bool:
        self._old_terminal_settings = None
        try:
            import termios
            import tty
            self._old_terminal_settings = termios.tcgetattr(sys.stdin)
            tty.setcbreak(sys.stdin.fileno())
            return True
        except Exception:
            if not self.QUIET:
                print("Note: Interactive commands not available on this system")
            return False

    def _submit_ready_tasks(self) -> None:
        for task in self.tasks:
            if task in self.handled_tasks:
                continue
            if task.is_ready_for_execution() or self.depioExecutor.handles_dependencies():
                if task.should_run():
                    if not self.SUBMIT_ONLY_IF_RUNNABLE:
                        self.depioExecutor.submit(task, task.task_dependencies)
                        self.handled_tasks.append(task)
                    elif task.is_ready_for_execution():
                        if self.depioExecutor.has_jobs_queued_limit:
                            if len(self._get_non_terminal_tasks()) >= self.depioExecutor.max_jobs_queued:
                                continue
                        elif self.depioExecutor.has_jobs_pending_limit:
                            if len(self._get_pending_tasks()) >= self.depioExecutor.max_jobs_pending:
                                continue
                        self.depioExecutor.submit(task, task.task_dependencies)
                        self.handled_tasks.append(task)

    def _poll_slurm_statuses(self) -> None:
        # Refresh SLURM task statuses so is_in_terminal_state stays current
        # even in quiet mode (where _render is a no-op and task.status is
        # never called via the TUI).
        for task in self.handled_tasks:
            if task.slurmjob is not None and not task.is_in_terminal_state:
                task._update_by_slurmjob()

    def _fire_task_hooks(self) -> None:
        for task in self.tasks:
            if task not in self._hook_fired_tasks and task.is_in_terminal_state:
                self._hook_fired_tasks.add(task)
                result = TaskResult(
                    name=task.name,
                    status=task.status[0],
                    stdout=task.get_stdout(),
                    stderr=task.get_stderr(),
                    duration=float(task.get_duration()),
                    outputs=list(task.products),
                )
                for hook in filter(None, [self.on_task_finished, task.on_finished]):
                    try:
                        hook(result)
                    except Exception as e:
                        self.tui.last_command_message = f"Hook error: {e}"
                if result.status == TaskStatus.FAILED:
                    for hook in filter(None, [self.on_task_failed, task.on_task_failed]):
                        try:
                            hook(result)
                        except Exception as e:
                            self.tui.last_command_message = f"Hook error: {e}"

    def _check_pipeline_completion(self) -> None:
        if not self.tui.pipeline_done and all(
                task.is_in_terminal_state for task in self.tasks):
            self.tui.pipeline_done = True
            self.tui.pipeline_failed = any(
                task.is_in_failed_terminal_state for task in self.tasks)
            self.tui.last_command_message = (
                "Pipeline finished with failures. Press Q to quit."
                if self.tui.pipeline_failed else
                "All tasks finished. Press Q to quit."
            )
            if self.on_pipeline_finished is not None:
                pipeline_result = PipelineResult(
                    name=self.name,
                    success=not self.tui.pipeline_failed,
                    task_results=[
                        TaskResult(
                            name=t.name,
                            status=t.status[0],
                            stdout=t.get_stdout(),
                            stderr=t.get_stderr(),
                            duration=float(t.get_duration()),
                            outputs=list(t.products),
                        ) for t in self.tasks
                    ],
                )
                try:
                    self.on_pipeline_finished(pipeline_result)
                except Exception as e:
                    self.tui.last_command_message = f"Pipeline hook error: {e}"

    def run(self) -> None:
        enable_proxy()
        self._solve_order()
        self.handled_tasks = []

        def _render(live, *, refresh=False):
            if self.QUIET:
                return
            renderable = (render_task_detail(self)
                          if self.tui.detail_mode and self.tui.selected_task_idx is not None
                          else render_task_list(self))
            live.update(renderable, refresh=refresh)

        try:
            # 1. Logic for QUIET mode (No Live/TUI)
            if self.QUIET:
                while True:
                    self._submit_ready_tasks()
                    self._poll_slurm_statuses()
                    self._fire_task_hooks()
                    self._check_pipeline_completion()

                    if self.tui.pipeline_done and (self.EXIT_WHEN_DONE or self.QUIET):
                        return

                    time.sleep(self.REFRESHRATE)

            # 2. Logic for Interactive mode
            # The TUI runs in a background daemon thread so the main thread can
            # block inside _submit_ready_tasks() (e.g. SequentialExecutor) without
            # freezing the display.  Task execution always stays on the calling
            # thread, which keeps nested-pipeline patterns safe.
            else:
                tui_stop = threading.Event()

                def _tui_loop():
                    _restore = self._setup_keyboard()
                    try:
                        with Live(screen=True, refresh_per_second=5,
                                  redirect_stdout=False, redirect_stderr=False) as live:
                            self._live = live
                            while not tui_stop.is_set():
                                _render(live)
                                deadline = time.time() + self.REFRESHRATE
                                while time.time() < deadline and not tui_stop.is_set():
                                    if _restore and check_for_keypress(self):
                                        _render(live, refresh=True)
                                    time.sleep(0.05)
                    finally:
                        if _restore:
                            self._restore_terminal()

                tui_thread = threading.Thread(
                    target=_tui_loop, name="depio-tui", daemon=True)
                tui_thread.start()

                try:
                    while True:
                        if self.tui.quit_requested:
                            break

                        self._submit_ready_tasks()
                        self._poll_slurm_statuses()
                        self._fire_task_hooks()
                        self._check_pipeline_completion()

                        if self.tui.pipeline_done and self.EXIT_WHEN_DONE:
                            return

                        # Sleep in small slices so a quit requested from the
                        # TUI thread is picked up promptly instead of after a
                        # full refresh interval.
                        deadline = time.time() + self.REFRESHRATE
                        while time.time() < deadline and not self.tui.quit_requested:
                            time.sleep(0.05)
                finally:
                    tui_stop.set()
                    tui_thread.join(timeout=2.0)

                # A quit was requested from the TUI thread.  Perform the actual
                # exit here on the main thread so SystemExit terminates the
                # process (raised off the main thread it would only kill the
                # daemon TUI thread, leaving the pipeline running).
                if self.tui.quit_requested:
                    if self.tui.pipeline_done and not self.tui.pipeline_failed:
                        self.exit_successful()
                    else:
                        self.exit_with_failed_tasks()

        except KeyboardInterrupt:
            print("\nStopping execution because of keyboard interrupt!")
            self.exit_with_failed_tasks()
    # ── Output saving ──────────────────────────────────────────────────────────

    @staticmethod
    def make_save_hook(output_dir: Path) -> Callable[[TaskResult], None]:
        """Convenience alias for :func:`depio.hooks.make_save_hook`."""
        return _make_save_hook(output_dir)

    def save_stdouts(self, output_dir: Optional[Path] = None) -> Path:
        """Immediately save all terminal tasks' outputs to disk.

        Useful for a one-shot manual save.  For continuous per-task saving,
        use :meth:`make_save_hook` instead.

        Args:
            output_dir: Where to write.  Defaults to
                ``depio_output/<pipeline-name>/``.
        """
        if output_dir is None:
            safe_name = re.sub(r'[^\w\-]', '_', self.name).strip('_') or 'pipeline'
            output_dir = Path("depio_output") / safe_name
        hook = _make_save_hook(output_dir)
        for task in self.tasks:
            if task.is_in_terminal_state:
                hook(TaskResult(
                    name=task.name,
                    status=task.status[0],
                    stdout=task.get_stdout(),
                    stderr=task.get_stderr(),
                    duration=float(task.get_duration()),
                    outputs=list(task.products),
                ))
        return Path(output_dir)

    # ── Visualization ────────────────────────────────────────────────────────

    def visualize(self,
                  filename: Optional[Path] = None,
                  format: str = "png",
                  view: bool = False,
                  orientation: str = "vertical") -> "graphviz.Digraph":
        """Render the pipeline structure as a directed graph using Graphviz.

        Each :class:`Task` becomes a node.  Dependencies between tasks are
        drawn as edges.  Path-based dependencies that are not produced by a
        task are represented as dashed box nodes.  Node colours reflect the
        current task status (via :meth:`depio.Task.Task.statuscolor`).

        The graph can be laid out either vertically (top-to-bottom) or
        horizontally (left-to-right) by specifying ``orientation``.  The
        underlying Graphviz attribute ``rankdir`` is set to ``LR`` when
        ``orientation`` is ``"horizontal"``; the default behaviour
        corresponds to ``"vertical"``.

        Args:
            filename: If provided the graph will be rendered to this file
                (Graphviz will append the appropriate suffix based on
                ``format``).  When ``None`` the method simply returns the
                :class:`graphviz.Digraph` instance without rendering to disk.
            format: Output format understood by Graphviz (``'png'``,
                ``'pdf'`` etc.).  Ignored when ``filename`` is ``None``.
            view: Passed through to :meth:`graphviz.Digraph.render` and
                controls whether the viewer is launched (only when
                ``filename`` is set).
            orientation: ``"vertical"`` (default) or ``"horizontal"``.
                ``"horizontal"`` produces a left-to-right layout; other
                values are case-insensitively accepted but treated the same
                as ``"vertical"``.

        Returns:
            The :class:`graphviz.Digraph` object representing the pipeline.

        Raises:
            ImportError: if the ``graphviz`` Python package is not installed.
        """
        try:
            import graphviz
        except ImportError as e:
            raise ImportError("The graphviz package is required for pipeline "
                              "visualization.  Install it with ``pip install "
                              "graphviz``") from e

        # Ensure DAG edges have been computed.
        self._solve_order()

        dot = graphviz.Digraph(name=self.name, format=format)

        # orient graph if requested
        if isinstance(orientation, str) and orientation.lower().startswith("h"):
            # left-to-right layout
            dot.attr(rankdir="LR")

        # create a unique node identifier for each task; use the queue id if
        # available to make output more readable.
        def _node_id(task: "Task") -> str:
            if getattr(task, "_queue_id", None) is not None:
                return f"t{task._queue_id}"
            return f"t{hex(id(task))}"

        # add nodes
        for task in self.tasks:
            nid = _node_id(task)
            colour = task.statuscolor()
            dot.node(nid, label=task.name, style="outlined", color="black", shape="rectangle")

        # add edges
        for task in self.tasks:
            dst = _node_id(task)
            for dep in task.task_dependencies or []:
                src = _node_id(dep)
                dot.edge(src, dst)
            for path in task.path_dependencies or []:
                # represent raw path dependencies as dashed boxes
                pid = f"p{str(path)}"
                dot.node(pid, label=str(path), shape="box", style="dashed")
                dot.edge(pid, dst)

        if filename is not None:
            dot.render(str(filename), view=view, cleanup=True)
        return dot

    # ── Terminal / exit helpers ────────────────────────────────────────────────

    def _restore_terminal(self):
        if hasattr(self, '_old_terminal_settings') and self._old_terminal_settings is not None:
            try:
                import termios
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self._old_terminal_settings)
            except Exception:
                pass

    def exit_with_failed_tasks(self) -> None:
        if self._live is not None:
            self._live.stop()
        self._restore_terminal()

        print()
        for task in self.tasks:
            task.is_ready_for_execution()
        if not self.QUIET:
            Console().print(render_task_list(self))

        failed_tasks = [task for task in self.tasks if task.status[0] == TaskStatus.FAILED]
        if failed_tasks:
            print("---> Summary of Failed Tasks:")
            print()
            for task in failed_tasks:
                print(f"Details for Task ID: {task.id} - Name: {task.name}")
                print("STDOUT")
                print(task.get_stdout())
                print()
                print("STDERR")
                print(task.get_stderr())

        print("Canceling running jobs...")
        self.depioExecutor.cancel_all_jobs()
        print("Exit.")
        exit(1)

    def exit_successful(self) -> None:
        if self._live is not None:
            self._live.stop()
        self._restore_terminal()

        for task in self.tasks:
            task.is_ready_for_execution()
        if not self.QUIET:
            Console().print(render_task_list(self))

        print("All jobs done! Exit.")


__all__ = ["Pipeline", "TaskResult", "PipelineResult"]
