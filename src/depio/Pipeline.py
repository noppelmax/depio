import pathlib
from logging import setLoggerClass
from typing import Set, Dict, List
from pathlib import Path
import time
import sys

from rich.table import Table
from rich.panel import Panel
from rich.console import Group
from rich.live import Live
from rich.text import Text

import threading
import queue


from termcolor import colored
from tabulate import tabulate

from .stdio_helpers import enable_proxy
from .Task import Task
from .TaskStatus import TaskStatus
from .Executors import AbstractTaskExecutor
from .exceptions import ProductAlreadyRegisteredException, TaskNotInQueueException, DependencyNotAvailableException


class Pipeline:
    def __init__(self, depioExecutor: AbstractTaskExecutor, name: str = "NONAME",
                 clear_screen: bool = True,
                 hide_successful_terminated_tasks: bool = False,
                 submit_only_if_runnable: bool = False,
                 quiet: bool = False,
                 refreshrate: float = 1.0):

        # Flags
        self.CLEAR_SCREEN: bool = clear_screen
        self.QUIET: bool = quiet
        self.REFRESHRATE: float = refreshrate
        self.HIDE_SUCCESSFUL_TERMINATED_TASKS: bool = hide_successful_terminated_tasks
        self.SUBMIT_ONLY_IF_RUNNABLE :bool = submit_only_if_runnable

        self.name: str = name
        self.handled_tasks: List[Task] = None
        self.tasks: List[Task] = []
        self.depioExecutor: AbstractTaskExecutor = depioExecutor
        self.registered_products: Set[Path] = set()
        if not self.QUIET: print("Pipeline initialized")

        self.paused = False
        self.command_queue = queue.Queue()
        self.last_command_message = ""
        self.last_key_press_time = 0
        self.key_sequence = []

    def add_tasks(self, tasks: List[Task]) -> None:
        for task in tasks:
            self.add_task(task)

    def add_task(self, task: Task) -> None:

        # Check if the exact task is already registered
        for registered_task in self.tasks:
            if task == registered_task:
                return registered_task


        # Check is a product is already registered
        products_already_registered: List[str] = [str(p) for p in task.products if
                                                  str(p) in set(map(str, self.registered_products))]
        if len(products_already_registered) > 0:
            print(task.cleaned_args)
            for p in products_already_registered:
                t = [t for t in self.tasks if str(p) in set(map(str, t.products))][0]
                print(f"Product {p} is already registered by task {t.name}. Now again registered by task {task.name}.")
            raise ProductAlreadyRegisteredException(
                f"The product/s {products_already_registered} is/are already registered. "
                f"Each output can only be registered from one task.")


        # Check if the task dependencies are registered already
        missing_tasks: List[Task] = [t for t in task.dependencies if isinstance(t, Task) and t not in self.tasks]
        if len(missing_tasks) > 0:
            raise TaskNotInQueueException(f"Add the tasks into the queue in the correct order. "
                                          f"The following task/s is/are missing: {missing_tasks}.")

        # Register products
        self.registered_products.update(task.products)

        # Register task
        self.tasks.append(task)
        task._queue_id = len(self.tasks)  # TODO Fix this!
        return task

    def _solve_order(self) -> None:
        # Generate a task to product mapping
        product_to_task: Dict[Path, Task] = {}
        for task in self.tasks:
            for product in task.products:
                product_to_task[product] = task
        
        unavailable_dependencies = []
        
        # Add the dependencies to the tasks
        for task in self.tasks:
            # Build task dependencies list - use id() for deduplication
            seen_ids = set()
            task.task_dependencies = []
            task.path_dependencies = []
            
            for d in task.dependencies:
                if isinstance(d, Task):
                    # Direct task dependency
                    t_id = id(d)
                    if t_id not in seen_ids:
                        seen_ids.add(t_id)
                        task.task_dependencies.append(d)
                else:  # Path dependency
                    # Check if path is produced by a task
                    producing_task = product_to_task.get(d)
                    if producing_task is not None:
                        t_id = id(producing_task)
                        if t_id not in seen_ids:
                            seen_ids.add(t_id)
                            task.task_dependencies.append(producing_task)
                    else:
                        # Path dependency that must already exist
                        task.path_dependencies.append(d)
                        if not d.exists():
                            unavailable_dependencies.append(d)
        
        # Raise error if there are unavailable dependencies
        if unavailable_dependencies:
            dep_list = ', '.join(str(d) for d in unavailable_dependencies)
            raise DependencyNotAvailableException(
                f"The following dependencies do not exist and cannot be produced: {dep_list}"
            )
        
        # Add backlinks from dependencies to dependents
        for task in self.tasks:
            for t_dep in task.task_dependencies:
                t_dep.add_dependent_task(task)

    def _get_non_terminal_tasks(self) -> List[Task]:
        """
        Get all tasks that are not in a terminal state.
        :return: List of tasks that are not in a terminal state.
        """
        return [task for task in self.tasks if not task.is_in_terminal_state]
    
    def _get_pending_tasks(self) -> List[Task]:
        """
        Get all tasks that are in pending or unknown state.
        :return: List of tasks that are pending or unknown.
        """
        return [task for task in self.tasks if task.status[0] in [TaskStatus.PENDING, TaskStatus.UNKNOWN]]

    def _check_for_keypress(self):
        """Check for single key commands (no Enter needed)."""
        try:
            import sys
            import select
            
            # Only works on Unix-like systems
            if not (hasattr(select, 'select') and hasattr(sys.stdin, 'fileno')):
                return
                
            # Check if there's input available (non-blocking)
            if select.select([sys.stdin], [], [], 0.0)[0]:
                char = sys.stdin.read(1)
                current_time = time.time()
                
                # Reset sequence if too much time has passed
                if current_time - self.last_key_press_time > 1.0:
                    self.key_sequence = []
                
                self.last_key_press_time = current_time
                self.key_sequence.append(char)
                
                # Process single key commands
                if char.lower() == 'p':
                    self.paused = True
                    self.last_command_message = "✓ Pipeline paused (press 'r' to resume)"
                    self.key_sequence = []
                elif char.lower() == 'r':
                    self.paused = False
                    self.last_command_message = "✓ Pipeline resumed"
                    self.key_sequence = []
                elif char.lower() == 'q':
                    # Check for 'qq' to quit (safety measure)
                    self.last_command_message = "✓ Shutting down..."
                    self.exit_with_failed_tasks()
        except (ImportError, OSError):
            # System doesn't support select, skip keyboard handling
            pass

    def run(self) -> None:
        enable_proxy()
        self._solve_order()
        self.handled_tasks = []

        # Try to set terminal to non-blocking mode for better UX
        self._old_terminal_settings = None
        try:
            import termios
            import tty
            self._old_terminal_settings = termios.tcgetattr(sys.stdin)
            tty.setcbreak(sys.stdin.fileno())
            restore_terminal = True
        except (ImportError, OSError, AttributeError):
            restore_terminal = False
            if not self.QUIET:
                print("Note: Interactive commands not available on this system")

        try:
            with Live(refresh_per_second=5, console=None) as live:
                while True:
                    try:
                        # Check for keyboard input
                        if restore_terminal:
                            self._check_for_keypress()

                        if self.paused:
                            # Update UI even when paused
                            if not self.QUIET:
                                live.update(self._print_tasks())
                            time.sleep(self.REFRESHRATE)
                            continue

                        # Submit new runnable tasks
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

                        # Update the rich UI
                        if not self.QUIET:
                            live.update(self._print_tasks())

                        # Exit conditions
                        if all(task.is_in_terminal_state for task in self.tasks):
                            if any(task.is_in_failed_terminal_state for task in self.tasks):
                                self.exit_with_failed_tasks()
                            else:
                                self.exit_successful()

                        time.sleep(self.REFRESHRATE)

                    except KeyboardInterrupt:
                        print("\nStopping execution because of keyboard interrupt!")
                        self.exit_with_failed_tasks()

        finally:
            # Restore terminal settings
            self._restore_terminal()


    def _get_text_for_task(self, task):
        status = task.status

        # Extract fields
        status_text = status[1].upper()
        color = status[2]
        slurm_status = status[3]

        # Build Rich text objects with color styles
        status_rich = Text(status_text, style=color)
        slurm_rich = Text(slurm_status, style=color)

        return [
            task.is_in_successful_terminal_state,
            task.id,
            task.name,
            task.slurmid,
            slurm_rich,
            status_text,
            [t._queue_id for t in task.task_dependencies],
        ]


    def _clear_screen(self):
        if self.CLEAR_SCREEN: sys.stdout.write("\033[2J\033[H")


    def _print_tasks(self):
        headers = ["ID", "Name", "Slurm ID", "Slurm Status", "Status", "Task Deps"]
        table = Table(
            show_lines=True, 
            expand=True,
            border_style="white",
            header_style="bold white",
            row_styles=["", "dim"]
        )
        for h in headers:
            table.add_column(h, style="white")
        
        histogram = {}
        for task in self.tasks:
            is_success, tid, name, slurm_id, slurm_status, status, deps = self._get_text_for_task(task)
            histogram[status] = histogram.get(status, 0) + 1
            if self.HIDE_SUCCESSFUL_TERMINATED_TASKS and is_success:
                continue
            table.add_row(
                str(tid),
                str(name),
                str(slurm_id),
                str(slurm_status),
                str(status),
                ", ".join(str(d) for d in deps)
            )
        
        # Summary table
        summary = Table(show_header=True, header_style="bold magenta", border_style="magenta", expand=True)
        summary.add_column("Status", style="bold")
        summary.add_column("Count", justify="right", style="cyan")
        for status, count in histogram.items():
            summary.add_row(status, str(count))
        
        # Command panel
        command_text = Text()
        command_text.append("Pipeline Status: ", style="bold")
        if self.paused:
            command_text.append("⏸ PAUSED", style="bold yellow")
        else:
            command_text.append("▶ RUNNING", style="bold green")
        
        if self.last_command_message:
            command_text.append("\n\n" + self.last_command_message, style="italic cyan")
        
        command_text.append("\n\nQuick Commands: ", style="bold")
        command_text.append("P", style="bold cyan")
        command_text.append("ause  ", style="dim")
        command_text.append("R", style="bold cyan")
        command_text.append("esume  ", style="dim")
        command_text.append("Q", style="bold cyan")
        command_text.append("uit", style="dim")
        
        command_panel = Panel(
            command_text,
            title="[bold]Interactive Commands[/bold]",
            border_style="blue",
            subtitle="[dim]Press keys directly (no Enter needed)[/dim]"
        )
        
        # Create side-by-side layout with table and summary
        from rich.columns import Columns
        top_section = Columns([table, summary], expand=True)
        
        return Panel(
            Group(top_section, command_panel), 
            title=f"Pipeline: {self.name}"
    )
   

    def _restore_terminal(self):
        """Restore terminal to normal mode."""
        if hasattr(self, '_old_terminal_settings') and self._old_terminal_settings is not None:
            try:
                import termios
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self._old_terminal_settings)
            except:
                pass

    def exit_with_failed_tasks(self) -> None:
        # Restore terminal first
        self._restore_terminal()
        
        print()

        # Print the overview with the updated status once more.
        for task in self.tasks:
            task.is_ready_for_execution()
        if not self.QUIET: self._print_tasks()


        failed_tasks = [
            [task.id, task.name, task.slurmid, task.status[1]]
            for task in self.tasks if task.status[0] == TaskStatus.FAILED
        ]

        if failed_tasks:
            headers = ["Task ID", "Name", "Slurm ID", "Status"]
            print("---> Summary of Failed Tasks:")
            print()

            for task in self.tasks:
                if task.status[0] == TaskStatus.FAILED:
                    print(f"Details for Task ID: {task.id} - Name: {task.name}")
                    print(f"STDOUT")
                    print(task.get_stdout())
                    print(f"")
                    print(f"STDERR")
                    print(task.get_stderr())

        print("Canceling running jobs...")
        self.depioExecutor.cancel_all_jobs()

        print("Exit.")
        exit(1)

    def exit_successful(self) -> None:
        # Restore terminal first
        self._restore_terminal()
        
        # Print the overview with the updated status once more.
        for task in self.tasks:
            task.is_ready_for_execution()
        if not self.QUIET: self._print_tasks()

        print("All jobs done! Exit.")
        exit(0)


__all__ = [Pipeline]