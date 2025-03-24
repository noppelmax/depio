import pathlib
from distutils.command.build_scripts import first_line_re
from logging import setLoggerClass
from typing import Set, Dict, List
from pathlib import Path
import time
import sys


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
        # Generate a task to product mapping.
        product_to_task: Dict[Path, Task] = {product: task for task in self.tasks for product in task.products}

        # Add the dependencies to the tasks
        for task in self.tasks:
            # First spit of into tasks and paths
            task_deps = [d for d in task.dependencies if isinstance(d, Task)]
            path_deps = [d for d in task.dependencies if isinstance(d, Path)]

            # Verify that each dependency is available and add if yes.
            unavailable_dependencies = [d for d in path_deps if d not in product_to_task and not d.exists()]
            if len(unavailable_dependencies) > 0:
                raise DependencyNotAvailableException(f"Dependency/ies '{unavailable_dependencies}' "
                                                      f"do/es not exist and can not be produced.")

            # Add the tasks that produce path_deps and remove such deps from the path_deps
            task.task_dependencies = []
            for t in ([product_to_task[d] for d in path_deps if d in product_to_task] + task_deps):
                if not t in task.task_dependencies: task.task_dependencies.append(t)

            task.path_dependencies = \
                [d for d in path_deps if d not in product_to_task]

        # Adding the backlinks
        for task in self.tasks:
            for t_dep in task.task_dependencies:
                t_dep.add_dependent_task(task)

    def run(self) -> None:
        enable_proxy()
        self._solve_order()
        self.handled_tasks = []
        while True:
            try:
                # Submit new runnable jobs.
                for task in self.tasks:
                    if task in self.handled_tasks: continue
                    if task.is_ready_for_execution() or self.depioExecutor.handles_dependencies():
                        if task.should_run():
                            if not self.SUBMIT_ONLY_IF_RUNNABLE:
                                self.depioExecutor.submit(task, task.task_dependencies)
                                self.handled_tasks.append(task)
                            elif task.is_ready_for_execution():
                                self.depioExecutor.submit(task, task.task_dependencies)
                                self.handled_tasks.append(task)


                # Check the status of all tasks
                if not self.QUIET: self._print_tasks()
                if all(task.is_in_terminal_state for task in self.tasks):
                    if any(task.is_in_failed_terminal_state for task in self.tasks):
                        self.exit_with_failed_tasks()
                    else:
                        self.exit_successful()

                time.sleep(self.REFRESHRATE)

            except KeyboardInterrupt:
                print("Stopping execution because of a keyboard interrupt!")
                self.exit_with_failed_tasks()

    def _get_text_for_task(self, task):
        status = task.status

        formatted_status = colored(f"{status[1].upper():<{len('DEP. FAILED')}s}", status[2])
        formatted_slurmstatus = colored(f"{status[3]:<{len('OUT_OF_MEMORY')}s}", status[2])
        return [
            task.is_in_successful_terminal_state,
            task.id,
            task.name,
            task.slurmid,
            formatted_slurmstatus,
            formatted_status,
            #task.get_duration(),
            [t._queue_id for t in task.task_dependencies],
            #[str(d) for d in task.dependencies if isinstance(d, Path)],
            #[str(p) for p in task.products]
        ]

    def _clear_screen(self):
        if self.CLEAR_SCREEN: sys.stdout.write("\033[2J\033[H")

    def _print_tasks(self):
        self._clear_screen()
        headers = ["ID", "Name", "Slurm ID", "Slurm Status", "Status", "Task Deps."]

        tasks_data = [self._get_text_for_task(task) for task in self.tasks]

        # Generate and print the histogram for tasks_data[5]
        histogram = {}
        for task_data in tasks_data:
            s = task_data[5]
            histogram[s] = histogram.get(s, 0) + 1

        if self.HIDE_SUCCESSFUL_TERMINATED_TASKS:
            tasks_data = [td[1:] for td in tasks_data if not td[0] == True]
        else:
            tasks_data = [td[1:] for td in tasks_data]

        table_str = tabulate(tasks_data, headers=headers, tablefmt="plain")
        print()
        print("Tasks:")
        print(table_str)

        print("\nSummary:")
        for string, count in histogram.items():
            print(f"{string}: {count:4d} tasks")


    def exit_with_failed_tasks(self) -> None:
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
        # Print the overview with the updated status once more.
        for task in self.tasks:
            task.is_ready_for_execution()
        if not self.QUIET: self._print_tasks()

        print("All jobs done! Exit.")
        exit(0)


__all__ = [Pipeline]
