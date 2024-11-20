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
                 quiet: bool = False,
                 refreshrate: float = 1.0):
        self.CLEAR_SCREEN: bool = clear_screen
        self.QUIET: bool = quiet
        self.REFRESHRATE: float = refreshrate

        self.name: str = name
        self.submitted_tasks: List[Task] = None
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
        products_already_registered: List[str] = [str(p) for p in task.products if str(p) in set(map(str, self.registered_products))]
        if len(products_already_registered) > 0:
            raise ProductAlreadyRegisteredException(
                f"The product\s {products_already_registered} is/are already registered. Each output can only be registered from one task.")

        # Check if the task dependencies are registered already
        missing_tasks: List[Task] = [t for t in task.dependencies if isinstance(t, Task) and t not in self.tasks]
        if len(missing_tasks) > 0:
            raise TaskNotInQueueException("Add the tasks into the queue in the correct order. The following task/s is/are missing: {missing_tasks}.")

        # Register products
        self.registered_products.update(task.products)

        # Register task
        self.tasks.append(task)
        task.queue_id = len(self.tasks)
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
                raise DependencyNotAvailableException(f"Dependency/ies '{unavailable_dependencies}' do/es not exist and can not be produced.")

            # Add the tasks that produce path_deps and remove such deps from the path_deps
            task.task_dependencies = ([product_to_task[d] for d in path_deps if d in product_to_task] + task_deps)
            task.path_dependencies = [d for d in path_deps if d not in product_to_task]

    def _submit_task(self, task: Task) -> bool:
        """
        Submits the task to the extractor if all dependencies are available.
        Otherwise, the function is called recursively for each dependency.

        :param task:
        :return:
        """
        if task in self.submitted_tasks: return

        all_dependencies_are_available = True
        is_new_depfail_found = False


        # Execute and check all dependencies first
        for t_dep in task.task_dependencies:
            assert isinstance(t_dep, Task)
            self._submit_task(t_dep)  # Recursive call for dependency
            if not t_dep.is_in_successful_terminal_state:
                all_dependencies_are_available = False

            if t_dep.is_in_failed_terminal_state and not task.is_in_failed_terminal_state:
                # If the task is not already in failed state:
                task.set_to_depfailed()  # set to depfailed
                is_new_depfail_found = True  # Remember that we propagated dependency failures

        for p_dep in task.path_dependencies:
            assert isinstance(p_dep, Path)
            if not p_dep.exists():
                all_dependencies_are_available = False

                if not task.is_in_failed_terminal_state:
                    # If the task is not already in failed state:
                    task.set_to_depfailed()  # set to depfailed
                    is_new_depfail_found = True  # Remember that we propagated dependency failures

        # Execute the task if all dependencies are given
        if all_dependencies_are_available or self.depioExecutor.handles_dependencies():
            self.depioExecutor.submit(task, task.task_dependencies)
            self.submitted_tasks.append(task)

        return is_new_depfail_found

    def run(self) -> None:
        enable_proxy()

        self._solve_order()
        self.submitted_tasks: List[Task] = []

        while True:
            try:
                # Iterate over all tasks in the queue until now new depfail is found
                while True:
                    if all(not self._submit_task(task) for task in self.tasks):
                        break

                # Check the status of all tasks
                all_tasks_in_terminal_state = all(task.is_in_terminal_state for task in self.tasks)
                if not self.QUIET: self._print_tasks()
                if all_tasks_in_terminal_state:
                    if any(task.is_in_failed_terminal_state for task in self.tasks):
                        self.exit_with_failed_tasks()
                    else:
                        self.exit_successful()

            except KeyboardInterrupt:
                print("Stopping execution bc of keyboard interrupt!")
                exit(1)
            time.sleep(self.REFRESHRATE)

    def _get_text_for_task(self, task):
        status = task.status

        formatted_status = colored(f"{status[1].upper():<{len('DEP. FAILED')}s}", status[2])
        formatted_slurmstatus = colored(f"{status[3]:<{len('OUT_OF_MEMORY')}s}", status[2])
        return [
            task.id,
            task.name,
            task.slurmid,
            formatted_slurmstatus,
            formatted_status,
            [t.queue_id for t in task.task_dependencies],
            [str(d) for d in task.dependencies if isinstance(d, Path)],
            [str(p) for p in task.products]
        ]

    def _clear_screen(self):
        if self.CLEAR_SCREEN: sys.stdout.write("\033[2J\033[H")

    def _print_tasks(self):
        self._clear_screen()
        headers = ["ID", "Name", "Slurm ID", "Slurm Status", "Status", "Task Deps.", "Path Deps.", "Products"]
        tasks_data = [self._get_text_for_task(task) for task in self.tasks]
        table_str = tabulate(tasks_data, headers=headers, tablefmt="plain")
        print()
        print("Tasks:")
        print(table_str)

    def exit_with_failed_tasks(self) -> None:
        print()
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
                    print(tabulate([[task.stdout.getvalue()]], headers=["STDOUT"], tablefmt="grid"))

        print("Exit.")
        exit(1)

    def exit_successful(self) -> None:
        print("All jobs done! Exit.")
        exit(0)


__all__ = [Pipeline]
