from typing import Set
import pathlib
import time

from termcolor import colored
from tabulate import tabulate

from .Task import Task, TaskStatus
from .Executors import AbstractTaskExecutor
from .exceptions import ProductAlreadyRegisteredException, TaskNotInQueueException


class TaskHandler:
    def __init__(self, depioExecutor: AbstractTaskExecutor):
        self.submitted_tasks = None
        self.tasks = []
        self.depioExecutor = depioExecutor
        self.registered_products = []
        print("depio-TaskHandler initialized")

    def add_task(self, task)  -> None:
        # Check is a output is already registered
        for product in task.products:
            if product in self.registered_products:
                raise ProductAlreadyRegisteredException(f"The product {product} is already registered. Each output can only be registered from one task.")

        # Check if the hard dependencies are registered already
        for t in task.dependencies_hard:
            if t not in self.tasks:
                raise TaskNotInQueueException("Add the task into the queue in the correct order.")

         # Register products
        for product in task.products:
            self.registered_products.append(product)

        # Register task
        self.tasks.append(task)
        task.queue_id = len(self.tasks)

    def _solve_order(self) -> None:
        # Obtain a output so task mapping.
        product_to_task = {product: task for task in self.tasks for product in task.products}

        # Assign the soft dependencies to the tasks
        for task in self.tasks:
            task.dependencies_soft = []
            # Verify that each dependency is available and add if yes.
            for dependency in task.dependencies:
                if dependency not in product_to_task and not (isinstance(dependency, pathlib.Path) and dependency.exists()):
                    raise DependencyNotAvailableException(f"No task produces '{dependency}' nor does it exist.")
                # Generate the tasks which generate dependencies
                task.dependencies_soft.append(product_to_task[dependency] if dependency in product_to_task else dependency)

        for task in self.tasks:
            task.task_dependencies = list(filter(lambda x: isinstance(x, Task), task.dependencies_hard + task.dependencies_soft))
            task.path_dependencies = list(filter(lambda x: isinstance(x, pathlib.Path), task.dependencies))

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
            self._submit_task(t_dep) # Recursive call for dependency
            if not t_dep.is_in_successful_terminal_state:
                all_dependencies_are_available = False

            if t_dep.is_in_failed_terminal_state and not task.is_in_failed_terminal_state:
                # If the task is not already in failed state:
                task.set_to_depfailed() # set to depfailed
                is_new_depfail_found = True # Remember that we propagated dependency failures

        for p_dep in task.path_dependencies:
            assert isinstance(p_dep, pathlib.Path)
            if not p_dep.exists():
                all_dependencies_are_available = False

                if not task.is_in_failed_terminal_state:
                    # If the task is not already in failed state:
                    task.set_to_depfailed() # set to depfailed
                    is_new_depfail_found = True # Remember that we propagated dependency failures

        # Execute the task if all dependencies are given
        if all_dependencies_are_available:
            self.depioExecutor.submit(task)
            self.submitted_tasks.add(task)

        return is_new_depfail_found

    def run(self) -> None:
        self._solve_order()
        self.submitted_tasks: Set[Task] = set()

        while True:
            try:
                # Iterate over all tasks in the queue until now new depfail is found
                while True:
                    if all(not self._submit_task(task) for task in self.tasks):
                        break

                # Check the status of all tasks
                all_tasks_in_terminal_state = all(task.is_in_terminal_state for task in self.tasks)
                self._print_tasks()
                if all_tasks_in_terminal_state:
                    if any(task.is_in_failed_terminal_state for task in self.tasks):
                        self.exit_with_failed_tasks()
                    else:
                        self.exit_successful()

            except KeyboardInterrupt:
                print("Stopping execution bc of keyboard interrupt!")
                exit(1)
            time.sleep(0.20)

    def _get_text_for_task(self, task: Task, length, status=None) -> list:
        if status is None:
            status = task.status
        formatted_status = colored(f"{task.statustext(status[0]):<{length}s}", task.statuscolor(status[0]))
        return [
            task.id,
            task.name,
            task.slurmid,
            task.slurmjob_status,
            formatted_status,
            [str(d) for d in task.path_dependencies],
            [str(p) for p in task.products]
        ]

    def _print_tasks(self) -> None:
        headers = ["ID", "Name", "Slurm ID", "Slurm Job Status", "Formatted Status", "Path Dependencies", "Products"]
        tasks_data = []

        statuse = [task.status for task in self.tasks]
        length = max([len(s[1]) for s in statuse])

        for status, task in zip(statuse, self.tasks):
            tasks_data.append(self._get_text_for_task(task, length, status))

        print("Tasks:")
        print(tabulate(tasks_data, headers=headers))

    def exit_with_failed_tasks(self) -> None:
        print()
        print("--------------------------------------------------------------------")
        print("  STDOUT/STDERR of failed tasks:")
        for task in self.tasks:
            if task.status[0] == TaskStatus.FAILED: # Only for task that failed on their own
                print("--------------------------------------------------------------------")
                print(f"  {task.id}: {task.name:20s} | {task.slurmid:10s} | {task.status[1]:15s}")
                print("------ STDOUT ------------------------------------------------------")
                print(task.stdout)
                if task.stderr != "":
                    print("------ STDERR ------------------------------------------------------")
                    print(task.stderr)

        print("--------------------------------------------------------------------")

        print("Exit.")
        exit(1)

    def exit_successful(self) -> None:
        print("All jobs done! Exit.")
        exit(0)
__all__ = [TaskHandler]
