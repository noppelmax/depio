from typing import Set
import pathlib

from .Task import Task
from .Executors import TaskExecutor


class TaskNotInQueueException(Exception):
    pass

class ProductAlreadyRegisteredException(Exception):
    pass

class DependencyNotAvailableException(Exception):
    pass

class TaskHandler:
    def __init__(self, executor: TaskExecutor):
        self.tasks = []
        self.executor = executor
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

         # Register output
        for product in task.products:
            self.registered_products.append(product)

        # Register task
        self.tasks.append(task)

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

    def run(self) -> None:
        self._solve_order()
        submitted_tasks: Set[Task] = set()

        def _submit_task(task: Task) -> None:
            if task in submitted_tasks:
                return

            # Execute all dependencies first
            for dependency in task.dependencies_hard:
                assert isinstance(dependency, Task)
                _submit_task(dependency)
            for dependency in task.dependencies_soft:
                if isinstance(dependency, Task):
                    _submit_task(dependency)
                else:
                    assert isinstance(dependency, pathlib.Path)

            # Execute the task
            self.executor.submit(task)
            submitted_tasks.add(task)

        # Execute all tasks in the queue
        for task in self.tasks:
            _submit_task(task)

__all__ = [TaskHandler]
