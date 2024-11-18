from typing import Set
import pathlib
import time

from .Task import Task, DependencyNotMetException, ProductNotProducedException, TaskStatus
from .Executors import AbstractTaskExecutor


class TaskNotInQueueException(Exception):
    pass

class ProductAlreadyRegisteredException(Exception):
    pass

class DependencyNotAvailableException(Exception):
    pass

class TaskHandler:
    def __init__(self, depioExecutor: AbstractTaskExecutor):
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
            if task in submitted_tasks: return

            all_dependencies_are_available = True

            # Execute all dependencies first
            for dependency in task.dependencies_hard:
                assert isinstance(dependency, Task)
                _submit_task(dependency)
                if not dependency.is_successfully_terminated:
                    all_dependencies_are_available = False

            for dependency in task.dependencies_soft:
                if isinstance(dependency, Task):
                    _submit_task(dependency)
                    if not dependency.is_successfully_terminated:
                        all_dependencies_are_available = False
                else:
                    assert isinstance(dependency, pathlib.Path)
                    if not dependency.exists():
                        all_dependencies_are_available = False

            # Execute the task
            if all_dependencies_are_available:
                self.depioExecutor.submit(task)
                submitted_tasks.add(task)


        while True:
            # Iterate over all tasks in the queue
            for task in self.tasks:
                try:
                    _submit_task(task)
                except DependencyNotMetException as e:
                    print(e)
                    print("Stopping execution bc of missing dependency!")
                    exit(1)
                except ProductNotProducedException as e:
                    print(e)
                    print("Stopping execution bc of not produced product!")
                    exit(1)
                except KeyboardInterrupt:
                    print("Stopping execution bc of keyboard interrupt!")
                    exit(1)

            # Check the status of all tasks
            all_tasks_in_terminal_state = all(task.is_in_terminal_state for task in self.tasks)
            try:
                if all_tasks_in_terminal_state:
                    self._visualize_tasks()
                    break
                else:
                    self._visualize_tasks()
            except KeyboardInterrupt:
                print("Stopping execution bc of keyboard interrupt!")
                exit(1)
            time.sleep(1)

    def _visualize_tasks(self) -> None:
        print("Tasks: ")
        for task in self.tasks:
            print(f"  {task.name:20s} | {task.status[1]:6s}")
            #print(f"  Hard dependencies: {[str(dep) for dep in task.dependencies_hard]}")
            #print(f"  Soft dependencies: {[str(dep) for dep in task.dependencies_soft]}")
            #print(f"  Products:          {[str(p) for p in task.products]}")


__all__ = [TaskHandler]
