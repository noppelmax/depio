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
        task.id = len(self.tasks)

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
            task.task_dependencies = filter(lambda x: isinstance(x, Task), task.dependencies_hard + task.dependencies_soft)
            task.path_dependencies = filter(lambda x: isinstance(x, pathlib.Path), task.dependencies_soft)

    def run(self) -> None:
        self._solve_order()
        submitted_tasks: Set[Task] = set()

        def _submit_task(task: Task) -> bool:
            if task in submitted_tasks: return

            all_dependencies_are_available = True
            is_set_to_depfailed_called = False

            # Execute and check all dependencies first
            for task_dependency in task.task_dependencies:
                assert isinstance(task_dependency, Task)
                _submit_task(task_dependency)
                if not task_dependency.is_in_successful_terminal_state:
                    all_dependencies_are_available = False
                if task_dependency.is_in_failed_terminal_state:
                    if not task.is_in_failed_terminal_state: # If the task is not already in failed state:
                        task.set_to_depfailed() # set to depfailed
                        is_set_to_depfailed_called = True # Remember that we propagated dependency failures

            for path_dependency in task.path_dependencies:
                assert isinstance(path_dependency, pathlib.Path)
                if not path_dependency.exists():
                    all_dependencies_are_available = False
                    if not task.is_in_failed_terminal_state: # If the task is not already in failed state:
                        task.set_to_depfailed() # set to depfailed
                        is_set_to_depfailed_called = True # Remember that we propagated dependency failures

            # Execute the task if all dependencies are given
            if all_dependencies_are_available:
                self.depioExecutor.submit(task)
                submitted_tasks.add(task)

            return is_set_to_depfailed_called

        while True:
            # Iterate over all tasks in the queue
            is_set_to_depfailed_called = True
            while is_set_to_depfailed_called:
                is_set_to_depfailed_called = False
                for task in self.tasks:
                    try:
                        if _submit_task(task):
                            is_set_to_depfailed_called = True
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
                self._visualize_tasks()
                if all_tasks_in_terminal_state:
                    if any(task.is_in_failed_terminal_state for task in self.tasks):
                        exit(1)
            except KeyboardInterrupt:
                print("Stopping execution bc of keyboard interrupt!")
                exit(1)
            time.sleep(1)

    def _visualize_tasks(self) -> None:
        print("Tasks: ")
        for task in self.tasks:
            print(f"  {task.id: 4d}: {task.name:20s} | {task.status[1]:6s}")
            #print(f"  Hard dependencies: {[str(dep) for dep in task.dependencies_hard]}")
            #print(f"  Soft dependencies: {[str(dep) for dep in task.dependencies_soft]}")
            #print(f"  Products:          {[str(p) for p in task.products]}")


__all__ = [TaskHandler]
