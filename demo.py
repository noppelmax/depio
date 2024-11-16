from typing import Annotated
import pathlib
from depio.Executors import DemoTaskExecutor
from depio.TaskHandler import TaskHandler
from depio.decorators import task
from depio.Task import Product, Dependency

executor = DemoTaskExecutor()
taskhandler = TaskHandler(executor=executor)

def func1():
    print("func 1 called")


# Use the decorator with args and kwargs
@task("datapipeline", taskhandler)
def funcdec(input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product]
            ):
    print(f"func dec reading from {input} and writing to {output}")

funcdec(pathlib.Path("output.txt"), pathlib.Path("final.txt"))
funcdec(pathlib.Path("input.txt"), pathlib.Path("output.txt"))

taskhandler.run()
