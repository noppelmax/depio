from typing import Annotated
import pathlib
import time

from depio.Executors import DemoTaskExecutor
from depio.TaskHandler import TaskHandler
from depio.decorators import task
from depio.Task import Product, Dependency

from src.depio.Executors import ParallelExecutor

depioExecutor = DemoTaskExecutor()
depioExecutor = ParallelExecutor()
taskhandler = TaskHandler(depioExecutor=depioExecutor)

# Use the decorator with args and kwargs
@task("datapipeline", taskhandler)
def funcdec(input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product],
            sec:int
            ):
    print(f"func dec reading from {input} and writing to {output}")
    time.sleep(sec)
    with open(output,'w') as f:
        f.write("Hallo from depio")
    return 1


BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

funcdec(BLD/"output.txt", BLD/"final.txt",sec=3)
funcdec(BLD/"input.txt", BLD/"output.txt",sec=5)

taskhandler.run()
