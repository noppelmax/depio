from typing import Annotated
import pathlib
from depio.Executors import DemoTaskExecutor
from depio.TaskHandler import TaskHandler
from depio.decorators import task
from depio.Task import Product, Dependency

executor = DemoTaskExecutor()
taskhandler = TaskHandler(executor=executor)

# Use the decorator with args and kwargs
@task("datapipeline", taskhandler)
def funcdec(input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product]
            ):
    print(f"func dec reading from {input} and writing to {output}")
    #with open(output,'w') as f:
    #    f.write("Hallo from depio")


funcdec(pathlib.Path("output.txt"), pathlib.Path("final.txt"))
funcdec(pathlib.Path("input.txt"), pathlib.Path("output.txt"))

taskhandler.run()
