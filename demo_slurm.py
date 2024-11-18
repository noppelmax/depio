from typing import Annotated
import pathlib
from depio.Executors import DemoTaskExecutor
from depio.TaskHandler import TaskHandler
from depio.decorators import task
from depio.Task import Product, Dependency

executor = SlurmExecutor()
taskhandler = TaskHandler(executor=executor)

# Use the decorator with args and kwargs
@task("datapipeline", taskhandler)
def funcdec(input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product]
            ):
    print(f"func dec reading from {input} and writing to {output}")
    #with open(output,'w') as f:
    #    f.write("Hallo from depio")


BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

funcdec(BLD/"output.txt", BLD/"final.txt")
funcdec(BLD/"input.txt", BLD/"output.txt")

taskhandler.run()
