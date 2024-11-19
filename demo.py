from typing import Annotated
import pathlib
import time

from depio.Executors import DemoTaskExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency

from src.depio.Executors import ParallelExecutor
from src.depio import stdio_helpers

depioExecutor = DemoTaskExecutor()
depioExecutor = ParallelExecutor()

defaultpipeline = Pipeline(depioExecutor=depioExecutor, clear_screen=False)

# Use the decorator with args and kwargs
@task("datapipeline")
def funcdec(input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product],
            sec:int
            ):
    print(f"func dec reading from {input} and writing to {output}")
    time.sleep(sec)
    if input == BLD/"output1.txt":
        raise Exception("Demo exception")
    with open(output,'w') as f:
        f.write("Hallo from depio")
    return 1


BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

#funcdec(BLD/"output.txt", BLD/"final.txt",sec=3)
#funcdec(BLD/"input.txt", BLD/"output.txt",sec=5)

defaultpipeline.add_task(funcdec(BLD/"input.txt", BLD/"output1.txt",sec=2))
defaultpipeline.add_task(funcdec(BLD/"output1.txt", BLD/"final1.txt",sec=1))
defaultpipeline.add_task(funcdec(BLD/"final1.txt", BLD/"final_final1.txt",sec=2))

exit(defaultpipeline.run())
