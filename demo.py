from typing import Annotated
import pathlib
import time

from depio.Executors import DemoTaskExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency

from src.depio.Executors import ParallelExecutor
from src.depio import stdio_helpers

depioExecutor = ParallelExecutor()
defaultpipeline = Pipeline(depioExecutor=depioExecutor, clear_screen=False)

# Use the decorator with args and kwargs
@task("datapipeline")
def funcdec(
        output: Annotated[pathlib.Path, Product],
        sec: int,
        input: Annotated[pathlib.Path, Dependency] = None
            ):
    print(f"func dec reading from {input} and writing to {output}")
    time.sleep(sec)
    if False and input == BLD/"output1.txt":
        raise Exception("Demo exception")
    with open(output,'w') as f:
        f.write("Hallo from depio")
    return 1


BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

defaultpipeline.add_task(funcdec(BLD/"output1.txt",sec=2,input=BLD/"input.txt"))
t1 = defaultpipeline.add_task(funcdec(BLD/"output2.txt",sec=1,input=BLD/"input.txt"))
defaultpipeline.add_task(funcdec(BLD/"final1.txt",sec=1,input=BLD/"output1.txt"))
defaultpipeline.add_task(funcdec(BLD/"final_final1.txt",sec=2,input=BLD/"final1.txt"))
defaultpipeline.add_task(funcdec(output=BLD/"final_final2.txt",sec=2))

exit(defaultpipeline.run())
