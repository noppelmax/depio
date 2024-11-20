import os
from typing import Annotated
import pathlib
import submitit
import time

from depio.Executors import SubmitItExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency

BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

SLURM = pathlib.Path("slurm")
SLURM.mkdir(exist_ok=True)


# Configure the slurm jobs
os.environ["SBATCH_RESERVATION"] = "isec-team"
defaultpipeline = Pipeline(depioExecutor=SubmitItExecutor(folder=SLURM))

# Use the decorator with args and kwargs
@task("datapipeline")
def slowfunction(output: Annotated[pathlib.Path, Product],
            input: Annotated[pathlib.Path, Dependency] = None,
            sec:int = 0
            ):
    print(f"A function that is reading from {input} and writing to {output} in {sec} seconds.")
    time.sleep(sec)
    with open(output,'w') as f:
        f.write("Hallo from depio")

defaultpipeline.add_task(slowfunction(BLD/"output1.txt",input=BLD/"input.txt", sec=2))
defaultpipeline.add_task(slowfunction(BLD/"output2.txt",input=BLD/"input.txt", sec=3))
defaultpipeline.add_task(slowfunction(BLD/"final1.txt",BLD/"output1.txt", sec=1))

exit(defaultpipeline.run())
