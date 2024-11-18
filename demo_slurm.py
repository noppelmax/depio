import os
from typing import Annotated
import pathlib
import submitit
import time

from depio.Executors import SubmitItExecutor
from depio.TaskHandler import TaskHandler
from depio.decorators import task
from depio.Task import Product, Dependency

BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

SLURM = pathlib.Path("slurm")
SLURM.mkdir(exist_ok=True)

internal_executor = submitit.AutoExecutor(folder=SLURM)

# Configure the slurm jobs
# TODO We might want to do this on a job level
os.environ["SBATCH_RESERVATION"] = "isec-team"
internal_executor.update_parameters(
            time="48:00:00",
            partition="gpu",
            mem_gb=32
)

depioExecutor = SubmitItExecutor(internal_executor=internal_executor)
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



funcdec(BLD/"output.txt", BLD/"final.txt",sec=4)
funcdec(BLD/"input.txt", BLD/"output.txt",sec=5)

taskhandler.run()
