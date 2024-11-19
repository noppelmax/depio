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
internal_executor = submitit.AutoExecutor(folder=SLURM)

# TODO We might want to do this on a job level
TIME_IN_MINUTES = 60 * 48  # 48 hours in minutes
default_params = {
    "time": TIME_IN_MINUTES,
    "partition": "gpu",
    "mem": 32,
    "gpus_per_node": 0
}
internal_executor.update_parameters(**default_params)

depioExecutor = SubmitItExecutor(internal_executor=internal_executor)
defaultpipeline = Pipeline(depioExecutor=depioExecutor)

# Use the decorator with args and kwargs
@task("datapipeline")
def funcdec(input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product],
            sec:int
            ):
    print(f"func dec reading from {input} and writing to {output}")
    time.sleep(sec)
    with open(output,'w') as f:
        f.write("Hallo from depio")



defaultpipeline.add_task(funcdec(BLD/"output.txt", BLD/"final.txt",sec=4))
defaultpipeline.add_task(funcdec(BLD/"input.txt", BLD/"output.txt",sec=5))

defaultpipeline.run()
