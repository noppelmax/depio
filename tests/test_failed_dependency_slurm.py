import pathlib
import os
import submitit
import unittest

from depio.Executors import SubmitItExecutor
from depio.Pipeline import Pipeline
from depio.Task import Task


def failingfunction():
    raise Exception("This function raises an exception")

def workingfunction(s: unittest.TestCase):
    s.fail("This function should never be called")

# class TestParseAnnotationForMetaclass(unittest.TestCase):
#
#     def test_failed_dependency_slurm(self):
#         SLURM = pathlib.Path("./slurm")
#         SLURM.mkdir(exist_ok=True)
#
#         # Configure the slurm jobs
#         os.environ["SBATCH_RESERVATION"] = "isec-team"
#         executor = submitit.AutoExecutor(folder=SLURM)
#         executor.update_parameters(slurm_partition="gpu", mem_gb=12, cpus_per_task=10, nodes=1, slurm_ntasks_per_node=1,
#                                    gpus_per_node=1, timeout_min=60 * 48)
#
#         pipeline = Pipeline(depioExecutor=SubmitItExecutor(internal_executor=executor))
#
#
#         t1 = Task("t1", failingfunction)
#         t2 = Task("t2", workingfunction , [self], depends_on=[t1])
#         pipeline.add_task(t2)
#         pipeline.add_task(t1)
#
#         pipeline.run()
