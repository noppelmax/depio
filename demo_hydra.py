from typing import Annotated
import pathlib
import time

from omegaconf import DictConfig, OmegaConf
import hydra

from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency, IgnoredForEq

SLURM = pathlib.Path("slurm")
SLURM.mkdir(exist_ok=True)

CONFIG = pathlib.Path("config")
CONFIG.mkdir(exist_ok=True)

# Configure the slurm jobs
depioExecutor = ParallelExecutor()
defaultpipeline = Pipeline(depioExecutor=depioExecutor, clear_screen=False)

# Use the decorator with args and kwargs
@task("datapipeline")
def slowfunction(
            input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product],
            cfg: Annotated[DictConfig,IgnoredForEq],
            sec:int = 0
            ):
    print(f"A function that is reading from {input} and writing to {output} in {sec} seconds.")
    time.sleep(sec)
    with open(output,'w') as f:
        f.write(OmegaConf.to_yaml(cfg))

@hydra.main(version_base=None, config_path=str(CONFIG), config_name="config")
def my_hydra(cfg: Annotated[DictConfig,IgnoredForEq]) -> None:

    BLD = pathlib.Path(cfg["bld_path"])
    BLD.mkdir(exist_ok=True)

    defaultpipeline.add_task(slowfunction(None, BLD/f"input.txt", cfg, sec=4))
    defaultpipeline.add_task(slowfunction(BLD/"input.txt", BLD/f"output_{cfg['attack'].name}.txt", cfg, sec=2))
    defaultpipeline.add_task(slowfunction(BLD/f"output_{cfg['attack'].name}.txt", BLD/f"final_{cfg['attack'].name}.txt", cfg, sec=1))


if __name__ == "__main__":
    my_hydra()
    exit(defaultpipeline.run())
