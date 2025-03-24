<h4 align="center">
  <a href="https://#">Install</a>
  ·
  <a href="https://#">Configure</a>
  ·
  <a href="https://#">Docs</a>
</h4>


# depio
![python-package.yml](https://github.com/noppelmax/depio/actions/workflows/python-package.yml/badge.svg)

A simple task manager with slurm integration.

## How to use
We start with setting up a **Pipeline**:
```python
from depio.Pipeline import Pipeline
from depio.Executors import ParallelExecutor

defaultpipeline = Pipeline(depioExecutor=ParallelExecutor())
```
To this pipeline object you can now add **Task**s.
There are two ways how you can add tasks. 
The first (1) is via decorators and the second (2) is a function interface.
Before we consider the differences we start with parts that are similar for both.

### (1) Use via decorators
To add tasks via decorators you need use the `@task("datapipeline")` decorator from `depio.decorators.task`:
```python
import time
import pathlib
from typing import Annotated

from depio.Pipeline import Pipeline
from depio.Executors import ParallelExecutor
from depio.Task import Product, Dependency
from depio.decorators import task

defaultpipeline = Pipeline(depioExecutor=ParallelExecutor())

BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

print("Touching an initial file")
(BLD/"input.txt").touch()

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
```

First, we add a folder `build` in which we want to produce our artifacts.
Then, we create an initial artifact `build/input.txt` via `touch`.
Thereafter, begins the interesting part: 
We define a function `slowfunction` that takes a couple of seconds to produce a output file from a given input file.
We annotate function with the `@task` decorator and use the `typing.Annotated` type to tell depio which arguments are depencendies and which are product of the function.
depion will parse this for us and setup the dependencies between the tasks.
Finally, we add the function calls to the pipeline via `add_task` and `run` the pipeline.
 


### (2) Use via the functional interface

```python
import time
import pathlib
from typing import Annotated

from depio.Pipeline import Pipeline
from depio.Executors import ParallelExecutor
from depio.Task import Product, Dependency
from depio.Task import Task

defaultpipeline = Pipeline(depioExecutor=ParallelExecutor())

BLD = pathlib.Path("build")
BLD.mkdir(exist_ok=True)

print("Touching an initial file")
(BLD/"input.txt").touch()

def slowfunction(
            input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product],
            sec:int = 0
            ):
    print(f"A function that is reading from {input} and writing to {output} in {sec} seconds.")
    time.sleep(sec)
    with open(output,'w') as f:
        f.write("Hallo from depio")


defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [BLD/"input.txt", BLD/"output1.txt", ], { "sec": 2}))
defaultpipeline.add_task(Task("functionaldemo2", slowfunction, [BLD/"output1.txt", BLD/"final1.txt", ],{ "sec": 1}))

exit(defaultpipeline.run())
```

This will produce the following output:
```
Tasks:
  ID  Name             Slurm ID    Slurm Status    Status       Task Deps.    Path Deps.             Products
   1  functionaldemo1                              FINISHED     []            ['build/input.txt']    ['build/output1.txt']
   2  functionaldemo2                              FINISHED     [1]           ['build/output1.txt']  ['build/final1.txt']
All jobs done! Exit.
```

The main difference is that you have to pass the args and kwargs manually, but therefore can also overwrite the task name.
However you can also define the DAG by yourself:
```python
import time

from depio.Pipeline import Pipeline
from depio.Executors import ParallelExecutor
from depio.Task import Task

defaultpipeline = Pipeline(depioExecutor=ParallelExecutor())

def slowfunction(sec:int = 0):
    print(f"A function that is doing something for {sec} seconds.")
    time.sleep(sec)

t1 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t2 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t3 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t4 = defaultpipeline.add_task(Task("functionaldemo2", slowfunction, [2], depends_on=[t3]))
t5 = defaultpipeline.add_task(Task("functionaldemo3", slowfunction, [3], depends_on=[t4]))

exit(defaultpipeline.run())
```

This should produce the following output:
```
Tasks:
  ID  Name             Slurm ID    Slurm Status    Status       Task Deps.    Path Deps.    Products
   1  functionaldemo1                              FINISHED     []            []            []
   2  functionaldemo2                              FINISHED     [1]           []            []
   3  functionaldemo3                              FINISHED     [2]           []            []
All jobs done! Exit.
```

Notice how it produced only three tasks instead of five.
The reason is that the first three task are the same function with the same arguments.
depio is merging these together.
When using the functional interface as above with hard coded dependencies between the task (`depends_on`), the `add_task` function will return the earliest registered task with the given function and arguments.
You hence have to save the return value as the task object and relate to this object.

## How to use with Slurm
You just have to replace the pipeline with a slurm pipeline like so:
```python
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
os.environ["SBATCH_RESERVATION"] = "<your reservation>"
defaultpipeline = Pipeline(depioExecutor=SubmitItExecutor(folder=SLURM))

# Use the decorator with args and kwargs
@task("datapipeline")
def slowfunction(
            input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product],
            sec:int = 0
            ):
    print(f"A function that is reading from {input} and writing to {output} in {sec} seconds.")
    time.sleep(sec)
    with open(output,'w') as f:
        f.write("Hallo from depio")

defaultpipeline.add_task(slowfunction(BLD/"input.txt", BLD/"output1.txt",sec=2))
defaultpipeline.add_task(slowfunction(BLD/"input.txt", BLD/"output2.txt",sec=3))
defaultpipeline.add_task(slowfunction(BLD/"output1.txt", BLD/"final1.txt",sec=1))

exit(defaultpipeline.run())
```

## How to use with Hydra
Here is how you can use it with hydra:
```python
import os
from typing import Annotated
import pathlib
import submitit
import time

from omegaconf import DictConfig, OmegaConf
import hydra

from depio.Executors import SubmitItExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency, IgnoredForEq

SLURM = pathlib.Path("slurm")
SLURM.mkdir(exist_ok=True)

CONFIG = pathlib.Path("config")
CONFIG.mkdir(exist_ok=True)

# Configure the slurm jobs
os.environ["SBATCH_RESERVATION"] = "isec-team"
defaultpipeline = Pipeline(depioExecutor=SubmitItExecutor(folder=SLURM))

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
```

Then you can run hydra's multiruns to generate a bunch of tasks:
```bash
python demo_hydra.py -m attack=ours,otherattack1,otherattack2
```

Or you can use it for sweeps also.


## How to skip/build Tasks
To use different skip and build mode you can set the `buildmode` parameter, when creating the task.

```python
@task("datapipeline", buildmode=BuildMode.ALWAYS)
def funcdec(output: Annotated[pathlib.Path, Product]):

    with open(output,'w') as f:
        f.write("Hallo from depio")
    return 1
```

There are three values to chose from
- `BuildMode.NEVER`: Never run the task.
- `BuildMode.IF_MISSING`: Run this tasks if one of the output files is missing. This option does not check if a new input is given or if a previous task (with a dependency) is run.  
- `BuildMode.ALWAYS`: Always run this task.
- `BuildMode.IF_NEW`: Run if the inputs are newer as the output file, or if any of the previous tasks (producing a dependency) is run.

In addition, there are flags, you can hand over to the pipeline:
- `clear_screen` : bool : If set, at every refresh it tries to clear the screen such that the table is always on the top of the screen. Does not work in all terminals right now.
- `hide_successful_terminated_tasks` : bool : If set, successfully terminated (skipped,finished) tasks do not show up in the list.
- `submit_only_if_runnable` : bool : If set, only ready for execution jobs get submitted. 
- `refreshrate` : float : The refreshrate of the list in seconds. It is just lower bound and added as a sleep before the next set of states is queried from the executor.

## How to develop
Create an editable egg and install it.

```bash
pip install -e .
```

## How to test
Run
```bash
pytest
```

## Licence
See [LICENCE](LICENSE).

## Security
See [SECURITY](SECURITY.md).


