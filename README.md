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

def slowfunction(output: Annotated[pathlib.Path, Product],
            input: Annotated[pathlib.Path, Dependency] = None,
            sec:int = 0
            ):
    print(f"A function that is reading from {input} and writing to {output} in {sec} seconds.")
    time.sleep(sec)
    with open(output,'w') as f:
        f.write("Hallo from depio")


t1 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t1 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t1 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t2 = defaultpipeline.add_task(Task("functionaldemo2", slowfunction, [2], depends_on=[t1]))
t3 = defaultpipeline.add_task(Task("functionaldemo3", slowfunction, [3], depends_on=[t2]))

exit(defaultpipeline.run())
```

However you can also define the DAG by yourself:
```python
import time
import pathlib

from depio.Pipeline import Pipeline
from depio.Executors import ParallelExecutor
from depio.Task import Task

defaultpipeline = Pipeline(depioExecutor=ParallelExecutor())

def slowfunction(sec:int = 0):
    print(f"A function that is doing something for {sec} seconds.")
    time.sleep(sec)


t1 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t1 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t1 = defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [1]))
t2 = defaultpipeline.add_task(Task("functionaldemo2", slowfunction, [2], depends_on=[t1]))
t3 = defaultpipeline.add_task(Task("functionaldemo3", slowfunction, [3], depends_on=[t2]))

exit(defaultpipeline.run())
```


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

