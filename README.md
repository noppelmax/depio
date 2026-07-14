<h4 align="center">
  <a href="https://#">Install</a>
  ·
  <a href="https://#">Configure</a>
  ·
  <a href="https://#">Docs</a>
</h4>


# depio
![python-package.yml](https://github.com/noppelmax/depio/actions/workflows/python-package.yml/badge.svg)

A Python task pipeline manager with DAG-based dependency resolution and an interactive TUI. Supports local parallel execution and SLURM/HPC clusters via `submitit`.

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
We define a function `slowfunction` that takes a couple of seconds to produce an output file from a given input file.
We annotate the function with the `@task` decorator and use `typing.Annotated` to tell depio which arguments are dependencies and which are products of the function.
depio will parse this for us and set up the dependencies between the tasks.
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


defaultpipeline.add_task(Task("functionaldemo1", slowfunction, [BLD/"input.txt", BLD/"output1.txt"], {"sec": 2}))
defaultpipeline.add_task(Task("functionaldemo2", slowfunction, [BLD/"output1.txt", BLD/"final1.txt"], {"sec": 1}))

exit(defaultpipeline.run())
```

The main difference is that you have to pass the args and kwargs manually, but you can also overwrite the task name.
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
t2 = defaultpipeline.add_task(Task("functionaldemo2", slowfunction, [1]))
t3 = defaultpipeline.add_task(Task("functionaldemo3", slowfunction, [1]))
t4 = defaultpipeline.add_task(Task("functionaldemo4", slowfunction, [2], depends_on=[t3]))
t5 = defaultpipeline.add_task(Task("functionaldemo5", slowfunction, [3], depends_on=[t4]))

exit(defaultpipeline.run())
```

Notice how depio deduplicates tasks: if the same function is called with identical arguments, `add_task` returns the already-registered instance rather than adding a duplicate.
When using the functional interface with hard-coded dependencies (`depends_on`), always save the return value of `add_task` and use that object when wiring up downstream tasks.

## How to use with Slurm
You just have to replace the executor with a `SubmitItExecutor` like so:
```python
import os
from typing import Annotated
import pathlib
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

defaultpipeline.add_task(slowfunction(BLD/"input.txt", BLD/"output1.txt", sec=2))
defaultpipeline.add_task(slowfunction(BLD/"input.txt", BLD/"output2.txt", sec=3))
defaultpipeline.add_task(slowfunction(BLD/"output1.txt", BLD/"final1.txt", sec=1))

exit(defaultpipeline.run())
```

SLURM executor settings (partition, time limit, memory, GPU count, job queue limits) can be configured in `.depio/config.json` — see the **Configuration** section below.

## How to use with Hydra
Here is how you can use it with hydra:
```python
import os
from typing import Annotated
import pathlib
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

os.environ["SBATCH_RESERVATION"] = "isec-team"
defaultpipeline = Pipeline(depioExecutor=SubmitItExecutor(folder=SLURM))

@task("datapipeline")
def slowfunction(
            input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product],
            cfg: Annotated[DictConfig, IgnoredForEq],
            sec:int = 0
            ):
    print(f"A function that is reading from {input} and writing to {output} in {sec} seconds.")
    time.sleep(sec)
    with open(output,'w') as f:
        f.write(OmegaConf.to_yaml(cfg))

@hydra.main(version_base=None, config_path=str(CONFIG), config_name="config")
def my_hydra(cfg: Annotated[DictConfig, IgnoredForEq]) -> None:

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
To use different build modes you can set the `buildmode` parameter when creating the task:

```python
from depio.BuildMode import BuildMode

@task("datapipeline", buildmode=BuildMode.ALWAYS)
def funcdec(output: Annotated[pathlib.Path, Product]):
    with open(output,'w') as f:
        f.write("Hallo from depio")
```

There are seven values to choose from:
- `BuildMode.NEVER` — Never run the task; always skip it.
- `BuildMode.IF_MISSING` — Run if any product file is missing. Does not check input timestamps or upstream task results.
- `BuildMode.ALWAYS` — Always run, unconditionally.
- `BuildMode.IF_NEW` — Run if any product is missing, or if any upstream task ran in this pipeline invocation.
- `BuildMode.IF_OLDER` — Run if any product is missing, or if any product is older than its path dependencies (make-style timestamp comparison).
- `BuildMode.IF_OLD` — Run if any product is missing, or if any product is older than a configurable age threshold (`max_age_seconds` in `.depio/config.json`, default 24 h). Can also be set per-task via `@task(..., max_age=3600)`.
- `BuildMode.IF_CODE_CHANGED` — Run if any product is missing, or if the task function's source code has changed since the last successful run. Hashes are stored in `.depio/task_hashes.json`. Enable per-task via `@task(..., track_code=True)`.

In addition, there are flags you can pass to the pipeline:
- `clear_screen` : `bool` — Clear the screen on each refresh so the TUI stays at the top.
- `hide_successful_terminated_tasks` : `bool` — Hide successfully finished or skipped tasks from the list.
- `submit_only_if_runnable` : `bool` — Only submit tasks that are immediately ready for execution.
- `refreshrate` : `float` — Polling interval in seconds. Can also be set in `.depio/config.json`.
- `quiet` : `bool` — Disable the TUI entirely; useful for scripted or CI runs.

## Per-task progress

Tasks can report structured progress that depio displays in the TUI. Call `depio.current_progress()` from inside a running task to get the `TaskProgress` object bound to that thread.

```python
import depio
from depio.decorators import task
from typing import Annotated
import pathlib
from depio.Task import Product

@task("mypipeline")
def train(output: Annotated[pathlib.Path, Product], epochs: int = 10):
    prog = depio.current_progress()
    prog.update(total=epochs, phase="training")

    for epoch in range(epochs):
        # ... do work ...
        prog.advance()                           # increment by 1
        prog.update(message=f"loss {loss:.4f}") # optional status text
```

`TaskProgress` fields (all optional):

| Field | Type | Meaning |
|-------|------|---------|
| `current` | `int` | Steps completed so far |
| `total` | `int \| None` | Total steps (enables `fraction` property) |
| `message` | `str` | Free-form status text (e.g. a metric) |
| `phase` | `str` | Current phase label (e.g. `"warmup"`, `"eval"`) |

Key methods:

- `prog.update(current=…, total=…, message=…, phase=…)` — atomically set any subset of fields
- `prog.advance(n=1)` — increment `current` by `n`
- `prog.snapshot()` — return a consistent `dict` copy of all fields
- `prog.fraction` — `float` 0–1 if `total` is set, else `None`

`current_progress()` returns `None` when called outside a running task, so callers that may be used both inside and outside a pipeline can guard with `if prog := depio.current_progress()`.

## Hooks
depio supports callbacks that fire when a task or the whole pipeline finishes:

```python
from depio.hooks import TaskResult, PipelineResult

def on_done(result: TaskResult):
    print(f"{result.name} finished with status {result.status}")

pipeline = Pipeline(
    depioExecutor=ParallelExecutor(),
    on_task_finished=on_done,
)
```

To automatically save each task's stdout/stderr to disk, use the built-in save hook:
```python
from pathlib import Path

pipeline = Pipeline(
    depioExecutor=ParallelExecutor(),
    on_task_finished=Pipeline.make_save_hook(Path("outputs/")),
)
```

Available callbacks on `Pipeline`: `on_task_finished`, `on_task_failed`, `on_pipeline_finished`.
Per-task callbacks can also be set directly on `Task` objects: `on_finished`, `on_task_failed`.

## Configuration
On first run, depio creates `.depio/config.json` with sensible defaults:

```json
{
  "pipeline": { "refreshrate": 1.0 },
  "task": {
    "default_buildmode": "IF_MISSING",
    "max_age_seconds": 86400,
    "code_hash_method": "source"
  },
  "executor": {
    "parallel": {},
    "slurm": {
      "max_jobs_pending": 45,
      "max_jobs_queued": 20,
      "partition": "gpu",
      "time_minutes": 2880,
      "mem_gb": 32,
      "gpus_per_node": 0
    }
  }
}
```

Edit this file to change defaults for your project without touching any code.

## How to develop
Create an editable install:

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
