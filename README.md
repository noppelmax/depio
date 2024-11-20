# depio
![python-package.yml](https://github.com/noppelmax/depio/actions/workflows/python-package.yml/badge.svg)

A simple task manager with slurm integration.

## How to use
There are two ways how you can use `depio`. The first (1) is via decorators and the second (2) is a function interface.
Before we consider the differences we start with parts that are similar for both.

Setting up a **Pipeline**:
```python
from depio.Pipeline import Pipeline
from depio.Executors import ParallelExecutor

defaultpipeline = Pipeline(depioExecutor=ParallelExecutor())
```
To this pipeline object you can now add `Stages`.



### (1) Use via decorators



### (2) Use via the functional interface


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

