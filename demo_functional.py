import time

from depio.Executors import DemoTaskExecutor, ParallelExecutor
from depio.Pipeline import Pipeline
from depio.Task import Task


depioExecutor = DemoTaskExecutor()
depioExecutor = ParallelExecutor()

defaultpipeline = Pipeline(depioExecutor=depioExecutor, clear_screen=False)

def func(x):
    time.sleep(5)
    return x + 1



t1 = defaultpipeline.add_task(Task("functionaldemo1", func, [1]))
t1 = defaultpipeline.add_task(Task("functionaldemo1", func, [1]))
t1 = defaultpipeline.add_task(Task("functionaldemo1", func, [1]))
t2 = defaultpipeline.add_task(Task("functionaldemo2", func, [2], depends_on=[t1]))
t3 = defaultpipeline.add_task(Task("functionaldemo3", func, [3], depends_on=[t2]))

defaultpipeline.run()