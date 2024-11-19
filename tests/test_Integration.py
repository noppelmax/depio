import pathlib
import time
import pytest
from typing import Annotated
from depio.Executors import DemoTaskExecutor
from depio.Pipeline import Pipeline
from depio.decorators import task
from depio.Task import Product, Dependency
from src.depio.Executors import ParallelExecutor
from src.depio import stdio_helpers

# Enable proxy for stdio_helpers
stdio_helpers.enable_proxy()

# Define paths
BLD = pathlib.Path("build_integration_tests")
BLD.mkdir(exist_ok=True)

# Initialize executors and pipeline
depioExecutor = DemoTaskExecutor()
depioExecutor = ParallelExecutor()
defaultpipeline = Pipeline(depioExecutor=depioExecutor)


# Use the decorator with args and kwargs
@task("datapipeline")
def funcdec(input: Annotated[pathlib.Path, Dependency],
            output: Annotated[pathlib.Path, Product],
            sec: int):
    print(f"func dec reading from {input} and writing to {output}")
    time.sleep(sec)
    if input == BLD / "output1.txt":
        raise Exception("Demo exception")
    with open(output, 'w') as f:
        f.write("Hallo from depio")
    return 1


# Create paths for integration tests
input_path = BLD / "input.txt"
output1_path = BLD / "output1.txt"
final1_path = BLD / "final1.txt"
final_final1_path = BLD / "final_final1.txt"


@pytest.fixture(scope="module")
def setup_files():
    # Setup initial files for the test
    with open(input_path, 'w') as f:
        f.write("Initial input for testing")
    yield
    # Teardown: Clean up files after tests
    for path in [input_path, output1_path, final1_path, final_final1_path]:
        if path.exists():
            path.unlink()


def test_pipeline_successful_execution(setup_files):
    local_pipeline = Pipeline(depioExecutor=ParallelExecutor())

    local_pipeline.add_task(funcdec(input_path, output1_path, sec=1))
    local_pipeline.add_task(funcdec(output1_path, final1_path, sec=1))
    local_pipeline.add_task(funcdec(final1_path, final_final1_path, sec=1))

    result = local_pipeline.run()

    assert result == 0
    assert output1_path.exists()
    assert final1_path.exists()
    assert final_final1_path.exists()


def test_pipeline_exception_handling(setup_files):
    local_pipeline = Pipeline(depioExecutor=ParallelExecutor())

    local_pipeline.add_task(funcdec(input_path, output1_path, sec=1))
    local_pipeline.add_task(funcdec(output1_path, final1_path, sec=1))
    local_pipeline.add_task(funcdec(final1_path, final_final1_path, sec=1))

    with pytest.raises(Exception, match="Demo exception"):
        local_pipeline.run()


def test_pipeline_individual_task_execution(setup_files):
    local_pipeline = Pipeline(depioExecutor=ParallelExecutor())

    task1 = funcdec(input_path, output1_path, sec=1)
    task2 = funcdec(output1_path, final1_path, sec=1)
    task3 = funcdec(final1_path, final_final1_path, sec=1)

    local_pipeline.add_task(task1)
    local_pipeline.add_task(task2)
    local_pipeline.add_task(task3)

    # Execute only the first task, should not raise exception
    local_pipeline.executor.execute_task(task1)
    assert output1_path.exists()

    # Now execute the entire pipeline, should raise exception on second task
    with pytest.raises(Exception, match="Demo exception"):
        local_pipeline.run()


def test_pipeline_custom_executor(setup_files):
    custom_executor = DemoTaskExecutor()
    local_pipeline = Pipeline(depioExecutor=custom_executor)

    local_pipeline.add_task(funcdec(input_path, output1_path, sec=1))
    local_pipeline.add_task(funcdec(output1_path, final1_path, sec=1))
    local_pipeline.add_task(funcdec(final1_path, final_final1_path, sec=1))

    with pytest.raises(Exception, match="Demo exception"):
        local_pipeline.run()
