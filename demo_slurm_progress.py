"""
demo_slurm_progress.py — progress reporting over SLURM
=======================================================
Runs three tasks on the local Docker SLURM cluster (partition=test).
Each task reports progress back to the main process via the TCP
ProgressServer, which is displayed live in the TUI.

Run from inside the depio-slurm container:
    python demo_slurm_progress.py
"""

import time
from pathlib import Path
from typing import Annotated

import depio
from depio.BuildMode import BuildMode
from depio.Executors import SubmitItExecutor
from depio.Pipeline import Pipeline
from depio.Task import Product, Dependency
from depio.decorators import task

SLURM_DIR = Path("/tmp/slurm-jobs/demo_progress")
BLD = Path("build") / "slurm_progress_demo"
BLD.mkdir(parents=True, exist_ok=True)
SLURM_DIR.mkdir(parents=True, exist_ok=True)

executor = SubmitItExecutor(
    folder=SLURM_DIR,
    parameters={
        "slurm_partition": "test",
        "slurm_time": 5,       # minutes
        "slurm_mem": "512M",
        "gpus_per_node": 0,
    },
)

pipeline = Pipeline(
    depioExecutor=executor,
    name="SLURM Progress Demo",
    clear_screen=True,
)


@task("generate", buildmode=BuildMode.ALWAYS)
def generate(output: Annotated[Path, Product]):
    prog = depio.current_progress()
    n = 10
    prog.update(total=n, phase="generate")
    for i in range(n):
        time.sleep(0.5)
        prog.advance()
        prog.update(message=f"writing row {i + 1}/{n}")
    output.write_text("\n".join(f"item_{i}" for i in range(100)))


@task("process_a", buildmode=BuildMode.ALWAYS)
def process_a(
    src: Annotated[Path, Dependency],
    dst: Annotated[Path, Product],
):
    prog = depio.current_progress()
    steps = 15
    prog.update(total=steps, phase="process_a")
    data = src.read_text().splitlines()
    results = []
    for i, line in enumerate(data[:steps]):
        time.sleep(0.4)
        results.append(line.upper())
        prog.advance()
        prog.update(message=f"processed {i + 1}/{steps}")
    dst.write_text("\n".join(results))


@task("process_b", buildmode=BuildMode.ALWAYS)
def process_b(
    src: Annotated[Path, Dependency],
    dst: Annotated[Path, Product],
):
    prog = depio.current_progress()
    steps = 20
    prog.update(total=steps, phase="process_b")
    data = src.read_text().splitlines()
    results = []
    for i, line in enumerate(data[:steps]):
        time.sleep(0.3)
        results.append(f"{line}_{i}")
        prog.advance()
        prog.update(message=f"step {i + 1}/{steps}")
    dst.write_text("\n".join(results))


pipeline.add_task(generate(BLD / "data.txt"))
pipeline.add_task(process_a(BLD / "data.txt", BLD / "result_a.txt"))
pipeline.add_task(process_b(BLD / "data.txt", BLD / "result_b.txt"))

exit(pipeline.run())
