"""
demo_progress.py — per-task progress reporting
===============================================
Demonstrates:
  - depio.current_progress() inside a task function
  - prog.update(total=, phase=, message=) to set context
  - prog.advance() to increment the step counter
  - Progress bar shown in the TUI list view and the task detail view (Enter)
  - Multiple tasks reporting progress concurrently (ParallelExecutor)

DAG
----
  [prepare]  ──►  data.txt
      │
      ├──►  [train_fast]   ──►  model_fast.bin
      └──►  [train_slow]   ──►  model_slow.bin
"""

import time
from typing import Annotated

import pathlib

import depio
from depio.BuildMode import BuildMode
from depio.Executors import ParallelExecutor
from depio.Pipeline import Pipeline
from depio.Task import Product, Dependency
from depio.decorators import task

BLD = pathlib.Path("build") / "progress_demo"
BLD.mkdir(parents=True, exist_ok=True)

pipeline = Pipeline(
    depioExecutor=ParallelExecutor(),
    name="Progress Demo",
    clear_screen=True,
)


@task("prepare", buildmode=BuildMode.ALWAYS)
def prepare(output: Annotated[pathlib.Path, Product]):
    prog = depio.current_progress()
    steps = 8
    prog.update(total=steps, phase="prepare")
    for i in range(steps):
        time.sleep(0.15)
        prog.advance()
        prog.update(message=f"writing batch {i + 1}/{steps}")
    output.write_text("\n".join(f"sample_{i}" for i in range(200)))


@task("train_fast", buildmode=BuildMode.ALWAYS)
def train_fast(
    data: Annotated[pathlib.Path, Dependency],
    model: Annotated[pathlib.Path, Product],
):
    prog = depio.current_progress()
    epochs = 12
    prog.update(total=epochs, phase="train")
    best = 0.0
    for epoch in range(epochs):
        time.sleep(0.2)
        acc = 0.5 + epoch * 0.04 + 0.01 * (epoch % 3)
        best = max(best, acc)
        prog.advance()
        prog.update(message=f"epoch {epoch + 1}  acc {acc:.3f}  best {best:.3f}")
    model.write_bytes(b"fast_model_weights")


@task("train_slow", buildmode=BuildMode.ALWAYS)
def train_slow(
    data: Annotated[pathlib.Path, Dependency],
    model: Annotated[pathlib.Path, Product],
):
    prog = depio.current_progress()
    phases = [("warmup", 4, 0.3), ("train", 20, 0.15), ("finetune", 6, 0.25)]
    total = sum(n for _, n, _ in phases)
    prog.update(total=total, phase="warmup")
    step = 0
    for phase_name, n_steps, delay in phases:
        prog.update(phase=phase_name)
        for i in range(n_steps):
            time.sleep(delay)
            step += 1
            prog.update(current=step, message=f"{phase_name} step {i + 1}/{n_steps}")
    model.write_bytes(b"slow_model_weights")


data = pipeline.add_task(prepare(BLD / "data.txt"))
pipeline.add_task(train_fast(BLD / "data.txt", BLD / "model_fast.bin"))
pipeline.add_task(train_slow(BLD / "data.txt", BLD / "model_slow.bin"))

exit(pipeline.run())
