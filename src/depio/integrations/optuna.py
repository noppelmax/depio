"""Optuna integration for depio.

Provides :func:`run_optuna_study` to execute Optuna hyperparameter
searches using any depio executor (Sequential, Parallel, SubmitIt/SLURM).

Requirements::

    pip install optuna          # or: pip install depio[optuna]

Basic usage::

    import optuna
    from depio.Executors import ParallelExecutor
    from depio.integrations.optuna import run_optuna_study

    def objective(trial: optuna.Trial) -> float:
        x = trial.suggest_float("x", -10, 10)
        return (x - 2) ** 2

    study = optuna.create_study(direction="minimize")
    run_optuna_study(study, objective, n_trials=50,
                     executor=ParallelExecutor())
    print(study.best_params)

SLURM / batch usage::

    from depio.Executors import SubmitItExecutor

    executor = SubmitItExecutor(folder="slurm_logs/",
                                parameters={"slurm_time": 30,
                                            "slurm_partition": "gpu"})
    run_optuna_study(study, objective, n_trials=200,
                     executor=executor,
                     batch_size=20)   # 10 SLURM batches of 20 trials each
"""

import math
from typing import Callable, Optional, TYPE_CHECKING

try:
    import optuna
    from optuna.trial import TrialState
except ImportError as _err:
    raise ImportError(
        "optuna is required for depio.integrations.optuna. "
        "Install it with: pip install optuna"
    ) from _err

from ..Task import Task
from ..Pipeline import Pipeline
from ..BuildMode import BuildMode

if TYPE_CHECKING:
    from ..Executors import AbstractTaskExecutor


def _format_params(params: dict, max_len: int = 40) -> str:
    """Format trial.params as a compact, truncated string for the TUI."""
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        parts.append(f"{k}={v:.2g}" if isinstance(v, float) else f"{k}={v}")
    label = ", ".join(parts)
    if len(label) > max_len:
        label = label[: max_len - 1] + "…"
    return label


class _TrackingTrial:
    """Proxy around optuna.Trial that updates task.description after each
    suggest_*() call, so hyperparameter values appear in the TUI while
    the trial is still running rather than only after it completes."""

    def __init__(self, trial: "optuna.Trial", task_ref: list) -> None:
        self._trial = trial
        self._ref = task_ref

    def __getattr__(self, name: str):
        attr = getattr(self._trial, name)
        if name.startswith("suggest_"):
            def _tracked(*args, **kwargs):
                result = attr(*args, **kwargs)
                self._ref[0].description = _format_params(self._trial.params)
                return result
            return _tracked
        return attr


def run_optuna_study(
    study: optuna.Study,
    objective: Callable[[optuna.Trial], float],
    n_trials: int,
    executor: "AbstractTaskExecutor",
    *,
    batch_size: Optional[int] = None,
    pipeline_name: str = "optuna",
    quiet: bool = False,
) -> optuna.Study:
    """Run an Optuna study using a depio executor.

    All ``n_trials`` tasks are registered in a **single** depio
    :class:`~depio.Pipeline.Pipeline`.  Batch boundaries are expressed as
    depio task dependencies: every task in batch *N+1* depends on every task
    in batch *N*, so depio's scheduler enforces the generational ordering
    without any external Python loop.

    ``study.ask()`` is deferred to execution time inside each task body.
    This guarantees that the sampler sees the complete results of the
    previous batch before suggesting the next one.

    Args:
        study:          An already-created ``optuna.Study``.
        objective:      Objective function; receives an ``optuna.Trial``
                        and returns a float to minimise (or maximise).
        n_trials:       Total number of trials to run.
        executor:       Any :class:`~depio.Executors.AbstractTaskExecutor`
                        instance (Sequential, Parallel, SubmitIt, …).
        batch_size:     Number of concurrent trials per generation.
                        Defaults to *n_trials* (all at once, no ordering).
                        Set to the CMA-ES / NSGA population size for
                        evolution-strategy samplers, or to your worker count
                        for TPE / GP-BO.
        pipeline_name:  Label shown in the TUI pipeline header.
        quiet:          Suppress depio's TUI output.  Defaults to ``False``.

    Returns:
        The same ``optuna.Study`` object, updated in-place with all results.

    Dependency structure (batch_size=3, n_trials=9)::

        batch 1:  trial_0  trial_1  trial_2
                      ↓        ↓        ↓   (all → all)
        batch 2:  trial_3  trial_4  trial_5
                      ↓        ↓        ↓
        batch 3:  trial_6  trial_7  trial_8

    Trial outcomes:

    * **Success** — objective returns normally → ``study.tell(trial, value)``.
    * **Pruned**  — objective raises :exc:`optuna.exceptions.TrialPruned`
      → ``study.tell(trial, state=PRUNED)``; depio marks the task FINISHED.
    * **Error**   — any other exception → ``study.tell(trial, state=FAIL)``;
      depio marks the task FAILED; downstream batches are marked DEPFAILED.
    """
    if batch_size is None:
        batch_size = n_trials

    total_batches = math.ceil(n_trials / batch_size)

    pipeline = Pipeline(
        depioExecutor=executor,
        name=pipeline_name,
        quiet=quiet,
        exit_when_done=True,
    )

    prev_batch: list[Task] = []

    for batch_num in range(total_batches):
        start = batch_num * batch_size
        end   = min(start + batch_size, n_trials)

        batch: list[Task] = []
        for idx in range(start, end):
            # task_ref is a one-element list used as a mutable back-reference
            # so that _run_trial can set task.description after the objective
            # populates trial.params (which only happens inside the objective).
            task_ref: list = [None]

            # study.ask() is intentionally inside _run_trial so it executes
            # after all previous-batch study.tell() calls have completed.
            def _make_fn(ref=task_ref) -> Callable[[], None]:
                def _run_trial() -> None:
                    trial = study.ask()
                    tracked = _TrackingTrial(trial, ref)
                    try:
                        value = objective(tracked)
                        study.tell(trial, value)
                    except optuna.exceptions.TrialPruned:
                        study.tell(trial, state=TrialState.PRUNED)
                    except Exception:
                        study.tell(trial, state=TrialState.FAIL)
                        raise  # re-raise so depio marks the task FAILED

                return _run_trial

            task = Task(
                name=f"Trial {idx}",
                func=_make_fn(),
                depends_on=list(prev_batch),  # enforce batch ordering
                buildmode=BuildMode.ALWAYS,
            )
            task_ref[0] = task  # fill the back-reference
            batch.append(task)
            pipeline.add_task(task)

        prev_batch = batch

    pipeline.run()
    return study
