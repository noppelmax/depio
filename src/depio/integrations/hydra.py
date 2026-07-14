"""Hydra integration for depio.

Provides :func:`run_hydra_multirun` to execute a depio pipeline across
multiple Hydra configuration variants in one go, using Hydra's compose
API rather than ``@hydra.main``.

Requirements::

    pip install hydra-core omegaconf   # already in depio's core deps

Basic single-config usage::

    from depio.integrations.hydra import run_hydra_multirun
    from depio.Executors import SequentialExecutor

    def build_pipeline(cfg, pipeline):
        pipeline.add_task(my_task(Path(cfg.output), cfg))

    run_hydra_multirun(
        build_pipeline,
        overrides_list=[[]],          # one run, default config
        executor=SequentialExecutor(),
        config_path="config",
        config_name="config",
    )

Multirun across config variants::

    run_hydra_multirun(
        build_pipeline,
        overrides_list=[
            ["attack=zhang"],
            ["attack=dombrowski"],
            ["attack=ours"],
        ],
        executor=ParallelExecutor(),
        config_path="config",
        config_name="config",
    )

Each task added during a variant's ``build_pipeline`` call is tagged with
a human-readable label derived from the overrides.  The label is stored in
``task.description`` and shown in the depio TUI as a "Variant" column.
"""

from pathlib import Path
from typing import Callable, List, Optional, TYPE_CHECKING

try:
    from hydra import compose, initialize_config_dir
    from hydra.core.global_hydra import GlobalHydra
    from omegaconf import DictConfig
except ImportError as _err:
    raise ImportError(
        "hydra-core and omegaconf are required for depio.integrations.hydra. "
        "Install them with: pip install hydra-core omegaconf"
    ) from _err

from ..Pipeline import Pipeline

if TYPE_CHECKING:
    from ..Executors import AbstractTaskExecutor


def _default_label(_cfg: "DictConfig", overrides: List[str]) -> str:
    """Derive a short human-readable label from the override list."""
    return ", ".join(overrides) if overrides else "default"


def run_hydra_multirun(
    pipeline_fn: Callable[[DictConfig, Pipeline], None],
    overrides_list: List[List[str]],
    executor: "AbstractTaskExecutor",
    *,
    config_path: str,
    config_name: str = "config",
    pipeline_name: str = "hydra",
    variant_label_fn: Optional[Callable[[DictConfig, List[str]], str]] = None,
    quiet: bool = False,
) -> Pipeline:
    """Run a depio pipeline across multiple Hydra configuration variants.

    For each set of overrides in *overrides_list*, Hydra's compose API is
    used to build a :class:`~omegaconf.DictConfig`.  The user-supplied
    *pipeline_fn* is then called with that config and the shared
    :class:`~depio.Pipeline.Pipeline`.  Tasks added during each call are
    automatically tagged with a variant label (stored in
    ``task.description``) which the TUI displays in a "Variant" column.

    Tasks that are deduplicated by depio (same function + same
    non-``IgnoredForEq`` arguments across variants) are **shared** and
    will not have a variant label, reflecting that they belong to no single
    variant.

    Args:
        pipeline_fn:        ``(cfg, pipeline) → None``.  Add tasks to
                            *pipeline* for the given config.
        overrides_list:     List of Hydra override lists, one per variant.
                            Use ``[[]]`` for a single default-config run.
        executor:           Any depio executor.
        config_path:        Path to the Hydra config directory (absolute or
                            relative to the calling script's cwd).
        config_name:        Hydra config file name (without ``.yaml``).
        pipeline_name:      Label shown in the TUI pipeline header.
        variant_label_fn:   ``(cfg, overrides) → str``.  Customise the
                            label shown in the Variant column.  Defaults to
                            joining the override strings.
        quiet:              Suppress depio's TUI.  Defaults to ``False``.

    Returns:
        The completed :class:`~depio.Pipeline.Pipeline`.
    """
    if variant_label_fn is None:
        variant_label_fn = _default_label

    abs_config_path = str(Path(config_path).resolve())

    pipeline = Pipeline(
        depioExecutor=executor,
        name=pipeline_name,
        quiet=quiet,
        exit_when_done=True,
    )

    # ── Pass 1: build pipeline, collect which tasks each variant claims ────
    # Wrap pipeline.add_task to record the task instance that is actually
    # stored (which may be an existing task when add_task deduplicates).
    # This is how we detect shared tasks (claimed by more than one variant).

    variant_labels: List[str] = []
    variant_claimed: List[set] = []

    for overrides in overrides_list:
        GlobalHydra.instance().clear()
        with initialize_config_dir(config_dir=abs_config_path, version_base=None):
            cfg = compose(config_name=config_name, overrides=overrides or [])

        variant_labels.append(variant_label_fn(cfg, overrides or []))

        claimed: set = set()
        _orig = pipeline.add_task

        def _tracking_add_task(task, _c=claimed, _o=_orig):
            result = _o(task)
            _c.add(result)
            return result

        pipeline.add_task = _tracking_add_task
        pipeline_fn(cfg, pipeline)
        pipeline.add_task = _orig

        variant_claimed.append(claimed)

    GlobalHydra.instance().clear()

    # ── Pass 2: label tasks that belong to exactly one variant ────────────
    # Tasks claimed by multiple variants are shared — leave description empty
    # so no Variant label appears in the TUI for them.
    for label, claimed in zip(variant_labels, variant_claimed):
        for task in claimed:
            if sum(1 for s in variant_claimed if task in s) == 1:
                task.description = label

    pipeline.run()
    return pipeline
