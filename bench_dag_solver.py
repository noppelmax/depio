#!/usr/bin/env python3
"""
Stress test / benchmark for Pipeline._solve_order() and Pipeline.add_task().

Covers six DAG topologies across increasing task counts and reports median
wall-clock time for construction (add_task loop) and solving (_solve_order)
separately, so hot-spots are easy to isolate before optimising.

Topology catalogue
------------------
linear       strict chain  t0 → t1 → … → tN-1
fan_out      single root   t0 → {t1 … tN-1}
fan_in       single sink   {t0 … tN-2} → tN-1
binary_tree  complete binary tree, children depend on their parent
grid         M×M grid, each cell depends on its left and top neighbours
random_dag   each task depends on up to K randomly chosen predecessors

Usage
-----
    poetry run python bench_dag_solver.py
    poetry run python bench_dag_solver.py --sizes 100 500 1000 5000
    poetry run python bench_dag_solver.py --repeats 10
"""
import argparse
import math
import random
import sys
import time
from pathlib import Path
from typing import Callable, List, Tuple

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent / "src"))

from depio.Pipeline import Pipeline
from depio.Task import Task

# ── topology builders ──────────────────────────────────────────────────────


def _fn() -> Callable:
    """Return a distinct callable.

    Two Task objects are considered equal when they share the same function
    *and* the same cleaned_args.  Using a freshly-created function per task
    guarantees that no two tasks are accidentally deduplicated inside
    add_task().  Each call to this factory returns a new function object.
    """
    def fn():
        pass
    return fn


def _pipeline() -> Pipeline:
    return Pipeline(None, quiet=True)


def build_linear(n: int) -> Pipeline:
    """t0 → t1 → … → t{n-1}  (strict chain)."""
    p = _pipeline()
    tasks: List[Task] = []
    for i in range(n):
        t = Task(f"t{i}", _fn(), depends_on=[tasks[i - 1]] if i else [])
        p.add_task(t)
        tasks.append(t)
    return p


def build_fan_out(n: int) -> Pipeline:
    """t0 → {t1, t2, …, t{n-1}}  (one root, N-1 independent leaves)."""
    p = _pipeline()
    root = Task("root", _fn())
    p.add_task(root)
    for i in range(1, n):
        p.add_task(Task(f"t{i}", _fn(), depends_on=[root]))
    return p


def build_fan_in(n: int) -> Pipeline:
    """{t0, …, t{n-2}} → t{n-1}  (N-1 roots, one sink)."""
    p = _pipeline()
    roots: List[Task] = []
    for i in range(n - 1):
        t = Task(f"t{i}", _fn())
        p.add_task(t)
        roots.append(t)
    p.add_task(Task("sink", _fn(), depends_on=roots))
    return p


def build_binary_tree(n: int) -> Pipeline:
    """Complete binary tree with n nodes; children depend on their parent."""
    p = _pipeline()
    tasks: List[Task] = []
    for i in range(n):
        deps = [tasks[(i - 1) // 2]] if i else []
        t = Task(f"t{i}", _fn(), depends_on=deps)
        p.add_task(t)
        tasks.append(t)
    return p


def build_grid(n: int) -> Pipeline:
    """M×M grid (M = ⌈√n⌉); each cell depends on its left and top neighbour."""
    m = math.ceil(math.sqrt(n))
    p = _pipeline()
    grid: List[List[Task]] = []
    idx = 0
    for r in range(m):
        row: List[Task] = []
        for c in range(m):
            deps: List[Task] = []
            if r > 0:
                deps.append(grid[r - 1][c])
            if c > 0:
                deps.append(row[c - 1])
            t = Task(f"t{idx}", _fn(), depends_on=deps)
            p.add_task(t)
            row.append(t)
            idx += 1
        grid.append(row)
    return p


def build_random_dag(n: int, max_parents: int = 4, seed: int = 42) -> Pipeline:
    """Each task depends on up to max_parents randomly chosen predecessors."""
    rng = random.Random(seed)
    p = _pipeline()
    tasks: List[Task] = []
    for i in range(n):
        if i == 0:
            deps: List[Task] = []
        else:
            k = min(i, rng.randint(1, max_parents))
            deps = rng.sample(tasks, k)
        t = Task(f"t{i}", _fn(), depends_on=deps)
        p.add_task(t)
        tasks.append(t)
    return p


# ── correctness smoke-checks (run once at startup) ────────────────────────


def _smoke_check():
    """Assert basic invariants hold for each topology at a small fixed size."""
    # linear: each task has exactly its predecessor as task_dependency
    p = build_linear(5)
    p._solve_order()
    for i, t in enumerate(p.tasks):
        assert len(t.task_dependencies) == (1 if i > 0 else 0), \
            f"linear: task {i} should have {1 if i > 0 else 0} dep(s)"

    # fan_out: every leaf has the root as its only task_dependency
    p = build_fan_out(6)
    p._solve_order()
    root = p.tasks[0]
    for leaf in p.tasks[1:]:
        assert leaf.task_dependencies == [root], "fan_out: leaf missing root dep"
    assert len(root.dependent_tasks) == 5, "fan_out: root should have 5 dependents"

    # fan_in: sink has all roots as task_dependencies
    p = build_fan_in(5)
    p._solve_order()
    sink = p.tasks[-1]
    assert len(sink.task_dependencies) == 4, "fan_in: sink should have 4 deps"

    # binary_tree depth-2: 7 nodes, children depend on their parent
    p = build_binary_tree(7)
    p._solve_order()
    assert p.tasks[1].task_dependencies == [p.tasks[0]]
    assert p.tasks[2].task_dependencies == [p.tasks[0]]
    assert p.tasks[3].task_dependencies == [p.tasks[1]]

    # grid 3×3: corner has 0 deps, interior has 2
    p = build_grid(9)
    p._solve_order()
    assert p.tasks[0].task_dependencies == []  # top-left corner
    assert len(p.tasks[4].task_dependencies) == 2  # centre

    # random: reproducible — check total dep count is > 0
    p = build_random_dag(20)
    p._solve_order()
    total_deps = sum(len(t.task_dependencies) for t in p.tasks)
    assert total_deps > 0, "random_dag: expected some dependencies"

    print("Smoke checks passed.\n")


# ── measurement ────────────────────────────────────────────────────────────


def _measure(build_fn: Callable[[int], Pipeline], n: int, repeats: int
             ) -> Tuple[float, float]:
    """Return (median_build_s, median_solve_s) over `repeats` runs."""
    build_times: List[float] = []
    solve_times: List[float] = []

    for _ in range(repeats):
        t0 = time.perf_counter()
        pipeline = build_fn(n)
        build_times.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        pipeline._solve_order()
        solve_times.append(time.perf_counter() - t0)

    mid = repeats // 2
    return sorted(build_times)[mid], sorted(solve_times)[mid]


# ── reporting ───────────────────────────────────────────────────────────────

_COL_W = 12

def _header():
    cols = ["topology", "n", "build (ms)", "solve (ms)", "solve/build"]
    widths = [14, 8] + [_COL_W] * 3
    row = "  ".join(f"{c:<{w}}" for c, w in zip(cols, widths))
    sep = "  ".join("-" * w for w in widths)
    print(row)
    print(sep)


def _row(topology: str, n: int, build_s: float, solve_s: float):
    ratio = solve_s / build_s if build_s > 0 else float("inf")
    vals = [
        topology,
        str(n),
        f"{build_s * 1000:.2f}",
        f"{solve_s * 1000:.2f}",
        f"{ratio:.3f}",
    ]
    widths = [14, 8] + [_COL_W] * 3
    print("  ".join(f"{v:<{w}}" for v, w in zip(vals, widths)))


# ── scenarios ───────────────────────────────────────────────────────────────

TOPOLOGIES = [
    ("linear",      build_linear),
    ("fan_out",     build_fan_out),
    ("fan_in",      build_fan_in),
    ("binary_tree", build_binary_tree),
    ("grid",        build_grid),
    ("random_dag",  build_random_dag),
]

DEFAULT_SIZES = [50, 200, 500, 1000, 2000]


# ── entry point ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", nargs="+", type=int, default=DEFAULT_SIZES,
                        metavar="N", help="task counts to benchmark")
    parser.add_argument("--repeats", type=int, default=5,
                        help="number of timed repetitions per (topology, size) pair")
    parser.add_argument("--skip-smoke", action="store_true",
                        help="skip correctness checks")
    args = parser.parse_args()

    if not args.skip_smoke:
        _smoke_check()

    print(f"repeats={args.repeats}  sizes={args.sizes}\n")

    prev_topology = None
    _header()

    for topology, build_fn in TOPOLOGIES:
        if prev_topology and topology != prev_topology:
            print()  # blank line between topology groups
        prev_topology = topology

        for n in args.sizes:
            build_s, solve_s = _measure(build_fn, n, args.repeats)
            _row(topology, n, build_s, solve_s)

    print()


if __name__ == "__main__":
    main()
