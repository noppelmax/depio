import enum


class TaskStatus(enum.Enum):
    PENDING = enum.auto()
    WAITING = enum.auto()
    RUNNING = enum.auto()
    FINISHED = enum.auto()
    CANCELED = enum.auto()
    FAILED = enum.auto()
    SKIPPED = enum.auto()
    DEPFAILED = enum.auto()
    HOLD = enum.auto()
    UNKNOWN = enum.auto()


TERMINAL_STATES: frozenset[TaskStatus] = frozenset({
    TaskStatus.FINISHED,
    TaskStatus.FAILED,
    TaskStatus.SKIPPED,
    TaskStatus.DEPFAILED,
    TaskStatus.CANCELED,
})
SUCCESSFUL_TERMINAL_STATES: frozenset[TaskStatus] = frozenset({
    TaskStatus.FINISHED,
    TaskStatus.SKIPPED,
})
FAILED_TERMINAL_STATES: frozenset[TaskStatus] = frozenset({
    TaskStatus.FAILED,
    TaskStatus.DEPFAILED,
    TaskStatus.CANCELED,
})

__all__ = ["TaskStatus", "TERMINAL_STATES", "SUCCESSFUL_TERMINAL_STATES", "FAILED_TERMINAL_STATES"]
