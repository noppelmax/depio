from __future__ import annotations

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


TERMINAL_STATES = [
            TaskStatus.FINISHED,
            TaskStatus.FAILED,
            TaskStatus.SKIPPED,
            TaskStatus.DEPFAILED,
            TaskStatus.CANCELED]
SUCCESSFUL_TERMINAL_STATES = [
            TaskStatus.FINISHED,
            TaskStatus.SKIPPED]
FAILED_TERMINAL_STATES = [
            TaskStatus.FAILED,
            TaskStatus.DEPFAILED,
            TaskStatus.CANCELED]
