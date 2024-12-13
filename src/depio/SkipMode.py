

import enum


class SkipMode(enum.Enum):
    NEVER = enum.auto()
    IF_MISSING = enum.auto()
    ALWAYS = enum.auto()