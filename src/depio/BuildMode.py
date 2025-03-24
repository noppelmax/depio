import enum


class BuildMode(enum.Enum):
    NEVER = enum.auto()
    IF_MISSING = enum.auto()
    ALWAYS = enum.auto()
    IF_NEW = enum.auto()
