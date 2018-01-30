from enum import Enum, auto


class DelayRequestAlgorithm(Enum):
    NORMAL = auto()
    DIFF = auto()
    INV = auto()
    STATIC = auto()


class DuplicateRequestAlgorithm(Enum):
    IBRC = auto()
    IBRC_X = auto()
    NIBIB = auto()
