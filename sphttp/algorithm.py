from enum import Enum, auto


class DelayRequestAlgorithm(Enum):
    NORMAL = auto()
    DIFFERENCES = auto()
    INVERSE = auto()
    STATIC = auto()
