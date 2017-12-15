from enum import Enum, auto


class DelayRequestAlgorithm(Enum):
    NORMAL = auto()
    DIFFERENCES = auto()
    ORIGINAL = auto()
    CONVEX_DOWNWARD = auto()
    INVERSE = auto()
    LOGARITHM = auto()
