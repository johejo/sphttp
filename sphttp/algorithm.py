from enum import Enum, auto


class DelayRequestAlgorithm(Enum):
    NORMAL = auto()
    ESTIMATE_DIFFERENCES = auto()
    ORIGINAL = auto()
    CONVEX_DOWNWARD = auto()
    INVERSE_PROPORTION = auto()
    LOGARITHM = auto()
