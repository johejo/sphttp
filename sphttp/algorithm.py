from enum import Enum, auto


class DelayRequestAlgorithm(Enum):
    NORMAL = auto()
    ESTIMATE_DIFFERENCES = auto()
    CONVEX_DOWNWARD_TO_HOST_USAGE_COUNT = auto()
    INVERSE_PROPORTION = auto()
