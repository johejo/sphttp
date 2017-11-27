from enum import Enum, auto


class DelayRequestAlgorithm(Enum):
    NORMAL = auto()
    ESTIMATE_DIFFERENCES = auto()
    LINEAR_TO_HOST_USAGE_COUNT = auto()
    CONVEX_DOWNWARD_TO_HOST_USAGE_COUNT = auto()
