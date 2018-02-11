from collections import deque


class AnyPoppableDeque(deque):
    def __init__(self):
        super().__init__()

    def pop_at_any_pos(self, pos):
        value = self[pos]
        del self[pos]
        return value


class RangeParam(object):
    def __init__(self, block_id=None, headers=None):
        self.block_id = block_id
        self.headers = headers
