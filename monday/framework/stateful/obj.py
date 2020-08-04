from monday.framework.stateful.state import A, B, C, D
from monday.framework.stateful.exceptions import InvalidStateException


def check_state(expected_states):
    expected_state_ids = {state.State.id() for state in expected_states}

    def decorator(func):
        def wrapper(*args, **kwargs):
            # 检查对象当前状态符合要求的状态
            current_state = args[0].get_current_state()
            if current_state not in expected_state_ids:
                raise InvalidStateException('invalid state "{}" for "{}", expected {}'.format(
                    current_state, func.__name__, expected_state_ids
                ))
            return func(*args, **kwargs)
        return wrapper
    return decorator


class StatefulObject(object):
    __init_state__ = A   # 初始状态
    __states__ = {       # 状态ID到状态类的映射表
        state.State.id(): state
        for state in [A, B, C, D]
    }

    def __init__(self, metadata):
        if metadata.get_state() is not None:
            state = self.__states__[metadata.get_state()]
        else:
            state = self.__init_state__
        self.obj = state.State.update_state(metadata=metadata)

    @property
    def metadata(self):
        return self.obj.metadata

    def to_state(self, new_state):
        """
        转换成目标状态
        """
        self.obj = new_state.State.update_state(metadata=self.metadata)

    def get_current_state(self):
        """
        获取当前状态 (对应ID)
        """
        return self.metadata.get_state()

    def __getattr__(self, item):
        # 各个状态转移无关的事件会
        if hasattr(self.obj, item):
            # 落到状态类的实例上
            return getattr(self.obj, item)
        elif hasattr(self.obj.metadata, item):
            return getattr(self.metadata, item)
        else:
            raise AttributeError("'{}' object has no attribute '{}'".format(self.__class__.__name__, item))

    def __str__(self):
        return str(self.obj.metadata)

    """
    [Example]
    A --> B <==> C --> D
          ^            |
          |------------|
    """

    @check_state([A, ])   # 此事件仅限哪些状态下触发
    def initialize(self):
        self.to_state(B)  # 此事件完成后的状态

    @check_state([B, ])
    def warm_up(self):
        self.to_state(C)

    @check_state([C, ])
    def boost(self):
        self.to_state(D)

    @check_state([C, D, ])
    def slow_down(self):
        self.to_state(B)
