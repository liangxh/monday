from monday.framework.stateful.state.base import BaseState


class State(BaseState):
    __id__ = 'B'

    def tick(self):
        self.metadata.count += 1
        print(self.metadata)
