from monday.framework.stateful.state.base import BaseState


class State(BaseState):
    __id__ = 'C'

    def tick(self):
        self.metadata.count += 2
        print(self.metadata)
