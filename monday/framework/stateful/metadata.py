
class MetaData(object):
    def __init__(self, name, count=0):
        self.name = name
        self.count = count
        self.state = None

    def update_state(self, new_state):
        self.state = new_state

    def get_state(self):
        return self.state

    def __str__(self):
        return '{}-{} ({})'.format(self.name, self.count, self.state)
