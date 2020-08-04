
class BaseState(object):
    __id__ = None

    def __init__(self, metadata):
        self.metadata = metadata

    @classmethod
    def id(cls):
        if cls.__id__ is None:
            raise NotImplementedError('{}.__id__'.format(cls.__name__))
        return cls.__id__

    @classmethod
    def update_state(cls, metadata):
        metadata.update_state(cls.id())
        return cls(metadata=metadata)

