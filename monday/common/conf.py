import os
import yaml


class Conf(object):
    DEFAULT = 'DEFAULT'
    DEFAULT_CONF_FILE = 'conf.yaml'
    ENVIRON_KEY = 'MONDAY_CONF'

    __shared_data__ = dict()

    def __init__(self, filename=None):
        if filename is not None:
            self._filename = filename
        elif self.ENVIRON_KEY in os.environ:
            self._filename = os.environ[self.ENVIRON_KEY]
        else:
            self._filename = 'conf.yaml'
        self.key = self.DEFAULT if filename is None else self._filename

    @property
    def data(self):
        if self.key not in self.__class__.__shared_data__:
            with open(self._filename, 'r') as file_obj:
                self.__class__.__shared_data__[self.key] = yaml.load(file_obj)
        return self.__class__.__shared_data__[self.key]

    def get(self, path, force=True):
        keys = path.split('.')
        parent = self.data
        for key in keys:
            try:
                parent = parent[key]
            except KeyError:
                if force:
                    raise Exception('path not found: {}'.format(path))
                else:
                    return None
        return parent


conf = Conf()
