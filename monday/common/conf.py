import os
import yaml
from monday.exceptions import NotFoundException


class Conf(object):
    DEFAULT = 'DEFAULT'
    DEFAULT_CONF_FILE = 'conf.yaml'
    ENVIRON_KEY = 'MONDAY_CONF'

    __shared_data__ = dict()

    def __init__(self, filename=None):
        if filename is not None:
            self._filename = filename
        elif self.ENVIRON_KEY in os.environ:
            # 根据环境变量读配置文件
            self._filename = os.environ[self.ENVIRON_KEY]
        else:
            # 上面都没有的话就默认是在运行目录下的conf.yaml
            self._filename = 'conf.yaml'
        self.key = self.DEFAULT if filename is None else self._filename

    @property
    def data(self):
        if self.key not in self.__class__.__shared_data__:
            with open(self._filename, 'r') as file_obj:
                self.__class__.__shared_data__[self.key] = yaml.load(file_obj)
        return self.__class__.__shared_data__[self.key]

    def get(self, path, prefix=None, force=True):
        # [design]
        # 设计上不同模块的配置会有一个默认前缀, 其配置有一定的格式, 所以路径被拆解成 (prefix, path)
        # 而在模块拓展和用户自定义时, 只要用不同的prefix就可以分开不同使用场景的配置
        keys = (prefix.split('.') if prefix is not None else list()) + path.split('.')
        node = self.data
        parent = ''
        for key in keys:
            try:
                node = node[key]
                parent += ('' if parent == '' else '.') + key
            except KeyError:
                if force:
                    raise NotFoundException('Config "{}" not found under "{}"'.format(key, parent))
                else:
                    return None
        return node


conf = Conf()
