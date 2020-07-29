import random
from redis import Redis

# 以下操作将分配给读节点的实例执行
READING_FUNCTIONS = {
    'bitcount', 'bitpos',
    'exists', 'get', 'getbit', 'getrange',
    'hexists', 'hget', 'hgetall', 'hkeys', 'hlen', 'hmget', 'hvals',
    'keys',
    'lindex', 'llen', 'lrange', 'mget', 'pttl', 'randomkey',
    'scard', 'sdiff', 'sinter', 'sismember', 'smembers', 'strlen', 'substr', 'sunion',
    'time', 'ttl', 'type',
    'zcard', 'zcount', 'zlexcount', 'zrange', 'zrangebylex', 'zrangebyscore', 'zrank', 'zrevrange',
    'zrevrangebylex', 'zrevrangebyscore', 'zrevrank', 'zscore'
}


class RedisCluster(object):
    DEFAULT_CONF_NAME = 'default'
    DEFAULT_CONF_PREFIX = 'database.redis.cluster'

    __created_instances__ = dict()

    def __init__(self, primary, readers=None):
        """
        :param primary: Redis实例
        :param readers: Redis实例列表 或 None
        """
        self.__primary = primary
        self.__readers = readers

    @classmethod
    def _get_redis_instance(cls, url, conf=None):
        """
        :param url: str, redis://{host}:{port}/{db}
        :param conf: dict 或 None
        :return:
        """
        if url not in cls.__created_instances__:
            instance = Redis.from_url(url=url, decode_responses=True, **conf or dict())
            cls.__created_instances__[url] = {
                'instance': instance,
                'conf': conf
            }
        else:
            record = cls.__created_instances__[url]
            _conf = record['conf']
            if _conf != conf:
                raise Exception('repeated url {} with different conf: {} != {}'.format(
                    url, _conf, conf
                ))
            instance = record['instance']
        return instance

    @classmethod
    def create(cls, primary=None, readers=None):
        """
        :param primary: URL
        :param readers: URL列表 或 None
        :return:
        """
        primary_instance = cls._get_redis_instance(**primary)
        if readers is not None:
            reader_instances = list()
            for reader in readers:
                reader_instances.append(cls._get_redis_instance(**reader))
        else:
            reader_instances = None
        return cls(primary=primary_instance, readers=reader_instances)

    @classmethod
    def from_conf(cls, name=None, path_prefix=None):
        """
        从 monday 的默认位置读取配置
        """
        from monday.common.conf import conf

        name = name if name is not None else cls.DEFAULT_CONF_NAME
        path_prefix = path_prefix if path_prefix is not None else cls.DEFAULT_CONF_PREFIX
        path = '{}.{}'.format(path_prefix, name)
        _conf = conf.get(path)
        primary = _conf.get('primary')
        readers = _conf.get('readers')
        if isinstance(readers, list) and len(readers) == 0:
            readers = None
        return cls.create(primary=primary, readers=readers)

    def get_instance(self, primary=False):
        """
        分配一个 Redis 实例
        """
        if primary is True or self.__readers is None:
            return self.__primary
        else:
            return self.__readers[random.randint(0, len(self.__readers) - 1)]

    def __getattr__(self, item):
        """
        根据操作的类型判断使用读节点或写节点
        """
        if item == 'pipeline' or 'scan' in item:
            # 这两种操作实要落在同一个节点上,
            raise Exception('use get_instance for scanning')
        elif item in READING_FUNCTIONS:
            # 若为预设的读操作则分配读节点
            instance = self.get_instance(primary=False)
            return getattr(instance, item)
        else:
            # 其他操作默认分配给主节点
            return getattr(self.__primary, item)
