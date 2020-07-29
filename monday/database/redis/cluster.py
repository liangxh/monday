import random
from redis import Redis


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
        self.__primary = primary
        self.__readers = readers

    @classmethod
    def _get_redis_instance(cls, url, conf=None):
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
        primary_instance = cls._get_redis_instance(**primary)
        if readers is not None:
            reader_instances = list()
            for reader in readers:
                reader_instances.append(cls._get_redis_instance(**reader))
        else:
            reader_instances = None
        return cls(primary=primary_instance, readers=reader_instances)

    @classmethod
    def create_by_conf(cls, name=None, path_prefix=None):
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
        randomly assign a redis instance
        """
        if primary is True or self.__readers is None:
            return self.__primary
        else:
            return self.__readers[random.randint(0, len(self.__readers) - 1)]

    def __getattr__(self, item):
        if item == 'pipeline' or 'scan' in item:
            raise Exception('use get_instance for scanning')
        elif item in READING_FUNCTIONS:
            instance = self.get_instance(primary=False)
            return getattr(instance, item)
        else:
            return getattr(self.__primary, item)
