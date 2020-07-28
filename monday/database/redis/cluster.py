import urllib
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
        self.__primary = primary  # 存放RedisManager实例
        self.__readers = readers

    @classmethod
    def _get_redis_instance(cls, url):
        if url not in cls.__created_instances__:
            parsed = urllib.parse.urlparse(url)
            conf = dict(host=parsed.hostname, port=parsed.port, db=int(parsed.path[1:].strip() or '0'))
            params = urllib.parse.parse_qs(parsed.query)
            for k, v in params.items():
                if k not in ['host', 'port', 'db']:
                    conf[k] = v[0]
            rds = Redis(decode_responses=True, **conf)
            cls.__created_instances__[url] = rds
        return cls.__created_instances__[url]

    @classmethod
    def create(cls, primary=None, readers=None):
        primary_instance = cls._get_redis_instance(url=primary)
        if readers is not None:
            reader_instances = list()
            for reader in readers:
                reader_instances.append(cls._get_redis_instance(url=reader))
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

    def pipeline(self, transaction=True, shard_hint=None, primary=False):
        """
        默认reader, 若有写操作则需要指定primary=True
        """
        _instance = self.__primary if primary else self.get_reader()
        return _instance.pipeline(transaction=transaction, shard_hint=shard_hint)

    def get_instance(self, primary=False):
        """
        分配一个读节点, 若没有读节点则返回主节点
        """
        if primary is True or self.__readers is None:
            return self.__primary
        else:
            return self.__readers[random.randint(0, len(self.__readers) - 1)]

    def __getattr__(self, item):
        if 'scan' in item:
            raise Exception('use get_instance for scanning')
        elif item in READING_FUNCTIONS:
            instance = self.get_instance(primary=False)
            return getattr(instance, item)
        else:
            return getattr(self.__primary, item)
