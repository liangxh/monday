from greenlet import getcurrent as get_ident
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


class SessionManager(object):
    SCHEMA = 'mysql+pymysql'
    DEFAULT_PORT = 3306
    DEFAULT_CONF_PREFIX = 'database.sql.session'
    DEFAULT_CONF_NAME = 'default'

    __created_sessions__ = dict()
    __local_sessions__ = dict()

    @classmethod
    def _register(cls, session):
        """
        注册本线程用到的 session
        """
        ident = get_ident()
        if ident not in cls.__local_sessions__:
            cls.__local_sessions__[ident] = set()
        cls.__local_sessions__[ident].add(session)

    @classmethod
    def get(cls, name, url, conf=None):
        """
        获取要用的 session
        """
        conf = conf if conf is not None else dict()
        if name not in cls.__created_sessions__:
            engine = create_engine(url, **conf)
            session = scoped_session(sessionmaker(bind=engine, autoflush=False))
            cls.__created_sessions__[name] = session
        else:
            session = cls.__created_sessions__[name]
        cls._register(session)
        return session

    @classmethod
    def get_by_conf(cls, name=None, path_prefix=None):
        """
        使用 monday 自带的 conf 模块读取配置
        """
        from monday.common.conf import conf

        name = name if name is not None else cls.DEFAULT_CONF_NAME
        path_prefix = path_prefix if path_prefix is not None else cls.DEFAULT_CONF_PREFIX
        path = '{}.{}'.format(path_prefix, name)
        sess_conf = conf.get(path)
        url = '{}://{}:{}@{}:{}/{}'.format(
            cls.SCHEMA,
            sess_conf['user'], sess_conf['pw'],
            sess_conf['host'], sess_conf.get('port', cls.DEFAULT_PORT), sess_conf['db']
        )
        return cls.get(name=path, url=url, conf=sess_conf.get('conf', dict()))

    @classmethod
    def remove(cls):
        """
        释放当前线程用到的 session 资源
        """
        ident = get_ident()
        for session in cls.__local_sessions__.pop(ident, list()):
            session.remove()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove()


session_manager = SessionManager()
