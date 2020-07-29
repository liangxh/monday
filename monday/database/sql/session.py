import json
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from monday.common.utils import get_ident


class SessionManager(object):
    SCHEMA = 'mysql+pymysql'
    DEFAULT_PORT = 3306
    DEFAULT_CONF_PREFIX = 'database.sql.session'
    DEFAULT_CONF_NAME = 'default'

    # 保存已创建的session
    # {
    #   "{url}": {
    #       "session": Session实例
    #       "conf": 该 Session 在 create_engine 时用的配置
    #   }
    # }
    # 这里意味着同一个 SessionManager 内相同的 url 只能对应一套配置
    __created_sessions__ = dict()

    # 保存各线程下用到的 Session
    # {
    #   "{ID}": [ Session实例 ]
    # }
    __local_sessions__ = dict()

    @classmethod
    def _register(cls, session):
        """
        记录当前 线程/协程 下用到此 Session
        """
        ident = get_ident()
        if ident not in cls.__local_sessions__:
            cls.__local_sessions__[ident] = set()
        cls.__local_sessions__[ident].add(session)

    @classmethod
    def get(cls, url, conf=None):
        """
        :param url: {schema}://{username}:{password}@{host}:{port}/{database}
        :param conf: dict or None, used in create_engine(...)
        :return:
        """
        if url not in cls.__created_sessions__:
            engine = create_engine(url, **conf or dict())
            session = scoped_session(sessionmaker(bind=engine, autoflush=False))
            cls.__created_sessions__[url] = {
                'session': session,
                'conf': conf
            }
        else:
            created = cls.__created_sessions__[url]
            if not conf == created['conf']:
                raise Exception(
                    'repeated target ({}) with different config: {} != {}'.format(url, created['conf'], conf))
            session = created['session']
        cls._register(session)
        return session

    @classmethod
    def from_conf(cls, name=None, path_prefix=None):
        """
        从 monday 的默认位置读取配置
        """
        from monday.common.conf import conf

        name = name if name is not None else cls.DEFAULT_CONF_NAME
        path_prefix = path_prefix if path_prefix is not None else cls.DEFAULT_CONF_PREFIX
        path = '{}.{}'.format(path_prefix, name)
        sess_conf = conf.get(path)
        return cls.get(url=sess_conf['url'], conf=sess_conf.get('conf'))

    @classmethod
    def remove(cls):
        """
        释放当前 线程/协程 下该用到的 Session 的连接
        """
        ident = get_ident()
        for session in cls.__local_sessions__.pop(ident, list()):
            session.remove()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove()


session_manager = SessionManager()
